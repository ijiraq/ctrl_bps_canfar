"""Enqueue ready DAG jobs and watch terminal directories for BPS status."""

from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path
from typing import Any

from .config import CanfarSiteConfig, load_site_config
from .pool import WorkerPool, default_session, default_worker_script
from .queue import QueueLayout
from .status import overall_run_state, state_for_location, summarize_job_states

_LOG = logging.getLogger(__name__)


class Orchestrator:
    """Drive the ARC queue for one prepared CanfarWorkflow submit directory."""

    def __init__(
        self,
        layout: QueueLayout,
        site: CanfarSiteConfig,
        *,
        pool: WorkerPool | None = None,
        session: Any | None = None,
    ) -> None:
        self.layout = layout
        self.site = site
        self.dag = layout.read_dag()
        self.jobs: dict[str, Any] = dict(self.dag.get("jobs") or {})
        self.pruned: set[str] = set()
        worker_script = layout.root / "run_worker.sh"
        if not worker_script.is_file():
            worker_script = default_worker_script()
        self.pool = pool or WorkerPool(
            submit_path=layout.root,
            site=site,
            session=session or default_session(timeout=site.http_timeout),
            worker_script=worker_script,
        )

    def parents_succeeded(self, job_name: str) -> bool:
        parents = list(self.jobs[job_name].get("parents") or [])
        for parent in parents:
            if parent in self.pruned:
                return False
            if self.layout.job_location(parent) != "processed":
                return False
        return True

    def mark_pruned_from_failures(self) -> None:
        failed = {name for name, loc in self.layout.iter_terminal() if loc == "failed"}
        failed |= self.pruned
        changed = True
        while changed:
            changed = False
            for name, meta in self.jobs.items():
                if name in failed:
                    continue
                parents = set(meta.get("parents") or [])
                if parents & failed:
                    failed.add(name)
                    changed = True
        self.pruned |= failed - {n for n, loc in self.layout.iter_terminal() if loc == "failed"}

    def enqueue_ready(self) -> list[str]:
        ready: list[str] = []
        for name in self.jobs:
            if name in self.pruned:
                continue
            location = self.layout.job_location(name)
            if location != "payloads":
                continue
            if self.parents_succeeded(name):
                self.layout.enqueue(name)
                ready.append(name)
                _LOG.info("enqueued %s", name)
        return ready

    def work_remaining(self) -> bool:
        self.mark_pruned_from_failures()
        for name in self.jobs:
            if name in self.pruned:
                continue
            location = self.layout.job_location(name)
            if location in {None, "payloads", "queue", "processing"}:
                return True
        return False

    def locations(self) -> dict[str, str | None]:
        return {name: self.layout.job_location(name) for name in self.jobs}

    def write_state(self, *, worker_ids: list[str] | None = None) -> dict[str, Any]:
        locations = self.locations()
        counts = summarize_job_states(locations, pruned=self.pruned)
        state = {
            "submitPath": str(self.layout.root),
            "runState": getattr(overall_run_state(counts, cancel_requested=self.layout.cancel_requested()), "name", str(overall_run_state(counts))),
            "jobLocations": locations,
            "pruned": sorted(self.pruned),
            "jobStateCounts": {
                getattr(state, "name", str(state)): count for state, count in counts.items()
            },
            "workerIds": worker_ids if worker_ids is not None else list(self.pool.session_ids),
            "cancelRequested": self.layout.cancel_requested(),
            "updatedAt": time.time(),
        }
        self.layout.write_state(state)
        return state

    def run(self, *, launch_workers: bool = True, once: bool = False) -> dict[str, Any]:
        self.layout.ensure()
        # Fresh submit clears a stale cancel flag; resume/cancel paths pass launch_workers=False.
        if launch_workers:
            self.layout.clear_cancel()
            self.pool.launch()
        while True:
            if self.layout.cancel_requested():
                _LOG.info("cancel requested; destroying workers")
                self.pool.destroy_all()
                return self.write_state()

            self.pool.reclaim_stalled_jobs(self.layout)
            self.layout.reclaim_orphan_processing(grace_seconds=self.site.reclaim_grace_seconds)
            self.mark_pruned_from_failures()
            self.enqueue_ready()
            remaining = self.work_remaining()
            self.pool.maintain(work_remaining=remaining)
            state = self.write_state()
            if not remaining:
                _LOG.info("all jobs finished")
                # Let idle workers exit on their own, or destroy immediately.
                self.pool.destroy_all()
                return self.write_state()
            if once:
                return state
            time.sleep(self.site.poll_interval)


def run_from_submit_path(submit_path: str | Path, *, once: bool = False) -> dict[str, Any]:
    layout = QueueLayout(submit_path)
    run_cfg = layout.read_run()
    site = load_site_config(run_cfg.get("siteConfig") or run_cfg)
    orch = Orchestrator(layout, site)
    # Restore known worker ids if restarting.
    prev = layout.read_state()
    if prev.get("workerIds"):
        orch.pool.session_ids = [str(x) for x in prev["workerIds"]]
    return orch.run(launch_workers=not bool(orch.pool.session_ids), once=once)


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="BPS CANFAR queue orchestrator")
    parser.add_argument("submit_path", type=Path)
    parser.add_argument("--once", action="store_true", help="single orchestration pass")
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    state = run_from_submit_path(args.submit_path, once=args.once)
    print(state.get("runState"))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
