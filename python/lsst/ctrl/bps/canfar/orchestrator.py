"""Enqueue ready DAG jobs and watch terminal directories for BPS status."""

from __future__ import annotations

import argparse
import logging
import time
from pathlib import Path
from typing import Any

from .config import CanfarSiteConfig, load_site_config, sanitize_name
from .pool import WorkerPool, default_session, default_worker_script
from .queue import QueueLayout
from .status import overall_run_state, summarize_job_states

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
        self._sanitized: dict[str, str] = {
            sanitize_name(name): name for name in self.jobs
        }
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

    def build_location_index(self) -> dict[str, str]:
        """Map sanitized job filename stem -> queue location (one readdir per dir)."""
        index: dict[str, str] = {}
        for label, directory in (
            ("queue", self.layout.queue),
            ("processing", self.layout.processing),
            ("processed", self.layout.processed),
            ("failed", self.layout.failed),
            ("payloads", self.layout.payloads),
        ):
            for path in self.layout.list_json(directory):
                # First match wins; priority matches job_location()'s search order.
                index.setdefault(path.stem, label)
        return index

    def _loc(self, job_name: str, index: dict[str, str]) -> str | None:
        return index.get(sanitize_name(job_name))

    def parents_succeeded(self, job_name: str, index: dict[str, str]) -> bool:
        parents = list(self.jobs[job_name].get("parents") or [])
        for parent in parents:
            if parent in self.pruned:
                return False
            if self._loc(parent, index) != "processed":
                return False
        return True

    def mark_pruned_from_failures(self, index: dict[str, str] | None = None) -> None:
        """Propagate FAILED parents to descendants.

        Uses the failed/ directory listing + DAG names (via sanitize map). Does
        **not** open every processed payload — that wedged the orchestrator on
        large /arc runs.
        """
        failed: set[str] = set(self.pruned)
        for path in self.layout.list_json(self.layout.failed):
            name = self._sanitized.get(path.stem)
            if name is None:
                payload = self.layout.try_read_payload(path)
                if payload is None:
                    continue
                name = payload.name
            failed.add(name)

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

        directly_failed = {
            self._sanitized[path.stem]
            for path in self.layout.list_json(self.layout.failed)
            if path.stem in self._sanitized
        }
        self.pruned |= failed - directly_failed

    def enqueue_ready(self, index: dict[str, str] | None = None) -> list[str]:
        if index is None:
            index = self.build_location_index()
        ready: list[str] = []
        for name in self.jobs:
            if name in self.pruned:
                continue
            if self._loc(name, index) != "payloads":
                continue
            if self.parents_succeeded(name, index):
                self.layout.enqueue(name)
                ready.append(name)
                # Keep index coherent within this pass.
                index[sanitize_name(name)] = "queue"
        if ready:
            _LOG.info("enqueued %d ready job(s)", len(ready))
        else:
            _LOG.debug("no ready jobs to enqueue")
        return ready

    def work_remaining(self, index: dict[str, str] | None = None) -> bool:
        if index is None:
            index = self.build_location_index()
            self.mark_pruned_from_failures(index)
        for name in self.jobs:
            if name in self.pruned:
                continue
            location = self._loc(name, index)
            if location in {None, "payloads", "queue", "processing"}:
                return True
        return False

    def locations(self, index: dict[str, str] | None = None) -> dict[str, str | None]:
        if index is None:
            index = self.build_location_index()
        return {name: self._loc(name, index) for name in self.jobs}

    def write_state(
        self,
        *,
        worker_ids: list[str] | None = None,
        index: dict[str, str] | None = None,
    ) -> dict[str, Any]:
        locations = self.locations(index)
        counts = summarize_job_states(locations, pruned=self.pruned)
        state = {
            "submitPath": str(self.layout.root),
            "runState": getattr(
                overall_run_state(
                    counts, cancel_requested=self.layout.cancel_requested()
                ),
                "name",
                str(overall_run_state(counts)),
            ),
            "jobLocations": locations,
            "pruned": sorted(self.pruned),
            "jobStateCounts": {
                getattr(state, "name", str(state)): count
                for state, count in counts.items()
            },
            "workerIds": worker_ids
            if worker_ids is not None
            else list(self.pool.session_ids),
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
            self.layout.reclaim_orphan_processing(
                grace_seconds=self.site.reclaim_grace_seconds
            )
            index = self.build_location_index()
            self.mark_pruned_from_failures(index)
            self.enqueue_ready(index)
            remaining = self.work_remaining(index)
            self.pool.maintain(work_remaining=remaining)
            state = self.write_state(index=index)
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
