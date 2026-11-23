"""Long-lived worker that claims jobs from the ARC queue and runs them."""

from __future__ import annotations

import argparse
import logging
import os
import subprocess
import sys
import time
from pathlib import Path

from .queue import QueueLayout

_LOG = logging.getLogger(__name__)


def run_worker(
    submit_path: str | Path,
    *,
    idle_timeout: int = 86900,
    poll_interval: int = 30,
    worker_id: str | None = None,
) -> int:
    """Poll ``queue/`` until idle too long or cancel is requested."""
    layout = QueueLayout(submit_path)
    layout.ensure()
    worker_id = worker_id or os.environ.get("BPS_CANFAR_WORKER_ID") or f"pid-{os.getpid()}"
    heartbeat = layout.workers / f"{worker_id}.alive"
    last_work = time.time()
    _LOG.info("worker %s watching %s (idleTimeout=%ss)", worker_id, layout.root, idle_timeout)

    while True:
        heartbeat.write_text(f"{time.time()}\n", encoding="utf-8")
        if layout.cancel_requested():
            _LOG.info("worker %s saw CANCEL; exiting", worker_id)
            return 0

        claimed = layout.try_claim()
        if claimed is None:
            if time.time() - last_work >= idle_timeout:
                _LOG.info("worker %s idle for %ss; exiting", worker_id, idle_timeout)
                return 0
            time.sleep(poll_interval)
            continue

        processing_path, job = claimed
        last_work = time.time()
        log_path = layout.log_path_for(job.name)
        log_path.parent.mkdir(parents=True, exist_ok=True)
        _LOG.info("worker %s running %s -> %s", worker_id, job.name, log_path)
        env = os.environ.copy()
        env.update(job.environment)
        env["BPS_CANFAR_JOB_NAME"] = job.name
        env["BPS_CANFAR_JOB_LABEL"] = job.label
        try:
            with log_path.open("w", encoding="utf-8") as log_fh:
                log_fh.write(f"# worker={worker_id}\n# job={job.name}\n# command={job.command}\n")
                log_fh.flush()
                completed = subprocess.run(
                    ["bash", "-lc", job.command],
                    stdout=log_fh,
                    stderr=subprocess.STDOUT,
                    env=env,
                    check=False,
                )
            if completed.returncode == 0:
                layout.mark_processed(processing_path)
                _LOG.info("worker %s succeeded %s", worker_id, job.name)
            else:
                layout.mark_failed(processing_path)
                _LOG.error(
                    "worker %s failed %s (exit %s)",
                    worker_id,
                    job.name,
                    completed.returncode,
                )
        except Exception:
            _LOG.exception("worker %s crashed while running %s", worker_id, job.name)
            if processing_path.exists():
                layout.mark_failed(processing_path)
        last_work = time.time()


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description="BPS CANFAR queue worker")
    parser.add_argument("submit_path", type=Path, help="BPS submitPath / queue root on shared storage")
    parser.add_argument("--idle-timeout", type=int, default=int(os.environ.get("BPS_CANFAR_IDLE_TIMEOUT", "86900")))
    parser.add_argument("--poll-interval", type=int, default=int(os.environ.get("BPS_CANFAR_POLL_INTERVAL", "30")))
    parser.add_argument("--worker-id", default=os.environ.get("BPS_CANFAR_WORKER_ID"))
    parser.add_argument("-v", "--verbose", action="store_true")
    args = parser.parse_args(argv)
    logging.basicConfig(
        level=logging.DEBUG if args.verbose else logging.INFO,
        format="%(asctime)s %(levelname)s %(name)s: %(message)s",
    )
    return run_worker(
        args.submit_path,
        idle_timeout=args.idle_timeout,
        poll_interval=args.poll_interval,
        worker_id=args.worker_id,
    )


if __name__ == "__main__":
    raise SystemExit(main())
