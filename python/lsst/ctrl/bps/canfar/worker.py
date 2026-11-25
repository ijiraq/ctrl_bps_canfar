"""Long-lived worker that claims jobs from the queue and runs them.

Each job runs in its own scratch directory (under ``$BPS_CANFAR_SCRATCH``
or the system temporary directory).  Files listed in the payload's
``fetch`` are copied there first; the directory is removed afterwards.
"""

from __future__ import annotations

import argparse
import logging
import os
import re
import shutil
import subprocess
import tempfile
import time
import traceback
from pathlib import Path

from .config import sanitize_name
from .job import ATTEMPT_ENV_VAR
from .queue import JobPayload, QueueLayout
from .staging import fetch

_LOG = logging.getLogger(__name__)

#: Directory under which each job gets a private working directory.
SCRATCH_ENV_VAR = "BPS_CANFAR_SCRATCH"
#: Exit status recorded when a job's files could not be staged (EX_IOERR).
FETCH_FAILED = 74

_SHELL_VAR_RE = re.compile(r"\$\{([A-Za-z_][A-Za-z0-9_]*)\}")


def _resolve_session_id(layout: QueueLayout, worker_id: str) -> str | None:
    env = os.environ.get("BPS_CANFAR_SESSION_ID")
    if env:
        return env
    return layout.read_worker_session(worker_id)


def job_environment(job: JobPayload, base: dict[str, str] | None = None) -> dict[str, str]:
    """Environment for one job: the worker's, plus the payload's settings.

    ``${VAR}`` in a payload value (what ``<ENV:VAR>`` becomes at prepare)
    is replaced from the environment built so far; unset names expand to
    an empty string, as in the shell.
    """
    env = dict(os.environ if base is None else base)
    env["BPS_CANFAR_JOB_NAME"] = job.name
    env["BPS_CANFAR_JOB_LABEL"] = job.label
    # Earlier starts of this job: 0 on the first run.
    env[ATTEMPT_ENV_VAR] = str(max(job.attempt - 1, 0))
    for key, value in job.environment.items():
        env[key] = _SHELL_VAR_RE.sub(lambda m: env.get(m.group(1), ""), value)
    return env


def make_work_dir(job_name: str) -> Path:
    """Create a private working directory for one run of a job."""
    base = os.environ.get(SCRATCH_ENV_VAR) or None
    if base:
        Path(base).mkdir(parents=True, exist_ok=True)
    return Path(tempfile.mkdtemp(prefix=f"bps-canfar-{sanitize_name(job_name)}-", dir=base))


def run_job(job: JobPayload, log_path: Path, *, worker_id: str) -> int:
    """Stage a job's files, run its command and return the exit status."""
    env = job_environment(job)
    log_path.parent.mkdir(parents=True, exist_ok=True)
    with log_path.open("w", encoding="utf-8") as log_fh:
        log_fh.write(
            f"# worker={worker_id}\n# job={job.name}\n# attempt={env[ATTEMPT_ENV_VAR]}\n"
            f"# command={job.command}\n"
        )
        log_fh.flush()
        work_dir = make_work_dir(job.name)
        try:
            try:
                fetch(job.fetch, work_dir)
            except Exception as exc:
                log_fh.write(f"# could not stage job files: {type(exc).__name__}: {exc}\n")
                traceback.print_exc(file=log_fh)
                return FETCH_FAILED
            # Non-login, no rc: worker already sourced its environment script.
            # A login shell re-runs ~/.bashrc and can emit "ERROR: .bashrc failed".
            completed = subprocess.run(
                ["bash", "--noprofile", "--norc", "-c", job.command],
                stdout=log_fh,
                stderr=subprocess.STDOUT,
                env=env,
                cwd=work_dir,
                check=False,
            )
            return completed.returncode
        finally:
            shutil.rmtree(work_dir, ignore_errors=True)


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
    session_id = _resolve_session_id(layout, worker_id)
    last_work = time.time()
    _LOG.info("worker %s watching %s (idleTimeout=%ss)", worker_id, layout.root, idle_timeout)

    def heartbeat(current_job: str | None = None, claimed_at: float | None = None) -> None:
        layout.write_worker_heartbeat(
            worker_id,
            current_job=current_job,
            claimed_at=claimed_at,
            session_id=session_id,
        )

    while True:
        heartbeat()
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
        claimed_at = time.time()
        last_work = claimed_at
        layout.write_claim_owner(
            job.name,
            worker_id=worker_id,
            session_id=session_id,
            claimed_at=claimed_at,
        )
        heartbeat(current_job=job.name, claimed_at=claimed_at)
        log_path = layout.log_path_for(job.name)
        _LOG.info("worker %s running %s -> %s", worker_id, job.name, log_path)
        try:
            job = layout.record_attempt(processing_path)
            returncode = run_job(job, log_path, worker_id=worker_id)
            if returncode == 0:
                layout.mark_processed(processing_path)
                _LOG.info("worker %s succeeded %s", worker_id, job.name)
            else:
                layout.mark_failed(processing_path)
                _LOG.error(
                    "worker %s failed %s (exit %s)",
                    worker_id,
                    job.name,
                    returncode,
                )
        except Exception:
            _LOG.exception("worker %s crashed while running %s", worker_id, job.name)
            if processing_path.exists():
                layout.mark_failed(processing_path)
        heartbeat()
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
