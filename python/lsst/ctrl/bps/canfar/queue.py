"""Filesystem job queue on shared storage (typically /arc)."""

from __future__ import annotations

import json
import logging
import os
import time
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Iterable, Iterator

from .config import QUEUE_SUBDIRS, sanitize_name

_LOG = logging.getLogger(__name__)

DAG_NAME = "dag.json"
RUN_NAME = "run.yaml"
STATE_NAME = "state.json"
CANCEL_FLAG = "CANCEL"


@dataclass
class JobPayload:
    """One BPS job written under ``payloads/`` and moved through the queue."""

    name: str
    label: str
    command: str
    parents: list[str] = field(default_factory=list)
    environment: dict[str, str] = field(default_factory=dict)
    request_cpus: int | None = None
    request_memory_mb: int | None = None

    @property
    def filename(self) -> str:
        return f"{sanitize_name(self.name)}.json"

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)

    @classmethod
    def from_dict(cls, data: MappingLike) -> JobPayload:
        return cls(
            name=str(data["name"]),
            label=str(data.get("label") or data["name"]),
            command=str(data["command"]),
            parents=[str(p) for p in data.get("parents") or []],
            environment={str(k): str(v) for k, v in (data.get("environment") or {}).items()},
            request_cpus=_as_optional_int(data.get("request_cpus")),
            request_memory_mb=_as_optional_int(data.get("request_memory_mb")),
        )


MappingLike = dict[str, Any]


def _as_optional_int(value: Any) -> int | None:
    if value is None:
        return None
    return int(value)


@dataclass
class QueueLayout:
    """Directory layout under a BPS ``submitPath``."""

    root: Path

    def __post_init__(self) -> None:
        self.root = Path(self.root)

    @property
    def payloads(self) -> Path:
        return self.root / "payloads"

    @property
    def queue(self) -> Path:
        return self.root / "queue"

    @property
    def processing(self) -> Path:
        return self.root / "processing"

    @property
    def processed(self) -> Path:
        return self.root / "processed"

    @property
    def failed(self) -> Path:
        return self.root / "failed"

    @property
    def logs(self) -> Path:
        return self.root / "logs"

    @property
    def workers(self) -> Path:
        return self.root / "workers"

    @property
    def dag_path(self) -> Path:
        return self.root / DAG_NAME

    @property
    def run_path(self) -> Path:
        return self.root / RUN_NAME

    @property
    def state_path(self) -> Path:
        return self.root / STATE_NAME

    @property
    def cancel_path(self) -> Path:
        return self.root / CANCEL_FLAG

    def ensure(self) -> None:
        for name in QUEUE_SUBDIRS:
            (self.root / name).mkdir(parents=True, exist_ok=True)

    def payload_path(self, job: JobPayload | str) -> Path:
        name = job.filename if isinstance(job, JobPayload) else f"{sanitize_name(job)}.json"
        return self.payloads / name

    def write_payload(self, job: JobPayload) -> Path:
        self.payloads.mkdir(parents=True, exist_ok=True)
        path = self.payload_path(job)
        path.write_text(json.dumps(job.to_dict(), indent=2) + "\n", encoding="utf-8")
        return path

    def read_payload(self, path: Path) -> JobPayload:
        return JobPayload.from_dict(json.loads(path.read_text(encoding="utf-8")))

    def write_dag(self, jobs: Iterable[JobPayload]) -> Path:
        payload = {
            "jobs": {
                job.name: {
                    "label": job.label,
                    "parents": list(job.parents),
                    "filename": job.filename,
                }
                for job in jobs
            }
        }
        self.dag_path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        return self.dag_path

    def read_dag(self) -> dict[str, Any]:
        return json.loads(self.dag_path.read_text(encoding="utf-8"))

    def write_state(self, state: MappingLike) -> Path:
        self.state_path.write_text(json.dumps(state, indent=2) + "\n", encoding="utf-8")
        return self.state_path

    def read_state(self) -> dict[str, Any]:
        if not self.state_path.is_file():
            return {}
        return json.loads(self.state_path.read_text(encoding="utf-8"))

    def request_cancel(self) -> None:
        self.cancel_path.write_text("1\n", encoding="utf-8")

    def cancel_requested(self) -> bool:
        return self.cancel_path.is_file()

    def clear_cancel(self) -> None:
        if self.cancel_path.exists():
            self.cancel_path.unlink()

    def job_location(self, job_name: str) -> str | None:
        filename = f"{sanitize_name(job_name)}.json"
        for label, directory in (
            ("queue", self.queue),
            ("processing", self.processing),
            ("processed", self.processed),
            ("failed", self.failed),
            ("payloads", self.payloads),
        ):
            if (directory / filename).is_file():
                return label
        return None

    def list_json(self, directory: Path) -> list[Path]:
        if not directory.is_dir():
            return []
        return sorted(p for p in directory.iterdir() if p.suffix == ".json" and p.is_file())

    def enqueue(self, job_name: str) -> Path:
        """Move a payload into ``queue/`` if it is still only in payloads/."""
        src = self.payload_path(job_name)
        dest = self.queue / src.name
        if dest.exists() or (self.processing / src.name).exists():
            return dest if dest.exists() else self.processing / src.name
        if (self.processed / src.name).exists() or (self.failed / src.name).exists():
            return self.processed / src.name if (self.processed / src.name).exists() else self.failed / src.name
        if not src.is_file():
            raise FileNotFoundError(f"payload missing for {job_name}: {src}")
        os.rename(src, dest)
        return dest

    def try_claim(self) -> tuple[Path, JobPayload] | None:
        """Atomically claim the next queued job into ``processing/``."""
        for src in self.list_json(self.queue):
            dest = self.processing / src.name
            try:
                os.rename(src, dest)
            except FileNotFoundError:
                continue
            except OSError as exc:
                _LOG.debug("claim race on %s: %s", src, exc)
                continue
            return dest, self.read_payload(dest)
        return None

    def mark_processed(self, processing_path: Path) -> Path:
        dest = self.processed / processing_path.name
        self.clear_claim_owner(processing_path.stem)
        os.rename(processing_path, dest)
        return dest

    def mark_failed(self, processing_path: Path) -> Path:
        dest = self.failed / processing_path.name
        self.clear_claim_owner(processing_path.stem)
        os.rename(processing_path, dest)
        return dest

    def worker_alive_path(self, worker_id: str) -> Path:
        return self.workers / f"{sanitize_name(worker_id)}.alive"

    def worker_session_path(self, worker_id: str) -> Path:
        return self.workers / f"{sanitize_name(worker_id)}.session"

    def claim_owner_path(self, job_name: str) -> Path:
        return self.processing / f"{sanitize_name(job_name)}.owner"

    def write_worker_heartbeat(
        self,
        worker_id: str,
        *,
        current_job: str | None = None,
        claimed_at: float | None = None,
        session_id: str | None = None,
    ) -> Path:
        """Write ``workers/<id>.alive`` JSON used to map sessions to claimed jobs."""
        self.workers.mkdir(parents=True, exist_ok=True)
        path = self.worker_alive_path(worker_id)
        payload = {
            "workerId": worker_id,
            "sessionId": session_id,
            "currentJob": current_job,
            "claimedAt": claimed_at,
            "updatedAt": time.time(),
        }
        path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        return path

    def read_worker_heartbeat(self, path: Path) -> dict[str, Any] | None:
        try:
            text = path.read_text(encoding="utf-8").strip()
        except OSError:
            return None
        if not text:
            return None
        # Legacy plain-timestamp heartbeats.
        if not text.startswith("{"):
            try:
                return {
                    "workerId": path.stem,
                    "sessionId": None,
                    "currentJob": None,
                    "claimedAt": None,
                    "updatedAt": float(text.splitlines()[0]),
                }
            except ValueError:
                return None
        try:
            data = json.loads(text)
        except json.JSONDecodeError:
            return None
        if not isinstance(data, dict):
            return None
        return data

    def iter_worker_heartbeats(self) -> Iterator[dict[str, Any]]:
        if not self.workers.is_dir():
            return
        for path in sorted(self.workers.glob("*.alive")):
            data = self.read_worker_heartbeat(path)
            if data:
                yield data

    def write_claim_owner(
        self,
        job_name: str,
        *,
        worker_id: str,
        session_id: str | None = None,
        claimed_at: float | None = None,
    ) -> Path:
        """Record which worker/session owns a job in ``processing/``."""
        path = self.claim_owner_path(job_name)
        payload = {
            "job": job_name,
            "workerId": worker_id,
            "sessionId": session_id,
            "claimedAt": claimed_at if claimed_at is not None else time.time(),
        }
        path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")
        return path

    def read_claim_owner(self, job_name: str) -> dict[str, Any] | None:
        path = self.claim_owner_path(job_name)
        if not path.is_file():
            return None
        try:
            data = json.loads(path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            return None
        return data if isinstance(data, dict) else None

    def clear_claim_owner(self, job_name: str) -> None:
        path = self.claim_owner_path(job_name)
        if path.exists():
            try:
                path.unlink()
            except OSError:
                pass

    def owner_for_processing_job(self, job_name: str) -> dict[str, Any] | None:
        """Resolve owner from claim sidecar, else from worker heartbeats."""
        owner = self.read_claim_owner(job_name)
        if owner and (owner.get("workerId") or owner.get("sessionId")):
            return owner
        for hb in self.iter_worker_heartbeats():
            if hb.get("currentJob") == job_name:
                return {
                    "job": job_name,
                    "workerId": hb.get("workerId"),
                    "sessionId": hb.get("sessionId"),
                    "claimedAt": hb.get("claimedAt"),
                }
        return None

    def reclaim_to_queue(self, job_name: str) -> Path | None:
        """Move a ``processing/`` job back to ``queue/`` and clear its owner sidecar."""
        filename = f"{sanitize_name(job_name)}.json"
        src = self.processing / filename
        if not src.is_file():
            return None
        dest = self.queue / filename
        try:
            os.rename(src, dest)
        except OSError as exc:
            _LOG.warning("failed to reclaim %s: %s", src, exc)
            return None
        self.clear_claim_owner(job_name)
        return dest

    def reclaim_orphan_processing(self, *, grace_seconds: int) -> list[str]:
        """Reclaim ``processing/`` jobs with no owning heartbeat older than ``grace_seconds``."""
        now = time.time()
        claimed_jobs = {
            str(hb.get("currentJob"))
            for hb in self.iter_worker_heartbeats()
            if hb.get("currentJob")
        }
        reclaimed: list[str] = []
        for path in self.list_json(self.processing):
            job = self.read_payload(path)
            if job.name in claimed_jobs:
                continue
            owner = self.read_claim_owner(job.name)
            if owner and owner.get("workerId"):
                # Owner sidecar exists but heartbeat gone — still wait for grace on claim mtime.
                pass
            age = now - path.stat().st_mtime
            if age < grace_seconds:
                continue
            if self.reclaim_to_queue(job.name) is None:
                continue
            reclaimed.append(job.name)
            _LOG.warning("reclaimed orphan processing job %s (age %.0fs)", job.name, age)
        return reclaimed

    def reclaim_stale_processing(self, *, grace_seconds: int) -> list[str]:
        """Backward-compatible alias for orphan reclaim (mtime + no heartbeat owner)."""
        return self.reclaim_orphan_processing(grace_seconds=grace_seconds)

    def iter_terminal(self) -> Iterator[tuple[str, str]]:
        for path in self.list_json(self.processed):
            yield self.read_payload(path).name, "processed"
        for path in self.list_json(self.failed):
            yield self.read_payload(path).name, "failed"

    def log_path_for(self, job_name: str) -> Path:
        return self.logs / f"{sanitize_name(job_name)}.log"
