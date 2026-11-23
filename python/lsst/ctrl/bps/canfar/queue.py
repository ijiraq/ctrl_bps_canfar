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
        os.rename(processing_path, dest)
        return dest

    def mark_failed(self, processing_path: Path) -> Path:
        dest = self.failed / processing_path.name
        os.rename(processing_path, dest)
        return dest

    def reclaim_stale_processing(self, *, grace_seconds: int) -> list[str]:
        """Move orphaned ``processing/`` jobs older than ``grace_seconds`` back to ``queue/``."""
        now = time.time()
        reclaimed: list[str] = []
        for path in self.list_json(self.processing):
            age = now - path.stat().st_mtime
            if age < grace_seconds:
                continue
            dest = self.queue / path.name
            try:
                os.rename(path, dest)
            except OSError as exc:
                _LOG.warning("failed to reclaim %s: %s", path, exc)
                continue
            job = self.read_payload(dest)
            reclaimed.append(job.name)
            _LOG.warning("reclaimed stale processing job %s (age %.0fs)", job.name, age)
        return reclaimed

    def iter_terminal(self) -> Iterator[tuple[str, str]]:
        for path in self.list_json(self.processed):
            yield self.read_payload(path).name, "processed"
        for path in self.list_json(self.failed):
            yield self.read_payload(path).name, "failed"

    def log_path_for(self, job_name: str) -> Path:
        return self.logs / f"{sanitize_name(job_name)}.log"
