"""Launch and manage a pool of Skaha headless workers."""

from __future__ import annotations

import logging
import shlex
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Protocol

from .config import CanfarSiteConfig, sanitize_name

_LOG = logging.getLogger(__name__)


class SessionClient(Protocol):
    def create(self, **kwargs: Any) -> list[str]: ...

    def info(self, ids: list[str], **kwargs: Any) -> list[dict[str, Any]]: ...

    def destroy(self, ids: list[str], **kwargs: Any) -> Any: ...


@dataclass
class WorkerPool:
    """Track headless worker session IDs for one BPS run."""

    submit_path: Path
    site: CanfarSiteConfig
    session: SessionClient
    worker_script: Path
    session_ids: list[str] = field(default_factory=list)

    def desired_size(self) -> int:
        return max(0, int(self.site.n_workers))

    def launch(self, *, count: int | None = None) -> list[str]:
        """Create ``count`` new headless workers (default: fill to nWorkers)."""
        need = self.desired_size() - len(self.session_ids) if count is None else count
        if need <= 0:
            return []
        created: list[str] = []
        script = shlex.quote(str(self.worker_script))
        submit = shlex.quote(str(self.submit_path))
        for index in range(need):
            ordinal = len(self.session_ids) + index + 1
            name = sanitize_name(f"{self.site.worker_name_prefix}-{self.submit_path.name}-{ordinal}")
            args = f"{script} {submit} {self.site.idle_timeout} {self.site.poll_interval} {name}"
            kwargs: dict[str, Any] = {
                "name": name,
                "image": self.site.image,
                "kind": "headless",
                "cmd": "bash",
                "args": args,
                "env": {
                    "BPS_CANFAR_SUBMIT_PATH": str(self.submit_path),
                    "BPS_CANFAR_IDLE_TIMEOUT": str(self.site.idle_timeout),
                    "BPS_CANFAR_POLL_INTERVAL": str(self.site.poll_interval),
                    "BPS_CANFAR_WORKER_ID": name,
                },
                "replicas": 1,
                "errors": "raise",
            }
            if self.site.cores is not None:
                kwargs["cores"] = self.site.cores
            if self.site.ram is not None:
                kwargs["ram"] = self.site.ram
            if self.site.gpu is not None:
                kwargs["gpu"] = self.site.gpu
            _LOG.info("creating worker %s", name)
            ids = self.session.create(**kwargs)
            created.extend(ids)
            self.session_ids.extend(ids)
        return created

    def alive_ids(self) -> list[str]:
        if not self.session_ids:
            return []
        try:
            infos = self.session.info(list(self.session_ids))
        except Exception as exc:  # pragma: no cover - network failures
            _LOG.warning("worker info failed: %s", exc)
            return list(self.session_ids)
        alive: list[str] = []
        status_by_id = {}
        for item in infos or []:
            sid = str(item.get("id") or item.get("sessionId") or "")
            status = str(item.get("status") or item.get("state") or "")
            if sid:
                status_by_id[sid] = status
        for sid in self.session_ids:
            status = status_by_id.get(sid, "")
            if status.lower() in {"completed", "succeeded", "failed", "deleted", "error"}:
                continue
            alive.append(sid)
        self.session_ids = alive
        return list(self.session_ids)

    def maintain(self, *, work_remaining: bool) -> list[str]:
        """Reap finished workers; optionally replace up to nWorkers while work remains."""
        self.alive_ids()
        if not work_remaining or not self.site.replace_workers:
            return []
        return self.launch()

    def destroy_all(self) -> None:
        if not self.session_ids:
            return
        ids = list(self.session_ids)
        _LOG.info("destroying %d workers", len(ids))
        try:
            self.session.destroy(ids)
        finally:
            self.session_ids = []


def default_session() -> Any:
    from canfar.sessions import Session

    return Session()


def default_worker_script() -> Path:
    """Prefer the packaged ``scripts/run_worker.sh``."""
    here = Path(__file__).resolve()
    candidates = [
        here.parent / "scripts" / "run_worker.sh",
        here.parents[5] / "scripts" / "run_worker.sh",
    ]
    for path in candidates:
        if path.is_file():
            return path
    raise FileNotFoundError("run_worker.sh not found; reinstall lsst-ctrl-bps-canfar")
