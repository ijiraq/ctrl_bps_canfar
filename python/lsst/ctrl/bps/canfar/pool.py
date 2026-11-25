"""Launch and manage a pool of Skaha headless workers."""

from __future__ import annotations

import logging
import shlex
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Protocol, TypeVar

from .config import CanfarSiteConfig, sanitize_name
from .queue import QueueLayout

_LOG = logging.getLogger(__name__)

_TERMINAL = {"completed", "succeeded", "failed", "deleted", "error"}
# canfar.Session.timeout is capped at 300 by the library.
_CANFAR_MAX_TIMEOUT = 300
_DEFAULT_HTTP_TIMEOUT = 120

_T = TypeVar("_T")


class SessionClient(Protocol):
    def create(self, **kwargs: Any) -> list[str]: ...

    def info(self, ids: list[str], **kwargs: Any) -> list[dict[str, Any]]: ...

    def destroy(self, ids: list[str], **kwargs: Any) -> Any: ...


def _as_float(value: Any, default: float = 0.0) -> float:
    if value is None or value == "":
        return default
    try:
        return float(value)
    except (TypeError, ValueError):
        return default


def cpu_utilization(info: dict[str, Any]) -> float | None:
    """Return ``cpuCoresInUse / requestedCPUCores``, or None if requested is missing/zero."""
    requested = _as_float(
        info.get("requestedCPUCores")
        if info.get("requestedCPUCores") is not None
        else info.get("requestedCpuCores"),
        default=0.0,
    )
    if requested <= 0:
        return None
    in_use = _as_float(
        info.get("cpuCoresInUse") if info.get("cpuCoresInUse") is not None else info.get("cpuCores"),
        default=0.0,
    )
    return in_use / requested


def clamp_http_timeout(timeout: int) -> int:
    """Clamp to canfar.Session allowed range (1..300)."""
    return max(1, min(int(timeout), _CANFAR_MAX_TIMEOUT))


def _is_transient_skaha_error(exc: BaseException) -> bool:
    name = type(exc).__name__.lower()
    text = str(exc).lower()
    markers = ("timeout", "timed out", "temporarily", "connection", "reset", "unavailable")
    return any(m in name for m in markers) or any(m in text for m in markers)


def call_with_retries(
    operation: str,
    func: Callable[[], _T],
    *,
    retries: int,
    base_delay: float = 2.0,
) -> _T:
    """Run ``func``, retrying transient Skaha/HTTP failures with exponential backoff."""
    attempts = max(1, int(retries))
    last: BaseException | None = None
    for attempt in range(1, attempts + 1):
        try:
            return func()
        except Exception as exc:
            last = exc
            if attempt >= attempts or not _is_transient_skaha_error(exc):
                raise
            delay = base_delay * (2 ** (attempt - 1))
            _LOG.warning(
                "%s failed (attempt %s/%s): %s; retrying in %.1fs",
                operation,
                attempt,
                attempts,
                exc,
                delay,
            )
            time.sleep(delay)
    assert last is not None
    raise last


@dataclass
class WorkerPool:
    """Track headless worker session IDs for one BPS run."""

    submit_path: Path
    site: CanfarSiteConfig
    session: SessionClient
    worker_script: Path
    session_ids: list[str] = field(default_factory=list)
    worker_name_by_session: dict[str, str] = field(default_factory=dict)
    # Consecutive low-CPU samples per session (after the post-claim grace poll).
    low_cpu_counts: dict[str, int] = field(default_factory=dict)
    # Sessions already past the first post-claim poll.
    seen_claim_poll: set[str] = field(default_factory=set)

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
        layout = QueueLayout(self.submit_path)
        layout.ensure()
        for index in range(need):
            ordinal = len(self.session_ids) + index + 1
            name = sanitize_name(f"{self.site.worker_name_prefix}-{self.submit_path.name}-{ordinal}")
            args = f"{script} {submit} {self.site.idle_timeout} {self.site.poll_interval} {name}"
            env = {
                "BPS_CANFAR_SUBMIT_PATH": str(self.submit_path),
                "BPS_CANFAR_IDLE_TIMEOUT": str(self.site.idle_timeout),
                "BPS_CANFAR_POLL_INTERVAL": str(self.site.poll_interval),
                "BPS_CANFAR_WORKER_ID": name,
            }
            if self.site.env_script:
                env["BPS_CANFAR_ENV_SH"] = self.site.env_script
            kwargs: dict[str, Any] = {
                "name": name,
                "image": self.site.image,
                "kind": "headless",
                "cmd": "bash",
                "args": args,
                "env": env,
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
            ids = call_with_retries(
                f"create worker {name}",
                lambda kw=dict(kwargs): self.session.create(**kw),
                retries=self.site.http_retries,
            )
            created.extend(ids)
            for sid in ids:
                self.session_ids.append(sid)
                self.worker_name_by_session[sid] = name
                layout.write_worker_session(name, sid)
        return created

    def _session_infos(self) -> dict[str, dict[str, Any]]:
        if not self.session_ids:
            return {}
        try:
            infos = self.session.info(list(self.session_ids))
        except Exception as exc:  # pragma: no cover - network failures
            _LOG.warning("worker info failed: %s", exc)
            return {}
        by_id: dict[str, dict[str, Any]] = {}
        for item in infos or []:
            sid = str(item.get("id") or item.get("sessionId") or "")
            if sid:
                by_id[sid] = item
                name = item.get("name")
                if name:
                    self.worker_name_by_session.setdefault(sid, str(name))
        return by_id

    def alive_ids(self) -> list[str]:
        if not self.session_ids:
            return []
        status_by_id = self._session_infos()
        if not status_by_id and self.session_ids:
            # info failed; keep current list
            return list(self.session_ids)
        alive: list[str] = []
        for sid in self.session_ids:
            item = status_by_id.get(sid) or {}
            status = str(item.get("status") or item.get("state") or "")
            if status.lower() in _TERMINAL:
                self.low_cpu_counts.pop(sid, None)
                self.seen_claim_poll.discard(sid)
                self.worker_name_by_session.pop(sid, None)
                continue
            alive.append(sid)
        self.session_ids = alive
        return list(self.session_ids)

    def _session_for_owner(self, owner: dict[str, Any]) -> str | None:
        sid = owner.get("sessionId")
        if sid and str(sid) in self.session_ids:
            return str(sid)
        worker_id = owner.get("workerId")
        if not worker_id:
            return None
        for sid, name in self.worker_name_by_session.items():
            if name == worker_id and sid in self.session_ids:
                return sid
        return None

    def reclaim_stalled_jobs(self, layout: QueueLayout) -> list[str]:
        """Destroy sessions whose claimed job stays under the CPU fraction for ``stallPolls``.

        Skips the first orchestrator poll after a claim (``seen_claim_poll``), then requires
        ``stallPolls`` consecutive samples with
        ``cpuCoresInUse / requestedCPUCores < stallCpuFraction``.
        """
        infos = self._session_infos()
        reclaimed: list[str] = []
        active_sessions: set[str] = set()

        for path in layout.list_json(layout.processing):
            job = layout.read_payload(path)
            owner = layout.owner_for_processing_job(job.name)
            if not owner:
                continue
            sid = self._session_for_owner(owner)
            if not sid:
                continue
            active_sessions.add(sid)
            info = infos.get(sid)
            if not info:
                continue

            if sid not in self.seen_claim_poll:
                # At least one full poll cycle after claim before counting.
                self.seen_claim_poll.add(sid)
                self.low_cpu_counts[sid] = 0
                _LOG.debug("stall grace poll for session %s job %s", sid, job.name)
                continue

            ratio = cpu_utilization(info)
            if ratio is None:
                continue
            if ratio < self.site.stall_cpu_fraction:
                self.low_cpu_counts[sid] = self.low_cpu_counts.get(sid, 0) + 1
                _LOG.info(
                    "low CPU %.4f for session %s job %s (%s/%s)",
                    ratio,
                    sid,
                    job.name,
                    self.low_cpu_counts[sid],
                    self.site.stall_polls,
                )
            else:
                self.low_cpu_counts[sid] = 0

            if self.low_cpu_counts.get(sid, 0) < self.site.stall_polls:
                continue

            _LOG.warning(
                "reclaiming stalled job %s (session %s, cpu ratio stayed < %.4f)",
                job.name,
                sid,
                self.site.stall_cpu_fraction,
            )
            try:
                call_with_retries(
                    f"destroy stalled session {sid}",
                    lambda s=sid: self.session.destroy([s]),
                    retries=self.site.http_retries,
                )
            except Exception as exc:
                _LOG.warning("destroy failed for stalled session %s: %s", sid, exc)
            if sid in self.session_ids:
                self.session_ids.remove(sid)
            self.worker_name_by_session.pop(sid, None)
            self.low_cpu_counts.pop(sid, None)
            self.seen_claim_poll.discard(sid)
            if layout.reclaim_to_queue(job.name) is not None:
                reclaimed.append(job.name)

        # Drop counters for sessions that no longer hold a claim.
        for sid in list(self.low_cpu_counts):
            if sid not in active_sessions:
                self.low_cpu_counts.pop(sid, None)
                self.seen_claim_poll.discard(sid)
        return reclaimed

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
            call_with_retries(
                "destroy workers",
                lambda: self.session.destroy(ids),
                retries=self.site.http_retries,
            )
        finally:
            self.session_ids = []
            self.worker_name_by_session.clear()
            self.low_cpu_counts.clear()
            self.seen_claim_poll.clear()


def default_session(*, timeout: int | None = None) -> Any:
    """Build a ``canfar.sessions.Session`` with a Skaha-friendly HTTP timeout.

    The canfar client default is 30s, which is often too short for ``create``.
    """
    from canfar.sessions import Session

    return Session(timeout=clamp_http_timeout(timeout if timeout is not None else _DEFAULT_HTTP_TIMEOUT))


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
