"""Site configuration for the CANFAR BPS plugin."""

from __future__ import annotations

from dataclasses import dataclass
from importlib import resources
from pathlib import Path
from typing import Any, Mapping

import yaml

QUEUE_SUBDIRS = ("payloads", "queue", "processing", "processed", "failed", "logs", "workers")


@dataclass(frozen=True)
class CanfarSiteConfig:
    """Settings under ``site.<computeSite>`` for CanfarService."""

    image: str = "images.canfar.net/lsst/sciplat-lab:latest_daily"
    n_workers: int = 8
    cores: int | None = 1
    ram: int | None = 4
    gpu: int | None = None
    idle_timeout: int = 86900
    poll_interval: int = 30
    worker_name_prefix: str = "bps-canfar"
    replace_workers: bool = True
    # Orphan processing/ with no worker heartbeat claiming it.
    reclaim_grace_seconds: int = 120
    # Stall detection: cpuCoresInUse / requestedCPUCores below this for stall_polls.
    stall_cpu_fraction: float = 0.01
    stall_polls: int = 5
    # canfar.Session HTTP inactivity timeout (seconds). Library max is 300.
    http_timeout: int = 120
    # Retries for transient Skaha create/destroy failures (timeouts, etc.).
    http_retries: int = 3
    # Script each worker sources before taking jobs (project env); when
    # unset the worker loads the image's stack with loadLSST.bash.
    env_script: str | None = None

    @classmethod
    def from_mapping(cls, data: Mapping[str, Any] | None) -> CanfarSiteConfig:
        data = dict(data or {})
        return cls(
            image=str(data.get("image", cls.image)),
            n_workers=int(data.get("nWorkers", data.get("n_workers", cls.n_workers))),
            cores=_optional_int(data.get("cores", cls.cores)),
            ram=_optional_int(data.get("ram", cls.ram)),
            gpu=_optional_int(data.get("gpu")),
            idle_timeout=int(
                data.get("idleTimeout", data.get("idle_timeout", cls.idle_timeout))
            ),
            poll_interval=int(
                data.get("pollInterval", data.get("poll_interval", cls.poll_interval))
            ),
            worker_name_prefix=str(
                data.get(
                    "workerNamePrefix",
                    data.get("worker_name_prefix", cls.worker_name_prefix),
                )
            ),
            replace_workers=bool(
                data.get(
                    "replaceWorkers",
                    data.get("replace_workers", cls.replace_workers),
                )
            ),
            reclaim_grace_seconds=int(
                data.get(
                    "reclaimGraceSeconds",
                    data.get("reclaim_grace_seconds", cls.reclaim_grace_seconds),
                )
            ),
            stall_cpu_fraction=float(
                data.get(
                    "stallCpuFraction",
                    data.get("stall_cpu_fraction", cls.stall_cpu_fraction),
                )
            ),
            stall_polls=int(
                data.get("stallPolls", data.get("stall_polls", cls.stall_polls))
            ),
            http_timeout=int(
                data.get("httpTimeout", data.get("http_timeout", cls.http_timeout))
            ),
            http_retries=int(
                data.get("httpRetries", data.get("http_retries", cls.http_retries))
            ),
            env_script=_optional_str(data.get("envScript", data.get("env_script"))),
        )

    def to_mapping(self) -> dict[str, Any]:
        """Inverse of `from_mapping`, using the BPS (camelCase) key names."""
        return {
            "image": self.image,
            "nWorkers": self.n_workers,
            "cores": self.cores,
            "ram": self.ram,
            "gpu": self.gpu,
            "idleTimeout": self.idle_timeout,
            "pollInterval": self.poll_interval,
            "workerNamePrefix": self.worker_name_prefix,
            "replaceWorkers": self.replace_workers,
            "reclaimGraceSeconds": self.reclaim_grace_seconds,
            "stallCpuFraction": self.stall_cpu_fraction,
            "stallPolls": self.stall_polls,
            "httpTimeout": self.http_timeout,
            "httpRetries": self.http_retries,
            "envScript": self.env_script,
        }


def _optional_str(value: Any) -> str | None:
    # Same convention as _optional_int: BPS reports unset keys as "".
    if value is None or value == "null":
        return None
    text = str(value).strip()
    return text or None


def _optional_int(value: Any) -> int | None:
    # BPS often leaves unset keys as "" rather than null/None.
    if value is None or value == "null":
        return None
    if isinstance(value, str) and value.strip() == "":
        return None
    return int(value)


def defaults_path() -> Path:
    """Path to packaged ``canfar_defaults.yaml``."""
    try:
        ref = resources.files("lsst.ctrl.bps.canfar").joinpath("etc/canfar_defaults.yaml")
        with resources.as_file(ref) as path:
            return Path(path)
    except (TypeError, FileNotFoundError, ModuleNotFoundError):
        return Path(__file__).resolve().parent / "etc" / "canfar_defaults.yaml"


def load_defaults() -> dict[str, Any]:
    path = defaults_path()
    with path.open(encoding="utf-8") as fh:
        data = yaml.safe_load(fh) or {}
    if not isinstance(data, dict):
        raise ValueError(f"defaults root must be a mapping: {path}")
    return data


def load_site_config(config: Any = None, compute_site: str | None = None) -> CanfarSiteConfig:
    """Merge packaged defaults with a BPS config / plain mapping."""
    defaults = load_defaults()
    site_name = compute_site or _get(config, "computeSite") or defaults.get("computeSite", "canfar")
    site_defaults = (defaults.get("site") or {}).get(site_name) or {}
    site_overrides: dict[str, Any] = {}
    if config is not None:
        site_block = _get(config, "site") or {}
        if isinstance(site_block, Mapping):
            site_overrides = dict(site_block.get(site_name) or {})
        # Flat overrides also accepted at the top level for convenience.
        for key in (
            "image",
            "nWorkers",
            "cores",
            "ram",
            "gpu",
            "idleTimeout",
            "pollInterval",
            "workerNamePrefix",
            "replaceWorkers",
            "reclaimGraceSeconds",
            "stallCpuFraction",
            "stallPolls",
            "httpTimeout",
            "httpRetries",
            "envScript",
        ):
            value = _get(config, key)
            if value is not None and key not in site_overrides:
                site_overrides[key] = value
    merged = {**site_defaults, **site_overrides}
    return CanfarSiteConfig.from_mapping(merged)


def _get(config: Any, key: str) -> Any:
    if config is None:
        return None
    if isinstance(config, Mapping):
        return config.get(key)
    try:
        return config[key]
    except Exception:
        try:
            return getattr(config, key)
        except Exception:
            return None


def sanitize_name(value: str, *, fallback: str = "job") -> str:
    """Skaha names allow letters, digits, and hyphens only."""
    out = []
    for ch in value.lower():
        if ch.isalnum() or ch == "-":
            out.append(ch)
        else:
            out.append("-")
    cleaned = "".join(out).strip("-")
    while "--" in cleaned:
        cleaned = cleaned.replace("--", "-")
    return cleaned or fallback
