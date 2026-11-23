"""Prepare a CANFAR queue layout from a BPS GenericWorkflow."""

from __future__ import annotations

import logging
import shutil
from pathlib import Path
from typing import Any

import yaml

from .config import CanfarSiteConfig, load_site_config
from .job import payload_from_generic_job
from .queue import JobPayload, QueueLayout

_LOG = logging.getLogger(__name__)

try:
    from lsst.ctrl.bps import BaseWmsWorkflow, BpsConfig, GenericWorkflow
except Exception:  # pragma: no cover
    BaseWmsWorkflow = object  # type: ignore[misc, assignment]
    BpsConfig = Any  # type: ignore[misc, assignment]
    GenericWorkflow = Any  # type: ignore[misc, assignment]


def _worker_script_source() -> Path:
    here = Path(__file__).resolve()
    packaged = here.parent / "scripts" / "run_worker.sh"
    if packaged.is_file():
        return packaged
    # python/lsst/ctrl/bps/canfar/workflow.py -> repo root
    repo_script = here.parents[5] / "scripts" / "run_worker.sh"
    return repo_script


def write_worker_script(dest: Path) -> Path:
    dest = Path(dest)
    source = _worker_script_source()
    if source.is_file():
        shutil.copyfile(source, dest)
    else:
        dest.write_text(
            "#!/bin/bash\n"
            "set -euo pipefail\n"
            'exec python -m lsst.ctrl.bps.canfar.worker "$@"\n',
            encoding="utf-8",
        )
    dest.chmod(0o755)
    return dest


class CanfarWorkflow(BaseWmsWorkflow):
    """WMS workflow that materializes an ARC job queue under ``submitPath``."""

    def __init__(self, name: str, config: Any, *, site: CanfarSiteConfig | None = None):
        if BaseWmsWorkflow is not object:
            super().__init__(name, config)
        else:  # pragma: no cover
            self.name = name
            self.config = config
        self.site = site or load_site_config(config)
        self.layout: QueueLayout | None = None
        self.payloads: list[JobPayload] = []
        self.run_id: str | None = None
        self.submit_path: str | None = None

    @classmethod
    def from_generic_workflow(
        cls,
        config: Any,
        generic_workflow: Any,
        out_prefix: str,
        service_class: str,
    ) -> CanfarWorkflow:
        site = load_site_config(config)
        workflow = cls(generic_workflow.name, config, site=site)
        workflow.submit_path = out_prefix
        layout = QueueLayout(out_prefix)
        layout.ensure()
        write_worker_script(layout.root / "run_worker.sh")

        payloads: list[JobPayload] = []
        for job_name in generic_workflow:
            job = generic_workflow.get_job(job_name)
            # Skip no-op / final special nodes that have no executable.
            if getattr(job, "executable", None) is None or not getattr(job.executable, "src_uri", None):
                _LOG.debug("skipping non-executable node %s", job_name)
                continue
            parents = []
            for parent_name in generic_workflow.predecessors(job_name):
                parent = generic_workflow.get_job(parent_name)
                if getattr(parent, "executable", None) is not None and getattr(
                    parent.executable, "src_uri", None
                ):
                    parents.append(parent_name)
            payload = payload_from_generic_job(job, parents=parents)
            layout.write_payload(payload)
            payloads.append(payload)

        layout.write_dag(payloads)
        run_doc = {
            "name": generic_workflow.name,
            "serviceClass": service_class,
            "submitPath": out_prefix,
            "siteConfig": {
                "image": site.image,
                "nWorkers": site.n_workers,
                "cores": site.cores,
                "ram": site.ram,
                "gpu": site.gpu,
                "idleTimeout": site.idle_timeout,
                "pollInterval": site.poll_interval,
                "workerNamePrefix": site.worker_name_prefix,
                "replaceWorkers": site.replace_workers,
                "reclaimGraceSeconds": site.reclaim_grace_seconds,
            },
        }
        # Preserve useful BPS labels when present.
        for key in ("project", "campaign", "payloadName", "operator", "computeSite"):
            try:
                value = config[key]
            except Exception:
                value = None
            if value is not None:
                run_doc[key] = value
        layout.run_path.write_text(yaml.safe_dump(run_doc, sort_keys=False), encoding="utf-8")
        workflow.layout = layout
        workflow.payloads = payloads
        workflow.run_id = out_prefix
        return workflow

    def write(self, out_prefix: str) -> None:
        # from_generic_workflow already wrote the queue; keep method for BaseWmsWorkflow.
        if self.layout is None:
            raise RuntimeError("workflow has not been prepared")
        _LOG.info("CANFAR workflow written under %s (%d jobs)", out_prefix, len(self.payloads))

    @classmethod
    def from_payloads(
        cls,
        name: str,
        submit_path: str | Path,
        payloads: list[JobPayload],
        *,
        site: CanfarSiteConfig | None = None,
        config: dict[str, Any] | None = None,
    ) -> CanfarWorkflow:
        """Test helper: build a queue without a GenericWorkflow."""
        site = site or load_site_config(config)
        workflow = cls(name, config or {}, site=site)
        layout = QueueLayout(submit_path)
        layout.ensure()
        write_worker_script(layout.root / "run_worker.sh")
        for payload in payloads:
            layout.write_payload(payload)
        layout.write_dag(payloads)
        layout.run_path.write_text(
            yaml.safe_dump(
                {
                    "name": name,
                    "submitPath": str(submit_path),
                    "siteConfig": {
                        "image": site.image,
                        "nWorkers": site.n_workers,
                        "cores": site.cores,
                        "ram": site.ram,
                        "idleTimeout": site.idle_timeout,
                        "pollInterval": site.poll_interval,
                        "workerNamePrefix": site.worker_name_prefix,
                        "replaceWorkers": site.replace_workers,
                        "reclaimGraceSeconds": site.reclaim_grace_seconds,
                    },
                },
                sort_keys=False,
            ),
            encoding="utf-8",
        )
        workflow.layout = layout
        workflow.payloads = payloads
        workflow.submit_path = str(submit_path)
        workflow.run_id = str(submit_path)
        return workflow
