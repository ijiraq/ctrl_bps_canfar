"""Prepare a CANFAR queue layout from a BPS GenericWorkflow."""

from __future__ import annotations

import logging
import os
import shutil
from pathlib import Path
from typing import Any

from .config import CanfarSiteConfig, load_site_config
from .job import job_files, payload_from_generic_job
from .queue import ACCESS_REMOTE, ACCESS_SHARED, JobPayload, QueueLayout
from .staging import validate_endpoint

_LOG = logging.getLogger(__name__)

try:
    from lsst.ctrl.bps import BaseWmsWorkflow, BpsConfig, GenericWorkflow
except Exception:  # pragma: no cover
    BaseWmsWorkflow = object  # type: ignore[misc, assignment]
    BpsConfig = Any  # type: ignore[misc, assignment]
    GenericWorkflow = Any  # type: ignore[misc, assignment]


def _config_value(config: Any, key: str, default: Any) -> Any:
    """Look up a BPS setting, honouring site-level overrides when possible."""
    value: Any = None
    search = getattr(config, "search", None)
    if callable(search):
        opt: dict[str, Any] = {}
        get_search_opts = getattr(config, "get_search_opts", None)
        if callable(get_search_opts):
            # Fills in curr_site so site.<computeSite>.<key> is searched.
            opt = dict(get_search_opts())
        opt["default"] = default
        _, value = search(key, opt=opt)
    elif config is not None:
        try:
            value = config[key]
        except (KeyError, TypeError):
            value = None
    # BPS reports unset keys as "".
    if value is None or value == "":
        return default
    return value


def _as_bool(value: Any) -> bool:
    if isinstance(value, str):
        return value.strip().lower() in {"1", "true", "yes", "on"}
    return bool(value)


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
    """WMS workflow that materializes a job queue under ``submitPath``."""

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
        self.access_mode: str = ACCESS_SHARED
        # Submit-side files CanfarService.submit publishes for the workers.
        self.files_to_distribute: list[str] = []

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

        use_shared = _as_bool(_config_value(config, "bpsUseShared", True))
        endpoint = ""
        if not use_shared:
            endpoint = validate_endpoint(str(_config_value(config, "fileDistributionEndPoint", "")))
        # basename at the endpoint -> submit-side file published under it.
        to_distribute: dict[str, str] = {}

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
            files = job_files(generic_workflow, job_name, use_shared=use_shared, endpoint=endpoint)
            for src in files.distribute:
                published_as = os.path.basename(src.rstrip("/"))
                other = to_distribute.setdefault(published_as, src)
                if other != src:
                    raise ValueError(
                        f"job {job_name}: {src} and {other} would both be published as "
                        f"{endpoint}{published_as}"
                    )
            payload = payload_from_generic_job(job, parents=parents, files=files)
            layout.write_payload(payload)
            payloads.append(payload)

        layout.write_dag(payloads)
        run_doc = {
            "name": generic_workflow.name,
            "serviceClass": service_class,
            "submitPath": out_prefix,
            "accessMode": ACCESS_SHARED if use_shared else ACCESS_REMOTE,
            "fileDistributionEndPoint": endpoint,
            "filesToDistribute": list(to_distribute.values()),
            "siteConfig": site.to_mapping(),
        }
        # Preserve useful BPS labels when present.
        for key in ("project", "campaign", "payloadName", "operator", "computeSite"):
            try:
                value = config[key]
            except Exception:
                value = None
            if value is not None:
                run_doc[key] = value
        layout.write_run(run_doc)
        workflow.layout = layout
        workflow.payloads = payloads
        workflow.run_id = out_prefix
        workflow.access_mode = run_doc["accessMode"]
        workflow.files_to_distribute = list(to_distribute.values())
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
        layout.write_run(
            {
                "name": name,
                "submitPath": str(submit_path),
                "accessMode": ACCESS_SHARED,
                "siteConfig": site.to_mapping(),
            }
        )
        workflow.layout = layout
        workflow.payloads = payloads
        workflow.submit_path = str(submit_path)
        workflow.run_id = str(submit_path)
        return workflow
