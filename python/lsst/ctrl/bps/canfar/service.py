"""BPS WMS service backed by a CANFAR Skaha warm worker pool."""

from __future__ import annotations

import logging
from pathlib import Path
from typing import Any

from .config import defaults_path, load_site_config
from .orchestrator import Orchestrator, run_from_submit_path
from .pool import default_session
from .queue import QueueLayout
from .staging import distribute
from .status import WmsStates, overall_run_state, state_for_location, summarize_job_states
from .workflow import CanfarWorkflow

_LOG = logging.getLogger(__name__)

try:
    from lsst.ctrl.bps import BaseWmsService, WmsJobReport, WmsRunReport
    from lsst.resources import ResourcePath
except Exception:  # pragma: no cover
    BaseWmsService = object  # type: ignore[misc, assignment]
    WmsJobReport = None  # type: ignore[misc, assignment]
    WmsRunReport = None  # type: ignore[misc, assignment]
    ResourcePath = None  # type: ignore[misc, assignment]


class CanfarService(BaseWmsService):
    """Submit BPS workflows to CANFAR using warm headless workers + a filesystem queue."""

    @property
    def defaults(self):
        try:
            from lsst.daf.butler import Config

            return Config(str(defaults_path()))
        except Exception:
            return None

    @property
    def defaults_uri(self):
        if ResourcePath is None:
            return None
        return ResourcePath(f"file://{defaults_path()}")

    def prepare(self, config, generic_workflow, out_prefix=None):
        service_class = f"{self.__class__.__module__}.{self.__class__.__name__}"
        if out_prefix is None:
            out_prefix = config["submitPath"]
        workflow = CanfarWorkflow.from_generic_workflow(
            config, generic_workflow, out_prefix, service_class
        )
        workflow.write(out_prefix)
        return workflow

    def submit(self, workflow, **kwargs):
        submit_path = getattr(workflow, "submit_path", None) or getattr(workflow, "run_id", None)
        if not submit_path:
            raise RuntimeError("workflow has no submit_path")
        site = load_site_config(getattr(workflow, "config", None))
        # Allow site overrides from kwargs / workflow.site
        if getattr(workflow, "site", None) is not None:
            site = workflow.site
        layout = QueueLayout(submit_path)
        # Publish files the workers cannot read in place.  Done here rather
        # than in prepare because BPS writes per-job QuantumGraph files after
        # the plugin's prepare when whenSaveJobQgraph is PREPARE.
        run_doc = layout.read_run()
        to_distribute = list(run_doc.get("filesToDistribute") or [])
        if to_distribute:
            published = distribute(to_distribute, str(run_doc.get("fileDistributionEndPoint") or ""))
            _LOG.info("published %d file(s) for workers", len(published))
        orch = Orchestrator(
            layout,
            site,
            session=kwargs.get("session") or default_session(timeout=site.http_timeout),
        )
        # Blocking orchestrator (foreground MVP). For smoke tests pass once=True.
        once = bool(kwargs.get("once", False))
        state = orch.run(launch_workers=True, once=once)
        workflow.run_id = str(submit_path)
        workflow.submit_path = str(submit_path)
        _LOG.info("submit finished: %s", state.get("runState"))

    def restart(self, wms_workflow_id):
        submit_path = str(wms_workflow_id)
        layout = QueueLayout(submit_path)
        if not layout.dag_path.is_file():
            return None, None, f"no dag.json under {submit_path}"
        # Move failed jobs back to payloads for retry; leave processed alone.
        layout.requeue_for_restart()
        layout.clear_cancel()
        try:
            state = run_from_submit_path(submit_path, once=False)
        except Exception as exc:
            return None, None, str(exc)
        run_name = layout.read_state().get("submitPath", submit_path)
        return submit_path, Path(run_name).name, ""

    def list_submitted_jobs(
        self, wms_id=None, user=None, require_bps=True, pass_thru=None, is_global=False
    ):
        if wms_id:
            path = Path(str(wms_id))
            return [str(path)] if (path / "dag.json").is_file() else []
        # Without a global registry, only the given id is meaningful.
        return []

    def report(
        self,
        wms_workflow_id=None,
        user=None,
        hist=0,
        pass_thru=None,
        is_global=False,
        return_exit_codes=False,
    ):
        if not wms_workflow_id:
            return [], "Pass --id <submitPath> for CANFAR BPS runs."
        layout = QueueLayout(wms_workflow_id)
        if not layout.dag_path.is_file():
            return [], f"No CANFAR BPS run at {wms_workflow_id}"
        dag = layout.read_dag()
        jobs_meta = dag.get("jobs") or {}
        pruned = set(layout.read_state().get("pruned") or [])
        locations = {name: layout.job_location(name) for name in jobs_meta}
        counts = summarize_job_states(locations, pruned=pruned)
        run_state = overall_run_state(counts, cancel_requested=layout.cancel_requested())

        job_reports = []
        job_summary: dict[str, dict[Any, int]] = {}
        for name, meta in jobs_meta.items():
            label = str(meta.get("label") or name)
            state = WmsStates.PRUNED if name in pruned else state_for_location(locations.get(name))
            if WmsJobReport is not None:
                job_reports.append(
                    WmsJobReport(wms_id=name, name=name, label=label, state=state)
                )
            per_label = job_summary.setdefault(label, {})
            per_label[state] = per_label.get(state, 0) + 1

        run_doc = layout.read_run()

        if WmsRunReport is None:
            return [
                {
                    "wms_id": str(wms_workflow_id),
                    "path": str(wms_workflow_id),
                    "state": run_state,
                    "job_state_counts": counts,
                }
            ], ""

        report = WmsRunReport(
            wms_id=str(wms_workflow_id),
            path=str(wms_workflow_id),
            label=run_doc.get("payloadName") or run_doc.get("name"),
            run=run_doc.get("name"),
            project=run_doc.get("project"),
            campaign=run_doc.get("campaign"),
            payload=run_doc.get("payloadName"),
            operator=run_doc.get("operator"),
            site=run_doc.get("computeSite") or "canfar",
            state=run_state,
            jobs=job_reports,
            total_number_jobs=len(jobs_meta),
            job_state_counts=counts,
            job_summary=job_summary,
        )
        return [report], f"Queue root: {wms_workflow_id}"

    def get_status(self, wms_workflow_id: str, hist: float = 1, is_global: bool = False):
        reports, message = self.report(wms_workflow_id=wms_workflow_id)
        if not reports:
            return WmsStates.UNKNOWN, message
        report = reports[0]
        state = getattr(report, "state", None) or report.get("state")  # type: ignore[union-attr]
        return state, message

    def cancel(self, wms_id, pass_thru=None):
        layout = QueueLayout(wms_id)
        if not layout.dag_path.is_file():
            return False, f"No CANFAR BPS run at {wms_id}"
        layout.request_cancel()
        state = layout.read_state()
        worker_ids = [str(x) for x in state.get("workerIds") or []]
        if worker_ids:
            try:
                session = default_session()
                session.destroy(worker_ids)
            except Exception as exc:
                return False, f"cancel flag written but destroy failed: {exc}"
        # Park queued work so workers that have not exited yet do not claim more.
        layout.park_queued()
        return True, f"Cancelled CANFAR BPS run {wms_id}"

    def run_submission_checks(self):
        site = load_site_config(self.config)
        # Constructing Session validates auth material; Images ping is best-effort.
        session = default_session(timeout=site.http_timeout)
        try:
            from canfar.images import Images

            images = Images()
            # canfar.Images.fetch returns list[str] image ids.
            names = {str(item) for item in (images.fetch(kind="headless") or [])}
            if names and site.image not in names and not any(site.image in n for n in names):
                _LOG.warning(
                    "image %s not obviously present in headless listing (%d images)",
                    site.image,
                    len(names),
                )
        except Exception as exc:
            _LOG.warning("could not verify headless image listing: %s", exc)
        _ = session
        return True

    def ping(self, pass_thru):
        try:
            self.run_submission_checks()
            return 0, "CANFAR BPS plugin reachable"
        except Exception as exc:
            return 1, str(exc)
