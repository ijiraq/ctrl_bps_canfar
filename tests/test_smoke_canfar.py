"""Optional live CANFAR smoke test.

Skipped unless ``BPS_CANFAR_SMOKE=1``. Requires:

- ``canfar login cadc`` already done
- ``BPS_CANFAR_SUBMIT_PATH`` under ``/arc`` (shared with headless workers)
- a headless image (default: lsst/sciplat-lab:latest)

Runs a one-job DAG with a single worker, then tears the session down.
"""

from __future__ import annotations

import os
from pathlib import Path

import pytest

from lsst.ctrl.bps.canfar.config import CanfarSiteConfig
from lsst.ctrl.bps.canfar.orchestrator import Orchestrator
from lsst.ctrl.bps.canfar.pool import WorkerPool, default_session
from lsst.ctrl.bps.canfar.queue import JobPayload, QueueLayout
from lsst.ctrl.bps.canfar.workflow import CanfarWorkflow

pytestmark = pytest.mark.skipif(
    os.environ.get("BPS_CANFAR_SMOKE") != "1",
    reason="set BPS_CANFAR_SMOKE=1 for live CANFAR smoke",
)


def test_live_one_worker_tiny_graph():
    submit = os.environ.get("BPS_CANFAR_SUBMIT_PATH")
    if not submit:
        pytest.skip("BPS_CANFAR_SUBMIT_PATH required for live smoke")
    submit_path = Path(submit)
    submit_path.mkdir(parents=True, exist_ok=True)

    image = os.environ.get(
        "BPS_CANFAR_IMAGE",
        "images.canfar.net/lsst/sciplat-lab:latest_daily",
    )
    site = CanfarSiteConfig(
        image=image,
        n_workers=1,
        cores=1,
        ram=4,
        idle_timeout=300,
        poll_interval=10,
        replace_workers=False,
        worker_name_prefix="bps-smoke",
    )
    CanfarWorkflow.from_payloads(
        "smoke",
        submit_path,
        [JobPayload(name="hello", label="smoke", command="echo smoke-ok && hostname")],
        site=site,
    )
    session = default_session()
    layout = QueueLayout(submit_path)
    orch = Orchestrator(
        layout,
        site,
        pool=WorkerPool(
            submit_path,
            site,
            session,
            submit_path / "run_worker.sh",
        ),
        session=session,
    )
    try:
        state = orch.run(launch_workers=True, once=False)
    finally:
        orch.pool.destroy_all()
        layout.request_cancel()

    assert layout.job_location("hello") == "processed"
    assert "smoke-ok" in layout.log_path_for("hello").read_text()
    assert state.get("cancelRequested") is False or layout.job_location("hello") == "processed"
