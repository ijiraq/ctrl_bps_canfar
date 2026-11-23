from __future__ import annotations

from pathlib import Path

from lsst.ctrl.bps.canfar.config import CanfarSiteConfig
from lsst.ctrl.bps.canfar.orchestrator import Orchestrator
from lsst.ctrl.bps.canfar.pool import WorkerPool
from lsst.ctrl.bps.canfar.queue import JobPayload, QueueLayout
from lsst.ctrl.bps.canfar.workflow import CanfarWorkflow


class FakeSession:
    def __init__(self):
        self.created = []
        self.destroyed = []
        self._n = 0

    def create(self, **kwargs):
        self._n += 1
        sid = f"sess-{self._n}"
        self.created.append(sid)
        return [sid]

    def info(self, ids, **kwargs):
        return [{"id": i, "status": "Running"} for i in ids if i not in self.destroyed]

    def destroy(self, ids, **kwargs):
        self.destroyed.extend(ids)


def test_failed_parent_prunes_child(tmp_path: Path):
    site = CanfarSiteConfig(n_workers=0, replace_workers=False)
    payloads = [
        JobPayload(name="parent", label="a", command="false"),
        JobPayload(name="child", label="b", command="true", parents=["parent"]),
    ]
    CanfarWorkflow.from_payloads("demo", tmp_path, payloads, site=site)
    layout = QueueLayout(tmp_path)
    # Simulate parent failed without ever enqueueing child.
    (layout.payloads / "parent.json").rename(layout.failed / "parent.json")
    orch = Orchestrator(layout, site, pool=WorkerPool(tmp_path, site, FakeSession(), tmp_path / "run_worker.sh"))
    orch.mark_pruned_from_failures()
    assert "child" in orch.pruned
    assert orch.work_remaining() is False


def test_cancel_stops_and_destroys(tmp_path: Path):
    site = CanfarSiteConfig(n_workers=2, poll_interval=1, replace_workers=False)
    CanfarWorkflow.from_payloads(
        "demo",
        tmp_path,
        [JobPayload(name="j", label="t", command="sleep 100")],
        site=site,
    )
    session = FakeSession()
    layout = QueueLayout(tmp_path)
    orch = Orchestrator(
        layout,
        site,
        pool=WorkerPool(tmp_path, site, session, tmp_path / "run_worker.sh"),
        session=session,
    )
    orch.pool.launch()
    assert len(session.created) == 2
    layout.request_cancel()
    state = orch.run(launch_workers=False, once=False)
    assert state["cancelRequested"] is True
    assert session.destroyed == session.created
