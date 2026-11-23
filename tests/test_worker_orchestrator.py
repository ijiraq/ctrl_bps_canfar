from __future__ import annotations

import threading
import time
from pathlib import Path

from lsst.ctrl.bps.canfar.config import CanfarSiteConfig
from lsst.ctrl.bps.canfar.orchestrator import Orchestrator
from lsst.ctrl.bps.canfar.pool import WorkerPool
from lsst.ctrl.bps.canfar.queue import JobPayload, QueueLayout
from lsst.ctrl.bps.canfar.worker import run_worker
from lsst.ctrl.bps.canfar.workflow import CanfarWorkflow


class FakeSession:
    def __init__(self):
        self.created = []
        self.destroyed = []
        self._n = 0

    def create(self, **kwargs):
        self._n += 1
        sid = f"sess-{self._n}"
        self.created.append((sid, kwargs))
        return [sid]

    def info(self, ids, **kwargs):
        return [{"id": i, "status": "Running"} for i in ids if i not in self.destroyed]

    def destroy(self, ids, **kwargs):
        self.destroyed.extend(ids)
        return ids


def test_dag_enqueue_order(tmp_path: Path):
    site = CanfarSiteConfig(n_workers=0, poll_interval=1, replace_workers=False)
    payloads = [
        JobPayload(name="parent", label="a", command="true", parents=[]),
        JobPayload(name="child", label="b", command="true", parents=["parent"]),
    ]
    CanfarWorkflow.from_payloads("demo", tmp_path, payloads, site=site)
    session = FakeSession()
    pool = WorkerPool(tmp_path, site, session, tmp_path / "run_worker.sh")
    orch = Orchestrator(QueueLayout(tmp_path), site, pool=pool, session=session)
    # First pass: only parent ready.
    ready = orch.enqueue_ready()
    assert ready == ["parent"]
    assert orch.layout.job_location("child") == "payloads"
    # Pretend parent succeeded.
    (orch.layout.queue / "parent.json").rename(orch.layout.processed / "parent.json")
    ready = orch.enqueue_ready()
    assert ready == ["child"]


def test_worker_runs_jobs(tmp_path: Path):
    site = CanfarSiteConfig(n_workers=0, idle_timeout=2, poll_interval=1, replace_workers=False)
    payloads = [
        JobPayload(name="j1", label="t", command="echo ok-one"),
        JobPayload(name="j2", label="t", command="echo ok-two", parents=["j1"]),
    ]
    CanfarWorkflow.from_payloads("demo", tmp_path, payloads, site=site)
    from lsst.ctrl.bps.canfar.queue import QueueLayout

    layout = QueueLayout(tmp_path)
    layout.enqueue("j1")

    def orch_loop():
        time.sleep(0.5)
        # After j1 processed, enqueue j2.
        for _ in range(20):
            if layout.job_location("j1") == "processed":
                layout.enqueue("j2")
                break
            time.sleep(0.2)

    threading.Thread(target=orch_loop, daemon=True).start()
    rc = run_worker(tmp_path, idle_timeout=3, poll_interval=1, worker_id="w1")
    assert rc == 0
    assert layout.job_location("j1") == "processed"
    assert layout.job_location("j2") == "processed"
    assert "ok-one" in layout.log_path_for("j1").read_text()


def test_pool_launch_and_maintain(tmp_path: Path):
    site = CanfarSiteConfig(n_workers=2, replace_workers=True, image="img:tag")
    CanfarWorkflow.from_payloads(
        "demo",
        tmp_path,
        [JobPayload(name="j", label="t", command="true")],
        site=site,
    )
    session = FakeSession()
    pool = WorkerPool(tmp_path, site, session, tmp_path / "run_worker.sh")
    created = pool.launch()
    assert len(created) == 2
    assert len(session.created) == 2
    assert session.created[0][1]["kind"] == "headless"
    assert session.created[0][1]["image"] == "img:tag"
    # Simulate one worker finished.
    session.destroyed.append(pool.session_ids[0])
    # alive_ids drops completed/destroyed when info filters — mark via info status instead
    session.info = lambda ids, **kw: [  # type: ignore[method-assign]
        {"id": i, "status": "Completed" if i == created[0] else "Running"} for i in ids
    ]
    pool.alive_ids()
    assert created[0] not in pool.session_ids
    replaced = pool.maintain(work_remaining=True)
    assert len(replaced) == 1
