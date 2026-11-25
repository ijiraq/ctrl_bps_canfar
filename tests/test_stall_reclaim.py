from __future__ import annotations

from pathlib import Path

from lsst.ctrl.bps.canfar.config import CanfarSiteConfig
from lsst.ctrl.bps.canfar.pool import WorkerPool, cpu_utilization
from lsst.ctrl.bps.canfar.queue import JobPayload, QueueLayout
from lsst.ctrl.bps.canfar.workflow import CanfarWorkflow


class FakeSession:
    def __init__(self, infos=None):
        self.created = []
        self.destroyed = []
        self._n = 0
        self._infos = infos or {}

    def create(self, **kwargs):
        self._n += 1
        sid = f"sess-{self._n}"
        self.created.append((sid, kwargs))
        self._infos[sid] = {
            "id": sid,
            "name": kwargs.get("name"),
            "status": "Running",
            "cpuCoresInUse": "1.0",
            "requestedCPUCores": "1",
        }
        return [sid]

    def info(self, ids, **kwargs):
        out = []
        for i in ids:
            if i in self.destroyed:
                continue
            item = dict(self._infos.get(i) or {"id": i, "status": "Running"})
            item["id"] = i
            out.append(item)
        return out

    def destroy(self, ids, **kwargs):
        self.destroyed.extend(ids)
        return ids

    def set_cpu(self, sid: str, in_use: str, requested: str = "1"):
        self._infos.setdefault(sid, {"id": sid, "status": "Running"})
        self._infos[sid]["cpuCoresInUse"] = in_use
        self._infos[sid]["requestedCPUCores"] = requested


def test_cpu_utilization_ratio():
    assert cpu_utilization({"cpuCoresInUse": "0.005", "requestedCPUCores": "1"}) == 0.005
    assert cpu_utilization({"cpuCoresInUse": "0", "requestedCPUCores": "8"}) == 0.0
    assert cpu_utilization({"requestedCPUCores": "0"}) is None


def test_reclaim_stalled_skips_vanished_processing(tmp_path: Path):
    site = CanfarSiteConfig(n_workers=0, stall_polls=1, stall_cpu_fraction=0.01)
    layout = QueueLayout(tmp_path)
    layout.ensure()
    job = JobPayload(name="gone", label="t", command="true")
    layout.write_payload(job)
    layout.enqueue("gone")
    claimed = layout.try_claim()
    assert claimed is not None
    path, _ = claimed
    path.unlink()  # worker finished between list and read
    pool = WorkerPool(tmp_path, site, FakeSession(), tmp_path / "run_worker.sh")
    assert pool.reclaim_stalled_jobs(layout) == []


def test_stall_requires_grace_then_n_polls(tmp_path: Path):
    site = CanfarSiteConfig(
        n_workers=1,
        stall_cpu_fraction=0.01,
        stall_polls=5,
        replace_workers=False,
        poll_interval=1,
    )
    CanfarWorkflow.from_payloads(
        "demo",
        tmp_path,
        [JobPayload(name="hung", label="t", command="sleep 999")],
        site=site,
    )
    layout = QueueLayout(tmp_path)
    layout.enqueue("hung")
    claimed = layout.try_claim()
    assert claimed is not None

    session = FakeSession()
    pool = WorkerPool(tmp_path, site, session, tmp_path / "run_worker.sh")
    created = pool.launch()
    sid = created[0]
    worker_name = pool.worker_name_by_session[sid]
    layout.write_claim_owner("hung", worker_id=worker_name, session_id=sid, claimed_at=0)
    layout.write_worker_heartbeat(
        worker_name, current_job="hung", claimed_at=0, session_id=sid
    )
    session.set_cpu(sid, "0.0", "1")

    # First poll after claim: grace, no reclaim.
    assert pool.reclaim_stalled_jobs(layout) == []
    assert layout.job_location("hung") == "processing"
    assert sid not in session.destroyed

    # Next 4 low-CPU polls: still under stallPolls=5.
    for _ in range(4):
        assert pool.reclaim_stalled_jobs(layout) == []
    assert layout.job_location("hung") == "processing"

    # 5th counting poll: reclaim + destroy.
    assert pool.reclaim_stalled_jobs(layout) == ["hung"]
    assert layout.job_location("hung") == "queue"
    assert sid in session.destroyed
    assert sid not in pool.session_ids


def test_high_cpu_resets_stall_counter(tmp_path: Path):
    site = CanfarSiteConfig(n_workers=1, stall_cpu_fraction=0.01, stall_polls=3, replace_workers=False)
    CanfarWorkflow.from_payloads(
        "demo",
        tmp_path,
        [JobPayload(name="busy", label="t", command="true")],
        site=site,
    )
    layout = QueueLayout(tmp_path)
    layout.enqueue("busy")
    layout.try_claim()
    session = FakeSession()
    pool = WorkerPool(tmp_path, site, session, tmp_path / "run_worker.sh")
    sid = pool.launch()[0]
    worker_name = pool.worker_name_by_session[sid]
    layout.write_claim_owner("busy", worker_id=worker_name, session_id=sid)
    layout.write_worker_heartbeat(worker_name, current_job="busy", session_id=sid)

    session.set_cpu(sid, "0.0")
    pool.reclaim_stalled_jobs(layout)  # grace
    pool.reclaim_stalled_jobs(layout)  # low #1
    pool.reclaim_stalled_jobs(layout)  # low #2
    session.set_cpu(sid, "0.5")
    pool.reclaim_stalled_jobs(layout)  # reset
    session.set_cpu(sid, "0.0")
    pool.reclaim_stalled_jobs(layout)  # low #1 again
    assert pool.reclaim_stalled_jobs(layout) == []  # low #2, not yet 3
    assert layout.job_location("busy") == "processing"
    assert sid not in session.destroyed
