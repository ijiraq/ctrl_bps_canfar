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


class ListingSession(FakeSession):
    """Client with ``fetch``: one list request covers every session of the user."""

    def __init__(self, infos=None, others=None):
        super().__init__(infos)
        self.fetch_calls = []
        self.info_calls = 0
        self.others = list(others or [])
        # Sessions Skaha has no record of: not listed, 404 when asked directly.
        self.gone = set()
        # Sessions left out of the list although they still exist.
        self.unlisted = set()
        self.info_error = None

    def fetch(self, kind=None, status=None):
        self.fetch_calls.append(kind)
        skip = set(self.destroyed) | self.gone | self.unlisted
        mine = [dict(v, id=k) for k, v in self._infos.items() if k not in skip]
        return mine + [dict(o) for o in self.others]

    def info(self, ids, **kwargs):
        self.info_calls += 1
        if self.info_error is not None:
            raise self.info_error
        missing = [i for i in ids if i in self.gone]
        if missing:
            raise FakeRequestError({i: FakeHTTPError(404) for i in missing})
        return super().info(ids, **kwargs)


class FakeHTTPError(Exception):
    def __init__(self, status_code):
        super().__init__(f"HTTP {status_code}")
        self.response = type("Response", (), {"status_code": status_code})()


class FakeRequestError(Exception):
    """Shaped like ``canfar.exceptions.session.SessionRequestError``."""

    def __init__(self, errors):
        super().__init__("info failed")
        self.errors = errors


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


def test_group_poll_uses_one_list_request(tmp_path: Path):
    site = CanfarSiteConfig(n_workers=3, replace_workers=False)
    session = ListingSession(
        others=[{"id": "other-run", "name": "someone-else", "status": "Running"}]
    )
    pool = WorkerPool(tmp_path, site, session, tmp_path / "run_worker.sh")
    created = pool.launch()
    assert len(created) == 3

    infos = pool._session_infos()

    assert session.fetch_calls == ["headless"]
    assert session.info_calls == 0
    # Sessions of other runs in the list are not adopted.
    assert sorted(infos) == sorted(created)
    assert "other-run" not in pool.worker_name_by_session


def test_group_poll_reaps_terminal_and_keeps_unlisted(tmp_path: Path):
    site = CanfarSiteConfig(n_workers=3, replace_workers=False)
    session = ListingSession()
    pool = WorkerPool(tmp_path, site, session, tmp_path / "run_worker.sh")
    done, unlisted, running = pool.launch()
    session._infos[done]["status"] = "Completed"
    session.unlisted.add(unlisted)

    alive = pool.alive_ids()

    assert session.info_calls == 0
    assert done not in alive
    # One poll without an entry is not proof the session ended.
    assert unlisted in alive and running in alive


def test_missing_worker_dropped_once_confirmed_gone(tmp_path: Path):
    site = CanfarSiteConfig(n_workers=2, replace_workers=False, missing_polls=3)
    session = ListingSession()
    pool = WorkerPool(tmp_path, site, session, tmp_path / "run_worker.sh")
    lost, running = pool.launch()
    session.gone.add(lost)

    assert pool.alive_ids() == [lost, running]
    assert pool.alive_ids() == [lost, running]
    # No direct request until the session has been absent for missingPolls polls.
    assert session.info_calls == 0
    assert pool.alive_ids() == [running]
    assert session.info_calls == 1
    assert lost not in pool.worker_name_by_session
    assert lost not in pool.missing_counts


def test_missing_worker_kept_when_still_running(tmp_path: Path):
    site = CanfarSiteConfig(n_workers=2, replace_workers=False, missing_polls=2)
    session = ListingSession()
    pool = WorkerPool(tmp_path, site, session, tmp_path / "run_worker.sh")
    created = pool.launch()
    session.unlisted.add(created[0])  # incomplete list; the session exists

    for _ in range(4):
        assert pool.alive_ids() == created
    # Checked directly on polls 2 and 4; the count restarts after each check.
    assert session.info_calls == 2


def test_missing_worker_kept_when_check_fails(tmp_path: Path):
    site = CanfarSiteConfig(n_workers=1, replace_workers=False, missing_polls=1)
    session = ListingSession()
    pool = WorkerPool(tmp_path, site, session, tmp_path / "run_worker.sh")
    created = pool.launch()
    session.gone.add(created[0])
    session.info_error = TimeoutError("skaha timed out")

    assert pool.alive_ids() == created
    # A 5xx answer is not a "no such session" answer either.
    session.info_error = FakeRequestError({created[0]: FakeHTTPError(503)})
    assert pool.alive_ids() == created

    session.info_error = None
    assert pool.alive_ids() == []


def test_missing_polls_zero_never_drops(tmp_path: Path):
    site = CanfarSiteConfig(n_workers=1, replace_workers=False, missing_polls=0)
    session = ListingSession()
    pool = WorkerPool(tmp_path, site, session, tmp_path / "run_worker.sh")
    created = pool.launch()
    session.gone.add(created[0])

    for _ in range(5):
        assert pool.alive_ids() == created
    assert session.info_calls == 0


def test_listed_again_resets_missing_count(tmp_path: Path):
    site = CanfarSiteConfig(n_workers=1, replace_workers=False, missing_polls=2)
    session = ListingSession()
    pool = WorkerPool(tmp_path, site, session, tmp_path / "run_worker.sh")
    created = pool.launch()

    session.unlisted.add(created[0])
    pool.alive_ids()
    session.unlisted.clear()
    pool.alive_ids()
    session.gone.add(created[0])
    # Absences must be consecutive: this is the first of a new streak.
    assert pool.alive_ids() == created
    assert pool.alive_ids() == []


def test_vanished_worker_is_replaced_under_its_own_name(tmp_path: Path):
    site = CanfarSiteConfig(n_workers=3, replace_workers=True, missing_polls=1)
    session = ListingSession()
    pool = WorkerPool(tmp_path, site, session, tmp_path / "run_worker.sh")
    first, lost, last = pool.launch()
    names = dict(pool.worker_name_by_session)
    session.gone.add(lost)

    created = pool.maintain(work_remaining=True)

    assert len(created) == 1
    assert pool.session_ids == [first, last, created[0]]
    # The replacement takes the lost worker's name, not a live worker's.
    assert pool.worker_name_by_session[created[0]] == names[lost]
    live = [pool.worker_name_by_session[sid] for sid in pool.session_ids]
    assert len(set(live)) == 3


def test_group_poll_failure_keeps_workers(tmp_path: Path):
    site = CanfarSiteConfig(n_workers=2, replace_workers=False)
    session = ListingSession()
    pool = WorkerPool(tmp_path, site, session, tmp_path / "run_worker.sh")
    created = pool.launch()

    def boom(kind=None, status=None):
        raise TimeoutError("skaha timed out")

    session.fetch = boom
    assert pool._session_infos() == {}
    assert pool.alive_ids() == created


def test_group_poll_still_reclaims_stalled_job(tmp_path: Path):
    site = CanfarSiteConfig(
        n_workers=1, stall_cpu_fraction=0.01, stall_polls=2, replace_workers=False
    )
    CanfarWorkflow.from_payloads(
        "demo",
        tmp_path,
        [JobPayload(name="hung", label="t", command="sleep 999")],
        site=site,
    )
    layout = QueueLayout(tmp_path)
    layout.enqueue("hung")
    assert layout.try_claim() is not None

    session = ListingSession()
    pool = WorkerPool(tmp_path, site, session, tmp_path / "run_worker.sh")
    sid = pool.launch()[0]
    worker_name = pool.worker_name_by_session[sid]
    layout.write_claim_owner("hung", worker_id=worker_name, session_id=sid, claimed_at=0)
    layout.write_worker_heartbeat(worker_name, current_job="hung", claimed_at=0, session_id=sid)
    session.set_cpu(sid, "0.0", "1")

    assert pool.reclaim_stalled_jobs(layout) == []  # grace
    assert pool.reclaim_stalled_jobs(layout) == []  # low #1
    assert pool.reclaim_stalled_jobs(layout) == ["hung"]  # low #2
    assert layout.job_location("hung") == "queue"
    assert sid in session.destroyed
    assert session.info_calls == 0
    assert len(session.fetch_calls) == 3
