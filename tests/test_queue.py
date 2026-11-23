from __future__ import annotations

import time
from pathlib import Path

from lsst.ctrl.bps.canfar.queue import JobPayload, QueueLayout


def test_claim_process_success(tmp_path: Path):
    layout = QueueLayout(tmp_path)
    layout.ensure()
    job = JobPayload(name="job-a", label="isr", command="echo hi")
    layout.write_payload(job)
    layout.write_dag([job])
    layout.enqueue("job-a")
    claimed = layout.try_claim()
    assert claimed is not None
    path, payload = claimed
    assert payload.name == "job-a"
    assert path.parent.name == "processing"
    assert layout.try_claim() is None
    layout.mark_processed(path)
    assert layout.job_location("job-a") == "processed"


def test_two_workers_cannot_claim_same_job(tmp_path: Path):
    layout = QueueLayout(tmp_path)
    layout.ensure()
    job = JobPayload(name="only", label="x", command="true")
    layout.write_payload(job)
    layout.enqueue("only")
    first = layout.try_claim()
    second = layout.try_claim()
    assert first is not None
    assert second is None


def test_reclaim_orphan_processing(tmp_path: Path):
    layout = QueueLayout(tmp_path)
    layout.ensure()
    job = JobPayload(name="stuck", label="x", command="true")
    layout.write_payload(job)
    layout.enqueue("stuck")
    claimed = layout.try_claim()
    assert claimed is not None
    path, _ = claimed
    old = time.time() - 999
    # Bump mtime into the past; no worker heartbeat claims this job.
    import os

    os.utime(path, (old, old))
    reclaimed = layout.reclaim_orphan_processing(grace_seconds=10)
    assert reclaimed == ["stuck"]
    assert layout.job_location("stuck") == "queue"


def test_heartbeat_maps_current_job(tmp_path: Path):
    layout = QueueLayout(tmp_path)
    layout.ensure()
    layout.write_worker_heartbeat(
        "w1",
        current_job="job-a",
        claimed_at=time.time(),
        session_id="sess-1",
    )
    owner = layout.owner_for_processing_job("job-a")
    assert owner is not None
    assert owner["workerId"] == "w1"
    assert owner["sessionId"] == "sess-1"
