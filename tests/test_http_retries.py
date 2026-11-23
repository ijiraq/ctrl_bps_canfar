from __future__ import annotations

import pytest

from lsst.ctrl.bps.canfar.config import CanfarSiteConfig
from lsst.ctrl.bps.canfar.pool import (
    WorkerPool,
    call_with_retries,
    clamp_http_timeout,
    default_session,
)


class TransientTimeout(Exception):
    """Stand-in for httpx ReadTimeout / SessionRequestError text."""

    def __init__(self) -> None:
        super().__init__("create encountered 1 failure: 1: ReadTimeout")


def test_clamp_http_timeout():
    assert clamp_http_timeout(0) == 1
    assert clamp_http_timeout(120) == 120
    assert clamp_http_timeout(999) == 300


def test_call_with_retries_recovers(monkeypatch):
    monkeypatch.setattr("lsst.ctrl.bps.canfar.pool.time.sleep", lambda _s: None)
    calls = {"n": 0}

    def flaky():
        calls["n"] += 1
        if calls["n"] < 3:
            raise TransientTimeout()
        return "ok"

    assert call_with_retries("create", flaky, retries=3, base_delay=0.01) == "ok"
    assert calls["n"] == 3


def test_call_with_retries_exhausted(monkeypatch):
    monkeypatch.setattr("lsst.ctrl.bps.canfar.pool.time.sleep", lambda _s: None)

    def always_fail():
        raise TransientTimeout()

    with pytest.raises(TransientTimeout):
        call_with_retries("create", always_fail, retries=2, base_delay=0.01)


def test_launch_retries_create(tmp_path, monkeypatch):
    monkeypatch.setattr("lsst.ctrl.bps.canfar.pool.time.sleep", lambda _s: None)

    class FlakySession:
        def __init__(self) -> None:
            self.creates = 0

        def create(self, **kwargs):
            self.creates += 1
            if self.creates < 2:
                raise TransientTimeout()
            return [f"sid-{self.creates}"]

        def info(self, ids, **kwargs):
            return []

        def destroy(self, ids, **kwargs):
            return None

    script = tmp_path / "run_worker.sh"
    script.write_text("#!/bin/bash\n", encoding="utf-8")
    site = CanfarSiteConfig(n_workers=1, http_retries=3)
    session = FlakySession()
    pool = WorkerPool(tmp_path, site, session, script)
    ids = pool.launch()
    assert ids == ["sid-2"]
    assert session.creates == 2


def test_default_session_timeout():
    pytest.importorskip("canfar")
    session = default_session(timeout=180)
    assert session.timeout == 180
