from __future__ import annotations

from types import SimpleNamespace

from lsst.ctrl.bps.canfar.config import CanfarSiteConfig, load_site_config, sanitize_name
from lsst.ctrl.bps.canfar.job import command_for_generic_job, payload_from_generic_job
from lsst.ctrl.bps.canfar.status import WmsStates, overall_run_state, summarize_job_states


def test_sanitize_name():
    assert sanitize_name("Foo_Bar.1") == "foo-bar-1"
    assert sanitize_name("@@@") == "job"


def test_load_site_config_overrides():
    cfg = load_site_config({"computeSite": "canfar", "site": {"canfar": {"nWorkers": 3, "image": "x:y"}}})
    assert cfg.n_workers == 3
    assert cfg.image == "x:y"


def test_load_site_config_http_timeout():
    cfg = load_site_config(
        {
            "computeSite": "canfar",
            "site": {"canfar": {"httpTimeout": 180, "httpRetries": 5}},
        }
    )
    assert cfg.http_timeout == 180
    assert cfg.http_retries == 5
    defaults = load_site_config({"computeSite": "canfar"})
    assert defaults.http_timeout == 120
    assert defaults.http_retries == 3


def test_load_site_config_blank_gpu():
    cfg = load_site_config(
        {"computeSite": "canfar", "site": {"canfar": {"gpu": ""}}}
    )
    assert cfg.gpu is None
    cfg2 = CanfarSiteConfig.from_mapping({"gpu": "  ", "cores": "", "ram": None})
    assert cfg2.gpu is None
    assert cfg2.cores is None
    assert cfg2.ram is None


def test_command_expansion():
    job = SimpleNamespace(
        name="j",
        label="isr",
        executable=SimpleNamespace(src_uri="/bin/pipetask"),
        arguments="run -b {butler} <ENV:HOME>/x",
        cmdvals={"butler": "/arc/b"},
        pre_cmdline=None,
        post_cmdline=None,
        environment={"OMP_NUM_THREADS": "2"},
        request_cpus=1,
        request_memory=4096,
    )
    cmd = command_for_generic_job(job)
    assert cmd == "/bin/pipetask run -b /arc/b ${HOME}/x"
    payload = payload_from_generic_job(job, parents=["p"])
    assert payload.parents == ["p"]
    assert payload.environment["OMP_NUM_THREADS"] == "2"


def test_overall_state():
    counts = summarize_job_states({"a": "processed", "b": "failed", "c": "queue"})
    assert counts[WmsStates.SUCCEEDED] == 1
    assert counts[WmsStates.FAILED] == 1
    assert counts[WmsStates.PENDING] == 1
    assert overall_run_state(counts) == WmsStates.RUNNING
    counts2 = summarize_job_states({"a": "processed", "b": "processed"})
    assert overall_run_state(counts2) == WmsStates.SUCCEEDED
