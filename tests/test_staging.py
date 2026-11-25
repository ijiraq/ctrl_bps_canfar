"""File staging and the worker's per-job working directory.

The non-shared access mode is exercised end to end against a ``file://``
distribution endpoint, so no network or shared mount is involved.
"""

from __future__ import annotations

import os
import stat
import subprocess
import sys
from pathlib import Path

import pytest
from helpers import FakeSession, FakeWorkflow, gwfile, gwjob

from lsst.ctrl.bps.canfar.config import CanfarSiteConfig, load_site_config
from lsst.ctrl.bps.canfar.pool import WorkerPool
from lsst.ctrl.bps.canfar.queue import ACCESS_REMOTE, JobPayload, QueueLayout
from lsst.ctrl.bps.canfar.service import CanfarService
from lsst.ctrl.bps.canfar.worker import FETCH_FAILED, job_environment, run_job, run_worker
from lsst.ctrl.bps.canfar.workflow import CanfarWorkflow

pytest.importorskip("lsst.resources")

from lsst.ctrl.bps.canfar.staging import distribute, fetch  # noqa: E402

REPO = Path(__file__).resolve().parents[1]


def make_service(config) -> CanfarService:
    try:
        return CanfarService(config)  # with ctrl_bps installed
    except TypeError:
        return CanfarService()  # plugin imported without ctrl_bps


def queue_single_job(tmp_path: Path, job: JobPayload) -> QueueLayout:
    site = CanfarSiteConfig(n_workers=0, replace_workers=False)
    CanfarWorkflow.from_payloads("demo", tmp_path, [job], site=site)
    layout = QueueLayout(tmp_path)
    layout.enqueue(job.name)
    return layout


# --- staging ----------------------------------------------------------------


def test_distribute_then_fetch_round_trip(tmp_path: Path):
    src = tmp_path / "submit" / "run.qg"
    src.parent.mkdir()
    src.write_text("quantum graph")
    endpoint = f"file://{tmp_path}/dist/"

    published = distribute([str(src)], endpoint)

    assert (tmp_path / "dist" / "run.qg").read_text() == "quantum graph"
    assert published == [f"{endpoint}run.qg"]
    work = tmp_path / "work"
    work.mkdir()
    fetched = fetch([{"source": published[0], "dest": "run.qg", "executable": False}], work)
    assert fetched == [work / "run.qg"]
    assert (work / "run.qg").read_text() == "quantum graph"
    assert not os.access(work / "run.qg", os.X_OK)


def test_distribute_overwrites_stale_copy(tmp_path: Path):
    src = tmp_path / "run.qg"
    endpoint = f"file://{tmp_path}/dist/"
    src.write_text("first")
    distribute([str(src)], endpoint)
    src.write_text("second")
    distribute([str(src)], endpoint)
    assert (tmp_path / "dist" / "run.qg").read_text() == "second"


def test_distribute_expands_environment_in_endpoint(tmp_path: Path, monkeypatch):
    src = tmp_path / "run.qg"
    src.write_text("qg")
    monkeypatch.setenv("BPS_TEST_DIST", str(tmp_path / "dist"))
    distribute([str(src)], "file://${BPS_TEST_DIST}/")
    assert (tmp_path / "dist" / "run.qg").is_file()


def test_fetch_marks_executable(tmp_path: Path):
    src = tmp_path / "final_job.bash"
    src.write_text("#!/bin/bash\n")
    src.chmod(0o644)
    work = tmp_path / "work"
    work.mkdir()
    fetch([{"source": str(src), "dest": "final_job.bash", "executable": True}], work)
    assert (work / "final_job.bash").stat().st_mode & stat.S_IXUSR


def test_fetch_refuses_to_write_outside_work_dir(tmp_path: Path):
    src = tmp_path / "x"
    src.write_text("x")
    with pytest.raises(ValueError, match="plain file name"):
        fetch([{"source": str(src), "dest": "../escape", "executable": False}], tmp_path / "work")


def test_distribute_rejects_directories(tmp_path: Path):
    (tmp_path / "butler").mkdir()
    with pytest.raises(NotImplementedError, match="directory"):
        distribute([str(tmp_path / "butler")], f"file://{tmp_path}/dist/")


def test_distribute_without_files_needs_no_endpoint():
    assert distribute([], "") == []


def test_distribute_missing_source_fails(tmp_path: Path):
    with pytest.raises(FileNotFoundError):
        distribute([str(tmp_path / "absent.qg")], f"file://{tmp_path}/dist/")


# --- worker -----------------------------------------------------------------


def test_job_runs_in_private_scratch_dir_that_is_removed(tmp_path: Path, monkeypatch):
    scratch = tmp_path / "scratch"
    monkeypatch.setenv("BPS_CANFAR_SCRATCH", str(scratch))
    layout = queue_single_job(tmp_path / "submit", JobPayload(name="where", label="t", command="pwd"))

    assert run_worker(layout.root, idle_timeout=1, poll_interval=1, worker_id="w1") == 0

    assert layout.job_location("where") == "processed"
    cwd = Path(layout.log_path_for("where").read_text().splitlines()[-1])
    assert cwd.parent.resolve() == scratch.resolve()
    assert cwd.name.startswith("bps-canfar-where-")
    assert list(scratch.iterdir()) == []


def test_attempt_counts_earlier_starts(tmp_path: Path):
    job = JobPayload(name="retry", label="t", command="echo attempt=${BPS_CANFAR_ATTEMPT}; exit 3")
    layout = queue_single_job(tmp_path, job)

    run_worker(layout.root, idle_timeout=1, poll_interval=1, worker_id="w1")
    assert layout.job_location("retry") == "failed"
    assert "attempt=0" in layout.log_path_for("retry").read_text()
    assert layout.read_payload(layout.failed / "retry.json").attempt == 1

    assert layout.requeue_for_restart() == ["retry"]
    layout.enqueue("retry")
    run_worker(layout.root, idle_timeout=1, poll_interval=1, worker_id="w1")
    assert "attempt=1" in layout.log_path_for("retry").read_text()
    assert layout.read_payload(layout.failed / "retry.json").attempt == 2


def test_fetch_failure_fails_job_and_is_logged(tmp_path: Path):
    job = JobPayload(
        name="needs-file",
        label="t",
        command="echo should-not-run",
        fetch=[{"source": str(tmp_path / "absent.qg"), "dest": "absent.qg", "executable": False}],
    )
    layout = queue_single_job(tmp_path / "submit", job)

    run_worker(layout.root, idle_timeout=1, poll_interval=1, worker_id="w1")

    assert layout.job_location("needs-file") == "failed"
    log = layout.log_path_for("needs-file").read_text()
    assert "could not stage job files" in log
    assert "absent.qg" in log
    assert "should-not-run" not in log.replace("# command=echo should-not-run", "")


def test_run_job_reports_fetch_failure_status(tmp_path: Path):
    job = JobPayload(
        name="j",
        label="t",
        command="true",
        fetch=[{"source": str(tmp_path / "absent"), "dest": "absent", "executable": False}],
    )
    assert run_job(job, tmp_path / "j.log", worker_id="w") == FETCH_FAILED


def test_job_environment_expands_worker_variables():
    job = JobPayload(
        name="j",
        label="isr",
        command="true",
        environment={"PATH": "/extra:${PATH}", "TRY": "${BPS_CANFAR_ATTEMPT}", "GONE": "x${UNSET_VAR}y"},
        attempt=3,
    )
    env = job_environment(job, base={"PATH": "/usr/bin", "HOME": "/home/u"})
    assert env["PATH"] == "/extra:/usr/bin"
    assert env["TRY"] == "2"
    assert env["GONE"] == "xy"
    assert env["HOME"] == "/home/u"
    assert env["BPS_CANFAR_JOB_NAME"] == "j"
    assert env["BPS_CANFAR_JOB_LABEL"] == "isr"


# --- non-shared mode, end to end -------------------------------------------


def test_non_shared_run_end_to_end(tmp_path: Path, monkeypatch):
    """Prepare, submit and run with workers that only see the endpoint."""
    submit = tmp_path / "submit"
    stage = tmp_path / "prepared"  # stands in for submit-side storage
    dist = tmp_path / "dist"
    scratch = tmp_path / "scratch"
    stage.mkdir()
    monkeypatch.setenv("BPS_CANFAR_SCRATCH", str(scratch))
    endpoint = f"file://{dist}/"

    qgraph = stage / "run.qg"
    qgraph.write_text("QGRAPH-CONTENT")
    # Like ctrl_bps's final_job.bash: the endpoint is baked into the script
    # and the QuantumGraph name arrives as $1.
    script = stage / "final_job.bash"
    script.write_text(
        "#!/bin/bash\nset -e\n"
        'echo "self=$0"\n'
        'echo "cwd=$(pwd)"\n'
        f'echo "final read: $(cat "{dist}/$1") butler=$2"\n'
    )
    script.chmod(0o644)

    reader = (
        "from lsst.resources import ResourcePath; "
        "uri = '{fileDistributionEndPoint}<FILE:runQgraphFile>'; "
        "print('direct read:', ResourcePath(uri).read().decode())"
    )
    workflow = FakeWorkflow("demo")
    workflow.add(
        gwjob(
            "reader",
            sys.executable,
            f'-c "{reader}"',
            cmdvals={"fileDistributionEndPoint": endpoint},
        ),
        inputs=[gwfile("runQgraphFile", str(qgraph))],
    )
    workflow.add(
        gwjob(
            "finalJob",
            str(script),
            "<FILE:runQgraphFile> {butlerConfig}",
            cmdvals={"butlerConfig": "https://butler.example/repo"},
            transfer_executable=True,
        ),
        inputs=[gwfile("runQgraphFile", str(qgraph))],
    )
    config = {
        "bpsUseShared": False,
        "fileDistributionEndPoint": endpoint,
        "computeSite": "canfar",
        "site": {"canfar": {"nWorkers": 0, "pollInterval": 1}},
    }

    prepared = CanfarWorkflow.from_generic_workflow(config, workflow, str(submit), "svc")
    assert prepared.access_mode == ACCESS_REMOTE
    assert not dist.exists()  # nothing is published before submit

    make_service(config).submit(prepared, session=FakeSession(), once=True)
    assert sorted(p.name for p in dist.iterdir()) == ["final_job.bash", "run.qg"]

    # Workers must not need the submit-side copies from here on.
    qgraph.unlink()
    script.unlink()
    assert run_worker(submit, idle_timeout=1, poll_interval=1, worker_id="w1") == 0

    layout = QueueLayout(submit)
    assert layout.job_location("reader") == "processed", layout.log_path_for("reader").read_text()
    assert layout.job_location("finalJob") == "processed", layout.log_path_for("finalJob").read_text()
    assert "direct read: QGRAPH-CONTENT" in layout.log_path_for("reader").read_text()
    final_log = layout.log_path_for("finalJob").read_text()
    assert "self=./final_job.bash" in final_log
    assert "final read: QGRAPH-CONTENT butler=https://butler.example/repo" in final_log
    cwd = Path(next(line for line in final_log.splitlines() if line.startswith("cwd=")).removeprefix("cwd="))
    assert cwd.parent.resolve() == scratch.resolve()
    assert list(scratch.iterdir()) == []


def test_shared_submit_publishes_nothing(tmp_path: Path):
    workflow = FakeWorkflow("demo")
    qgraph = tmp_path / "run.qg"
    qgraph.write_text("QG")
    workflow.add(
        gwjob("show", "/bin/cat", "<FILE:runQgraphFile>"), inputs=[gwfile("runQgraphFile", str(qgraph))]
    )
    config = {"site": {"canfar": {"nWorkers": 0, "pollInterval": 1}}, "computeSite": "canfar"}
    submit = tmp_path / "submit"
    prepared = CanfarWorkflow.from_generic_workflow(config, workflow, str(submit), "svc")

    make_service(config).submit(prepared, session=FakeSession(), once=True)
    run_worker(submit, idle_timeout=1, poll_interval=1, worker_id="w1")

    layout = QueueLayout(submit)
    assert layout.job_location("show") == "processed"
    assert layout.log_path_for("show").read_text().splitlines()[-1] == "QG"


# --- queue access and worker environment script ----------------------------


def test_queue_layout_owns_restart_cancel_and_session_files(tmp_path: Path):
    layout = QueueLayout(tmp_path)
    layout.ensure()
    for name in ("done", "bad", "ready", "claimed"):
        layout.write_payload(JobPayload(name=name, label="t", command="true"))
    (layout.payloads / "done.json").rename(layout.processed / "done.json")
    (layout.payloads / "bad.json").rename(layout.failed / "bad.json")
    (layout.payloads / "claimed.json").rename(layout.processing / "claimed.json")
    layout.enqueue("ready")

    assert layout.park_queued() == ["ready"]
    assert layout.job_location("ready") == "payloads"

    layout.enqueue("ready")
    assert sorted(layout.requeue_for_restart()) == ["bad", "claimed", "ready"]
    assert {n: layout.job_location(n) for n in ("done", "bad", "ready", "claimed")} == {
        "done": "processed",
        "bad": "payloads",
        "ready": "payloads",
        "claimed": "payloads",
    }

    assert layout.read_worker_session("w-1") is None
    layout.write_worker_session("w-1", "sess-9")
    assert layout.read_worker_session("w-1") == "sess-9"


def test_env_script_setting_reaches_worker_sessions(tmp_path: Path):
    site = load_site_config(
        {"computeSite": "canfar", "site": {"canfar": {"nWorkers": 1, "envScript": "/proj/env.sh"}}}
    )
    assert site.env_script == "/proj/env.sh"
    assert CanfarSiteConfig.from_mapping(site.to_mapping()) == site
    CanfarWorkflow.from_payloads(
        "demo", tmp_path, [JobPayload(name="j", label="t", command="true")], site=site
    )
    # Restart reads the site settings back from run.yaml.
    assert load_site_config(QueueLayout(tmp_path).read_run()["siteConfig"]).env_script == "/proj/env.sh"

    session = FakeSession()
    WorkerPool(tmp_path, site, session, tmp_path / "run_worker.sh").launch()
    assert session.created[0][1]["env"]["BPS_CANFAR_ENV_SH"] == "/proj/env.sh"


def test_no_env_script_by_default(tmp_path: Path):
    site = load_site_config({"computeSite": "canfar", "site": {"canfar": {"nWorkers": 1, "envScript": ""}}})
    assert site.env_script is None
    session = FakeSession()
    WorkerPool(tmp_path, site, session, tmp_path / "run_worker.sh").launch()
    assert "BPS_CANFAR_ENV_SH" not in session.created[0][1]["env"]


WORKER_SCRIPTS = ["scripts/run_worker.sh", "python/lsst/ctrl/bps/canfar/scripts/run_worker.sh"]
SHOW_MARKER = JobPayload(name="j", label="t", command="echo marker=${MARKER:-unset}")


@pytest.mark.parametrize("copy", WORKER_SCRIPTS)
def test_run_worker_script_sources_configured_env(tmp_path: Path, copy):
    layout = queue_single_job(tmp_path / "submit", SHOW_MARKER)
    env_script = tmp_path / "env.sh"
    env_script.write_text("export MARKER=from-env-script\n")
    env = {**os.environ, "PYTHONPATH": str(REPO / "python"), "BPS_CANFAR_ENV_SH": str(env_script)}

    done = subprocess.run(
        ["bash", str(REPO / copy), str(layout.root), "1", "1", "w1"], env=env, capture_output=True, text=True
    )

    assert done.returncode == 0, done.stderr
    assert "marker=from-env-script" in layout.log_path_for("j").read_text()


def test_run_worker_script_warns_when_env_script_missing(tmp_path: Path):
    layout = queue_single_job(tmp_path / "submit", SHOW_MARKER)
    env = {**os.environ, "PYTHONPATH": str(REPO / "python"), "BPS_CANFAR_ENV_SH": str(tmp_path / "absent.sh")}
    env.pop("MARKER", None)

    done = subprocess.run(
        ["bash", str(REPO / "scripts" / "run_worker.sh"), str(layout.root), "1", "1", "w1"],
        env=env,
        capture_output=True,
        text=True,
    )

    assert "absent.sh not found" in done.stderr
    assert "marker=unset" in layout.log_path_for("j").read_text()
