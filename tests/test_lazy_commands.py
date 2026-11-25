"""Resolution of BPS lazy-command placeholders at prepare time."""

from __future__ import annotations

import json
from pathlib import Path

import pytest
from helpers import FakeWorkflow, gwfile, gwjob

from lsst.ctrl.bps.canfar.job import (
    command_for_generic_job,
    environment_for_generic_job,
    job_files,
    payload_from_generic_job,
    resolve_file_uri,
)
from lsst.ctrl.bps.canfar.queue import ACCESS_REMOTE, ACCESS_SHARED, JobPayload, QueueLayout
from lsst.ctrl.bps.canfar.workflow import CanfarWorkflow

QGRAPH = "/arc/projects/P/bps/run/run.qg"
ENDPOINT = "https://example.org/dist/run/"

# What ctrl_bps hands the plugin for pipetaskInit with useLazyCommands: true.
INIT_ARGUMENTS = (
    "{initPreCmdOpts} pre-exec-init-qbb {butlerConfig} "
    "{fileDistributionEndPoint}<FILE:runQgraphFile> {extraInitOptions}"
)


def init_cmdvals(endpoint: str = "", butler: str = "<ENV:BUTLER_DIR>") -> dict[str, str]:
    return {
        "initPreCmdOpts": "--long-log --log-level=VERBOSE",
        "butlerConfig": butler,
        "fileDistributionEndPoint": endpoint,
        "extraInitOptions": "",
    }


def init_workflow(endpoint: str = "", butler: str = "<ENV:BUTLER_DIR>") -> FakeWorkflow:
    workflow = FakeWorkflow()
    workflow.add(
        gwjob("pipetaskInit", "pipetask", INIT_ARGUMENTS, cmdvals=init_cmdvals(endpoint, butler)),
        inputs=[gwfile("runQgraphFile", QGRAPH)],
    )
    return workflow


# --- <FILE:...> rules -------------------------------------------------------


def test_file_shared_and_job_shared_uses_src_uri():
    assert resolve_file_uri(gwfile("q", QGRAPH), use_shared=True) == QGRAPH


def test_file_not_wms_transfer_uses_src_uri_in_both_modes():
    untouched = gwfile("q", QGRAPH, wms_transfer=False)
    assert resolve_file_uri(untouched, use_shared=True) == QGRAPH
    assert resolve_file_uri(untouched, use_shared=False) == QGRAPH


def test_file_non_shared_uses_basename():
    assert resolve_file_uri(gwfile("q", QGRAPH), use_shared=False) == "run.qg"


def test_file_shared_but_private_copy_uses_basename_and_is_fetched():
    workflow = FakeWorkflow()
    workflow.add(
        gwjob("custom", "/bin/cat", "<FILE:table>"),
        inputs=[gwfile("table", "/arc/in/table.csv", job_shared=False, job_access_remote=False)],
    )
    files = job_files(workflow, "custom", use_shared=True)
    assert files.paths == {"table": "table.csv"}
    assert files.fetch == [{"source": "/arc/in/table.csv", "dest": "table.csv", "executable": False}]
    assert files.distribute == []


# --- the bias failure -------------------------------------------------------


def test_bias_init_command_shared():
    workflow = init_workflow()
    files = job_files(workflow, "pipetaskInit", use_shared=True)
    command = command_for_generic_job(workflow.get_job("pipetaskInit"), file_paths=files.paths)
    assert command == (
        f"pipetask --long-log --log-level=VERBOSE pre-exec-init-qbb ${{BUTLER_DIR}} {QGRAPH}"
    )
    assert "<" not in command
    assert files.fetch == [] and files.distribute == []
    assert files.access_mode == ACCESS_SHARED


def test_bias_init_command_non_shared_reads_through_endpoint():
    workflow = init_workflow(endpoint=ENDPOINT)
    files = job_files(workflow, "pipetaskInit", use_shared=False, endpoint=ENDPOINT)
    command = command_for_generic_job(workflow.get_job("pipetaskInit"), file_paths=files.paths)
    assert command.endswith(f"pre-exec-init-qbb ${{BUTLER_DIR}} {ENDPOINT}run.qg")
    assert files.distribute == [QGRAPH]
    # job_access_remote: read in place at the endpoint, not copied to the job.
    assert files.fetch == []
    assert files.access_mode == ACCESS_REMOTE


def test_remote_butler_config_passes_through_unchanged():
    butler = "https://ws-uv.canfar.net/butler/repo/newhorizons"
    workflow = init_workflow(endpoint=ENDPOINT, butler=butler)
    files = job_files(workflow, "pipetaskInit", use_shared=False, endpoint=ENDPOINT)
    command = command_for_generic_job(workflow.get_job("pipetaskInit"), file_paths=files.paths)
    assert f" pre-exec-init-qbb {butler} {ENDPOINT}run.qg" in command


# --- other placeholders -----------------------------------------------------


def test_cmdvals_value_carrying_env_placeholder_becomes_shell_variable():
    cmdvals = {"butler": "<ENV:X>/repo"}
    job = gwjob("j", "<ENV:CTRL_MPEXEC_DIR>/bin/pipetask", "run -b {butler}", cmdvals=cmdvals)
    assert command_for_generic_job(job) == "${CTRL_MPEXEC_DIR}/bin/pipetask run -b ${X}/repo"


def test_wms_attempt_number_becomes_worker_variable():
    job = gwjob("j", "pipetask", "run --log-file log.<WMS:attemptNum>.json")
    assert command_for_generic_job(job) == "pipetask run --log-file log.${BPS_CANFAR_ATTEMPT}.json"


def test_environment_placeholders():
    job = gwjob("j", "true", environment={"PATH": "/extra/bin:<ENV:PATH>", "TRY": "<WMS:attemptNum>", "N": 2})
    assert environment_for_generic_job(job) == {
        "PATH": "/extra/bin:${PATH}",
        "TRY": "${BPS_CANFAR_ATTEMPT}",
        "N": "2",
    }


def test_staged_executable_runs_from_work_dir():
    workflow = FakeWorkflow()
    workflow.add(
        gwjob(
            "finalJob",
            "/arc/submit/final_job.bash",
            "<FILE:runQgraphFile> {butlerConfig}",
            cmdvals={"butlerConfig": "<ENV:BUTLER_DIR>"},
            transfer_executable=True,
        ),
        inputs=[gwfile("runQgraphFile", QGRAPH)],
    )
    remote = job_files(workflow, "finalJob", use_shared=False, endpoint=ENDPOINT)
    payload = payload_from_generic_job(workflow.get_job("finalJob"), parents=[], files=remote)
    assert payload.command == "./final_job.bash run.qg ${BUTLER_DIR}"
    assert payload.fetch == [
        {"source": f"{ENDPOINT}final_job.bash", "dest": "final_job.bash", "executable": True}
    ]
    assert remote.distribute == [QGRAPH, "/arc/submit/final_job.bash"]
    assert payload.access_mode == ACCESS_REMOTE

    shared = job_files(workflow, "finalJob", use_shared=True)
    payload = payload_from_generic_job(workflow.get_job("finalJob"), parents=[], files=shared)
    assert payload.command == f"/arc/submit/final_job.bash {QGRAPH} ${{BUTLER_DIR}}"
    assert payload.fetch == []


# --- failures surface at prepare -------------------------------------------


def test_unknown_file_placeholder_raises():
    job = gwjob("j", "pipetask", "run <FILE:missingFile>")
    with pytest.raises(ValueError, match="<FILE:missingFile>"):
        command_for_generic_job(job, file_paths={"runQgraphFile": QGRAPH})


def test_unknown_wms_placeholder_raises():
    with pytest.raises(ValueError, match="<WMS:clusterId>"):
        command_for_generic_job(gwjob("j", "pipetask", "run <WMS:clusterId>"))


def test_missing_cmdvals_key_raises():
    with pytest.raises(ValueError, match="butlerConfig"):
        command_for_generic_job(gwjob("j", "pipetask", "run -b {butlerConfig}"))


def test_malformed_placeholder_raises():
    with pytest.raises(ValueError, match="unresolved <FILE:>"):
        command_for_generic_job(gwjob("j", "pipetask", "run <FILE:>"))


def test_unresolvable_environment_value_raises():
    with pytest.raises(ValueError, match="environment variable X"):
        environment_for_generic_job(gwjob("j", "true", environment={"X": "<FILE:runQgraphFile>"}))


@pytest.mark.parametrize("use_shared", [True, False])
def test_output_needing_copy_back_raises(use_shared):
    workflow = FakeWorkflow()
    workflow.add(
        gwjob("custom", "/bin/prog", "<FILE:result>"),
        outputs=[gwfile("result", "/arc/out/result.txt", job_shared=False, job_access_remote=False)],
    )
    with pytest.raises(NotImplementedError, match="result"):
        job_files(workflow, "custom", use_shared=use_shared, endpoint=ENDPOINT)


def test_output_written_in_place_on_shared_filesystem():
    workflow = FakeWorkflow()
    workflow.add(
        gwjob("custom", "/bin/prog", "<FILE:result>"),
        outputs=[gwfile("result", "/arc/out/result.txt", wms_transfer=False)],
    )
    assert job_files(workflow, "custom", use_shared=True).paths == {"result": "/arc/out/result.txt"}


# --- CanfarWorkflow.from_generic_workflow ----------------------------------


def test_prepare_shared_writes_concrete_payload(tmp_path: Path):
    config = {"bpsUseShared": True, "computeSite": "canfar"}
    workflow = CanfarWorkflow.from_generic_workflow(config, init_workflow(), str(tmp_path), "svc")
    payload = json.loads((tmp_path / "payloads" / "pipetaskinit.json").read_text())
    assert payload["command"].endswith(f"${{BUTLER_DIR}} {QGRAPH}")
    assert payload["access_mode"] == ACCESS_SHARED
    assert payload["fetch"] == []
    run_doc = QueueLayout(tmp_path).read_run()
    assert run_doc["accessMode"] == ACCESS_SHARED
    assert run_doc["filesToDistribute"] == []
    assert workflow.files_to_distribute == []


def test_prepare_defaults_to_shared(tmp_path: Path):
    CanfarWorkflow.from_generic_workflow({}, init_workflow(), str(tmp_path), "svc")
    assert QueueLayout(tmp_path).read_run()["accessMode"] == ACCESS_SHARED


@pytest.mark.parametrize("value", [False, "false", "False"])
def test_prepare_non_shared_records_files_to_distribute(tmp_path: Path, value):
    config = {"bpsUseShared": value, "fileDistributionEndPoint": ENDPOINT}
    workflow = CanfarWorkflow.from_generic_workflow(
        config, init_workflow(endpoint=ENDPOINT), str(tmp_path), "svc"
    )
    run_doc = QueueLayout(tmp_path).read_run()
    assert run_doc["accessMode"] == ACCESS_REMOTE
    assert run_doc["fileDistributionEndPoint"] == ENDPOINT
    assert run_doc["filesToDistribute"] == [QGRAPH]
    assert workflow.files_to_distribute == [QGRAPH]
    assert workflow.access_mode == ACCESS_REMOTE


@pytest.mark.parametrize("endpoint", ["", "https://example.org/dist"])
def test_prepare_non_shared_needs_usable_endpoint(tmp_path: Path, endpoint):
    config = {"bpsUseShared": False, "fileDistributionEndPoint": endpoint}
    with pytest.raises(ValueError, match="fileDistributionEndPoint"):
        CanfarWorkflow.from_generic_workflow(config, init_workflow(), str(tmp_path), "svc")


def test_prepare_non_shared_rejects_basename_collision(tmp_path: Path):
    workflow = FakeWorkflow()
    workflow.add(gwjob("a", "true", "<FILE:qa>"), inputs=[gwfile("qa", "/arc/a/job.qg")])
    workflow.add(gwjob("b", "true", "<FILE:qb>"), inputs=[gwfile("qb", "/arc/b/job.qg")])
    config = {"bpsUseShared": False, "fileDistributionEndPoint": ENDPOINT}
    with pytest.raises(ValueError, match="both be published"):
        CanfarWorkflow.from_generic_workflow(config, workflow, str(tmp_path), "svc")


def test_prepare_fails_instead_of_writing_placeholder(tmp_path: Path):
    workflow = FakeWorkflow()
    # The job's files were never registered: the original bug's shape.
    workflow.add(gwjob("pipetaskInit", "pipetask", "pre-exec-init-qbb repo <FILE:runQgraphFile>"))
    with pytest.raises(ValueError, match="<FILE:runQgraphFile>"):
        CanfarWorkflow.from_generic_workflow({}, workflow, str(tmp_path), "svc")
    assert not (tmp_path / "payloads" / "pipetaskinit.json").exists()


def test_parents_and_skipped_nodes(tmp_path: Path):
    workflow = init_workflow()
    workflow.add(gwjob("noop", ""), parents=["pipetaskInit"])
    workflow.add(
        gwjob("isr_1", "pipetask", "run-qbb repo <FILE:runQgraphFile>"),
        inputs=[gwfile("runQgraphFile", QGRAPH)],
        parents=["pipetaskInit", "noop"],
    )
    prepared = CanfarWorkflow.from_generic_workflow({}, workflow, str(tmp_path), "svc")
    by_name = {p.name: p for p in prepared.payloads}
    assert set(by_name) == {"pipetaskInit", "isr_1"}
    assert by_name["isr_1"].parents == ["pipetaskInit"]


# --- payload format ---------------------------------------------------------


def test_payload_written_before_staging_fields_still_loads():
    old = {
        "name": "pipetaskInit",
        "label": "pipetaskInit",
        "command": "pipetask run",
        "parents": [],
        "environment": {},
        "request_cpus": 1,
        "request_memory_mb": 4096,
    }
    payload = JobPayload.from_dict(old)
    assert payload.fetch == []
    assert payload.access_mode == ACCESS_SHARED
    assert payload.attempt == 0


def test_payload_round_trip_keeps_staging_fields():
    payload = JobPayload(
        name="j",
        label="l",
        command="./x",
        fetch=[{"source": "s3://b/x", "dest": "x", "executable": True}],
        access_mode=ACCESS_REMOTE,
        attempt=3,
    )
    assert JobPayload.from_dict(json.loads(json.dumps(payload.to_dict()))) == payload
