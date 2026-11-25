"""Stand-ins for the parts of ``lsst.ctrl.bps`` the plugin touches."""

from __future__ import annotations

from types import SimpleNamespace
from typing import Any


def gwfile(
    name: str,
    src_uri: str,
    *,
    wms_transfer: bool = True,
    job_access_remote: bool = True,
    job_shared: bool = True,
) -> SimpleNamespace:
    """A ``GenericWorkflowFile``; defaults match BPS QuantumGraph files."""
    return SimpleNamespace(
        name=name,
        src_uri=src_uri,
        wms_transfer=wms_transfer,
        job_access_remote=job_access_remote,
        job_shared=job_shared,
    )


def gwjob(
    name: str,
    executable: str,
    arguments: str = "",
    *,
    cmdvals: dict[str, Any] | None = None,
    transfer_executable: bool = False,
    environment: dict[str, Any] | None = None,
    label: str | None = None,
) -> SimpleNamespace:
    """A ``GenericWorkflowJob``."""
    return SimpleNamespace(
        name=name,
        label=label or name,
        executable=SimpleNamespace(
            name=name, src_uri=executable, transfer_executable=transfer_executable
        ),
        arguments=arguments,
        cmdvals=dict(cmdvals or {}),
        pre_cmdline=None,
        post_cmdline=None,
        environment=dict(environment or {}),
        request_cpus=1,
        request_memory=2048,
    )


class FakeWorkflow:
    """Minimal ``GenericWorkflow``: jobs, their files and their parents."""

    def __init__(self, name: str = "run"):
        self.name = name
        self._jobs: dict[str, Any] = {}
        self._inputs: dict[str, list[Any]] = {}
        self._outputs: dict[str, list[Any]] = {}
        self._parents: dict[str, list[str]] = {}

    def add(self, job: Any, *, inputs=(), outputs=(), parents=()) -> Any:
        self._jobs[job.name] = job
        self._inputs[job.name] = list(inputs)
        self._outputs[job.name] = list(outputs)
        self._parents[job.name] = list(parents)
        return job

    def __iter__(self):
        return iter(self._jobs)

    def get_job(self, job_name: str) -> Any:
        return self._jobs[job_name]

    def predecessors(self, job_name: str):
        return iter(self._parents[job_name])

    def _files(self, files: list[Any], data: bool, transfer_only: bool) -> list[Any]:
        chosen = [f for f in files if not transfer_only or f.wms_transfer]
        return chosen if data else [f.name for f in chosen]

    def get_job_inputs(self, job_name: str, data: bool = True, transfer_only: bool = False):
        return self._files(self._inputs.get(job_name, []), data, transfer_only)

    def get_job_outputs(self, job_name: str, data: bool = True, transfer_only: bool = False):
        return self._files(self._outputs.get(job_name, []), data, transfer_only)


class FakeSession:
    """Skaha session client that records calls and creates nothing."""

    def __init__(self):
        self.created = []
        self.destroyed = []

    def create(self, **kwargs):
        sid = f"sess-{len(self.created) + 1}"
        self.created.append((sid, kwargs))
        return [sid]

    def info(self, ids, **kwargs):
        return [{"id": i, "status": "Running"} for i in ids if i not in self.destroyed]

    def destroy(self, ids, **kwargs):
        self.destroyed.extend(ids)
        return ids
