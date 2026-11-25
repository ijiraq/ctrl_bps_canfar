"""Build executable job payloads from BPS GenericWorkflow jobs.

With ``useLazyCommands: true`` (the BPS default) the command line reaches the
WMS plugin with placeholders still in it.  They are resolved here, at
prepare time, in the order ``ctrl_bps_htcondor`` uses:

``{key}``
    Filled from ``GenericWorkflowJob.cmdvals``.  Runs first because the
    values can themselves carry ``<ENV:...>``.
``<WMS:attemptNum>``
    Becomes ``${BPS_CANFAR_ATTEMPT}``, which the worker exports.
``<FILE:name>``
    Becomes a path chosen by the BPS file-access rules, see
    `resolve_file_uri`.
``<ENV:VAR>``
    Becomes ``${VAR}`` so it is evaluated by the worker's shell, never on
    the submit host.

Anything that cannot be resolved raises, so a bad command fails ``bps
prepare`` instead of reaching ``bash`` on a worker.
"""

from __future__ import annotations

import logging
import os
import re
from dataclasses import dataclass, field
from typing import Any

from .queue import ACCESS_REMOTE, ACCESS_SHARED, JobPayload

_LOG = logging.getLogger(__name__)
_ENV_RE = re.compile(r"<ENV:([^>]+)>")
_FILE_RE = re.compile(r"<FILE:([^>]+)>")
_WMS_RE = re.compile(r"<WMS:([^>]+)>")
_LEFTOVER_RE = re.compile(r"<(?:ENV|FILE|WMS):[^>]*>")

#: Environment variable the worker sets to the number of earlier starts of
#: the job (0 on the first run, like HTCondor's ``NumJobStarts``).
ATTEMPT_ENV_VAR = "BPS_CANFAR_ATTEMPT"

_WMS_VALUES = {"attemptNum": f"${{{ATTEMPT_ENV_VAR}}}"}


@dataclass
class JobFiles:
    """How one job reaches the files BPS registered for it."""

    paths: dict[str, str] = field(default_factory=dict)
    """Logical file name -> path to put on the command line."""

    executable: str | None = None
    """Command-line form of the executable when it is staged into the job's
    working directory; `None` means use ``executable.src_uri`` as given."""

    distribute: list[str] = field(default_factory=list)
    """Submit-side files to copy to ``fileDistributionEndPoint`` at submit."""

    fetch: list[dict[str, Any]] = field(default_factory=list)
    """Files the worker copies into the job's working directory."""

    access_mode: str = ACCESS_SHARED


def _basename(uri: str) -> str:
    return os.path.basename(uri.rstrip("/"))


def _used_in_place(gwfile: Any, *, use_shared: bool) -> bool:
    """Whether the job reads ``gwfile`` at its submit-side location."""
    if not getattr(gwfile, "wms_transfer", False):
        # BPS: the WMS is not responsible for this file; trust the URI.
        return True
    return bool(use_shared and getattr(gwfile, "job_shared", False))


def resolve_file_uri(gwfile: Any, *, use_shared: bool) -> str:
    """Return the command-line path for a ``GenericWorkflowFile``.

    Same rules as ``lsst.ctrl.bps.transform._fill_arguments`` and the
    HTCondor plugin:

    * ``wms_transfer`` false: ``src_uri`` as given;
    * shared filesystem and ``job_shared``: ``src_uri``;
    * otherwise the basename of ``src_uri``; the plugin has to make that
      name resolvable for the job.
    """
    src = getattr(gwfile, "src_uri", None)
    if not src:
        raise ValueError(f"file {getattr(gwfile, 'name', '?')} has no src_uri")
    src = str(src)
    if _used_in_place(gwfile, use_shared=use_shared):
        return src
    return _basename(src)


def job_files(generic_workflow: Any, job_name: str, *, use_shared: bool, endpoint: str = "") -> JobFiles:
    """Work out file paths and staging for one job.

    Parameters
    ----------
    generic_workflow : `lsst.ctrl.bps.GenericWorkflow`
        Workflow holding the job and its input/output files.
    job_name : `str`
        Name of the job.
    use_shared : `bool`
        Value of ``bpsUseShared``: whether workers see the submit-side
        filesystem.
    endpoint : `str`, optional
        Value of ``fileDistributionEndPoint``; where files are published
        for workers when ``use_shared`` is false.
    """
    files = JobFiles(access_mode=ACCESS_SHARED if use_shared else ACCESS_REMOTE)

    for gwfile in generic_workflow.get_job_inputs(job_name, data=True, transfer_only=False):
        path = resolve_file_uri(gwfile, use_shared=use_shared)
        files.paths[gwfile.name] = path
        if _used_in_place(gwfile, use_shared=use_shared):
            continue
        src = str(gwfile.src_uri)
        if use_shared:
            # Visible to the worker, but the job wants its own copy.
            files.fetch.append({"source": src, "dest": path, "executable": False})
            continue
        files.distribute.append(src)
        if not getattr(gwfile, "job_access_remote", False):
            files.fetch.append({"source": f"{endpoint}{path}", "dest": path, "executable": False})
        # job_access_remote files are read where they are published: the
        # BPS command templates write {fileDistributionEndPoint}{qgraphFile}.

    for gwfile in generic_workflow.get_job_outputs(job_name, data=True, transfer_only=False):
        if not _used_in_place(gwfile, use_shared=use_shared):
            raise NotImplementedError(
                f"job {job_name}: output file {gwfile.name} ({gwfile.src_uri}) would have to be "
                "copied back from the job's working directory, which the CANFAR plugin does not do"
            )
        files.paths[gwfile.name] = resolve_file_uri(gwfile, use_shared=use_shared)

    job = generic_workflow.get_job(job_name)
    executable = getattr(job, "executable", None)
    src = getattr(executable, "src_uri", None) if executable is not None else None
    if src and not use_shared and getattr(executable, "transfer_executable", False):
        name = _basename(str(src))
        files.executable = f"./{name}"
        files.distribute.append(str(src))
        files.fetch.append({"source": f"{endpoint}{name}", "dest": name, "executable": True})
    return files


def _replace_cmd_vars(arguments: str, job: Any) -> str:
    cmdvals = dict(getattr(job, "cmdvals", None) or {})
    try:
        return arguments.format(**cmdvals)
    except (KeyError, IndexError, ValueError) as exc:
        raise ValueError(
            f"job {getattr(job, 'name', '?')}: cannot fill command line variables "
            f"({type(exc).__name__}: {exc}) in {arguments!r}"
        ) from exc


def _replace_wms_vars(text: str, job_name: str) -> str:
    def repl(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in _WMS_VALUES:
            raise ValueError(f"job {job_name}: unsupported WMS placeholder {match.group(0)}")
        return _WMS_VALUES[key]

    return _WMS_RE.sub(repl, text)


def _replace_file_vars(text: str, file_paths: dict[str, str], job_name: str) -> str:
    def repl(match: re.Match[str]) -> str:
        key = match.group(1)
        if key not in file_paths:
            raise ValueError(
                f"job {job_name}: {match.group(0)} is not one of the job's files "
                f"({', '.join(sorted(file_paths)) or 'none registered'})"
            )
        return file_paths[key]

    return _FILE_RE.sub(repl, text)


def _replace_env_vars(text: str) -> str:
    return _ENV_RE.sub(r"${\1}", text)


def _check_resolved(text: str, job_name: str, what: str) -> None:
    leftover = _LEFTOVER_RE.findall(text)
    if leftover:
        raise ValueError(f"job {job_name}: unresolved {', '.join(sorted(set(leftover)))} in {what}: {text!r}")


def command_for_generic_job(
    job: Any,
    *,
    file_paths: dict[str, str] | None = None,
    executable: str | None = None,
) -> str:
    """Expand a GenericWorkflowJob into a concrete bash command line.

    Parameters
    ----------
    job : `lsst.ctrl.bps.GenericWorkflowJob`
        Job to translate.
    file_paths : `dict` [`str`, `str`], optional
        Logical file name -> path used to replace ``<FILE:name>``.
    executable : `str`, optional
        Overrides ``job.executable.src_uri`` (staged executables).

    Raises
    ------
    ValueError
        If the job has no executable or a placeholder cannot be resolved.
    """
    name = str(getattr(job, "name", "?"))
    job_executable = getattr(job, "executable", None)
    src = executable or (getattr(job_executable, "src_uri", None) if job_executable is not None else None)
    if not src:
        raise ValueError(f"job {name} has no executable.src_uri")
    arguments = _replace_cmd_vars(getattr(job, "arguments", None) or "", job)
    command = f"{src} {arguments}".strip()
    command = _replace_wms_vars(command, name)
    command = _replace_file_vars(command, file_paths or {}, name)
    pre = getattr(job, "pre_cmdline", None)
    post = getattr(job, "post_cmdline", None)
    parts = []
    if pre:
        parts.append(pre)
    parts.append(command)
    if post:
        parts.append(post)
    command = _replace_env_vars(" && ".join(parts))
    _check_resolved(command, name, "command")
    return command


def environment_for_generic_job(job: Any) -> dict[str, str]:
    """Job environment with BPS placeholders turned into ``${VAR}``.

    The worker expands ``${VAR}`` against its own environment before it
    starts the job, so ``PATH: /extra:<ENV:PATH>`` behaves as it reads.
    """
    name = str(getattr(job, "name", "?"))
    environment: dict[str, str] = {}
    for key, raw in (getattr(job, "environment", None) or {}).items():
        value = _replace_env_vars(_replace_wms_vars(str(raw), name))
        _check_resolved(value, name, f"environment variable {key}")
        environment[str(key)] = value
    return environment


def payload_from_generic_job(
    job: Any,
    *,
    parents: list[str],
    file_paths: dict[str, str] | None = None,
    files: JobFiles | None = None,
) -> JobPayload:
    """Translate a GenericWorkflowJob into a queue payload.

    ``files`` (from `job_files`) carries the file map and staging lists;
    ``file_paths`` is a plain name -> path map that takes precedence over
    ``files.paths`` when both are given.
    """
    files = files or JobFiles()
    paths = {**files.paths, **(file_paths or {})}
    return JobPayload(
        name=str(job.name),
        label=str(getattr(job, "label", job.name)),
        command=command_for_generic_job(job, file_paths=paths, executable=files.executable),
        parents=list(parents),
        environment=environment_for_generic_job(job),
        request_cpus=getattr(job, "request_cpus", None),
        request_memory_mb=getattr(job, "request_memory", None),
        fetch=[dict(entry) for entry in files.fetch],
        access_mode=files.access_mode,
    )


def shell_script_for_payload(job: JobPayload) -> str:
    """Wrap a payload command in a small bash script for the worker."""
    exports = "\n".join(f'export {k}="{v}"' for k, v in sorted(job.environment.items()))
    body = f"""#!/bin/bash
set -euo pipefail
# BPS job: {job.name} ({job.label})
{exports}
{job.command}
"""
    return body
