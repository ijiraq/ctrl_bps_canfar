"""Build executable job payloads from BPS GenericWorkflow jobs."""

from __future__ import annotations

import logging
import re
from typing import Any

from .queue import JobPayload

_LOG = logging.getLogger(__name__)
_ENV_RE = re.compile(r"<ENV:(\S+)>")
_FILE_RE = re.compile(r"<FILE:(\S+)>")


def command_for_generic_job(job: Any, *, file_paths: dict[str, str] | None = None) -> str:
    """Expand a GenericWorkflowJob into a concrete bash command line."""
    executable = getattr(job, "executable", None)
    src = getattr(executable, "src_uri", None) if executable is not None else None
    arguments = getattr(job, "arguments", None) or ""
    if not src:
        raise ValueError(f"job {getattr(job, 'name', '?')} has no executable.src_uri")
    command = f"{src} {arguments}".strip()
    cmdvals = dict(getattr(job, "cmdvals", None) or {})
    if cmdvals:
        try:
            command = command.format(**cmdvals)
        except (KeyError, ValueError) as exc:
            _LOG.warning("partial cmdvals expansion for %s: %s", job.name, exc)
    file_paths = file_paths or {}
    prev = None
    while prev != command:
        prev = command
        command = _ENV_RE.sub(r"${\1}", command)
        command = _FILE_RE.sub(lambda m: file_paths.get(m.group(1), m.group(0)), command)
    pre = getattr(job, "pre_cmdline", None)
    post = getattr(job, "post_cmdline", None)
    parts = []
    if pre:
        parts.append(pre)
    parts.append(command)
    if post:
        parts.append(post)
    return " && ".join(parts) if pre or post else command


def payload_from_generic_job(job: Any, *, parents: list[str], file_paths: dict[str, str] | None = None) -> JobPayload:
    environment = {str(k): str(v) for k, v in (getattr(job, "environment", None) or {}).items()}
    return JobPayload(
        name=str(job.name),
        label=str(getattr(job, "label", job.name)),
        command=command_for_generic_job(job, file_paths=file_paths),
        parents=list(parents),
        environment=environment,
        request_cpus=getattr(job, "request_cpus", None),
        request_memory_mb=getattr(job, "request_memory", None),
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
