"""Move job files between the submit host, a distribution endpoint and workers.

Only used when a job cannot read a file where BPS left it: always in the
non-shared access mode, and in the shared mode for files a job needs its
own copy of.  Transport is whatever `lsst.resources.ResourcePath` supports
for the URIs involved (``file://``, ``https://``/``davs://``, ``s3://``, ...),
so it is chosen by ``fileDistributionEndPoint`` rather than by this code.
"""

from __future__ import annotations

import logging
import os
import stat
from pathlib import Path
from typing import Any, Iterable, Mapping

_LOG = logging.getLogger(__name__)


def _resource_path_class() -> Any:
    # Imported on use: a shared-filesystem run with nothing to stage does
    # not need lsst.resources at all.
    from lsst.resources import ResourcePath

    return ResourcePath


def validate_endpoint(endpoint: str) -> str:
    """Check a ``fileDistributionEndPoint`` value and return it.

    BPS command templates concatenate the endpoint and a file name, so it
    has to end in ``/``.
    """
    if not endpoint:
        raise ValueError(
            "bpsUseShared is false, so files must be published for the workers: "
            "set fileDistributionEndPoint to a URI ending in '/'"
        )
    if not endpoint.endswith("/"):
        raise ValueError(
            f"fileDistributionEndPoint must end in '/' (got {endpoint!r}); it is joined to file "
            "names as-is when bpsUseShared is false"
        )
    return endpoint


def distribute(files: Iterable[str], endpoint: str) -> list[str]:
    """Copy submit-side files to ``endpoint`` under their basenames.

    Returns the destination URIs.
    """
    files = list(files)
    if not files:
        return []
    resource_path = _resource_path_class()
    # ${VAR} in the endpoint is resolved where the copy happens: here on
    # the submit host, and in fetch() on the worker.
    root = resource_path(os.path.expandvars(validate_endpoint(endpoint)), forceDirectory=True)
    published: list[str] = []
    for src in files:
        source = resource_path(src)
        if source.isdir():
            raise NotImplementedError(f"cannot distribute a directory to workers: {src}")
        dest = root.join(os.path.basename(str(src).rstrip("/")))
        _LOG.info("distributing %s -> %s", src, dest)
        dest.transfer_from(source, transfer="copy", overwrite=True)
        published.append(str(dest))
    return published


def fetch(entries: Iterable[Mapping[str, Any]], work_dir: str | Path) -> list[Path]:
    """Copy each entry's ``source`` to ``work_dir/<dest>``.

    Entries flagged ``executable`` are made executable for the owner.
    Returns the local paths written.
    """
    entries = list(entries)
    if not entries:
        return []
    resource_path = _resource_path_class()
    work_dir = Path(work_dir)
    fetched: list[Path] = []
    for entry in entries:
        name = str(entry["dest"])
        if not name or Path(name).name != name:
            raise ValueError(f"fetch destination must be a plain file name, got {name!r}")
        local = work_dir / name
        source = os.path.expandvars(str(entry["source"]))
        _LOG.info("fetching %s -> %s", source, local)
        resource_path(str(local)).transfer_from(resource_path(source), transfer="copy", overwrite=True)
        if entry.get("executable"):
            local.chmod(local.stat().st_mode | stat.S_IXUSR)
        fetched.append(local)
    return fetched
