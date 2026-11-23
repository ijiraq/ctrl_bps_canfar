"""Map queue placement and run state onto BPS WmsStates values."""

from __future__ import annotations

from enum import Enum
from typing import Any

# Mirror lsst.ctrl.bps.WmsStates exit-code-friendly values so this module
# works in unit tests without the Science Pipelines stack installed.
try:
    from lsst.ctrl.bps import WmsStates
except Exception:  # pragma: no cover - stack not present in pure unit tests

    class WmsStates(Enum):  # type: ignore[no-redef]
        UNKNOWN = 10
        MISFIT = 11
        UNREADY = 12
        READY = 13
        PENDING = 14
        RUNNING = 15
        DELETED = 16
        HELD = 17
        SUCCEEDED = 0
        FAILED = 19
        PRUNED = 20


LOCATION_TO_STATE = {
    "payloads": WmsStates.UNREADY,
    "queue": WmsStates.PENDING,
    "processing": WmsStates.RUNNING,
    "processed": WmsStates.SUCCEEDED,
    "failed": WmsStates.FAILED,
}


def state_for_location(location: str | None) -> Any:
    if location is None:
        return WmsStates.UNKNOWN
    return LOCATION_TO_STATE.get(location, WmsStates.UNKNOWN)


def summarize_job_states(locations: dict[str, str | None], *, pruned: set[str] | None = None) -> dict[Any, int]:
    pruned = pruned or set()
    counts: dict[Any, int] = {}
    for name, location in locations.items():
        state = WmsStates.PRUNED if name in pruned else state_for_location(location)
        counts[state] = counts.get(state, 0) + 1
    return counts


def overall_run_state(counts: dict[Any, int], *, cancel_requested: bool = False) -> Any:
    if cancel_requested and counts.get(WmsStates.RUNNING, 0) == 0 and counts.get(WmsStates.PENDING, 0) == 0:
        return WmsStates.DELETED
    if counts.get(WmsStates.FAILED, 0) or counts.get(WmsStates.PRUNED, 0):
        # Still running other work?
        if counts.get(WmsStates.RUNNING, 0) or counts.get(WmsStates.PENDING, 0) or counts.get(WmsStates.UNREADY, 0):
            return WmsStates.RUNNING
        return WmsStates.FAILED
    if counts.get(WmsStates.RUNNING, 0) or counts.get(WmsStates.PENDING, 0):
        return WmsStates.RUNNING
    if counts.get(WmsStates.UNREADY, 0):
        return WmsStates.PENDING
    if counts.get(WmsStates.SUCCEEDED, 0):
        return WmsStates.SUCCEEDED
    return WmsStates.UNKNOWN
