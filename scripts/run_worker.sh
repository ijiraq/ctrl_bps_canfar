#!/bin/bash
# Headless entrypoint for a BPS CANFAR worker.
# Usage: run_worker.sh SUBMIT_PATH [idleTimeout] [pollInterval] [workerId]
set -euo pipefail

SUBMIT_PATH=${1:?submit path required}
IDLE_TIMEOUT=${2:-${BPS_CANFAR_IDLE_TIMEOUT:-86900}}
POLL_INTERVAL=${3:-${BPS_CANFAR_POLL_INTERVAL:-30}}
WORKER_ID=${4:-${BPS_CANFAR_WORKER_ID:-${REPLICA_ID:-$$}}}

export SHELL=${SHELL:-/bin/bash}
if [[ -f /opt/lsst/software/stack/loadLSST.bash ]]; then
  # shellcheck disable=SC1091
  . /opt/lsst/software/stack/loadLSST.bash
  setup lsst_distrib >/dev/null 2>&1 || true
fi

export BPS_CANFAR_WORKER_ID="${WORKER_ID}"
export BPS_CANFAR_IDLE_TIMEOUT="${IDLE_TIMEOUT}"
export BPS_CANFAR_POLL_INTERVAL="${POLL_INTERVAL}"

if command -v bps-canfar-worker >/dev/null 2>&1; then
  exec bps-canfar-worker "${SUBMIT_PATH}" \
    --idle-timeout "${IDLE_TIMEOUT}" \
    --poll-interval "${POLL_INTERVAL}" \
    --worker-id "${WORKER_ID}"
fi

# Fallback when the console script is not on PATH but the package is importable.
exec python -m lsst.ctrl.bps.canfar.worker "${SUBMIT_PATH}" \
  --idle-timeout "${IDLE_TIMEOUT}" \
  --poll-interval "${POLL_INTERVAL}" \
  --worker-id "${WORKER_ID}"
