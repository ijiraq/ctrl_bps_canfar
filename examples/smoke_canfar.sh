#!/usr/bin/env bash
# Live CANFAR smoke: one headless worker, one-job DAG on /arc.
# Usage:
#   canfar login cadc
#   export BPS_CANFAR_SUBMIT_PATH=/arc/projects/YOUR_PROJECT/bps-smoke/$USER
#   ./examples/smoke_canfar.sh
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
export PYTHONPATH="${ROOT}/python${PYTHONPATH:+:$PYTHONPATH}"
export BPS_CANFAR_SMOKE=1
: "${BPS_CANFAR_SUBMIT_PATH:?set BPS_CANFAR_SUBMIT_PATH to an /arc path}"

python -m pytest "${ROOT}/tests/test_smoke_canfar.py" -v --tb=short
