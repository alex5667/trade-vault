#!/usr/bin/env bash
set -euo pipefail

# Tick-Quality Gated Ramp runner (Step 20)
#
# Usage:
#   export $(grep -v '^#' python-worker/infra/ops/tick_quality_gate.env.example | xargs)  # or source your env file
#   ./python-worker/infra/ops/run_tick_quality_gated_ramp.sh
#
# Notes:
# - This script is designed to be used from the repo root.
# - It runs the gate first; only on PASS does it run the ramp command.

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../.." && pwd)"
cd "${ROOT_DIR}"

METRICS_URL="${TICK_GATE_METRICS_URL:-http://localhost:8000/metrics}"
WINDOW_S="${TICK_GATE_WINDOW_S:-60}"
FAIL_MODE="${TICK_GATE_FAIL_MODE:-fail_open}"
SYMBOL="${TICK_GATE_SYMBOL:-}"

CMD="${TICK_GATE_COMMAND:-python -m tools.nightly_meta_enforce_ramp_bundle}"

ARGS=(--metrics-url "${METRICS_URL}" --window-s "${WINDOW_S}" --fail-mode "${FAIL_MODE}")
if [[ -n "${SYMBOL}" ]]; then
  ARGS+=(--symbol "${SYMBOL}")
fi

echo "[tick-gate] metrics_url=${METRICS_URL} window_s=${WINDOW_S} fail_mode=${FAIL_MODE} symbol=${SYMBOL:-<all>}"
echo "[tick-gate] command=${CMD}"

python3 -m tools.run_tick_quality_gated_command "${ARGS[@]}" -- bash -lc "${CMD}"
