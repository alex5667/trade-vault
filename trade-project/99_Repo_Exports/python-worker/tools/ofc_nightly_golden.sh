#!/usr/bin/env bash
set -euo pipefail

# Nightly OFC golden pipeline (validate -> fill_expected -> strict replay -> bench).
#
# Usage:
#   cd python-worker
#   tools/ofc_nightly_golden.sh /tmp/ofc_inputs.ndjson /tmp/ofc_golden.ndjson
#
# Exit codes:
#   0  success
#   2  validation/replay/bench failed (tool exits 2)
#   1  script error

CAPTURE_PATH="${1:-}"
GOLDEN_PATH="${2:-}"

if [[ -z "${CAPTURE_PATH}" || -z "${GOLDEN_PATH}" ]]; then
  echo "Usage: $0 <capture_ndjson_path> <golden_ndjson_path>" >&2
  exit 1
fi

PY="${PY:-python}"

echo "[1/4] Validate capture: ${CAPTURE_PATH}"
${PY} tools/ofc_validate_capture.py --input "${CAPTURE_PATH}" --strict-runtime

echo "[2/4] Fill expected -> golden: ${GOLDEN_PATH}"
${PY} tools/ofc_capture_fill_expected.py --input "${CAPTURE_PATH}" --output "${GOLDEN_PATH}"

echo "[3/4] Strict replay: ${GOLDEN_PATH}"
${PY} tools/ofc_replay.py --input "${GOLDEN_PATH}" --strict

echo "[4/4] Bench build latency (budgets optional)"
WARMUP="${OFC_BENCH_WARMUP:-200}"
ITERS="${OFC_BENCH_ITERS:-2000}"
MODE="${OFC_BENCH_MODE:-restore_each}"
P95="${OFC_BUDGET_P95_US:-350}"
P99="${OFC_BUDGET_P99_US:-900}"

${PY} tools/bench_ofc_build.py --input "${GOLDEN_PATH}" --warmup "${WARMUP}" --iters "${ITERS}" \
  --mode "${MODE}" --budget-p95-us "${P95}" --budget-p99-us "${P99}"

echo "{\"status\":\"ok\",\"capture\":\"${CAPTURE_PATH}\",\"golden\":\"${GOLDEN_PATH}\",\"bench\":{\"warmup\":${WARMUP},\"iters\":${ITERS},\"mode\":\"${MODE}\",\"budget_p95_us\":${P95},\"budget_p99_us\":${P99}}}"

