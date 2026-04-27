#!/usr/bin/env bash
set -euo pipefail

# SHADOW Nightly Meta v5 runner
# - Trains meta model with META_SCHEMA=meta_feat_v5
# - Generates quality report (v3/v2/v1 fallback handled by pipeline)
# - Runs guardrails + auto-ramp in DRY RUN mode (no Redis writes for ramp)
# - Writes status snapshot + Prometheus textfiles (optional)
#
# Requirements:
#   - python-worker container / venv with tools.nightly_meta_pipeline_v1 available
#   - parquet dataset path exists
#   - node_exporter textfile directory exists (optional)
#
# Usage:
#   ./python-worker/ops/run_shadow_meta_v5.sh \
#     --in-parquet /var/lib/trade/of_reports/datasets/nightly_meta_v4.parquet

IN_PARQUET=""
LABEL_COL="${LABEL_COL:-y}"

OUT_DIR_MODELS="${OUT_DIR_MODELS:-/var/lib/trade/of_reports/models}"
OUT_DIR_REPORTS="${OUT_DIR_REPORTS:-/var/lib/trade/of_reports/reports}"
TEXTFILE_DIR="${TEXTFILE_DIR:-/var/lib/node_exporter/textfile_collector}"

APPLY_GUARD="${APPLY_GUARD:-1}"
APPLY_RAMP="${APPLY_RAMP:-1}"
RAMP_DRY_RUN="${RAMP_DRY_RUN:-1}"

GROUP_COLS="${META_REPORT_GROUP_COLS:-regime_bucket,session_bucket}"
MIN_GROUP_N="${META_REPORT_MIN_GROUP_N:-200}"
INCLUDE_DQ_BUCKET="${META_REPORT_INCLUDE_DQ_BUCKET:-1}"

while [[ $# -gt 0 ]]; do
  case "$1" in
    --in-parquet)
      IN_PARQUET="$2"; shift 2;;
    --label-col)
      LABEL_COL="$2"; shift 2;;
    *)
      echo "Unknown arg: $1" >&2; exit 2;;
  esac
done

if [[ -z "${IN_PARQUET}" ]]; then
  echo "--in-parquet is required" >&2
  exit 2
fi

mkdir -p "${OUT_DIR_MODELS}" "${OUT_DIR_REPORTS}"

OUT_MODEL_JSON="${OUT_DIR_MODELS}/meta_model_v5.json"
OUT_REPORT_JSON="${OUT_DIR_REPORTS}/meta_report_v5.json"
OUT_STATUS_JSON="${OUT_DIR_REPORTS}/meta_status_v5.json"
OUT_RAMP_STATE_JSON="${OUT_DIR_REPORTS}/meta_ramp_state_v5.json"

PROM_QUALITY="${TEXTFILE_DIR}/meta_quality_v5.prom"
PROM_STATUS="${TEXTFILE_DIR}/meta_status_v5.prom"

export META_SCHEMA="meta_feat_v5"
export META_REPORT_GROUP_COLS="${GROUP_COLS}"
export META_REPORT_MIN_GROUP_N="${MIN_GROUP_N}"
export META_REPORT_INCLUDE_DQ_BUCKET="${INCLUDE_DQ_BUCKET}"

PIPELINE_ARGS=(
  --in-parquet "${IN_PARQUET}"
  --label-col "${LABEL_COL}"
  --out-model-json "${OUT_MODEL_JSON}"
  --out-report-json "${OUT_REPORT_JSON}"
  --prom-textfile "${PROM_QUALITY}"
  --group-cols "${GROUP_COLS}"
  --min-group-n "${MIN_GROUP_N}"
  --ramp-state "${OUT_RAMP_STATE_JSON}"
  --out-status-json "${OUT_STATUS_JSON}"
  --status-prom-textfile "${PROM_STATUS}"
)

if [[ "${APPLY_GUARD}" == "1" ]]; then
  PIPELINE_ARGS+=(--apply-guard)
fi
if [[ "${APPLY_RAMP}" == "1" ]]; then
  PIPELINE_ARGS+=(--apply-ramp)
fi
if [[ "${RAMP_DRY_RUN}" == "1" ]]; then
  PIPELINE_ARGS+=(--ramp-dry-run)
fi

echo "[shadow-v5] META_SCHEMA=${META_SCHEMA}"
echo "[shadow-v5] in_parquet=${IN_PARQUET}"
echo "[shadow-v5] out_model=${OUT_MODEL_JSON}"
echo "[shadow-v5] out_report=${OUT_REPORT_JSON}"
echo "[shadow-v5] out_status=${OUT_STATUS_JSON}"
echo "[shadow-v5] out_ramp_state=${OUT_RAMP_STATE_JSON}"

python -m tools.nightly_meta_pipeline_v1 "${PIPELINE_ARGS[@]}"

echo "[shadow-v5] done"
