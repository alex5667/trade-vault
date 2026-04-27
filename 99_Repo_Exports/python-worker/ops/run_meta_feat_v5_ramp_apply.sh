#!/bin/bash
# python-worker/ops/run_meta_feat_v5_ramp_apply.sh

REPORT_JSON=${1:-/var/lib/trade/of_reports/reports/meta_report_v5.json}
MODEL_JSON=${2:-/var/lib/trade/of_reports/models/meta_model_v5.json}

if [ ! -f "$REPORT_JSON" ]; then
    echo "Report not found: $REPORT_JSON"
    exit 1
fi

export PYTHONPATH=.:$PYTHONPATH

python3 tools/meta_ramp_apply_v3.py \
    --report-json "$REPORT_JSON" \
    --model-json "$MODEL_JSON" \
    --apply ${APPLY:-0} \
    --redis-url ${REDIS_URL:-redis://redis-worker-1:6379/0}
