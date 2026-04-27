#!/bin/bash
set -e

# Configuration
# DATA_DIR points to the edge_stack_v1 root (active training pipeline).
# The nightly bundle writes datasets to: $DATA_DIR/runs/<RUN_ID>/edge_train.jsonl
# We search recursively to find the latest non-empty file.
DATA_DIR=${DATA_DIR:-"/var/lib/trade/ml_models/edge_stack_v1"}
REPORT_DIR=${REPORT_DIR:-"/var/lib/trade/of_reports/drift"}
TELEGRAM_SCRIPT=${TELEGRAM_SCRIPT:-"tools/send_telegram_report.py"}
PYTHON_BIN=${PYTHON_BIN:-"python3"}

mkdir -p "$REPORT_DIR"

# 1. Find latest non-empty dataset from training runs.
# Searches $DATA_DIR/runs/*/edge_train*.jsonl recursively, sorted by mtime,
# picks latest file with size > 0.
LATEST_DATASET=$(find "$DATA_DIR" -name "edge_train*.jsonl" -type f -size +0c -printf '%T@ %p\n' | sort -n | tail -1 | cut -f2- -d" ")

if [ -z "$LATEST_DATASET" ]; then
    echo "No dataset found in $DATA_DIR"
    exit 0
fi

echo "Using dataset: $LATEST_DATASET"

TODAY=$(date +%Y-%m-%d)
REPORT_FILE="$REPORT_DIR/confidence_drift_${TODAY}.json"

# 2. Run drift report
echo "Running drift report..."
$PYTHON_BIN -m ml_analysis.tools.confidence_parts_drift_report_v1 \
    --in_jsonl "$LATEST_DATASET" \
    --out_json "$REPORT_FILE" \
    --baseline_days 7 \
    --top_n 20

ln -sf "$REPORT_FILE" "$REPORT_DIR/latest.json"

# 3. Send to Telegram (if drift detected)
# We can use a simple python inline script to parse the JSON and send if Z > threshold
echo "Checking for drift alerts..."

$PYTHON_BIN -c "
import json
import os
import sys

try:
    with open('$REPORT_FILE') as f:
        rep = json.load(f)
    
    alerts = []
    for g in rep.get('groups', []):
        group_name = '/'.join(g['group'])
        for p in g['parts']:
            z = abs(p.get('drift_z', 0))
            if z > 4.0: # Threshold
                alerts.append(f'{group_name} {p[\"key\"]}: Z={z:.1f}')
    
    if alerts:
        msg = '⚠️ Confidence Drift Alert:\n' + '\n'.join(alerts[:10])
        if len(alerts) > 10:
            msg += f'\n...and {len(alerts)-10} more.'
        
        # Send via existing mechanism (e.g. redis publish to notify:telegram)
        import redis
        r = redis.from_url(os.getenv('REDIS_URL', 'redis://redis-worker-1:6379/0'))
        r.xadd('notify:telegram', {'message': msg, 'parse_mode': 'Markdown'})
        print(f'Sent {len(alerts)} alerts')
    else:
        print('No significant drift.')

except Exception as e:
    print(f'Error sending alert: {e}')
"

# 4. Apply Guardrails (Freeze/Scale)
echo "Applying guardrails..."
APPLY="${APPLY_GUARDRAILS:-0}"
REDIS_URL="${REDIS_URL:-redis://redis-worker-1:6379/0}"

$PYTHON_BIN -m orderflow_services.conf_score_guardrails_apply_v1 \
  --drift-report "$REPORT_FILE" \
  --apply "$APPLY" \
  --redis-url "$REDIS_URL" \
  --state-path "/var/lib/trade/of_reports/conf_score_guard_state.json" || echo "Guardrails apply failed but continuing."

