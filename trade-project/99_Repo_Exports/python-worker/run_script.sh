#!/bin/bash
# Скрипт для запуска малого прогона dataset builder и извлечения 4 ключевых чисел

set -e

export PYTHONPATH=.:/app:/app/ml_analysis

OUT_DIR="./ml_models/edge_stack_v1_oof"
mkdir -p "$OUT_DIR"

echo "=== Запуск малого прогона dataset builder v5 ==="
echo "Параметры:"
echo "  - signals_count: 20000"
echo "  - closes_count: 20000"
echo "  - join_tolerance_ms: 10000"
echo "  - join_secondary: dir_scenario_soft"
echo "  - nearest_max_scan: 80"
echo ""

python3 -m ml_analysis.tools.build_edge_stack_dataset_from_redis \
  --redis_url "${REDIS_URL:-redis://redis-worker-1:6379/0}" \
  --signal_stream "${ML_REPLAY_STREAM:-ml_replay_inputs_v1}" \
  --closed_stream "${TRADES_CLOSED_STREAM:-trades:closed}" \
  --signals_count 20000 \
  --closes_count 20000 \
  --y_min_r 0.10 \
  --drop_invalid_risk 0 \
  --join_strategy sid_or_nearest \
  --join_tolerance_ms 10000 \
  --join_secondary dir_scenario_soft \
  --nearest_max_scan 80 \
  --diagnose_mismatch 1 \
  --max_examples 50 \
  --out_jsonl "$OUT_DIR/edge_train.small.jsonl" \
  --out_quarantine_jsonl "$OUT_DIR/edge_quarantine.small.jsonl" \
  --out_report_json "$OUT_DIR/edge_dataset_report.small.json"

echo ""
echo "=== Результаты малого прогона ==="
if [ -f "$OUT_DIR/edge_dataset_report.small.json" ]; then
  python3 << 'PYEOF'
import json
import sys

try:
    with open('./ml_models/edge_stack_v1_oof/edge_dataset_report.small.json', 'r') as f:
        d = json.load(f)
    
    joined = d.get('joined', 0)
    joined_by_sid = d.get('joined_by_sid', 0)
    joined_by_nearest = d.get('joined_by_nearest', 0)
    nearest_join = d.get('nearest_join', {})
    ambiguous = nearest_join.get('ambiguous', 0) if isinstance(nearest_join, dict) else 0
    
    print(f"joined={joined}")
    print(f"joined_by_sid={joined_by_sid}")
    print(f"joined_by_nearest={joined_by_nearest}")
    print(f"nearest_join.ambiguous={ambiguous}")
    
    sys.exit(0)
except Exception as e:
    print(f"Ошибка чтения отчета: {e}", file=sys.stderr)
    sys.exit(1)
PYEOF
else
    echo "❌ Файл отчета не найден: $OUT_DIR/edge_dataset_report.small.json"
    exit 1
fi

