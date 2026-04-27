#!/usr/bin/env bash
set -euo pipefail

REDIS_URL="${REDIS_URL:-redis://localhost:6379/0}"
OUT_DIR="/home/alex/front/trade/scanner_infra/reference/audit_plan_improvements/redis"
mkdir -p "$OUT_DIR"

QUEUE="orders:queue:binance"
PROC="orders:queue:binance:processing"
DLQ="orders:queue:binance:dlq"
EXEC_STREAM="orders:exec"
STATE_PREFIX="orders:state:"

N_QUEUE="2000"
N_DLQ="2000"
N_STREAM="10000"
N_STATE="300"

redis-cli -u "$REDIS_URL" LRANGE "$QUEUE" "-$N_QUEUE" -1 > "$OUT_DIR/queue_binance_last_${N_QUEUE}.jsonl" || true
redis-cli -u "$REDIS_URL" LRANGE "$PROC" 0 -1 > "$OUT_DIR/queue_binance_processing.jsonl" || true
redis-cli -u "$REDIS_URL" LRANGE "$DLQ" "-$N_DLQ" -1 > "$OUT_DIR/queue_binance_dlq_last_${N_DLQ}.jsonl" || true
redis-cli -u "$REDIS_URL" XREVRANGE "$EXEC_STREAM" + - COUNT "$N_STREAM" > "$OUT_DIR/orders_exec_xrevrange_${N_STREAM}.txt" || true

redis-cli -u "$REDIS_URL" --scan --pattern "${STATE_PREFIX}*" | head -n "$N_STATE" > "$OUT_DIR/state_keys_sample.txt" || true
if [ -s "$OUT_DIR/state_keys_sample.txt" ]; then
    while read -r k; do
      redis-cli -u "$REDIS_URL" GET "$k" > "$OUT_DIR/state_${k//[:\/]/_}.json" || true
    done < "$OUT_DIR/state_keys_sample.txt"
fi

echo "[OK] Redis export: $OUT_DIR"
