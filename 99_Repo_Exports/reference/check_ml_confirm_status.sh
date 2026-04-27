#!/bin/bash
# Скрипт для проверки статуса ML Confirm Gate после обновления

set -e

REDIS_URL=${REDIS_URL:-redis://redis-worker-1:6379/0}
WORKER_CONTAINER=${1:-$(docker ps --format "{{.Names}}" | grep -E "python.*worker|of.*confirm" | head -1)}

echo "=== ML Confirm Gate Status Check ==="
echo "Redis URL: $REDIS_URL"
echo "Worker container: ${WORKER_CONTAINER:-not found}"
echo ""

# Проверка конфигурации в Redis
echo "📋 Step 1: Checking Redis configuration"
echo ""

python3 <<EOF
import redis
import json
import os

redis_url = os.getenv("REDIS_URL", "$REDIS_URL")
r = redis.Redis.from_url(redis_url, decode_responses=True)

champion_key = "cfg:ml_confirm:champion"
challenger_key = "cfg:ml_confirm:challenger"

# Check champion
champion_raw = r.get(champion_key)
if champion_raw:
    champion = json.loads(champion_raw)
    print(f"✅ Champion config exists")
    print(f"   Kind: {champion.get('kind', 'unknown')}")
    print(f"   Model path: {champion.get('model_path', 'unknown')}")
    print(f"   P_min: {champion.get('p_min', 'unknown')}")
else:
    print(f"❌ Champion config not found")

# Check challenger
challenger_raw = r.get(challenger_key)
if challenger_raw:
    challenger = json.loads(challenger_raw)
    print(f"✅ Challenger config exists")
    print(f"   Kind: {challenger.get('kind', 'unknown')}")
    print(f"   Model path: {challenger.get('model_path', 'unknown')}")
else:
    print(f"ℹ️  Challenger config not found (optional)")
EOF

echo ""
echo "📋 Step 2: Checking model file"
echo ""

MODEL_PATH="/var/lib/trade/of_reports/models/model.joblib"
if [ -f "$MODEL_PATH" ]; then
    echo "✅ Model file exists: $MODEL_PATH"
    ls -lh "$MODEL_PATH" 2>/dev/null || echo "   (Cannot access file - may need sudo)"
    
    # Проверка формата модели
    python3 <<EOF
import sys
sys.path.insert(0, 'python-worker')
import joblib
import os

model_path = "$MODEL_PATH"
if os.path.exists(model_path):
    try:
        model = joblib.load(model_path)
        print(f"   Model type: {type(model).__name__}")
        print(f"   Has predict_util: {hasattr(model, 'predict_util')}")
        print(f"   Has predict_unc: {hasattr(model, 'predict_unc')}")
        
        if hasattr(model, 'predict_util') and hasattr(model, 'predict_unc'):
            print("   ✅ Model format is correct")
        else:
            print("   ❌ Model format is incorrect")
    except Exception as e:
        print(f"   ❌ Error loading model: {e}")
else:
    print("   ❌ Model file not found")
EOF
else
    echo "❌ Model file not found: $MODEL_PATH"
fi

echo ""
echo "📋 Step 3: Checking worker logs"
echo ""

if [ -n "$WORKER_CONTAINER" ]; then
    echo "Checking logs from: $WORKER_CONTAINER"
    echo ""
    docker logs "$WORKER_CONTAINER" --tail 50 2>&1 | grep -i "ml_confirm\|predict_util\|AttributeError\|no_cfg\|ERR_NO" | tail -20 || echo "   No relevant log entries found"
else
    echo "⚠️  Worker container not found. Please specify container name:"
    echo "   ./check_ml_confirm_status.sh <container-name>"
fi

echo ""
echo "📋 Step 4: Checking ML SRE metrics"
echo ""

python3 <<EOF
import redis
import os
from datetime import datetime, timedelta

redis_url = os.getenv("REDIS_URL", "$REDIS_URL")
r = redis.Redis.from_url(redis_url, decode_responses=True)

stream = "metrics:ml_confirm"
now_ms = int(datetime.now().timestamp() * 1000)
window_ms = 10 * 60 * 1000  # 10 minutes
start_ms = now_ms - window_ms

# Read recent metrics
messages = r.xrevrange(stream, max="+", min="-", count=100)
recent = [m for m in messages if int(m[1].get("ts_ms", 0) or 0) >= start_ms]

if recent:
    print(f"✅ Found {len(recent)} metrics in last 10 minutes")
    
    # Count errors
    errors = [m for m in recent if m[1].get("err", "").strip()]
    no_cfg = [m for m in recent if m[1].get("err", "") == "no_cfg"]
    
    print(f"   Total errors: {len(errors)}/{len(recent)}")
    print(f"   no_cfg errors: {len(no_cfg)}/{len(recent)}")
    
    if len(no_cfg) == 0:
        print("   ✅ No 'no_cfg' errors found")
    else:
        print(f"   ⚠️  {len(no_cfg)} 'no_cfg' errors found")
        
    # Check p_edge
    p_edges = [float(m[1].get("p_edge", 0) or 0) for m in recent if m[1].get("p_edge")]
    if p_edges:
        avg_p_edge = sum(p_edges) / len(p_edges)
        print(f"   Average p_edge: {avg_p_edge:.3f}")
        if avg_p_edge > 0:
            print("   ✅ Model is producing predictions")
        else:
            print("   ⚠️  Model predictions are zero")
else:
    print("ℹ️  No recent metrics found (may be normal if worker just started)")
EOF

echo ""
echo "=== Status Check Complete ==="

