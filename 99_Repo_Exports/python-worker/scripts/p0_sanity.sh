#!/usr/bin/env bash
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

# Compile check (ignore permission errors on __pycache__)
python -m compileall -q . 2>/dev/null || true

python - <<'PY'
import importlib
mods = [
  "common.decision_trace",
  "core.redis_stream_consumer",
  "services.orderflow.utils",
]
for m in mods:
    importlib.import_module(m)
print("smoke-import ok")
PY

ruff check --select F811,S110 \
  common/decision_trace.py \
  core/redis_stream_consumer.py \
  services/orderflow/utils.py

ruff format --check \
  common/decision_trace.py \
  core/redis_stream_consumer.py \
  services/orderflow/utils.py

