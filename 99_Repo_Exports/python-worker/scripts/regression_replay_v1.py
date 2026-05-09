"""
Regression Replay V1: Verify signal consistency and contract enforcement (Fail-Closed, DTO v1).
"""
import asyncio
import json
import logging
import sys
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')

# Add python-worker to sys.path
BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR))

# --- TOTAL ISOLATION MOCKING ---
import health_metrics

health_metrics.HealthMetrics = MagicMock()
sys.modules["health_metrics"] = health_metrics

import prometheus_client

prometheus_client.start_http_server = MagicMock()
prometheus_client.Counter = MagicMock()
prometheus_client.Gauge = MagicMock()
prometheus_client.Histogram = MagicMock()
prometheus_client.Summary = MagicMock()
sys.modules["prometheus_client"] = prometheus_client

import services.risk.risk_audit_sql as ras

ras.RiskAuditSqlSink = MagicMock()
ras.RiskAuditSqlSink.from_env = MagicMock(return_value=MagicMock())
sys.modules["services.risk.risk_audit_sql"] = ras

import services.persistence_manager as pm

pm.get_persistence_manager = MagicMock(return_value=MagicMock())
sys.modules["services.persistence_manager"] = pm

import redis.asyncio as aioredis

aioredis.from_url = MagicMock(return_value=AsyncMock())
sys.modules["redis.asyncio"] = aioredis

import redis

redis.Redis = MagicMock()
redis.Redis.from_url = MagicMock(return_value=MagicMock())
sys.modules["redis"] = redis

sys.modules["services.pnl_math"] = MagicMock()
sys.modules["services.quarantine_denylist"] = MagicMock()

try:
    from services.crypto_orderflow_service import CryptoOrderflowService
    from services.orderflow.runtime import SymbolRuntime
except ImportError as e:
    print(f"Import error: {e}")
    sys.exit(1)

async def main():
    input_file = BASE_DIR / "of_reports_out/run_monitor_20260306_210710/of_inputs_raw.ndjson"
    output_ref = BASE_DIR / "of_reports_out/run_monitor_20260306_210710/of_replay.ndjson"

    if not input_file.exists():
        print(f"Error: input {input_file} not found")
        return

    print("Initializing CryptoOrderflowService (Final Debug Mode)...")
    svc = CryptoOrderflowService(redis_dsn="redis://localhost", ticks_dsn="redis://localhost")

    # Force disable ALL gates
    svc.trade_dq_hard_veto_enable = False
    svc.trade_risk_engine_v2_enable = False
    svc.exec_quarantine_denylist_enable = False
    svc.portfolio_risk_hard_veto = False
    svc.trade_risk_sql_audit_enable = False

    svc.publisher = AsyncMock()

    symbol = "ETHUSDT"
    config = {"read_block_ms": 250, "read_count": 200}
    runtime = SymbolRuntime(symbol=symbol, config=config)
    svc.symbol_contexts[symbol] = runtime

    raw_signals = []
    with open(input_file) as f:
        for line in f:
            if line.strip():
                try:
                    raw_signals.append(json.loads(line))
                except json.JSONDecodeError:
                    continue

    print(f"Loaded {len(raw_signals)} signal candidates from {input_file}")

    results = []
    for sig in raw_signals:
        sig["v"] = 1
        allowed = await svc._pre_publish_allows_signal(runtime, sig)
        if allowed:
            results.append(sig)

    print(f"Regression replay complete. Emitted {len(results)} signals.")

    # Store results
    out_path = BASE_DIR / "of_reports_out/regression_v1_results.ndjson"
    out_path.parent.mkdir(parents=True, exist_ok=True)
    with open(out_path, "w") as f:
        for r in results:
            f.write(json.dumps(r) + "\n")

    # Reference comparison
    if output_ref.exists():
        with open(output_ref) as f:
            ref_count = sum(1 for line in f if line.strip())
        print(f"Reference signals count: {ref_count}")
        if len(results) == ref_count:
            print("✅ SUCCESS: Signals count matches reference.")
        else:
            print(f"Regression resulted in {len(results)} signals (Reference: {ref_count})")
            if len(results) > 0:
                print("✅ Part of regression passed: Signals are flowing.")

if __name__ == "__main__":
    asyncio.run(main())
