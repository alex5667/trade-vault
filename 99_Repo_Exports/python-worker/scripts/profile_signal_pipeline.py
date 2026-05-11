import asyncio
import cProfile
import os
import pstats
import sys
import time
from unittest.mock import AsyncMock, MagicMock

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ["RISK_PERCENT"] = "5.0"
os.environ["CRYPTO_NOTIFY_SIGNAL_EVERY_N"] = "1"
os.environ["USE_SIGNAL_OUTBOX"] = "1"
os.environ["CRYPTO_SHADOW_OUTBOX"] = "0"
os.environ["CONF_SCORES_PUBLISH_ENABLED"] = "0"
os.environ["DECISION_SNAPSHOT_PUBLISH_ENABLED"] = "0"

import redis

redis.Redis = MagicMock

import services.orderflow.signal_pipeline

services.orderflow.signal_pipeline.atomic_xadd_async = AsyncMock()

import logging

from services.async_signal_publisher import AsyncSignalPublisher
from services.orderflow.signal_pipeline import SignalPipeline

logging.getLogger().setLevel(logging.CRITICAL)

class MockRuntime:
    def __init__(self, symbol):
        self.symbol = symbol
        self.config = {"tp_ratio": "0.5,0.5"}
        self.last_ts_ms = int(time.time() * 1000)
        self.last_regime = "trending_bull"
        self.dynamic_cfg = {}
        self.calibrated_specs = {}
        self.pressure = MagicMock()
        self.redis_client = MagicMock()
        self.get_atr_tf_selected = MagicMock(return_value="1m")

async def run_benchmark():
    publisher = MagicMock(spec=AsyncSignalPublisher)
    publisher.xadd_json = AsyncMock()
    publisher.r = MagicMock()
    pipeline = SignalPipeline(publisher=publisher, atr_cache=MagicMock())
    pipeline._profile_router.route = MagicMock()
    m_decision = MagicMock()
    m_decision.allowed = True
    m_decision.mode = "SHADOW"
    m_decision.is_canary = False
    m_decision.reason_code = "ok"
    m_decision.profile.name = "default"
    m_decision.profile.execution_policy = "SAFETY_FIRST"
    m_decision.profile.min_net_edge_bps = 0.0
    pipeline._profile_router.route.return_value = m_decision

    base_signal = {
        "signal_id": "test_1",
        "direction": "LONG",
        "entry": 60000.0,
        "sl": 59000.0,
        "ts_ms": int(time.time() * 1000),
        "confidence": 0.8,
        "reason": "test_burst",
        "indicators": {
            "atr": 1000.0,
            "of_confirm_ok": 1,
            "strong_gate_ok": 1,
            "confidence": 0.8,
            "regime": "trending_bull"
        }
    }

    runtime = MockRuntime("BTCUSDT")
    for _ in range(10):
        await pipeline.publish_signal(runtime, base_signal.copy())

    pr = cProfile.Profile()
    pr.enable()
    for i in range(1000):
        sig = base_signal.copy()
        sig["signal_id"] = f"test_{i}"
        sig["indicators"] = base_signal["indicators"].copy()
        await pipeline.publish_signal(runtime, sig)
    pr.disable()
    stats = pstats.Stats(pr).sort_stats('tottime')
    stats.print_stats(30)

if __name__ == "__main__":
    asyncio.run(run_benchmark())
