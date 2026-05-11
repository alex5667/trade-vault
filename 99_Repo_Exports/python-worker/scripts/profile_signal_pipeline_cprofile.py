import asyncio
import cProfile
import os
import pstats
import sys
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

os.environ["RISK_PERCENT"] = "5.0"
os.environ["CRYPTO_NOTIFY_SIGNAL_EVERY_N"] = "1"
os.environ["USE_SIGNAL_OUTBOX"] = "1"
os.environ["CRYPTO_SHADOW_OUTBOX"] = "0"
os.environ["CONF_SCORES_PUBLISH_ENABLED"] = "0"
os.environ["DECISION_SNAPSHOT_PUBLISH_ENABLED"] = "0"

class FakeRedis:
    async def eval(self, *args, **kwargs): return b"1-0"
    async def xadd(self, *args, **kwargs): return b"1-0"
    def pipeline(self): return self
    async def execute(self): return []

class FakePublisher:
    def __init__(self): self.r = FakeRedis()
    async def xadd_json(self, *args, **kwargs): pass

import services.orderflow.signal_pipeline


async def fake_atomic_xadd_async(*args, **kwargs): pass
services.orderflow.signal_pipeline.atomic_xadd_async = fake_atomic_xadd_async

from services.orderflow.signal_pipeline import SignalPipeline


class FakePressure:
    def get_pressure(self, *args): return 0

class MockRuntime:
    def __init__(self, symbol):
        self.symbol = symbol
        self.config = {"tp_ratio": "0.5,0.5"}
        self.last_ts_ms = int(time.time() * 1000)
        self.last_regime = "trending_bull"
        self.dynamic_cfg = {}
        self.calibrated_specs = {}
        self.pressure = FakePressure()
        self.redis_client = FakeRedis()
        self.get_atr_tf_selected = lambda: "1m"

class FakeDecision:
    def __init__(self):
        self.allowed = True
        self.mode = "SHADOW"
        self.is_canary = False
        self.reason_code = "ok"
        class Profile:
            name = "default"
            execution_policy = "SAFETY_FIRST"
            min_net_edge_bps = 0.0
        self.profile = Profile()

async def run_benchmark():
    publisher = FakePublisher()
    class FakeCache:
        def get(self, *args): return None
    pipeline = SignalPipeline(publisher=publisher, atr_cache=FakeCache())
    pipeline._profile_router.route = lambda *args, **kwargs: FakeDecision()

    base_signal = {
        "signal_id": "test_1",
        "direction": "LONG",
        "entry": 60000.0,
        "sl": 59000.0,
        "ts_ms": int(time.time() * 1000),
        "confidence": 0.8,
        "reason": "test_burst",
        "indicators": {"atr": 1000.0, "of_confirm_ok": 1, "strong_gate_ok": 1, "confidence": 0.8, "regime": "trending_bull"}
    }

    runtime = MockRuntime("BTCUSDT")
    for _ in range(10):
        await pipeline.publish_signal(runtime, base_signal.copy())

    pr = cProfile.Profile()
    pr.enable()
    for i in range(100):
        sig = base_signal.copy()
        sig["signal_id"] = f"test_{i}"
        sig["indicators"] = base_signal["indicators"].copy()
        await pipeline.publish_signal(runtime, sig)
    pr.disable()

    stats = pstats.Stats(pr).sort_stats('tottime')
    stats.print_stats(30)

if __name__ == "__main__":
    asyncio.run(run_benchmark())
