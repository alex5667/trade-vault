import asyncio
import os
import sys
import time

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))
os.environ["RISK_PERCENT"] = "5.0"
os.environ["CRYPTO_NOTIFY_SIGNAL_EVERY_N"] = "1"
os.environ["USE_SIGNAL_OUTBOX"] = "1"

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

import logging

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
        self.pressure = type("F", (), {"get_pressure": lambda s, *a: 0})()
        self.redis_client = FakeRedis()
        self.get_atr_tf_selected = lambda: "1m"

async def run_benchmark():
    publisher = FakePublisher()
    pipeline = SignalPipeline(publisher=publisher, atr_cache=type("C", (), {"get": lambda s, *a: None})())
    pipeline._profile_router.route = lambda *args, **kwargs: type("D", (), {"allowed": True, "mode": "SHADOW", "is_canary": False, "reason_code": "ok", "profile": type("P", (), {"name": "default", "execution_policy": "SAFETY_FIRST", "min_net_edge_bps": 0.0})()})()

    base_signal = {
        "signal_id": "test_1", "direction": "LONG", "entry": 60000.0, "sl": 59000.0, "ts_ms": int(time.time() * 1000), "confidence": 0.8, "reason": "test_burst",
        "indicators": {"atr": 1000.0, "of_confirm_ok": 1, "strong_gate_ok": 1, "confidence": 0.8, "regime": "trending_bull"}
    }
    runtime = MockRuntime("BTCUSDT")
    for _ in range(100): await pipeline.publish_signal(runtime, base_signal.copy())

    latencies = []
    t_start = time.perf_counter_ns()
    for i in range(1000):
        sig = base_signal.copy()
        sig["signal_id"] = f"test_{i}"
        sig["indicators"] = base_signal["indicators"].copy()
        t0 = time.perf_counter_ns()
        await pipeline.publish_signal(runtime, sig)
        t1 = time.perf_counter_ns()
        latencies.append((t1 - t0) / 1e6)

    t_end = time.perf_counter_ns()
    latencies.sort()

    print("--- FakeRedis Benchmark Results: Latency ---")
    print(f"p50: {latencies[int(len(latencies)*0.5)]:.3f} ms")
    print(f"p95: {latencies[int(len(latencies)*0.95)]:.3f} ms")
    print(f"p99: {latencies[int(len(latencies)*0.99)]:.3f} ms")
    print(f"Throughput (sequential): {1000 / ((t_end - t_start)/1e9):.2f} ops/sec")

if __name__ == "__main__":
    asyncio.run(run_benchmark())
