import asyncio
import os
import sys
import time
import tracemalloc
from unittest.mock import AsyncMock, MagicMock

# Устанавливаем пути, чтобы можно было импортировать модули python-worker
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "..")))

# Устанавливаем переменные окружения, чтобы избежать вызовов сети или ошибок отсутствия переменных
os.environ["RISK_PERCENT"] = "5.0"
os.environ["CRYPTO_NOTIFY_SIGNAL_EVERY_N"] = "1"
os.environ["USE_SIGNAL_OUTBOX"] = "1"
os.environ["CRYPTO_SHADOW_OUTBOX"] = "0"
os.environ["CONF_SCORES_PUBLISH_ENABLED"] = "0"
os.environ["DECISION_SNAPSHOT_PUBLISH_ENABLED"] = "0"

# Mock Redis BEFORE importing SignalPipeline
import redis

redis.Redis = MagicMock

import logging

import services.outbox.atomic_outbox
from services.async_signal_publisher import AsyncSignalPublisher
from services.orderflow.signal_pipeline import SignalPipeline

logging.getLogger().setLevel(logging.CRITICAL)
logging.getLogger("of_signal_pipeline").setLevel(logging.CRITICAL)

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
    publisher.r.xadd = AsyncMock()
    publisher.r.eval = AsyncMock()

    services.outbox.atomic_outbox.atomic_xadd_async = AsyncMock()
    import services.orderflow.signal_pipeline
    services.orderflow.signal_pipeline.atomic_xadd_async = AsyncMock()

    pipeline = SignalPipeline(publisher=publisher, atr_cache=MagicMock())

    # Мокаем TradeProfileRouter для предотвращения лишних походов в сеть, если он это делает
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

    # Базовый сигнал для тестов
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

    print("Starting warmup...")
    # Warmup (100 iterations) to let Python JIT (if any) or internal cache structures warm up
    for _ in range(100):
        await pipeline.publish_signal(runtime, base_signal.copy())

    print("Warmup done. Starting steady state latency test (1000 iterations)...")
    latencies = []

    tracemalloc.start()
    for i in range(1000):
        sig = base_signal.copy()
        sig["signal_id"] = f"test_{i}"
        # Make a deep copy of indicators to prevent mutation bleed across iterations
        sig["indicators"] = base_signal["indicators"].copy()

        t0 = time.perf_counter_ns()
        await pipeline.publish_signal(runtime, sig)
        t1 = time.perf_counter_ns()

        latencies.append((t1 - t0) / 1e6)  # ms

    current, peak = tracemalloc.get_traced_memory()
    tracemalloc.stop()

    latencies.sort()
    p50 = latencies[int(len(latencies) * 0.5)]
    p95 = latencies[int(len(latencies) * 0.95)]
    p99 = latencies[int(len(latencies) * 0.99)]

    print("\n--- Benchmark Results: Latency ---")
    print(f"Iterations: {len(latencies)}")
    print(f"p50: {p50:.3f} ms")
    print(f"p95: {p95:.3f} ms")
    print(f"p99: {p99:.3f} ms")
    print(f"Peak memory allocation: {peak / 1024 / 1024:.2f} MB")

    print("\nStarting throughput test (1000 concurrent ops)...")
    tasks = []
    t_burst_start = time.perf_counter_ns()

    for i in range(1000):
        sig = base_signal.copy()
        sig["signal_id"] = f"burst_{i}"
        sig["indicators"] = base_signal["indicators"].copy()
        tasks.append(pipeline.publish_signal(runtime, sig))

    await asyncio.gather(*tasks)
    t_burst_end = time.perf_counter_ns()

    duration_sec = (t_burst_end - t_burst_start) / 1e9
    throughput = 1000 / duration_sec
    print("--- Benchmark Results: Throughput ---")
    print(f"Time for 1000 ops: {duration_sec:.3f} s")
    print(f"Throughput: {throughput:.2f} ops/sec")

if __name__ == "__main__":
    asyncio.run(run_benchmark())
