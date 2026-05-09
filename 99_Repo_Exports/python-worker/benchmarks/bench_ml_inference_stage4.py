import json
import os
import sys
import time
from types import SimpleNamespace

import numpy as np

from utils.time_utils import get_ny_time_millis

# Force python-worker to be the primary search path to avoid root-level search conflicts
BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

from core.of_confirm_engine import OFConfirmEngine
from services.ml_confirm_gate import MLConfirmDecision, MLConfirmGate, _load_model_cached, _safe_loads


def benchmark_engine_build():
    print("\n--- Benchmarking OFConfirmEngine.build() ---")

    # Mock ML Gate to avoid lazy initialization with real Redis
    class MockMLGate:
        def check(self, **kwargs):
            return MLConfirmDecision(mode="OFF", kind="none", allow=True, reason="mock")

    engine = OFConfirmEngine(version=3, ml_gate=MockMLGate())

    # Mock runtime and indicators
    runtime = SimpleNamespace(
        symbol="BTCUSDT",
        last_obi_event=None,
        last_iceberg_event=None,
        last_ofi_event=None,
        dynamic_cfg={},
        last_regime="trend"
    )

    indicators = {
        "delta_z": 2.5,
        "spread_bps": 5.0,
        "book_health_ok": 1,
        "data_health": 1.0
    }

    cfg = {
        "of_confirm_v3_enable": 1,
        "strong_gate_mode": "enforce"
    }

    # Warmup
    for _ in range(100):
        engine.build(
            symbol="BTCUSDT", tf="tick", direction="LONG",
            tick_ts_ms=get_ny_time_millis(), price=100.0, delta_z=2.5,
            runtime=runtime, cfg=cfg, indicators=indicators
        )

    # Benchmark
    latencies = []
    iterations = 5000
    for _ in range(iterations):
        t0 = time.perf_counter_ns()
        engine.build(
            symbol="BTCUSDT", tf="tick", direction="LONG",
            tick_ts_ms=get_ny_time_millis(), price=100.0, delta_z=2.5,
            runtime=runtime, cfg=cfg, indicators=indicators
        )
        latencies.append((time.perf_counter_ns() - t0) / 1000.0) # us

    print_stats(latencies)

def benchmark_ml_gate_check():
    print("\n--- Benchmarking MLConfirmGate.check() (Stage 4) ---")

    # Загружаем реальную модель v8 для теста, если она есть
    model_path = "/var/lib/trade/models/meta_model_lr_v8.json"
    if not os.path.exists(model_path):
        print(f"WARNING: Model {model_path} not found. Benchmark might be unrealistic.")
        # Fallback to dummy or skip if critical

    # Mock Redis client
    class MockRedis:
        def get(self, k): return None
        def set(self, k, v, **kwargs): pass
        def xadd(self, *args, **kwargs): pass

    # Mock config in Redis
    cfg_payload = json.dumps({
        "kind": "meta_lr",
        "model_path": model_path,
        "enforce_share": 1.0,
        "sample_rate": 1.0,
        "p_min": 0.5,
        "p_margin": 0.05
    })

    gate = MLConfirmGate(
        r=MockRedis(),
        mode="ENFORCE",
        fail_policy="OPEN",
        champion_key="test_cfg",
        challenger_key="test_cfg_ch"
    )

    gate._cfg, gate._cfg_parse_err, _ = _safe_loads(cfg_payload), "", len(cfg_payload)
    gate._model = _load_model_cached(model_path, "meta_lr")

    indicators = {
        "sid": "crypto-of:BTCUSDT:1700000000000",
        "delta_z": 2.5,
        "ofi_z": 1.5,
        "obi_z": -0.5,
        "spread_bps": 4.2
    }

    # Warmup
    for _ in range(100):
        gate.check(
            symbol="BTCUSDT", ts_ms=1700000000000,
            direction="LONG", scenario="trend",
            indicators=indicators, rule_score=0.8,
            rule_have=3, rule_need=2,
            cancel_spike_veto=0, ok_rule=1
        )

    # Benchmark
    latencies = []
    iterations = 5000
    for _ in range(iterations):
        t0 = time.perf_counter_ns()
        gate.check(
            symbol="BTCUSDT", ts_ms=1700000000000,
            direction="LONG", scenario="trend",
            indicators=indicators, rule_score=0.8,
            rule_have=3, rule_need=2,
            cancel_spike_veto=0, ok_rule=1
        )
        latencies.append((time.perf_counter_ns() - t0) / 1000.0) # us

    print_stats(latencies)

def print_stats(latencies):
    l = np.array(latencies)
    print(f"P50: {np.percentile(l, 50):.2f} us")
    print(f"P95: {np.percentile(l, 95):.2f} us")
    print(f"P99: {np.percentile(l, 99):.2f} us")
    print(f"Max: {np.max(l):.2f} us")

    p99_ms = np.percentile(l, 99) / 1000.0
    if p99_ms < 5.0:
        print(f"✅ PASS: P99 {p99_ms:.3f} ms < 5.0 ms")
    else:
        print(f"❌ FAIL: P99 {p99_ms:.3f} ms >= 5.0 ms")

if __name__ == "__main__":
    benchmark_engine_build()
    benchmark_ml_gate_check()
