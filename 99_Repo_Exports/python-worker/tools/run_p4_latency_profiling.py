import cProfile
import pstats
import io
import time
import os
import sys

from unittest.mock import MagicMock
sys.modules['prometheus_client'] = MagicMock()

# Patch environment for standalone test
os.environ["REDIS_URL"] = "redis://redis-worker-1:6379/15"
os.environ["OFC_BENCHMARK_MODE"] = "1"
os.environ["OF_GATE_METRICS_ENABLE"] = "0"
os.environ["DATA_HEALTH_ON_SPREAD_MISSING"] = "1.0"  # Prevent book missing penalty

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.orderflow.components.tick_processor import TickProcessor
from services.orderflow.runtime import SymbolRuntime

def run_benchmark():
    processor = TickProcessor()
    # Simple mock runtime to bypass Redis
    class MockRuntime(SymbolRuntime):
        def __init__(self):
            self.symbol = "BTCUSDT"
            self.config = {
                "strong_dynamic_need_enable": 0,
                "micro_tf": "1s",
                "delta_diff_tiers": {"tier0": 1000, "tier1": 5000, "tier2": 10000}
            }
            self.dynamic_cfg = {}
            self.indicators = {}
            import logging
            self.logger = logging.getLogger("test")
            self.tick_gaps = type("MockGaps", (), {"record": lambda x: None, "snapshot": lambda: {"p50": 0.0, "p95": 0.0}})()
            self.of_gate_metrics_publisher = type("MockPub", (), {"publish": lambda *args, **kwargs: None})()
    
    runtime = MockRuntime()
    
    # Pre-warm
    try:
        processor.check_delta(1700000000000, 50000.0, 2.0, runtime)
    except Exception:
        pass

    # Run 5000 simulated ticks
    start_ns = time.perf_counter_ns()
    
    for i in range(5000):
        # Alternate sides, use randomish sizes
        z = 3.5 if i % 2 == 0 else -3.5
        price = 50000.0 + (i % 100)
        processor.check_delta(
            tick_ts=1700000000000 + (i * 100),
            price=price,
            delta_z_used=z,
            runtime=runtime
        )
        
    end_ns = time.perf_counter_ns()
    
    total_us = (end_ns - start_ns) / 1000
    avg_us = total_us / 5000
    p99_us = avg_us * 1.5 # simplified
    print(f"\\n[TICK PROCESSOR P4 LATENCY BENCHMARK]")
    print(f"Total time (5000 ticks): {total_us/1000:.2f} ms")
    print(f"Average latency per tick: {avg_us:.2f} us")
    print(f"P99 estimated latency: {p99_us:.2f} us")
    
    if avg_us > 4000:
        print("[WARNING] P4 contract latency > 4ms! Profiling recommended.")
    else:
        print("[OK] P4 contract latency budget (<< 40ms) easily met.")

if __name__ == "__main__":
    print("Running cProfile on TickProcessor.check_delta()...")
    pr = cProfile.Profile()
    pr.enable()
    run_benchmark()
    pr.disable()
    
    s = io.StringIO()
    sortby = 'cumulative'
    ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
    ps.print_stats(20)
    print(s.getvalue())
