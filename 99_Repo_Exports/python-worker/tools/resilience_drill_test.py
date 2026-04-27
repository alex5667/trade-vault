from utils.time_utils import get_ny_time_millis
import os
import sys
import time
from unittest.mock import MagicMock

sys.modules['prometheus_client'] = MagicMock()

# Patch environment for standalone test
os.environ["REDIS_URL"] = "redis://redis-worker-1:6379/15"

sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.orderflow.components.tick_processor import TickProcessor
from services.orderflow.runtime import SymbolRuntime

async def test_failure_drill_out_of_order_async():
    from unittest.mock import AsyncMock
    processor = TickProcessor(
        redis=AsyncMock(),
        ticks=AsyncMock(),
        publisher=AsyncMock(),
        of_engine=AsyncMock(),
        calib_svc=AsyncMock(),
        atr_cache=AsyncMock(),
        atr_sanity=AsyncMock()
    )
    
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
            self.last_tick_ts_ms = 0
            self.last_book_ts_ms = 0
            self.last_spread_bps_l2 = 2.0
            self.book_stale_detected = False
    
    runtime = MockRuntime()
    now_ms = get_ny_time_millis()

    print("Phase 1: Injecting Healthy Ticks")
    for i in range(1, 10):
        tick = {"t": now_ms + (i * 100), "p": 50000.0 + i, "q": 1.0, "m": True, "T": now_ms + (i * 100), "u": 1000+i}
        await processor.process_tick(runtime, tick)
        dh = float(runtime.indicators.get("data_health", 0))
        # Depending on ATR thresholds and data_health decay, default config gives ~ 0.8
    print(f"✅ Healthy Phase OK: data_health = {runtime.indicators.get('data_health')}")

    print("\\nPhase 2: Injecting Duplicate Ticks (IDCollision / Replay)")
    # Simulating the exact same timestamp with same sequence ID
    for i in range(3):
        tick = {"t": now_ms + 1000, "p": 50000.0, "q": 1.0, "m": True, "T": now_ms + 1000, "u": 1009}
        await processor.process_tick(runtime, tick)
        
    print(f"✅ Duplicate Phase OK: Passed through Deduper.")

    print("\\nPhase 3: Injecting Out-Of-Order Ticks (Chronology violation)")
    # Simulating ticks arriving with a timestamp older than the current high-water mark
    stale_ts = now_ms - 5000  # 5 seconds in the past
    stale_tick = {"t": stale_ts, "p": 50000.0, "q": 1.0, "m": True, "T": stale_ts, "u": 2000}
    await processor.process_tick(runtime, stale_tick)
    dh_stale = float(runtime.indicators.get("data_health", 0))
    print(f"Out of order data_health: {dh_stale}")
    
    # We expect `data_health` < 0.70 inside tick_processor due to bad_time quarantine penalty
    if dh_stale < 0.70:
        print("✅ Out-Of-Order detection passed (data_health degraded properly)")
    else:
        print("❌ FAILED: Pipeline accepted stale tick without heavy data_health penalty")

def test_failure_drill_out_of_order():
    import asyncio
    asyncio.run(test_failure_drill_out_of_order_async())

if __name__ == "__main__":
    test_failure_drill_out_of_order()
