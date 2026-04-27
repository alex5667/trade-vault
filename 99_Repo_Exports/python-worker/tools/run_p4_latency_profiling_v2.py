
import asyncio
import time
import json
import os
import cProfile
import pstats
import io
from typing import Dict, Any, Optional

# Mock dependencies before imports to avoid side effects if possible, 
# but here we just import and then monkeypatch or provide mocks to constructor.

import sys
from unittest.mock import MagicMock

# Mock prometheus_client and aioredis to avoid network/registry errors
mock_prom = MagicMock()
sys.modules["prometheus_client"] = mock_prom

mock_redis_mod = MagicMock()
sys.modules["redis.asyncio"] = mock_redis_mod
sys.modules["aioredis"] = mock_redis_mod # fallback for older imports
sys.modules["core.redis_client"] = MagicMock() # Mock the resilient client

from services.orderflow.components.tick_processor import TickProcessor
from services.orderflow.runtime import SymbolRuntime

class MockRedis:
    async def xadd(self, name, fields, maxlen=None, approximate=True):
        return "12345-0"
    
    async def get(self, key):
        return None
    
    async def hgetall(self, key):
        return {}
    
    async def hget(self, key, field):
        return None

    async def hset(self, key, mapping=None, **kwargs):
        return 1

    async def set(self, key, value, ex=None):
        return True
    
    async def exists(self, key):
        return 0

    def pipeline(self):
        return self

    async def execute(self):
        return []

class MockPublisher:
    async def publish(self, signal):
        pass

def run_benchmark():
    # Setup
    mock_redis = MockRedis()
    mock_publisher = MockPublisher()
    
    # TickProcessor(redis, ticks, publisher, of_engine, calib_svc, atr_cache, atr_sanity)
    # We can pass None for those that are not used in process_tick or provide simple mocks.
    processor = TickProcessor(
        redis=mock_redis,
        ticks=mock_redis,
        publisher=mock_publisher,
        of_engine=None,
        calib_svc=None,
        atr_cache=None,
        atr_sanity=None
    )
    
    config = {
        "symbol": "BTCUSDT",
        "delta_tier_min": 0,
        "dn_tier0_usd": 1000.0,
        "dn_tier1_usd": 5000.0,
        "dn_tier2_usd": 20000.0,
    }
    
    runtime = SymbolRuntime(symbol="BTCUSDT", config=config)
    runtime.redis_client = mock_redis
    
    # Warmup
    print("Warming up...")
    loop = asyncio.get_event_loop()
    
    async def warmup():
        for i in range(100):
            tick = {
                "ts_ms": 1700000000000 + i,
                "price": 50000.0,
                "qty": 0.1,
                "is_buyer_maker": False,
                "written_at": 1700000000000 + i + 2 # simulate 2ms ingest lag
            }
            await processor.process_tick(runtime, tick)
    
    loop.run_until_complete(warmup())
    
    # Benchmark
    print("Starting benchmark (5000 ticks)...")
    
    async def benchmark():
        start_ns = time.perf_counter_ns()
        for i in range(5000):
            tick = {
                "ts_ms": 1700000100000 + i,
                "price": 50000.0 + (i % 100),
                "qty": 0.5 if i % 10 == 0 else 0.01, # Occasionally trigger delta?
                "is_buyer_maker": (i % 2 == 0),
                "written_at": 1700000100000 + i + 1
            }
            # Note: process_tick is async because it might call redis.xadd or calibrate
            await processor.process_tick(runtime, tick)
        
        end_ns = time.perf_counter_ns()
        total_ms = (end_ns - start_ns) / 1_000_000
        avg_us = (end_ns - start_ns) / 5000 / 1000
        print(f"Total time: {total_ms:.2f} ms")
        print(f"Avg time per tick: {avg_us:.2f} us")
        return total_ms, avg_us

    # Profile
    pr = cProfile.Profile()
    pr.enable()
    
    total_ms, avg_us = loop.run_until_complete(benchmark())
    
    pr.disable()
    s = io.StringIO()
    sortby = "cumulative"
    ps = pstats.Stats(pr, stream=s).sort_stats(sortby)
    ps.print_stats(30)
    print(s.getvalue())

if __name__ == "__main__":
    # Ensure environment is clean for bench
    os.environ["OF_GATE_METRICS_ENABLE"] = "0" 
    run_benchmark()
