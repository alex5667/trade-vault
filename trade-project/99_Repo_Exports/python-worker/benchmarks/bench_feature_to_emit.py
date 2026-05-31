import asyncio
import os
import sys
import time

import numpy as np

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if BASE_DIR not in sys.path:
    sys.path.insert(0, BASE_DIR)

import redis.asyncio as aioredis


async def bench_xadd(redis_url, n=2000):
    print("--- Benchmarking Redis XADD (feature_to_emit part) ---")
    print(f"Connecting to {redis_url}...")
    r = aioredis.from_url(redis_url, decode_responses=True)

    # Warmup
    try:
        await r.ping()
    except Exception as e:
        print(f"Failed to connect to Redis: {e}")
        return

    latencies = []
    payload = {"ts": "1700000000000", "sym": "BTCUSDT", "val": "1.5"}

    # Warmup
    for _ in range(100):
        await r.xadd("bench:out:warmup", payload, maxlen=100, approximate=True)

    print(f"Running {n} iterations...")
    for _ in range(n):
        t0 = time.perf_counter_ns()
        await r.xadd("bench:out", payload, maxlen=100, approximate=True)
        latencies.append((time.perf_counter_ns() - t0) / 1e6)  # ms

    await r.aclose()

    a = np.array(latencies)
    p50 = np.percentile(a, 50)
    p95 = np.percentile(a, 95)
    p99 = np.percentile(a, 99)
    pmax = np.max(a)

    print(f"Redis XADD p50: {p50:.2f} ms")
    print(f"Redis XADD p95: {p95:.2f} ms")
    print(f"Redis XADD p99: {p99:.2f} ms")
    print(f"Redis XADD Max: {pmax:.2f} ms")

    ok = p99 < 15.0
    if ok:
        print(f"✅ PASS: p99 {p99:.2f} ms < 15.0 ms")
    else:
        print(f"❌ FAIL: p99 {p99:.2f} ms >= 15.0 ms")

if __name__ == "__main__":
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    asyncio.run(bench_xadd(redis_url))
