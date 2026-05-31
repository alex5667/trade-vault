import asyncio
import time
import os

import redis.asyncio as redis
from utils.time_utils import get_ny_time_millis

from orderflow_services.ml_recommendation_commit_executor_v1 import _process_apply_batch

async def benchmark():
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    
    # 1. Prepare 1000 mock payloads
    payloads = []
    for i in range(1000):
        payloads.append({
            "ts_ms": str(get_ny_time_millis()),
            "recommendation_id": f"bench_{i}",
            "action_type": "update_sl",
            "target_kind": "position",
            "target_ref": f"BTCUSDT_{i}",
            "executor_mode": "COMMIT"
        })
    
    print("--- POST-OPTIMIZATION BENCHMARK (Batch + Pipeline) ---")
    t0 = time.perf_counter()
    
    # Process in batches of 50
    batch_size = 50
    for i in range(0, 1000, batch_size):
        batch = payloads[i:i+batch_size]
        await _process_apply_batch(r, batch)
        
    t1 = time.perf_counter()
    
    duration = t1 - t0
    rate = 1000 / duration
    print(f"Processed 1000 items in {duration:.3f} seconds.")
    print(f"Throughput: {rate:.2f} msgs/sec")
    
    await r.aclose()

if __name__ == "__main__":
    asyncio.run(benchmark())
