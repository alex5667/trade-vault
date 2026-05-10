import asyncio
import time
import os
import redis.asyncio as redis
from utils.time_utils import get_ny_time_millis

INPUT_STREAM = "stream:ml:recommendation_commit_requests"

async def main():
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    
    # 1. Clear stream for clean test
    await r.delete(INPUT_STREAM)
    
    print("Generating 1000 synthetic requests...")
    t0 = time.perf_counter()
    pipe = r.pipeline()
    for i in range(1000):
        payload = {
            "ts_ms": str(get_ny_time_millis()),
            "recommendation_id": f"bench_{i}",
            "action_type": "update_sl",
            "target_kind": "position",
            "target_ref": f"BTCUSDT_{i}",
            "executor_mode": "COMMIT"
        }
        pipe.xadd(INPUT_STREAM, payload)
    await pipe.execute()
    t1 = time.perf_counter()
    print(f"Injected 1000 requests in {t1-t0:.3f}s")
    await r.aclose()

if __name__ == "__main__":
    asyncio.run(main())
