
import asyncio
import os
import sys
import time

# Add python-worker to path
sys.path.insert(0, os.path.abspath(os.path.join(os.path.dirname(__file__), "../python-worker")))

from services.ab_winner_evaluator_job import _run_once, _acquire_lock, _release_lock
import redis.asyncio as aioredis

async def test_locking():
    print("Testing Redis Locking mechanism...")
    
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = aioredis.from_url(redis_url, decode_responses=True)
    
    lock_key = f"lock:ab_winner_evaluator:test:{int(time.time())}"
    token1 = "test-token-1"
    token2 = "test-token-2"
    ttl = 5000
    
    # 1. Acquire lock 1
    ok1 = await _acquire_lock(r, key=lock_key, ttl_ms=ttl, token=token1)
    if ok1:
        print("SUCCESS: Lock 1 acquired")
    else:
        print("FAILURE: Lock 1 NOT acquired")
        
    # 2. Try acquire lock 2 (should fail)
    ok2 = await _acquire_lock(r, key=lock_key, ttl_ms=ttl, token=token2)
    if not ok2:
        print("SUCCESS: Lock 2 denied (correct)")
    else:
        print("FAILURE: Lock 2 acquired (incorrect)")
        
    # 3. Release lock 1
    await _release_lock(r, key=lock_key, token=token1)
    print("Lock 1 released")
    
    # 4. Try acquire lock 2 again (should succeed)
    ok3 = await _acquire_lock(r, key=lock_key, ttl_ms=ttl, token=token2)
    if ok3:
        print("SUCCESS: Lock 2 acquired after release")
    else:
        print("FAILURE: Lock 2 NOT acquired after release")
        
    # Clean up
    await _release_lock(r, key=lock_key, token=token2)
    await r.aclose()
    print("Locking test complete.")

if __name__ == "__main__":
    asyncio.run(test_locking())
