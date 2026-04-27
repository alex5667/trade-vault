import os
import sys
import redis
import json

# Add project root to sys.path
sys.path.append("/app/python-worker")
sys.path.append("/app")

from infra.redis_repo import RedisTradeRepository

def check():
    redis_url = os.getenv("REDIS_URL")
    print(f"Testing with REDIS_URL={redis_url}")
    
    for decode in [True, False]:
        print(f"\n--- Testing with decode_responses={decode} ---")
        r = redis.from_url(redis_url, decode_responses=decode)
        repo = RedisTradeRepository(r)
        
        try:
            rows = repo.load_open_positions(limit=10)
            print(f"load_open_positions returned {len(rows)} rows")
            if rows:
                print(f"Sample row keys: {list(rows[0].keys())}")
                print(f"Sample status: {rows[0].get('status')}")
            else:
                # If 0 rows, check manual SSCAN
                cursor, batch = r.sscan("orders:open", count=10)
                print(f"Manual SSCAN returned {len(batch)} items, cursor={cursor}")
                if batch:
                    oid = batch[0]
                    h = r.hgetall(f"order:{oid}")
                    print(f"Manual HGETALL for {oid} returned {len(h)} fields")
                    print(f"Status in hash: {h.get('status') if isinstance(h, dict) else 'N/A'}")
        except Exception as e:
            print(f"Error during test: {e}")
            import traceback
            traceback.print_exc()

if __name__ == "__main__":
    check()
