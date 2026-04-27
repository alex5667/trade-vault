import os
import sys
import redis
import json
import time

# Add project root to sys.path
sys.path.append("/app/python-worker")
sys.path.append("/app")

print("STARTING DIAGNOSTIC SCRIPT", flush=True)

from core.redis_client import get_redis
from infra.redis_repo import RedisTradeRepository

def check():
    redis_url = os.getenv("REDIS_URL")
    print(f"Connecting to Redis at {redis_url}", flush=True)
    try:
        r = redis.from_url(redis_url, decode_responses=True) if redis_url else get_redis()
        r.ping()
        print("Redis PING OK", flush=True)
    except Exception as e:
        print(f"Redis connection FAILED: {e}", flush=True)
        return

    print("Checking orders:open set...", flush=True)
    try:
        cnt = r.scard("orders:open")
        print(f"orders:open cardinality: {cnt}", flush=True)
    except Exception as e:
        print(f"FAILED to check orders:open: {e}", flush=True)

    print("Loading positions via repo...", flush=True)
    try:
        repo = RedisTradeRepository(r)
        # We don't use monitor here to avoid hanging if it hangs on init
        out = []
        cursor = 0
        while True:
            print(f"SSCAN cursor={cursor}", flush=True)
            cursor, batch = r.sscan("orders:open", cursor=cursor, count=50)
            if not batch:
                print("Batch empty, breaking", flush=True)
                break
            print(f"Batch size: {len(batch)}", flush=True)
            for oid in batch:
                h = r.hgetall(f"order:{oid}")
                if h:
                    status = h.get("status")
                    print(f"  {oid}: status={status}", flush=True)
                    if status == "open":
                        out.append(oid)
            if cursor == 0:
                break
        print(f"Total 'open' positions found: {len(out)}", flush=True)
    except Exception as e:
        print(f"FAILED during manual recovery: {e}", flush=True)

    print("Now trying to import TradeMonitorService...", flush=True)
    try:
        from services.trade_monitor import TradeMonitorService
        print("Import SUCCESS", flush=True)
    except Exception as e:
        print(f"Import FAILED: {e}", flush=True)

if __name__ == "__main__":
    check()
