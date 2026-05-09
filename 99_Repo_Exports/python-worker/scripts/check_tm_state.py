import os
import sys

import redis

# Add project root to sys.path
sys.path.append("/app/python-worker")
sys.path.append("/app")

from core.redis_client import get_redis
from infra.redis_repo import RedisTradeRepository
from services.trade_monitor import TradeMonitorService


def check():
    redis_url = os.getenv("REDIS_URL")
    r = redis.from_url(redis_url, decode_responses=True) if redis_url else get_redis()

    print(f"Checking Redis at {redis_url}")
    open_ids = r.smembers("orders:open")
    print(f"orders:open count: {len(open_ids)}")

    repo = RedisTradeRepository(r)
    monitor = TradeMonitorService(redis_url=redis_url)

    print(f"Monitor open_positions count: {len(monitor.open_positions)}")
    for sym, ids in monitor.open_by_symbol.items():
        print(f"Symbol {sym}: {len(ids)} positions")
        for oid in list(ids)[:3]:
            print(f"  - {oid}")

    if not monitor.open_positions and open_ids:
        print("CRITICAL: Monitor failed to recover any positions even though they exist in Redis!")
        # Let's try to manual recover one
        oid = list(open_ids)[0]
        h_raw = r.hgetall(f"order:{oid}")
        print(f"Sample hash for {oid}: {h_raw}")
        pos = monitor._position_from_hash(h_raw)
        print(f"Parsed position: {pos}")

if __name__ == "__main__":
    check()
