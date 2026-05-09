import time

import redis

REDIS_URL = "redis://localhost:6379/0"

def check_open_virtual_trades():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    open_ids = r.smembers("orders:open")
    print(f"Total open orders in Redis: {len(open_ids)}")

    ten_minutes_ago_ms = int((time.time() - 600) * 1000)
    virtual_count = 0
    recent_virtual_count = 0

    for oid in open_ids:
        data = r.hgetall(f"order:{oid}")
        if not data:
            continue

        is_virtual = data.get("is_virtual") == "1"
        entry_ts_ms = int(data.get("entry_ts_ms", 0) or 0)

        if is_virtual:
            virtual_count += 1
            if entry_ts_ms > ten_minutes_ago_ms:
                recent_virtual_count += 1
                print(f"MATCH: {oid} | Symbol: {data.get('symbol')} | Side: {data.get('direction')} | Entry: {entry_ts_ms} | Virtual: {is_virtual}")

    print("\nSummary:")
    print(f"  Total virtual open trades: {virtual_count}")
    print(f"  Virtual open trades in last 10 minutes: {recent_virtual_count}")

if __name__ == "__main__":
    check_open_virtual_trades()
