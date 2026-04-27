import redis
import os
import time

redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
r = redis.from_url(redis_url, decode_responses=True)
stream = "events:trades"

try:
    first_entries = r.xrange(stream, count=1)
    last_entries = r.xrevrange(stream, count=1)
    
    first_ts = int(first_entries[0][1].get("ts_ms", 0)) if first_entries else 0
    last_ts = int(last_entries[0][1].get("ts_ms", 0)) if last_entries else 0
    now_ts = int(time.time() * 1000)
    
    print(f"First: {first_ts}")
    print(f"Last: {last_ts}")
    print(f"Now: {now_ts}")
    if first_ts > 0:
        print(f"Duration (hours): {(last_ts - first_ts) / 3600000:.2f}")
        print(f"Age of first (hours ago): {(now_ts - first_ts) / 3600000:.2f}")
    else:
        print("Stream empty or invalid Format")

except Exception as e:
    print(f"Error: {e}")
