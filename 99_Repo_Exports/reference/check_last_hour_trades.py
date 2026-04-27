import os
import time
import redis
from datetime import datetime, timezone

# Adjust for local environment
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

def main():
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
    except Exception as e:
        print(f"Failed to connect to Redis: {e}")
        return

    now_ms = int(time.time() * 1000)
    hour_ago_ms = now_ms - (3600 * 1000)
    min_id = f"{hour_ago_ms}-0"

    print(f"Checking trades since {datetime.fromtimestamp(hour_ago_ms/1000, tz=timezone.utc)}")

    # 1. Check trades:closed stream
    try:
        entries = r.xrevrange("trades:closed", min=min_id, max="+")
        print(f"Found {len(entries)} entries in 'trades:closed' stream.")
    except Exception as e:
        print(f"Error checking 'trades:closed': {e}")
        entries = []

    if entries:
        print("Sample entry keys:", entries[0][1].keys())

    # 2. Check ZSET indices if any
    # Listing zsets
    zsets = r.keys("closed_z:*")
    print(f"Found {len(zsets)} ZSET keys (closed_z:*).")
    
    total_zset_trades = 0
    for zkey in zsets:
        count = r.zcount(zkey, hour_ago_ms, now_ms)
        if count > 0:
            print(f"  {zkey}: {count} trades")
            total_zset_trades += count
            
    print(f"Total trades in ZSETs (last 1h): {total_zset_trades}")

    # 3. Check legacy lists
    lists = r.keys("closed:*")
    # Filter for lists that look like closed:strategy:symbol:tf:source
    print(f"Found {len(lists)} LIST keys (closed:*).")
    # Sampling a few lists isn't easy for time-window without lrange all
    
    # 4. Check PeriodicReporter counters
    counters = r.keys("report_counter:*")
    print(f"Found {len(counters)} report counters.")
    for k in counters:
        val = r.get(k)
        print(f"  {k}: {val}")

if __name__ == "__main__":
    main()
