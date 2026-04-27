import sys
import redis
import time

r = redis.from_url("redis://go_gateway:fdb98a081579737da0d6a5b25746a3c9d63abdad70e7d47f0d24159726146130@redis-worker-1:6379/0", decode_responses=True)

open_ids = r.smembers("orders:open")
now_ms = time.time() * 1000

ages = []
for order_id in open_ids:
    h = r.hgetall(f"order:{order_id}")
    if h:
        ts = float(h.get("entry_ts_ms") or 0)
        age_ms = now_ms - ts
        ages.append((age_ms, h.get("symbol")))
        
ages.sort()
print(f"Total open positions: {len(ages)}")
if ages:
    print(f"Newest: {ages[0][0]/1000/60:.2f} mins ago ({ages[0][1]})")
    print(f"Oldest: {ages[-1][0]/1000/60:.2f} mins ago ({ages[-1][1]})")

# Print top 10 oldest
print("\nTop 10 oldest positions:")
for age_ms, sym in ages[-10:]:
    print(f"{age_ms/1000/60:.2f} mins - {sym}")
