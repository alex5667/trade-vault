import redis
import time
from datetime import datetime, timezone

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

open_ids = r.smembers("orders:open") or set()
print(f"Total open orders in orders:open = {len(open_ids)}")

now = time.time() * 1000
ten_mins_ago = now - 10 * 60 * 1000

recent_open = []
all_open = []

for oid in open_ids:
    data = r.hgetall(f"order:{oid}")
    if not data:
        continue
    
    ts_str = data.get("ts", "0")
    # some orders use entry_ts_ms
    if data.get("entry_ts_ms"):
        ts_str = data.get("entry_ts_ms")
        
    ts = int(ts_str) if str(ts_str).isdigit() else 0
    updated_str = data.get("updated_at", str(ts))
    updated = int(updated_str) if str(updated_str).isdigit() else ts
    
    status = data.get("status", "unknown")
    dt = datetime.fromtimestamp(ts/1000, tz=timezone.utc).strftime('%H:%M:%S') if ts > 0 else "unknown"
    
    o_info = f"ID: {oid} | Sym: {data.get('symbol')} | Side: {data.get('side', data.get('direction', '?'))} | Status: {status} | Time: {dt} | Virtual: {data.get('is_virtual')}"
    
    all_open.append(o_info)
    
    # Are they from the last 10 minutes?
    if ts > ten_mins_ago or updated > ten_mins_ago:
        recent_open.append(o_info)

print(f"\nOrders opened/updated in the last 10 minutes ({len(recent_open)}):")
for o in recent_open:
    print(o)

if not recent_open and all_open:
    print("\nBut there ARE open orders (older than 10 mins):")
    for o in all_open[:10]:
        print(o)

