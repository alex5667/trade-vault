import redis
import json
import time

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

now = time.time() * 1000
ten_mins_ago = now - 10 * 60 * 1000

print("Scanning for open orders...")
recent_open_orders = []
all_open_orders = []

for key in r.scan_iter('order:*', count=10000):
    try:
        data = r.hgetall(key)
        if not data:
            continue
        
        status = data.get('status', '')
        if status in ['NEW', 'OPEN', 'PARTIALLY_FILLED']:
            all_open_orders.append(data)
            
            ts_str = data.get('ts', '0')
            ts = int(ts_str) if ts_str.isdigit() else 0
            
            updated_str = data.get('updated_at', '0')
            updated = int(updated_str) if updated_str.isdigit() else ts
            
            if ts > ten_mins_ago or updated > ten_mins_ago:
                recent_open_orders.append(data)
    except Exception as e:
        pass

print(f"Total currently open orders: {len(all_open_orders)}")
print(f"Open orders created/updated in the last 10 minutes: {len(recent_open_orders)}")

for o in sorted(recent_open_orders, key=lambda x: x.get('ts', '0'), reverse=True):
    print(f"ID: {o.get('id', 'N/A')} | Sym: {o.get('symbol', 'N/A')} | Side: {o.get('side', '?')} | status: {o.get('status')} | virtual: {o.get('is_virtual')}")

