import redis
import time
from datetime import datetime, timezone

# We have redis 6379 (central), redis-worker-1 63791
PORTS = [6379, 63791]

now_ms = int(time.time() * 1000)
ten_m_ago = now_ms - 10 * 60 * 1000

for port in PORTS:
    print(f"\n--- Checking Redis on port {port} ---")
    try:
        r = redis.Redis(host='localhost', port=port, db=0, decode_responses=True, socket_timeout=3)
        open_ids = r.smembers("orders:open") or set()
        print(f"Total open orders in orders:open = {len(open_ids)}")
        
        recent_open = []
        for oid in open_ids:
            data = r.hgetall(f"order:{oid}")
            if not data:
                continue
                
            ts_str = data.get("ts", "0")
            if data.get("entry_ts_ms"):
                ts_str = data.get("entry_ts_ms")
            
            ts = int(ts_str) if str(ts_str).isdigit() else 0
            updated_str = data.get("updated_at", str(ts))
            updated = int(updated_str) if str(updated_str).isdigit() else ts
            
            status = data.get("status", "unknown")
            if ts > ten_m_ago or updated > ten_m_ago:
                dt = datetime.fromtimestamp(ts/1000, tz=timezone.utc).strftime('%H:%M:%S') if ts > 0 else "unknown"
                o_info = f"ID: {oid} | Sym: {data.get('symbol')} | Side: {data.get('side', data.get('direction', '?'))} | Status: {status} | Time: {dt} | Virtual: {data.get('is_virtual')}"
                recent_open.append(o_info)
        
        print(f"Orders opened/updated in the last 10 minutes: {len(recent_open)}")
        for o in recent_open:
            print("  - " + o)
            
        # Check closed trades
        entries = r.xrevrange("trades:closed", count=100)
        recent_closed = 0
        for entry_id, fields in (entries or []):
            try:
                ts_str = entry_id.split('-')[0] if '-' in entry_id else entry_id
                if int(ts_str) > ten_m_ago:
                    recent_closed += 1
            except Exception:
                pass
        print(f"Trades closed in the last 10 minutes: {recent_closed}")
        
    except Exception as e:
        print(f"Failed to check port {port}: {e}")
