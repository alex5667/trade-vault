import redis
import time

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

try:
    print("Checking trades:closed stream for recent trades...")
    # Read last 100 entries
    entries = r.xrevrange("trades:closed", count=100)
    
    now_ms = int(time.time() * 1000)
    ten_mins_ago_ms = now_ms - 10 * 60 * 1000
    
    recent_closed = []
    
    for entry_id, fields in entries:
        try:
            ts_str = entry_id.split('-')[0] if '-' in entry_id else entry_id
            ts = int(ts_str)
            
            if ts > ten_mins_ago_ms:
                # Need to parse json if it's in 'payload'
                # but let's just count them
                recent_closed.append(entry_id)
        except Exception:
            pass
            
    print(f"Trades closed in the last 10 minutes: {len(recent_closed)}")
except Exception as e:
    print(f"Error checking trades:closed: {e}")

