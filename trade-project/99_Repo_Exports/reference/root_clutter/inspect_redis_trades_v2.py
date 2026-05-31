
import os
import json
import redis
import time

# Use the specific redis worker url or default
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")

def inspect():
    try:
        r = redis.from_url(REDIS_URL, decode_responses=True)
        print(f"Connected to {REDIS_URL}")
    except Exception as e:
        print(f"Failed to connect to Redis: {e}")
        return

    # Check trades:closed stream
    print("--- Inspecting trades:closed stream (last 10) ---")
    try:
        stream_entries = r.xrevrange("trades:closed", count=10)
    except Exception as e:
        print(f"Error reading stream: {e}")
        stream_entries = []

    if not stream_entries:
        print("No entries found in trades:closed.")
    
    for _id, data in stream_entries:
        order_id = data.get("order_id") or data.get("id")
        sp = data.get("signal_payload")
        
        print(f"ID: {_id} | Order: {order_id} | Payload Len: {len(sp) if sp else 'None'}")
        
        if sp:
            try:
                js = json.loads(sp)
                ind = js.get("indicators", {})
                print(f"  -> Indicators: {list(ind.keys())}")
                if "of_confirm" in ind:
                     conf = ind["of_confirm"]
                     print(f"  -> of_confirm keys: {list(conf.keys()) if isinstance(conf, dict) else conf}")
            except Exception as e:
                print(f"  -> JSON Error: {e}")
        else:
            print("  -> MISSING signal_payload")

        # Also check separate order hash
        if order_id:
            hkey = f"order:{order_id}"
            hdata = r.hgetall(hkey)
            h_sp = hdata.get("signal_payload")
            print(f"  -> Hash {hkey} Payload: {len(h_sp) if h_sp else 'None'}")
            if not h_sp and not sp:
                print("  -> CRITICAL: Payload missing in both stream and hash")

if __name__ == "__main__":
    inspect()
