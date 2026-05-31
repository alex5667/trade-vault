import redis
import json

r = redis.from_url("redis://go_gateway:fdb98a081579737da0d6a5b25746a3c9d63abdad70e7d47f0d24159726146130@redis-worker-1:6379/0", decode_responses=True)

open_ids = r.smembers("orders:open")
if not open_ids:
    print("No open positions found in 'orders:open'.")
else:
    print(f"Found {len(open_ids)} open positions.")
    v_count = 0
    for pos_id in list(open_ids)[:5]:
        k = f"order:{pos_id}"
        h = r.hgetall(k)
        if not h:
            print(f"--- Key: {k} (MISSING) ---")
            continue
            
        print(f"--- Key: {k} ---")
        print("is_virtual:", h.get("is_virtual"))
        print("symbol:", h.get("symbol"))
        print("entry_price:", h.get("entry_price"))
        print("sl:", h.get("sl"))
        print("tp_levels:", h.get("tp_levels"))
        print("direction:", h.get("direction"))
        print("status:", h.get("status"))
        if h.get("is_virtual") == "1":
            v_count += 1
            
    print(f"Previewed {min(len(open_ids), 5)} positions.")
