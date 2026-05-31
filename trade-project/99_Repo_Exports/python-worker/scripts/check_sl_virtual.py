import time
import redis
import json

REDIS_URL = "redis://localhost:6379/0"

def check_sl():
    r = redis.from_url(REDIS_URL, decode_responses=True)
    open_ids = r.smembers("orders:open")
    
    ten_minutes_ago_ms = int((time.time() - 3600) * 1000)
    
    found = 0
    for oid in open_ids:
        data = r.hgetall(f"order:{oid}")
        if not data:
            continue
            
        is_virtual = data.get("is_virtual") == "1"
        entry_ts_ms = int(data.get("entry_ts_ms", 0) or 0)
        
        if is_virtual and entry_ts_ms > ten_minutes_ago_ms:
            print(f"Trade: {oid} | Symbol: {data.get('symbol')} | Side: {data.get('direction')}")
            print(f"  Entry Px: {data.get('entry_price')} | SL Px: {data.get('sl_price')} | TP Px: {data.get('tp_price')}")
            print(f"  SL Dist %: {data.get('sl_dist_pct')} | SL ATR: {data.get('sl_atr_dist')}")
            print(f"  Context: {data.get('entry_reason', 'N/A')}")
            print(f"  Raw SL fields: {[k for k in data.keys() if 'sl' in k.lower() or 'stop' in k.lower()]}")
            found += 1
            if found >= 5:
                break
                
    if found == 0:
        print("No recent virtual trades found.")

if __name__ == "__main__":
    check_sl()
