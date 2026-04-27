import redis
import json

def verify_trades():
    r = redis.from_url("redis://redis-worker-1:6379/0", decode_responses=True)
    open_ids = list(r.smembers("orders:open"))
    
    found = 0
    for id_ in open_ids:
        pos = r.hgetall("order:" + id_)
        symbol = pos.get("symbol")
        dir = pos.get("direction")
        entry = float(pos.get("entry_price", 0))
        sl = float(pos.get("sl", 0))
        atr = float(pos.get("atr_value", 1))
        
        distance = abs(sl - entry)
        mult = distance / atr if atr else 0
        
        print(f"--- {symbol} ({id_[:8]}) ---")
        print(f"Dir: {dir}")
        print(f"Entry: {entry}")
        print(f"SL: {sl} (dist: {distance:.5f})")
        print(f"ATR: {atr:.5f}")
        print(f"Calculated STOP_ATR_MULT: {mult:.2f}")
        print(f"tf: {pos.get('tf')}")
        print(f"atr_tf_ms: {pos.get('atr_tf_ms')}")
        
        found += 1
        if found >= 5:
            break

if __name__ == "__main__":
    verify_trades()
