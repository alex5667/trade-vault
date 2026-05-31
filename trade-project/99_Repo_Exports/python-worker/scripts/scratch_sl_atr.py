import os
import json
import redis
import sys

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)

try:
    r.ping()
except Exception as e:
    print(f"Redis not available: {e}")
    sys.exit(1)

# Check recent closed trades
keys = r.keys("trade:closed:*")
keys = sorted(keys, key=lambda k: r.hget(k, "exit_ts_ms") or "0", reverse=True)[:20]

print("Last 20 Closed Trades:")
for k in keys:
    data = r.hgetall(k)
    symbol = data.get("symbol", "unknown")
    direction = data.get("direction", "unknown")
    entry_price = float(data.get("entry_price") or 0.0)
    sl = float(data.get("sl") or data.get("sl_price") or 0.0)
    
    # Try to get atr from signal payload or metadata
    atr = float(data.get("atr") or 0.0)
    payload_str = data.get("signal_payload", "{}")
    if atr == 0.0:
        try:
            payload = json.loads(payload_str)
            atr = float(payload.get("atr", 0.0))
        except:
            pass
            
    is_v = data.get("is_virtual", "0")
            
    if atr > 0 and entry_price > 0 and sl > 0:
        sl_dist = abs(entry_price - sl)
        sl_atr = sl_dist / atr
        print(f"{symbol} {direction} | Entry: {entry_price:.5f} | SL: {sl:.5f} | ATR: {atr:.5f} | SL_ATR: {sl_atr:.2f} | Virtual: {is_v}")
    else:
        print(f"{symbol} {direction} | Entry: {entry_price:.5f} | SL: {sl:.5f} | ATR: {atr:.5f} | SL_ATR: N/A | Virtual: {is_v}")

# Also check open trades
print("\nOpen Trades:")
open_keys = r.keys("trade:open:*")
for k in open_keys:
    data = r.hgetall(k)
    symbol = data.get("symbol", "unknown")
    direction = data.get("direction", "unknown")
    entry_price = float(data.get("entry_price") or 0.0)
    sl = float(data.get("sl") or data.get("sl_price") or 0.0)
    
    atr = float(data.get("atr") or 0.0)
    payload_str = data.get("signal_payload", "{}")
    if atr == 0.0:
        try:
            payload = json.loads(payload_str)
            atr = float(payload.get("atr", 0.0))
        except:
            pass
            
    is_v = data.get("is_virtual", "0")
            
    if atr > 0 and entry_price > 0 and sl > 0:
        sl_dist = abs(entry_price - sl)
        sl_atr = sl_dist / atr
        print(f"{symbol} {direction} | Entry: {entry_price:.5f} | SL: {sl:.5f} | ATR: {atr:.5f} | SL_ATR: {sl_atr:.2f} | Virtual: {is_v}")
    else:
        print(f"{symbol} {direction} | Entry: {entry_price:.5f} | SL: {sl:.5f} | ATR: {atr:.5f} | SL_ATR: N/A | Virtual: {is_v}")

