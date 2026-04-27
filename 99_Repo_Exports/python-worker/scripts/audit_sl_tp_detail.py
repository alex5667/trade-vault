#!/usr/bin/env python3
"""Detail audit on problematic position 0fefb75f (LONG but SL > entry)"""
import redis, json

r = redis.from_url('redis://127.0.0.1:63791/0', decode_responses=True)
pid = "0fefb75f-859c-49dc-8884-9651ec3e2b2f"
h = r.hgetall(f"order:{pid}")
if not h:
    print("Not found"); exit()

print("=== INVERTED LONG POSITION ===")
print(f"PID: {pid}")
print(f"symbol: {h.get('symbol')}")
print(f"direction: {h.get('direction')}")
print(f"entry_price: {h.get('entry_price')}")
print(f"sl: {h.get('sl')}")
print(f"tp1: {h.get('tp1')}")
print(f"tp_levels: {h.get('tp_levels')}")
print(f"is_virtual: {h.get('is_virtual')}")
print(f"entry_ts_ms: {h.get('entry_ts_ms')}")
print(f"trail_profile: {h.get('trail_profile')}")
print(f"source: {h.get('source')}")

try:
    sp = json.loads(h.get("signal_payload", "{}"))
    print(f"\nSignal payload direction: {sp.get('direction')}")
    print(f"Signal payload side: {sp.get('side')}")
    inds = sp.get("indicators", {})
    for k in sorted(inds.keys()):
        if any(x in k for x in ("atr", "sl", "tp", "mult", "floor", "regime", "trail")):
            print(f"  {k} = {inds[k]}")
except Exception as e:
    print(f"Error: {e}")
