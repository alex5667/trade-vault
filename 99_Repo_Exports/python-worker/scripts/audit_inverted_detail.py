#!/usr/bin/env python3
"""Full detail on inverted position"""
import redis, json

r = redis.from_url('redis://127.0.0.1:63791/0', decode_responses=True)
pid = "0fefb75f-859c-49dc-8884-9651ec3e2b2f"
h = r.hgetall(f"order:{pid}")
if not h:
    print("Not found"); exit()

sp = json.loads(h.get("signal_payload", "{}"))
print("=== FULL signal_payload keys ===")
for k in sorted(sp.keys()):
    if k == "indicators":
        continue
    print(f"  {k} = {sp.get(k)}")

print(f"\n=== sl from hash: {h.get('sl')}")
print(f"=== tp_levels from hash: {h.get('tp_levels')}")
print(f"=== sl from signal_payload: {sp.get('sl')}")
print(f"=== tp_levels from signal_payload: {sp.get('tp_levels')}")
print(f"=== direction from signal_payload: {sp.get('direction')}")
print(f"=== side from signal_payload: {sp.get('side')}")

# Check all BTCUSDT positions
open_ids = r.smembers("orders:open")
btc_positions = []
for oid in open_ids:
    od = r.hgetall(f"order:{oid}")
    if od.get("symbol") == "BTCUSDT":
        btc_positions.append(od)

print(f"\n=== All BTCUSDT open positions: {len(btc_positions)} ===")
for p in btc_positions:
    entry = float(p.get("entry_price", 0))
    sl = float(p.get("sl", 0))
    d = p.get("direction", "?")
    ok = (d.upper() == "LONG" and sl < entry) or (d.upper() == "SHORT" and sl > entry)
    print(f"  {d:6s} entry={entry:.2f} sl={sl:.2f} ok={ok} tp1={p.get('tp1','?')} trail={p.get('trail_profile','?')}")
