import os
import time
import json
from redis import Redis

r = Redis.from_url('redis://redis-worker-1:6379/0', decode_responses=True)
now = int(time.time() * 1000)
cutoff = now - 3600 * 1000
min_id = f"{cutoff}-0"

entries = r.xrevrange("trades:closed", max="+", min=min_id, count=50000)
print(f"Total closed trades in last hour: {len(entries)}")

c_all = 0
c_crypto = 0
c_crypto_conf_70 = 0

for eid, fields in entries:
    c_all += 1
    src = fields.get("source") or fields.get("strategy") or ""
    if "CryptoOrderFlow" in src or "orderflow" in src.lower():
        c_crypto += 1
        conf = fields.get("conf") or fields.get("confidence") or 0
        try:
            if float(conf) * (100 if float(conf) <= 1 else 1) >= 70:
                c_crypto_conf_70 += 1
        except:
            pass

print(f"CryptoOrderFlow trades: {c_crypto}")
print(f"CryptoOrderFlow trades with conf>=70: {c_crypto_conf_70}")
