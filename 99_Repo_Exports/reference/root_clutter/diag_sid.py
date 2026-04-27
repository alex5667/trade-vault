import redis
import json
import time

def _norm_sid(sid: str) -> str:
    if not sid:
        return ""
    if sid.startswith("crypto-of:"):
        return sid[len("crypto-of:") :]
    return sid

r = redis.Redis.from_url("redis://redis-worker-1:6379/0", decode_responses=True)

# Get some recent inputs
inputs = r.xrevrange("signals:of:inputs", count=50)
print(f"Total inputs retrieved: {len(inputs)}")

input_sids = set()
for msg_id, fields in inputs:
    p = fields.get("payload", "{}")
    try:
        obj = json.loads(p)
        raw_sid = obj.get("sid", "")
        norm = _norm_sid(raw_sid)
        input_sids.add(norm)
        # print(f"Input: raw={raw_sid} norm={norm}")
    except:
        continue

print(f"Unique normalized input SIDs: {len(input_sids)}")
sample_inputs = list(input_sids)[:5]
print(f"Sample input SIDs: {sample_inputs}")

# Get some recent trades
trades = r.xrevrange("events:trades", count=1000)
print(f"Total trades retrieved: {len(trades)}")

trade_sids = set()
matches = 0
for msg_id, fields in trades:
    raw_sid = fields.get("sid", "")
    norm = _norm_sid(raw_sid)
    trade_sids.add(norm)
    if norm in input_sids:
        matches += 1
        print(f"MATCH FOUND: {norm}")

print(f"Unique normalized trade SIDs: {len(trade_sids)}")
print(f"Total matches found in trial: {matches}")
if not matches:
    sample_trades = list(trade_sids)[:5]
    print(f"Sample trade SIDs: {sample_trades}")
