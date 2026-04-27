import redis
import json

r = redis.Redis(host="localhost", port=6379, db=0)
messages = r.xrevrange("signals:crypto:veto_audit", "+", "-", count=2000)
found = 0
for msg_id, payload in messages:
    data_str = payload.get(b"payload")
    if data_str:
        data = json.loads(data_str)
        if data.get("symbol") == "XAGUSDT":
            print(f"Found XAGUSDT veto! ID: {data.get('signal_id')} Reason: {data.get('validation_reason')}")
            found += 1
if found == 0:
    print("No XAGUSDT signals found in the last 2000 vetoes.")
