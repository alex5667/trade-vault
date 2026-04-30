import redis
import json

r = redis.Redis(host="localhost", port=6379, db=0)
messages = r.xrevrange("signals:crypto:veto_audit", "+", "-", count=5000)
found = 0
for msg_id, payload in messages:
    data_str = payload.get(b"payload")
    if data_str:
        data = json.loads(data_str)
        if data.get("symbol") in ("DOGEUSDT", "APTUSDT"):
            print(f"Found {data.get('symbol')} veto! Reason: {data.get('pre_publish_reason') or data.get('validation_reason')} | Gate: {data.get('pre_publish_gate')}")
            found += 1
            if found >= 10:
                break
if found == 0:
    print("No vetoes found.")
