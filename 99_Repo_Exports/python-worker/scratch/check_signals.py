import redis
import json

r = redis.Redis(host="localhost", port=6379, db=0)
messages = r.xrevrange("signals:crypto:raw", "+", "-", count=1000)
found = 0
for msg_id, payload in messages:
    data_str = payload.get(b"payload")
    if data_str:
        data = json.loads(data_str)
        if data.get("symbol") == "XAGUSDT":
            print(f"Found XAGUSDT signal! ID: {data.get('signal_id')} Confidence: {data.get('confidence')}")
            found += 1
if found == 0:
    print("No XAGUSDT signals found in the last 1000.")
