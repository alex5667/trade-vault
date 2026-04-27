import redis
import json

r = redis.Redis.from_url("redis://redis-worker-1:6379/0", decode_responses=True)
msgs = r.xrevrange("signals:crypto:raw", count=5)

for msg_id, data in msgs:
    payload = data.get("payload")
    if not payload:
        continue
    p = json.loads(payload)
    if "evidence" in p:
        ev = p["evidence"]
        ml = ev.get("ml", {})
        print(ml)

