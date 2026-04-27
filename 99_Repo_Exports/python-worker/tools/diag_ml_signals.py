import redis
import json

r = redis.Redis.from_url("redis://redis-worker-1:6379/0", decode_responses=True)
msgs = r.xrevrange("signals:crypto:raw", count=50)

for msg_id, data in msgs:
    payload = data.get("payload")
    if not payload:
        continue
    p = json.loads(payload)
    if "ev" in p:
        ev = p["ev"]
        ml = ev.get("ml_decision", {})
        mode = ml.get("mode")
        reason = ml.get("reason")
        status = ev.get("validation_status")
        print(f"{p.get('symbol')} {status} - ML Mode: {mode}, Reason: {reason}")
        if ml.get("error"):
            print(f"ML Error: {ml.get('error')}")

