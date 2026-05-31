import json

import redis
from core.redis_keys import RedisStreams as RS

r = redis.Redis.from_url("redis://redis-worker-1:6379/0", decode_responses=True)
msgs = r.xrevrange(RS.CRYPTO_RAW, count=10)

for msg_id, data in msgs:
    payload = data.get("payload")
    if not payload:
        continue
    p = json.loads(payload)
    if "evidence" in p:
        ev = p["evidence"]
        ml = ev.get("ml", {})
        mode = ml.get("mode")
        reason = ml.get("reason")
        status = ev.get("validation_status", p.get("validation_status"))
        print(f"{p.get('symbol', p.get('runtime', {}).get('symbol'))} {status} - ML Mode: {mode}, Reason: {reason}")
        if ml.get("error"):
            print(f"ML Error: {ml.get('error')}")

