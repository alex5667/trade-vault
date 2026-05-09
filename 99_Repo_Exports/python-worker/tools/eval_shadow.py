import json

import redis
from core.redis_keys import RedisStreams as RS

r = redis.Redis(host='redis-worker-1', port=6379, db=0)
msgs = r.xrevrange(RS.CRYPTO_RAW, count=100)
scores = []
for msg in msgs:
    try:
        data = {k.decode(): v.decode() for k, v in msg[1].items()}
        ml_data = json.loads(data.get("ml", "{}"))
        if "p_edge" in ml_data:
            scores.append(ml_data["p_edge"])
    except Exception:
        pass

print(f"Evaluated {len(scores)} shadow predictions.")
if scores:
    print(f"Mean p_edge: {sum(scores)/len(scores):.4f}")
    print(f"Max p_edge: {max(scores):.4f}")
    print(f"Min p_edge: {min(scores):.4f}")
