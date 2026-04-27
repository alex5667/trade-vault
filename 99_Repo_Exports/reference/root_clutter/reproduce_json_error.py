import os
import redis
import json
import traceback

redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
r = redis.Redis.from_url(redis_url, decode_responses=True)
sid = "meta_freeze:1771089203860:27d887c3"
prefix = "cfg:suggestions:entry_policy"
meta_key = f"{prefix}:meta:{sid}"

print(f"Reading key: {meta_key}")
meta_raw = r.get(meta_key)
print(f"Raw content type: {type(meta_raw)}")
print(f"Raw content: {meta_raw!r}")

if meta_raw:
    try:
        meta = json.loads(meta_raw)
        print("JSON parse SUCCESS")
        print(json.dumps(meta, indent=2))
    except Exception:
        print("JSON parse FAILED")
        traceback.print_exc()
else:
    print("Key not found or empty")
