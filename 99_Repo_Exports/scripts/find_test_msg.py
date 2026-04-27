import redis
import json
import os

# Use environment variable REDIS_URL if available
redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
print(f"Connecting to Redis at {redis_url}")

r = redis.Redis.from_url(redis_url, decode_responses=True)

stream = "notify:telegram"
print(f"Searching {stream} for 'Consolidated Notification Test'...")

# Search last 2000 messages
try:
    msgs = r.xrevrange(stream, count=2000)
    found = False
    for msg_id, fields in msgs:
        text = str(fields.get('text', ''))
        if 'Consolidated Notification Test' in text:
            print(f"FOUND! ID: {msg_id}")
            print(f"Fields: {json.dumps(fields, indent=2)}")
            found = True
            break

    if not found:
        print("Not found in last 2000 messages.")
except Exception as e:
    print(f"Error: {e}")
