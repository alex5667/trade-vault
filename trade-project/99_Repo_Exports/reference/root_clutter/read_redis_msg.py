import redis
import os

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

msg_id = "1771151724360-0"
stream = "notify:telegram"

try:
    msgs = r.xrange(stream, min=msg_id, max=msg_id)
    if msgs:
        print(f"Message ID: {msgs[0][0]}")
        print(f"Content: {msgs[0][1]}")
    else:
        print("Message not found in stream.")
except Exception as e:
    print(f"Error: {e}")
