import redis
r = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
items = r.xrevrange("trades:closed", max="+", min="-", count=5)
for msg_id, fields in items:
    print(f"ID: {msg_id}, fields: {fields}")
