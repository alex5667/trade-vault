import redis
import json

url = "redis://:fdb98a081579737da0d6a5b25746a3c9d63abdad70e7d47f0d24159726146130@127.0.0.1:6379/0"
print(f"Connecting to {url}")
r = redis.from_url(url, decode_responses=True, socket_timeout=5)

try:
    r.ping()
    print("Ping ok")
    entries = r.xrevrange("trades:closed", count=50)
    total_found = 0
    with open("redis_dump3.txt", "w") as f:
        for eid, fields in entries:
            f.write(json.dumps(fields) + "\n")
            total_found += 1
    print(f"Dumped {total_found} entries to redis_dump3.txt")
except Exception as e:
    print(f"Error: {e}")
