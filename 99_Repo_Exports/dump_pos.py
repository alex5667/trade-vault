import redis
r = redis.Redis(host='localhost', port=6379, decode_responses=True)
keys = r.keys("state:open_positions:*")
if keys:
    print(keys[0])
    print(r.hgetall(keys[0]))
