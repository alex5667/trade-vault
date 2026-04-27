import redis
r = redis.Redis(host='redis-worker-1', port=6379, db=0, decode_responses=True)
entries = r.xrevrange('trades:closed', count=2)
for e in entries:
    print(list(e[1].keys()))
