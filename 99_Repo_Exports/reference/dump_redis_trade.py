import redis
import json

r = redis.Redis(host='localhost', port=6379, db=0, decode_responses=True)
# Since I found trades:closed in redis-worker-1 in docker, 
# and redis-worker-1 is mapped to local 6379? No, it's not.
# Only 'redis' is mapped to 6379.
# I need to run this inside a container or use the right port.
# But wait, I can just use docker exec.
