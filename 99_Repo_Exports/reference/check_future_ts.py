import redis
import time
import os

def check_redis(r, name):
    print(f"--- Checking {name} ---")
    threshold = 1775000000000 # April 2026
    try:
        keys = r.keys("*")
        for key in keys:
            key_str = key.decode()
            try:
                t = r.type(key)
                if t == b'stream':
                    msgs = r.xrevrange(key, count=1)
                    if msgs:
                        msg_id, fields = msgs[0]
                        ts = int(msg_id.decode().split('-')[0])
                        if ts > threshold:
                            print(f"FUTURE ID in {key_str}: {ts}")
                        for f, v in fields.items():
                            f_str = f.decode()
                            if f_str in ('ts', 'ts_ms', 'E', 'T', 'timestamp'):
                                try:
                                    val = int(v.decode())
                                    if val > threshold:
                                        print(f"FUTURE FIELD in {key_str}.{f_str}: {val}")
                                except: pass
            except Exception as e:
                print(f"Error checking {key_str}: {e}")
    except Exception as e:
        print(f"Error connecting to {name}: {e}")

if __name__ == "__main__":
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    redis_ticks_url = os.getenv("REDIS_TICKS_URL", "redis://redis-ticks:6379/0")
    print(f"DEBUG: main={redis_url}, ticks={redis_ticks_url}")

    r_main = redis.Redis.from_url(redis_url)
    r_ticks = redis.Redis.from_url(redis_ticks_url)
    
    check_redis(r_main, "MAIN")
    check_redis(r_ticks, "TICKS")
