
import os
import redis

def main():
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = redis.from_url(redis_url, decode_responses=True)
    
    stream = "trades:ticks"
    try:
        length = r.xlen(stream)
        print(f"Stream {stream}: len={length}")
        if length > 0:
            entries = r.xrevrange(stream, count=3)
            for _id, fields in entries:
                print(f"  ID={_id} Fields={fields.keys()}")
    except Exception as e:
        print(f"Error checking {stream}: {e}")

if __name__ == "__main__":
    main()
