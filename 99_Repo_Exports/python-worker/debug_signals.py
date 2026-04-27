
import os
import redis

def main():
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = redis.from_url(redis_url, decode_responses=True)
    
    streams = ["signals:crypto:raw", "signals:cryptoorderflow", "trades:closed"]
    
    for s in streams:
        try:
            length = r.xlen(s)
            print(f"Stream {s}: len={length}")
            if length > 0:
                entries = r.xrevrange(s, count=3)
                print(f"  Last 3 entries in {s}:")
                for _id, fields in entries:
                    print(f"  ID={_id} Fields={fields.keys()}")
            print("-" * 20)
        except Exception as e:
            print(f"Error checking {s}: {e}")

if __name__ == "__main__":
    main()
