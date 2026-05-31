
import os
import redis
import sys

def main():
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = redis.from_url(redis_url, decode_responses=True)
    stream_name = "trades:closed"
    
    print(f"Connecting to {redis_url}, stream={stream_name}")
    
    try:
        # Read last 20 items
        entries = r.xrevrange(stream_name, max="+", count=20)
        print(f"Found {len(entries)} entries in last 20:")
        for _id, fields in entries:
            print(f"ID: {_id}")
            print(f"  Symbol: {fields.get('symbol')}")
            print(f"  Source: {fields.get('source')}")
            print(f"  PnL net: {fields.get('pnl_net')}")
            print(f"  Exit TS: {fields.get('exit_ts_ms')}")
            
        print("-" * 20)
        # Check total length (approx)
        length = r.xlen(stream_name)
        print(f"Stream length: {length}")
        
    except Exception as e:
        print(f"Error: {e}")

if __name__ == "__main__":
    main()
