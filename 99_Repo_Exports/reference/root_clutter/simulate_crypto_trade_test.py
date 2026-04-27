
import os
import sys
import time
import redis
import json
import uuid


# Connect to Redis
redis_host = os.getenv("REDIS_HOST", "redis-worker-1")
r = redis.Redis(host=redis_host, port=6379, db=0)

def simulate_trade():
    source = "CryptoOrderFlow"
    symbol = "BTCUSDT"
    sid = f"{source}-{symbol}-{uuid.uuid4()}"
    ts = int(time.time() * 1000)
    
    # 1. Order Hash
    order_data = {
        "sid": sid,
        "symbol": symbol,
        "source": source,
        "strategy": "orderflow",
        "status": "closed",
        "pnl": "10.5",
        "pnl_net": "10.0",
        "pnl_gross": "11.0",
        "fees": "1.0",
        "entry_price": "50000.0",
        "exit_price": "50100.0",
        "direction": "LONG",
        "entry_ts_ms": str(ts - 60000),
        "exit_ts_ms": str(ts),
        "closed_time": str(ts),
        "duration_ms": "60000",
        "r_multiple": "1.5"
    }
    r.hset(f"order:{sid}", mapping=order_data)
    print(f"Created order:{sid}")
    
    # 2. ZSET
    z_key = f"closed_z:{source}:{symbol}"
    r.zadd(z_key, {sid: ts})
    print(f"Added to {z_key}")
    
    # 3. Stream
    stream_key = "trades:closed"
    stream_entry = {
        "sid": sid,
        "symbol": symbol,
        "source": source,
        "pnl": "10.0",
        "ts": str(ts),
        "strategy": "orderflow"
    }
    r.xadd(stream_key, stream_entry)
    print(f"Added to {stream_key}")

    # 4. Update stats:strategies and stats:symbols:{strategy} to ensure discovery
    r.sadd("stats:strategies", "orderflow") # or CryptoOrderFlow?
    # PeriodicReporter maps "orderflow" -> "CryptoOrderFlow".
    r.sadd("stats:symbols:orderflow", symbol)
    r.sadd("stats:strategies", "CryptoOrderFlow") # Add both just in case
    r.sadd("stats:symbols:CryptoOrderFlow", symbol)
    print("Updated stats sets")

if __name__ == "__main__":
    try:
        # Check connection
        r.ping()
        print("Connected to Redis")
        simulate_trade()
    except Exception as e:
        print(f"Error: {e}")
