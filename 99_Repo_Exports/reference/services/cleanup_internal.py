import redis

ALLOWED = {
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "PEPEUSDT", "DOGEUSDT", "SHIBUSDT", "FLOKIUSDT", "BONKUSDT",
    "WIFUSDT", "SUIUSDT", "APTUSDT", "ARBUSDT"
}

HOSTS = ["redis", "redis-worker-1", "redis-worker-2", "redis-ticks"]

def clean_host(host):
    print(f"Connecting to {host}...")
    try:
        r = redis.Redis(host=host, port=6379, db=0, decode_responses=True)
        r.ping()
    except Exception as e:
        print(f"Skipping {host}: {e}")
        return

    # Clean symbol:details:*
    cursor = '0'
    deleted = 0
    kept = 0
    while True:
        cursor, keys = r.scan(cursor=cursor, match='symbol:details:*', count=1000)
        for key in keys:
            # symbol:details:btcusdt
            parts = key.split(':')
            if len(parts) >= 3:
                sym = parts[2].upper()
                if sym not in ALLOWED:
                    r.delete(key)
                    deleted += 1
                else:
                    kept += 1
        if cursor == 0:
            break
    print(f"[{host}] symbol:details: Kept {kept}, Deleted {deleted}")

    # Clean binance:futures:usdtm:symbols
    key_set = "binance:futures:usdtm:symbols"
    if r.exists(key_set):
        members = r.smembers(key_set)
        removed_set = 0
        for m in members:
            if m.upper() not in ALLOWED:
                r.srem(key_set, m)
                removed_set += 1
        print(f"[{host}] {key_set}: Removed {removed_set}")

if __name__ == "__main__":
    print("Starting cleanup of forbidden symbols...")
    for h in HOSTS:
        clean_host(h)
    print("Cleanup complete.")
