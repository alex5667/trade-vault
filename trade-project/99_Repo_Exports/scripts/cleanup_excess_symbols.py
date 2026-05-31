import redis
import sys

# List of symbols to KEEP (All others will be deleted)
ALLOWED_SYMBOLS = {
    "BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT",
    "PEPEUSDT", "DOGEUSDT", "SHIBUSDT", "FLOKIUSDT", "BONKUSDT",
    "WIFUSDT", "SUIUSDT", "APTUSDT", "ARBUSDT", "XAUUSDT"
}

def clean_redis(host, port=6379, db=0):
    print(f"--- Cleaning {host}:{port}/{db} ---")
    try:
        r = redis.Redis(host=host, port=port, db=db, decode_responses=True)
        r.ping()
    except Exception as e:
        print(f"Failed to connect to {host}: {e}")
        return

    cursor = '0'
    deleted_count = 0
    kept_count = 0
    total_scanned = 0

    # We use a loop with SCAN to avoid blocking Redis
    while True:
        cursor, keys = r.scan(cursor=cursor, match='symbol:details:*', count=1000)
        total_scanned += len(keys)

        for key in keys:
            # key format: symbol:details:btcusdt
            # Extract symbol part
            parts = key.split(':')
            if len(parts) != 3:
                continue

            symbol_raw = parts[2]
            symbol_upper = symbol_raw.upper()

            if symbol_upper in ALLOWED_SYMBOLS:
                kept_count += 1
                # Optional: print(f"Keeping {symbol_upper}")
            else:
                r.delete(key)
                deleted_count += 1
                if deleted_count % 100 == 0:
                    print(f"Deleted {deleted_count} symbols (last: {symbol_upper})...")

        if cursor == 0:
            break

    print(f"Done for {host}. Scanned: {total_scanned}. Kept: {kept_count}. DELETED: {deleted_count}")

if __name__ == "__main__":
    # Allow passing host as argument, default to checking both familiar hosts
    if len(sys.argv) > 1:
        hosts = sys.argv[1:]
    else:
        hosts = ["redis", "redis-worker-1"]

    for host in hosts:
        clean_redis(host)
