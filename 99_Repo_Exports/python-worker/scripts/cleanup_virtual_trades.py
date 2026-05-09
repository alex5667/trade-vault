from utils.time_utils import get_ny_time_millis

"""
Cleanup script for virtual trades created before the risk-based lot sizing fix.
This script scans Redis for closed trades, identifies virtual ones,
and cleans them up to prevent them from corrupting the R-multiples and Expectancy R in periodic reports.
"""
import redis


def main():
    r = redis.Redis(host='redis-worker-1', port=6379, decode_responses=True)

    # In some dev environments redis might be localhost
    try:
        r.ping()
    except Exception:
        r = redis.Redis(host='127.0.0.1', port=6379, decode_responses=True)
        r.ping()

    print("Connecting to Redis...")
    zkeys = r.keys('closed_z:*')
    print(f"Found {len(zkeys)} closed_z keys")

    deleted_count = 0
    now_ms = get_ny_time_millis()
    # We only care about trades in the last 24 hours to be safe
    cutoff_ms = now_ms - (24 * 3600 * 1000)

    for zkey in zkeys:
        # Get all trade IDs in the last 24h
        trade_ids = r.zrevrangebyscore(zkey, now_ms, cutoff_ms)
        if not trade_ids:
            continue

        for tid in trade_ids:
            hash_key = f"trades:closed_hash:{tid}"
            trade_data = r.hgetall(hash_key)
            if not trade_data:
                continue

            # Check if it's a virtual trade
            is_virt = trade_data.get('is_virtual', '0')
            if is_virt in ('1', 'true', 'True'):
                # We can either delete it completely or recalculate.
                # Deletion is safer for "garbage" virtual trades.
                print(f"Deleting garbage virtual trade: {tid}")
                r.zrem(zkey, tid)
                r.delete(hash_key)

                # Also remove from general stream if possible, but stream elements are immutable.
                # Just deleting the hash and zset reference removes it from the Reporter.
                deleted_count += 1

    print(f"Cleanup complete. Deleted {deleted_count} legacy virtual trades.")

if __name__ == "__main__":
    main()
