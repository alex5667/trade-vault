import os

import redis
from core.redis_keys import RedisStreams as RS


def main():
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    stream = RS.TRADES_CLOSED

    print(f"Connecting to {redis_url}...")
    r = redis.from_url(redis_url, decode_responses=True)

    print(f"Reading last 50 entries from {stream}...")
    # Read last 50 entries
    entries = r.xrevrange(stream, count=50)

    if not entries:
        print("No entries found.")
        return

    print(f"{'ID':<15} | {'Symbol':<10} | {'PnL':<8} | {'1R ($)':<8} | {'MFE PnL':<10} | {'MFE R':<10} | {'Giveback':<10}")
    print("-" * 100)

    for _id, fields in entries:
        symbol = fields.get("symbol", "N/A")
        pnl_net = float(fields.get("pnl_net") or 0.0)
        one_r = float(fields.get("one_r_money") or 0.0)
        mfe_pnl = float(fields.get("mfe_pnl") or 0.0)
        giveback = float(fields.get("giveback") or 0.0)

        mfe_r = 0.0
        if one_r > 1e-9:
            mfe_r = mfe_pnl / one_r

        print(f"{_id:<15} | {symbol:<10} | {pnl_net:<8.2f} | {one_r:<8.2f} | {mfe_pnl:<10.2f} | {mfe_r:<10.2f} | {giveback:<10.2f}")

if __name__ == "__main__":
    main()
