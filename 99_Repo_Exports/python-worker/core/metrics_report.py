import asyncio
import json
import os
import sys

from utils.time_utils import get_ny_time_millis

# Add /app to path (default in container)
sys.path.append("/app")

import numpy as np
import redis.asyncio as aioredis

from core.instrument_config import OrderFlowConfig
from core.microbar_streams import pick_stream_key
import contextlib

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT", "1000PEPEUSDT", "1000SHIBUSDT"]

async def main():
    # Inside container, redis-worker-1 should resolve
    url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")

    print(f"Connecting to {url}...")
    try:
        r = aioredis.from_url(url, decode_responses=True, socket_connect_timeout=2)
        await r.ping()
        print("Connected.")
    except Exception as e:
        print(f"Failed to connect: {e}")
        return

    now_ms = get_ny_time_millis()
    window_ms = 2 * 60 * 60 * 1000 # 2 hours
    start_ms = now_ms - window_ms

    print("\n=== METRICS REPORT (Last 2 Hours) ===\n")

    # Global Configs
    # In container, ENV vars from docker-compose should be present
    book_rate_alpha = os.getenv("BOOK_RATE_EMA_ALPHA", "N/A")
    print(f"Global book_rate_ema_alpha: {book_rate_alpha}")

    # Check for book_age params
    age_floor = os.getenv("BOOK_AGE_FLOOR_MS")
    if not age_floor:
         age_floor = os.getenv("BOOK_STALE_PENALTY_START_MS", "N/A") + " (BOOK_STALE_PENALTY_START_MS)"

    age_mult = os.getenv("BOOK_AGE_MULT", "N/A")
    print(f"Global book_age_floor_ms: {age_floor}")
    print(f"Global book_age_mult: {age_mult}")
    print("-" * 50)

    for sym in SYMBOLS:
        print(f"Processing {sym}...")
        try:
            cfg = OrderFlowConfig.from_env(sym)
            br_window = cfg.book_rate_stats_window
            print(f"  Config: book_rate_stats_window={br_window}")
        except Exception as e:
            print(f"  Config Error: {e}")

        # Metrics
        stream_key = await pick_stream_key(r, sym)
        start_id = f"{start_ms}-0"
        end_id = f"{now_ms}-999999"
        try:
            bars = await r.xrange(stream_key, min=start_id, max=end_id)
        except Exception as e:
            print(f"  Error reading stream {stream_key}: {e}")
            bars = []

        rates = []
        delta_vals = []

        for _, fields in bars:
            try:
                row = fields
                if "payload" in row and row["payload"]:
                    row = json.loads(row["payload"])

                # inst_rate_hz
                ticks = int(row.get("ticks", 0) or 0)
                # duration_ms might not be present if fixed tf
                dur = float(row.get("duration_ms", 1000.0) or 1000.0) / 1000.0
                if dur > 0:
                    rates.append(ticks / dur)

                # delta val
                if "delta" in row and "close" in row:
                    d = float(row["delta"])
                    pr = float(row["close"])
                    delta_vals.append(abs(d) * pr)
            except Exception:
                pass

        if rates:
            p10 = np.percentile(rates, 10)
            p50 = np.percentile(rates, 50)
            print(f"  inst_rate_hz (p10/p50): {p10:.2f} / {p50:.2f}  (n={len(rates)})")
        else:
            print(f"  inst_rate_hz: No data (found {len(bars)} bars)")
            # Print first bar fields to debug
            if len(bars) > 0:
                print(f"  Debug bar fields: {bars[0][1]}")

        if delta_vals:
            p50_d = np.percentile(delta_vals, 50)
            p80_d = np.percentile(delta_vals, 80)
            p95_d = np.percentile(delta_vals, 95)
            print(f"  abs(delta)*price (p50/p80/p95): {p50_d:.1f} / {p80_d:.1f} / {p95_d:.1f}")
        else:
            print("  abs(delta)*price: No data")

        # Check Depth
        book_key = f"stream:book_{sym}"
        try:
            books = await r.xrevrange(book_key, count=5)
            if books:
                ts_last = int(books[0][0].split('-')[0])
                lag_ms = now_ms - ts_last

                payload = books[0][1]
                depth_found = "Unknown"

                # Decode logic matching service
                data = payload
                # Sometimes it's nested "data"
                if "b" not in data and "data" in data:
                     with contextlib.suppress(Exception):
                        data = json.loads(data["data"])

                if "b" in data:
                    try:
                        bids = json.loads(data["b"]) if isinstance(data["b"], str) else data["b"]
                        asks = json.loads(data["a"]) if isinstance(data["a"], str) else data["a"]
                        depth_len = max(len(bids), len(asks))
                        depth_found = f"{depth_len} levels"
                    except Exception:
                        depth_found = "ParseErr"

                print(f"  Depth: {depth_found}, Lag: {lag_ms}ms")
            else:
                print("  Depth: No stream data found (stream:book_...)")
        except Exception as e:
            print(f"  Depth check error: {e}")

        print("")

    await r.close()

if __name__ == "__main__":
    asyncio.run(main())
