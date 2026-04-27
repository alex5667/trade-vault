from utils.time_utils import get_ny_time_millis
import asyncio
import os
import json
import logging
import time
import sys
import numpy as np
import redis.asyncio as aioredis
from collections import defaultdict

from core.microbar_streams import read_microbars

# Add /app to path
sys.path.append("/app")

# Attempt import
try:
    from core.instrument_config import OrderFlowConfig
except ImportError:
    OrderFlowConfig = None
    print("Warning: Could not import OrderFlowConfig")

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT", "1000PEPEUSDT", "1000SHIBUSDT"]

async def main():
    url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    print(f"Connecting to {url}...")
    try:
        r = aioredis.from_url(url, decode_responses=True, socket_connect_timeout=2)
        await r.ping()
    except Exception as e:
        print(f"Failed: {e}")
        return

    now_ms = get_ny_time_millis()
    # Fetch per-symbol microbars (split-streams aware)
    count_per = int(os.getenv("MICROBAR_READ_COUNT", "10000"))
    bars_by_sym = defaultdict(list)
    total = 0
    for sym in SYMBOLS:
        try:
            rows = await read_microbars(r, sym=sym, count=count_per, reverse=False)
            # ensure sorted
            rows = [x for x in rows if x and x.get("ts_ms") is not None]
            rows.sort(key=lambda x: int(x.get("ts_ms", 0) or 0))
            bars_by_sym[sym] = rows
            total += len(rows)
        except Exception:
            bars_by_sym[sym] = []

    print(f"Fetched {total} microbars across {len(SYMBOLS)} symbols.")

    print("\n=== METRICS REPORT ===\n")

    # Global Env
    alpha = os.getenv("BOOK_RATE_EMA_ALPHA", "0.2 (default)")
    age_floor = os.getenv("BOOK_STALE_PENALTY_START_MS", "N/A")
    age_mult = os.getenv("BOOK_AGE_MULT", "N/A - Not found in env")
    
    print(f"Global Configs:")
    print(f"  book_rate_ema_alpha: {alpha}")
    print(f"  book_rate_stats_window: {os.getenv('BOOK_RATE_STATS_WINDOW', '300 (default)')}")
    print(f"  book_age_floor_ms (BOOK_STALE_PENALTY_START_MS): {age_floor}")
    print(f"  book_age_mult: {age_mult}")
    print("-" * 50)

    for sym in SYMBOLS:
        print(f"Symbol: {sym}")
        
        # 1. Delta Stats
        # Use CVD diff
        deltas = []
        sym_bars = bars_by_sym.get(sym, [])
        
        if len(sym_bars) > 1:
            for i in range(1, len(sym_bars)):
                b_curr = sym_bars[i]
                b_prev = sym_bars[i-1]
                
                delta = b_curr["cvd"] - b_prev["cvd"]
                price = b_curr["close"]
                val = abs(delta) * price
                deltas.append(val)
                
            # Time coverage
            duration_s = (sym_bars[-1]["ts_ms"] - sym_bars[0]["ts_ms"]) / 1000.0
            print(f"  Data Window: {duration_s/60:.1f} min ({len(sym_bars)} bars)")
            
            if deltas:
                p50 = np.percentile(deltas, 50)
                p80 = np.percentile(deltas, 80)
                p95 = np.percentile(deltas, 95)
                print(f"  abs(delta_base)*price (p50/p80/p95): {p50:.2f} / {p80:.2f} / {p95:.2f}")
            else:
                print("  abs(delta_base)*price: Insufficient data for diff")
        else:
            print(f"  Data Window: 0 min (Found {len(sym_bars)} bars)")
            print("  abs(delta_base)*price: No data")

        # 2. Inst Rate Hz (Tick Rate)
        # Scan ticks stream for sample
        tick_stream = f"stream:tick_{sym}"
        try:
            # Fetch last 5000 ticks
            ticks = await r.xrevrange(tick_stream, count=5000)
            if len(ticks) > 10:
                # Group into 1s bins
                # Or simply measure density. 
                # Calculating distribution of rates is better.
                # Bin by 1000ms
                
                bins = defaultdict(int)
                min_ts = int(ticks[-1][0].split('-')[0])
                max_ts = int(ticks[0][0].split('-')[0])
                
                for tid, _ in ticks:
                    ts = int(tid.split('-')[0])
                    bin_k = ts // 1000
                    bins[bin_k] += 1
                
                # Rates (Hz)
                rates = list(bins.values())
                if rates:
                    rp10 = np.percentile(rates, 10)
                    rp50 = np.percentile(rates, 50)
                    print(f"  inst_rate_hz (p10/p50): {rp10:.1f} / {rp50:.1f} (Sample: {(max_ts-min_ts)/1000:.1f}s)")
                else:
                    print("  inst_rate_hz: No bins")
            else:
                print(f"  inst_rate_hz: Not enough ticks ({len(ticks)})")
        except Exception as e:
            print(f"  inst_rate_hz Error: {e}")

        # 3. Depth & Book
        book_stream = f"stream:book_{sym}"
        try:
            books = await r.xrevrange(book_stream, count=20)
            if books:
                # Check depth of first
                payload = books[0][1]
                data = payload
                if "b" not in data and "data" in data:
                     try: data = json.loads(data["data"])
                     except: pass
                
                if "b" in data:
                    try:
                        bids = json.loads(data["b"]) if isinstance(data["b"], str) else data["b"]
                        asks = json.loads(data["a"]) if isinstance(data["a"], str) else data["a"]
                        print(f"  Depth: {max(len(bids), len(asks))} levels (sample msg)")
                        # Also check if it looks like snapshot (large) or partial (small)
                        # Usually snapshot > 10
                    except:
                        print("  Depth: Parse Error")
                
                # Calc Book Rate (hz) from last 20 msgs
                ts_latest = int(books[0][0].split('-')[0])
                ts_oldest = int(books[-1][0].split('-')[0])
                dur = (ts_latest - ts_oldest) / 1000.0
                if dur > 0:
                    br = len(books) / dur
                    print(f"  Book Rate: ~{br:.1f} Hz (last {len(books)} msgs)")
            else:
                print("  Book Stream: Empty")

        except Exception as e:
            print(f"  Book Error: {e}")

        print("")

    await r.close()

if __name__ == "__main__":
    asyncio.run(main())
