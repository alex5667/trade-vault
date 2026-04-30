from utils.time_utils import get_ny_time_millis
import asyncio
import os
import json
import logging
import time
import sys

# Add current directory to path
sys.path.append(os.getcwd())

import numpy as np
import redis.asyncio as aioredis

from core.microbar_streams import pick_stream_key
from core.instrument_config import OrderFlowConfig

SYMBOLS = ["BTCUSDT", "ETHUSDT", "SOLUSDT", "BNBUSDT", "XRPUSDT", "DOGEUSDT", "1000PEPEUSDT", "1000SHIBUSDT"]

async def main():
    # Attempt to connect to Redis
    # Try local host mapping first if running outside container, or service name if inside
    urls = [
        os.getenv("REDIS_URL")
        "redis://redis-worker-1:6379/0"
        "redis://localhost:6379/0"
    ]
    
    r = None
    for url in urls:
        if not url: continue
        try:
            print(f"Connecting to {url}...")
            client = aioredis.from_url(url, decode_responses=True, socket_connect_timeout=2)
            await client.ping()
            r = client
            print("Connected.")
            break
        except Exception as e:
            print(f"Failed to connect to {url}: {e}")
            
    if not r:
        print("Could not connect to Redis.")
        return

    now_ms = get_ny_time_millis()
    window_ms = 2 * 60 * 60 * 1000 # 2 hours
    start_ms = now_ms - window_ms

    print("\n=== METRICS REPORT (Last 2 Hours) ===\n")
    
    # Global Configs
    book_rate_alpha = os.getenv("BOOK_RATE_EMA_ALPHA", "N/A (env not set)")
    print(f"Global book_rate_ema_alpha: {book_rate_alpha}")
    
    age_floor = os.getenv("BOOK_AGE_FLOOR_MS", os.getenv("BOOK_STALE_PENALTY_START_MS", "N/A"))
    age_mult = os.getenv("BOOK_AGE_MULT", "N/A")
    print(f"Global book_age_floor_ms (or substitute): {age_floor}")
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
        try:
            start_id = f"{start_ms}-0"
            end_id = f"{now_ms}-999999"
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
            print(f"  inst_rate_hz (p10/p50): {p10:.2f} / {p50:.2f}  (samples={len(rates)})")
        else:
            print("  inst_rate_hz: No data")

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
            # Get last few items
            books = await r.xrevrange(book_key, count=5)
            if books:
                # Check latency/depth
                ts_last = int(books[0][0].split('-')[0])
                lag_ms = now_ms - ts_last
                
                # Check depth
                payload = books[0][1]
                depth_found = "Unknown"
                
                # Usually keys are "b" and "a" directly or JSON
                data = payload
                if "b" not in data and "data" in data:
                     try:
                        data = json.loads(data["data"])
                     except: pass
                
                if "b" in data:
                    try:
                        bids = json.loads(data["b"]) if isinstance(data["b"], str) else data["b"]
                        asks = json.loads(data["a"]) if isinstance(data["a"], str) else data["a"]
                        depth_len = max(len(bids), len(asks))
                        depth_found = f"{depth_len} lvls"
                    except:
                        depth_found = "ParseErr"
                
                print(f"  Depth: {depth_found}, Lag: {lag_ms}ms")
            else:
                print("  Depth: No stream data found")
        except Exception as e:
            print(f"  Depth check error: {e}")

        print("")

    await r.aclose()

if __name__ == "__main__":
    asyncio.run(main())
