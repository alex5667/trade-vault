#!/usr/bin/env python3
"""
Script to measure typical p50/p90 for taker_*_rate_ema on BTC/ETH.
Reads from Redis streams and calculates robust stats.

Usage:
  python3 scripts/measure_taker_rates.py --symbol BTCUSDT --duration 60
"""
import asyncio
import argparse
import json
import numpy as np
import redis.asyncio as aioredis

async def measure(symbol: str, duration: int, redis_url: str):
    print(f"--- Measuring taker rates for {symbol} for {duration} seconds ---")
    r = aioredis.from_url(redis_url, decode_responses=True)

    # Try multiple stream patterns
    streams = [
        f"signals:orderflow:{symbol}",
        f"stream:signals:{symbol}",
        "signals:crypto:raw"
    ]

    # Check which stream exists/has data
    target_stream = None
    for s in streams:
        if await r.exists(s):
            target_stream = s
            break

    if not target_stream:
        print(f"Warning: Could not find specific stream for {symbol}. Listening to all likely streams...")
        # fallback to first if none found (maybe waiting for data)
        target_stream = streams[0]

    print(f"Listening to stream: {target_stream}")

    buy_rates = []
    sell_rates = []

    start_time = asyncio.get_running_loop().time()
    last_id = "$"

    while (asyncio.get_running_loop().time() - start_time) < duration:
        try:
            # Block for 1 sec
            results = await r.xread({target_stream: last_id}, count=50, block=1000)
            if not results:
                continue

            for _, messages in results:
                for msg_id, fields in messages:
                    last_id = msg_id

                    # Attempt to extract indicators
                    payload = {}
                    # Case 1: Fields are flat (in stream)
                    if "taker_buy_rate_ema" in fields:
                        payload = fields
                    # Case 2: Packed in 'data' json
                    elif "data" in fields:
                        try:
                            payload = json.loads(fields["data"])
                        except (json.JSONDecodeError, ValueError):
                            pass
                    # Case 3: Packed in 'indicators' json inside 'data' (common in signals)
                    if "indicators" in payload:
                        payload = payload["indicators"]

                    # Extract rates
                    try:
                        br = float(payload.get("taker_buy_rate_ema", 0.0))
                        sr = float(payload.get("taker_sell_rate_ema", 0.0))

                        # filter zeroes if metric missing
                        if br > 0 or sr > 0:
                            buy_rates.append(br)
                            sell_rates.append(sr)
                    except (TypeError, ValueError, KeyError):
                        pass

        except Exception as e:
            print(f"Error reading stream: {e}")
            await asyncio.sleep(1)

    # Stats
    print("\n--- Results ---")
    if not buy_rates:
        print("No rate data found.")
        return

    buy_rates = np.array(buy_rates)
    sell_rates = np.array(sell_rates)

    print(f"Samples: {len(buy_rates)}")

    print("\n[Taker Buy Rate EMA]")
    print(f"  p50: {np.percentile(buy_rates, 50):.4f}")
    print(f"  p90: {np.percentile(buy_rates, 90):.4f}")
    print(f"  Max: {np.max(buy_rates):.4f}")

    print("\n[Taker Sell Rate EMA]")
    print(f"  p50: {np.percentile(sell_rates, 50):.4f}")
    print(f"  p90: {np.percentile(sell_rates, 90):.4f}")
    print(f"  Max: {np.max(sell_rates):.4f}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument("--symbol", default="BTCUSDT")
    parser.add_argument("--duration", type=int, default=30)
    parser.add_argument("--redis", default="redis://localhost:6379")
    args = parser.parse_args()

    try:
        asyncio.run(measure(args.symbol, args.duration, args.redis))
    except KeyboardInterrupt:
        pass
