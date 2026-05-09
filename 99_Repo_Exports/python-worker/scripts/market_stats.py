#!/usr/bin/env python3
import asyncio
import json
import os
from collections import defaultdict

import numpy as np
import redis.asyncio as aioredis

from utils.time_utils import get_ny_time_millis

# Configuration
SYMBOLS = ["BTCUSDT", "ETHUSDT"]
LOOKBACK_MS = 3600 * 1000 * 2  # 2 hours
MAX_ITEMS = 50000

# Redis Hosts (Container names)
HOST_MAIN = os.getenv("REDIS_HOST_MAIN", "redis-worker-1")
HOST_TICKS = os.getenv("REDIS_HOST_TICKS", "redis-ticks")
PORT = 6379

async def get_redis_client(host, label):
    try:
        url = f"redis://{host}:{PORT}/0"
        r = aioredis.from_url(url, decode_responses=True)
        await r.ping()
        print(f"Connected to {label} ({host})")
        return r
    except Exception as e:
        print(f"Failed to connect to {label} ({host}): {e}")
        return None

def safe_float(v, default=0.0):
    try:
        return float(v)
    except Exception:
        return default

def safe_int(v, default=0):
    try:
        return int(float(v))
    except Exception:
        return default

async def fetch_stream_data(r, stream_key, start_ms):
    """Fetch recent items from stream since start_ms."""
    if not r: return []
    entries = []
    try:
        min_id = f"{start_ms}-0"
        # Reduce count to avoid timeouts on large tick payloads
        count = 5000
        # Loop to fetch more if needed? For now just sample 5000 is enough for distribution
        data = await r.xrevrange(stream_key, min=min_id, max="+", count=count)
        for _, payload in data:
            entries.append(payload)
    except Exception as e:
        print(f"Error reading {stream_key}: {e}")
    return entries

async def analyze_symbol(r_main, r_ticks, symbol):
    print(f"\nEvaluating {symbol}...")
    now_ms = get_ny_time_millis()
    start_ms = now_ms - LOOKBACK_MS

    # 1. Microbars (Delta, CVD) - stored in MAIN
    # Split-stream aware: read from per-symbol stream if available, else legacy
    stream_template = os.getenv("MICROBAR_PER_SYMBOL_STREAM_TEMPLATE", "events:microbar_closed:{sym}")
    symbols_set = os.getenv("MICROBAR_SYMBOLS_SET", "events:microbar_closed:symbols")
    legacy_key = os.getenv("MICROBAR_LEGACY_STREAM", "events:microbar_closed")

    all_microbars = []
    # Try per-symbol stream first
    if "{sym}" in stream_template:
        try:
            sym_key = stream_template.format(sym=symbol)
            all_microbars = await fetch_stream_data(r_main, sym_key, start_ms)
        except Exception:
            # Fallback to legacy if per-symbol fails
            all_microbars = await fetch_stream_data(r_main, legacy_key, start_ms)
    else:
        # Use legacy stream
        all_microbars = await fetch_stream_data(r_main, legacy_key, start_ms)

    symbol_bars = []
    for p in all_microbars:
        try:
            if "payload" in p:
                d = json.loads(p["payload"])
            else:
                d = p

            if d.get("symbol") == symbol:
                ts = safe_int(d.get("ts_ms") or d.get("ts"))
                if ts >= start_ms:
                    symbol_bars.append(d)
        except Exception:
            pass

    symbol_bars.sort(key=lambda x: safe_int(x.get("ts_ms") or x.get("ts"), 0))

    prev_cvd = None
    delta_metrics = [] # abs(delta)*price

    # Try to detect if delta is available explicitly, else derive
    derive_cvd = True

    for b in symbol_bars:
        cvd = safe_float(b.get("cvd"))
        close = safe_float(b.get("close"))

        current_delta = 0.0
        # If payload has explicit 'delta_sum' (from microbar logic) use it
        # The payload in code: "microbar_delta_sum" or "delta_sum"?
        # Checking crypto_orderflow_service.py: "microbar_delta_sum" in indicators,
        # but in events:microbar_closed payload it has "cvd".
        # It does NOT seem to have explicit delta in the event payload based on my read.
        # So we stick to cvd diff.

        if prev_cvd is not None:
            current_delta = cvd - prev_cvd

        if prev_cvd is not None and close > 0:
            metric = abs(current_delta) * close
            delta_metrics.append(metric)

        prev_cvd = cvd

    # 2. Spreads (Ticks) -> NO, Ticks have no spread. Use BOOK.
    # We will compute spread from BOOKS.

    # 3. Book Rate (Updates Hz) - stored in TICKS
    book_key = f"stream:book_{symbol}"
    raw_books = await fetch_stream_data(r_ticks, book_key, start_ms)
    print(f"DEBUG: Fetched {len(raw_books)} books for {symbol}")
    if raw_books:
        # Avoid printing huge payload
        s = str(raw_books[0])
        print(f"DEBUG: Sample book: {s[:200]}...")

    books_1s_count = defaultdict(int)
    spreads_bps = []

    for b_raw in raw_books:
        try:
            # Parse book
            if "payload" in b_raw:
                b = json.loads(b_raw["payload"])
            else:
                b = b_raw

            ts = safe_int(b.get("ts") or b.get("ts_ms") or 0)
            if ts < start_ms: continue

            bucket = ts // 1000
            books_1s_count[bucket] += 1

            # Spread
            # Expect "bids": [[px, qty], ...], "asks": ...
            bids = b.get("bids", [])
            asks = b.get("asks", [])

            if isinstance(bids, str):
                try: bids = json.loads(bids)
                except Exception: bids = []

            if isinstance(asks, str):
                try: asks = json.loads(asks)
                except Exception: asks = []

            # If payload is different (e.g. standard Binance bookTicker: "b", "a", "B", "A")
            # But the service suggests "bids"/"asks" lists.
            best_bid = 0.0
            best_ask = 0.0

            if bids and isinstance(bids, list) and len(bids) > 0:
                # [px, qty]
                if isinstance(bids[0], list): best_bid = safe_float(bids[0][0])

            if asks and isinstance(asks, list) and len(asks) > 0:
                if isinstance(asks[0], list): best_ask = safe_float(asks[0][0])

            if best_bid > 0 and best_ask > 0 and best_ask >= best_bid:
                mid = (best_ask + best_bid) / 2.0
                if mid > 0:
                    bps = 10000.0 * (best_ask - best_bid) / mid
                    spreads_bps.append(bps)
        except Exception:
            # print(f"DEBUG: Book parse error: {ex}")
            pass

    book_rates = list(books_1s_count.values())

    # 4. Signal Rate (Pre-cooldown) - stored in MAIN
    sig_key = "events:delta_spike"
    raw_sigs = await fetch_stream_data(r_main, sig_key, start_ms)

    sigs_1min_count = defaultdict(int)
    for s_raw in raw_sigs:
        try:
            if "payload" in s_raw:
                s = json.loads(s_raw["payload"])
            else:
                s = s_raw

            if s.get("symbol") == symbol:
                ts = safe_int(s.get("ts_ms") or s.get("ts")),
                if ts < start_ms: continue

                bucket = ts // 60000,
                sigs_1min_count[bucket] += 1,
        except Exception:
            pass

    signal_rates = list(sigs_1min_count.values()),
    if not signal_rates: signal_rates = [0],

    # Helper
    def get_percentile(data, p):
        if not data: return 0.0,
        return np.percentile(data, p),

    stats = {
        "abs_delta_price": {
            "p50": get_percentile(delta_metrics, 50),
            "p80": get_percentile(delta_metrics, 80),
            "p95": get_percentile(delta_metrics, 95),
            "count": len(delta_metrics)
        },
        "spread_bps": {
            "p50": get_percentile(spreads_bps, 50),
            "p90": get_percentile(spreads_bps, 90),
            "p99": get_percentile(spreads_bps, 99),
            "count": len(spreads_bps)
        },
        "book_rate_hz": {
            "p50": get_percentile(book_rates, 50),
            "p10": get_percentile(book_rates, 10),
            "count": len(book_rates)
        },
        "signals_per_min": {
            "avg": np.mean(signal_rates) if signal_rates else 0.0,
            "max": np.max(signal_rates) if signal_rates else 0.0,
            "total": sum(signal_rates),
            "minutes_active": len(sigs_1min_count) if sigs_1min_count else 0
        }
    }
    return stats

async def main():
    r_main = await get_redis_client(HOST_MAIN, "Main (Worker)")
    r_ticks = await get_redis_client(HOST_TICKS, "Ticks")

    if not r_main and not r_ticks:
        print("No Redis connections available.")
        return

    print(f"Time Window: Last {LOOKBACK_MS/1000/3600:.1f} hours")

    for sym in SYMBOLS:
        stats = await analyze_symbol(r_main, r_ticks, sym)

        print(f"\n--- {sym} Report ---")
        print("1. abs(delta_base)*price (Distribution on microbar_tf)")
        print(f"   p50: {stats['abs_delta_price']['p50']:.4f}")
        print(f"   p80: {stats['abs_delta_price']['p80']:.4f}")
        print(f"   p95: {stats['abs_delta_price']['p95']:.4f}")
        print(f"   (Based on {stats['abs_delta_price']['count']} microbars)")

        print("\n2. spread_bps")
        print(f"   p50: {stats['spread_bps']['p50']:.4f}")
        print(f"   p90: {stats['spread_bps']['p90']:.4f}")
        print(f"   p99: {stats['spread_bps']['p99']:.4f}")
        print(f"   (Based on {stats['spread_bps']['count']} ticks/sec)")

        print("\n3. book_rate_hz")
        print(f"   p50: {stats['book_rate_hz']['p50']:.1f}")
        print(f"   p10: {stats['book_rate_hz']['p10']:.1f}")
        print(f"   (Based on {stats['book_rate_hz']['count']} secs)")

        print("\n4. Signals/min (Pre-cooldown/Raw)")
        print(f"   Avg: {stats['signals_per_min']['avg']:.2f}")
        print(f"   Max: {stats['signals_per_min']['max']:.2f}")
        print(f"   Total: {stats['signals_per_min']['total']}")

    if r_main: await r_main.aclose()
    if r_ticks: await r_ticks.aclose()

if __name__ == "__main__":
    asyncio.run(main())
