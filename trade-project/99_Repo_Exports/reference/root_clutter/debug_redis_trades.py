#!/usr/bin/env python3
"""
Debug script to check Redis trades data and understand why PeriodicReporter
can't find OrderFlow/BTCUSDT trades.
"""

import redis
import sys
import os

# Add python-worker to path
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'python-worker'))

from domain.normalizers import canon_source, canon_symbol

def main():
    # Connect to Redis
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = redis.Redis.from_url(redis_url, decode_responses=True)

    print(f"🔍 Checking Redis: {redis_url}")
    print("="*60)

    # Check stats:strategies
    print("📊 stats:strategies:")
    strategies = r.smembers("stats:strategies") or set()
    for strategy in sorted(strategies):
        print(f"  - {strategy}")

        # Check symbols for this strategy
        symbols_key = f"stats:symbols:{strategy}"
        symbols = r.smembers(symbols_key) or set()
        if symbols:
            print(f"    symbols: {sorted(symbols)}")
    print()

    # Check trades:closed stream
    print("📊 trades:closed stream (last 20 entries):")
    entries = r.xrevrange("trades:closed", max="+", count=20) or []

    if not entries:
        print("  No entries found!")
        return

    print(f"  Total entries: {len(entries)}")
    print()

    for i, (entry_id, fields) in enumerate(entries):
        print(f"  Entry {i+1} (id: {entry_id}):")
        for key, value in fields.items():
            print(f"    {key}: {value}")

        # Show normalized values
        raw_source = fields.get("source") or fields.get("strategy") or ""
        raw_symbol = fields.get("symbol") or ""
        norm_source = canon_source(raw_source)
        norm_symbol = canon_symbol(raw_symbol)

        print(f"    -> normalized: source='{norm_source}', symbol='{norm_symbol}'")

        # Check if this matches OrderFlow/BTCUSDT
        matches = (norm_source == "OrderFlow" and norm_symbol == "BTCUSDT")
        print(f"    -> matches OrderFlow/BTCUSDT: {matches}")
        print()

        if i >= 4:  # Show only first 5 entries
            break

    # Check for OrderFlow/BTCUSDT specifically
    print("🔍 Checking for OrderFlow/BTCUSDT specifically:")
    orderflow_count = 0
    btusdt_count = 0

    for entry_id, fields in entries:
        raw_source = fields.get("source") or fields.get("strategy") or ""
        raw_symbol = fields.get("symbol") or ""
        norm_source = canon_source(raw_source)
        norm_symbol = canon_symbol(raw_symbol)

        if norm_source == "OrderFlow":
            orderflow_count += 1
        if norm_symbol == "BTCUSDT":
            btusdt_count += 1

    print(f"  OrderFlow entries: {orderflow_count}")
    print(f"  BTCUSDT entries: {btcusdt_count}")
    print(f"  OrderFlow + BTCUSDT entries: {orderflow_count if orderflow_count == btusdt_count else 'mismatch'}")

if __name__ == "__main__":
    main()
