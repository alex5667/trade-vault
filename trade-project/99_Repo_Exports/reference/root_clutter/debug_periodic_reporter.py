#!/usr/bin/env python3
"""
Diagnostic script for periodic_reporter issues.
Checks configuration, Redis data, and filtering logic.
"""

import os
import sys
from pathlib import Path

# Add python-worker to path
sys.path.insert(0, str(Path(__file__).parent / "python-worker"))

def check_configuration():
    """Check current configuration values."""
    print("🔧 Configuration Check:")

    # Import here to avoid issues
    try:
        from services.periodic_reporter import REPORT_TRIGGER_COUNT, RECENT_WINDOW_SECONDS, TRADE_WINDOW_COUNT
        print(f"  REPORT_TRIGGER_COUNT: {REPORT_TRIGGER_COUNT}")
        print(f"  RECENT_WINDOW_SECONDS: {RECENT_WINDOW_SECONDS}")
        print(f"  TRADE_WINDOW_COUNT: {TRADE_WINDOW_COUNT}")
    except Exception as e:
        print(f"  ❌ Import error: {e}")
        return

    print(f"  REDIS_URL: {os.getenv('REDIS_URL', 'redis://localhost:6381/0')}")

    # Check other relevant env vars
    env_vars = [
        'PERIODIC_REPORT_SEND_EMPTY',
        'PERIODIC_REPORT_MIN_TRADES',
        'PERIODIC_REPORT_TRADE_WINDOW_COUNT',
        'REPORT_TRIGGER_COUNT'
    ]

    print("  Environment variables:")
    for var in env_vars:
        value = os.getenv(var, 'not set')
        print(f"    {var}: {value}")

def check_redis_connection():
    """Test Redis connection."""
    print("\n🔗 Redis Connection Test:")

    try:
        import redis
        r = redis.from_url(os.getenv('REDIS_URL', 'redis://localhost:6381/0'), decode_responses=True)
        r.ping()
        print("  ✅ Redis connection successful")

        # Check some basic keys
        info = r.info('memory')
        print(f"  Memory used: {info.get('used_memory_human', 'unknown')}")

        return r
    except Exception as e:
        print(f"  ❌ Redis connection failed: {e}")
        return None

def check_redis_data(redis_client):
    """Check data in Redis streams and counters."""
    if not redis_client:
        return

    print("\n💾 Redis Data Check:")

    try:
        # Check trades:closed stream
        entries = redis_client.xrevrange("trades:closed", max="+", min="-", count=5) or []
        print(f"  trades:closed stream: {len(entries)} recent entries")

        if entries:
            print("  Recent entries:")
            for entry_id, fields in entries:
                order_id = fields.get("order_id") or fields.get("id") or ""
                source = fields.get("source") or ""
                symbol = fields.get("symbol") or ""
                print(f"    {order_id}: source={source}, symbol={symbol}")

        # Check report counters
        counters = []
        for key in redis_client.scan_iter(match="report_counter:*"):
            count = redis_client.get(key)
            counters.append((key, count))

        print(f"  Report counters: {len(counters)}")
        for key, count in sorted(counters):
            print(f"    {key}: {count}")

        # Check stats:strategies
        strategies = redis_client.smembers("stats:strategies") or set()
        print(f"  stats:strategies: {len(strategies)} - {list(strategies)[:3]}")

        # Check orders:open
        open_orders = redis_client.smembers("orders:open") or set()
        print(f"  orders:open: {len(open_orders)}")

    except Exception as e:
        print(f"  ❌ Redis data check error: {e}")

def main():
    print("🔬 Periodic Reporter Diagnostics")
    print("=" * 50)

    check_configuration()
    redis_client = check_redis_connection()
    check_redis_data(redis_client)

    print("\n" + "=" * 50)
    print("📋 Most Common Issues:")
    print("  1. REPORT_TRIGGER_COUNT=30 (reports every 30 trades)")
    print("  2. PERIODIC_REPORT_SEND_EMPTY=false (no empty reports)")
    print("  3. PERIODIC_REPORT_MIN_TRADES=30 (need 30+ trades)")
    print("  4. Source/symbol filtering mismatch")
    print("  5. No trades in trades:closed stream")
    print("  6. Redis connection issues")

    print("\n🔧 Quick Fixes:")
    print("  export REPORT_TRIGGER_COUNT=1")
    print("  export PERIODIC_REPORT_SEND_EMPTY=true")
    print("  export PERIODIC_REPORT_MIN_TRADES=1")

if __name__ == "__main__":
    main()
