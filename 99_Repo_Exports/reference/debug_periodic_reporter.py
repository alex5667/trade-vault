#!/usr/bin/env python3
"""
Diagnostic script for periodic_reporter issues.
Checks configuration, Redis data, and filtering logic.
"""

import os
import sys
from pathlib import Path

# Add current directory to path
sys.path.insert(0, str(Path(__file__).parent))

from services.periodic_reporter import (
    REPORT_TRIGGER_COUNT, RECENT_WINDOW_SECONDS, TRADE_WINDOW_COUNT,
    PeriodicReporter, _norm_map, _to_str, _si, _normalize_ts_ms
)
from domain.normalizers import canon_source, canon_symbol

def check_configuration():
    """Check current configuration values."""
    print("🔧 Configuration Check:")
    print(f"  REPORT_TRIGGER_COUNT: {REPORT_TRIGGER_COUNT}")
    print(f"  RECENT_WINDOW_SECONDS: {RECENT_WINDOW_SECONDS}")
    print(f"  TRADE_WINDOW_COUNT: {TRADE_WINDOW_COUNT}")
    print(f"  REDIS_URL: {os.getenv('REDIS_URL', 'redis://redis-worker-1:6379/0')}")

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

def check_redis_data():
    """Check data in Redis streams and counters."""
    try:
        reporter = PeriodicReporter()
        redis = reporter.redis

        print("\n💾 Redis Data Check:")

        # Check trades:closed stream
        entries = redis.xrevrange("trades:closed", max="+", min="-", count=5) or []
        print(f"  trades:closed stream: {len(entries)} recent entries")

        if entries:
            print("  Recent entries:")
            for entry_id, fields in entries:
                t = _norm_map(fields or {})
                order_id = t.get("order_id") or t.get("id") or ""
                raw_source = t.get("source") or ""
                raw_strategy = t.get("strategy") or ""
                t_source = canon_source(raw_source or raw_strategy or "")
                t_symbol = canon_symbol(t.get("symbol") or "")
                print(f"    {order_id}: source={t_source}, symbol={t_symbol}")

        # Check report counters
        counters = []
        for key in redis.scan_iter(match="report_counter:*"):
            count = redis.get(key)
            counters.append((key, count))

        print(f"  Report counters: {len(counters)}")
        for key, count in sorted(counters):
            print(f"    {key}: {count}")

        # Check stats:strategies
        strategies = redis.smembers("stats:strategies") or set()
        print(f"  stats:strategies: {len(strategies)} - {list(strategies)[:5]}")

        # Check orders:open
        open_orders = redis.smembers("orders:open") or set()
        print(f"  orders:open: {len(open_orders)}")

    except Exception as e:
        print(f"  ❌ Redis error: {e}")

def test_filtering_logic():
    """Test the filtering logic with sample data."""
    print("\n🔍 Filtering Logic Test:")

    # Test data samples
    test_cases = [
        {
            "source": "CryptoOrderFlow",
            "symbol": "ETHUSDT",
            "fields": {"source": "CryptoOrderFlow", "symbol": "ETHUSDT"}
        },
        {
            "source": "OrderFlow",
            "symbol": "BTCUSDT",
            "fields": {"source": "OrderFlow", "symbol": "BTCUSDT"}
        },
        {
            "source": "TechnicalAnalysis",
            "symbol": "XAUUSD",
            "fields": {"strategy": "ta", "symbol": "XAUUSD"}
        }
    ]

    for i, test_case in enumerate(test_cases):
        source_param = test_case["source"]
        symbol_param = test_case["symbol"]
        fields = test_case["fields"]

        # Simulate filtering logic
        t = _norm_map(fields)
        raw_source = t.get("source") or ""
        raw_strategy = t.get("strategy") or ""
        t_source = canon_source(raw_source or raw_strategy or "")
        t_symbol = canon_symbol(t.get("symbol") or "")

        matches = t_source == source_param and t_symbol == symbol_param

        print(f"  Test {i+1}: {source_param}/{symbol_param}")
        print(f"    Raw: source='{raw_source}', strategy='{raw_strategy}', symbol='{t.get('symbol')}'")
        print(f"    Canonical: source='{t_source}', symbol='{t_symbol}'")
        print(f"    Match: {matches}")

def simulate_trigger():
    """Simulate the trigger logic."""
    print("\n🚀 Trigger Simulation:")

    if REPORT_TRIGGER_COUNT <= 0:
        print("  ❌ REPORT_TRIGGER_COUNT <= 0, triggers disabled")
        return

    # Simulate some triggers
    test_pairs = [
        ("CryptoOrderFlow", "ETHUSDT"),
        ("OrderFlow", "BTCUSDT"),
        ("TechnicalAnalysis", "XAUUSD")
    ]

    try:
        reporter = PeriodicReporter()

        for source, symbol in test_pairs:
            print(f"\n  Testing {source}/{symbol}:")

            # Check current counter
            src = canon_source(source)
            sym = canon_symbol(symbol)
            counter_key = f"report_counter:trades:{src}:{sym}"

            current_count = int(reporter.redis.get(counter_key) or 0)
            print(f"    Current counter: {current_count}")

            # Check if would trigger
            would_trigger = (current_count + 1) % REPORT_TRIGGER_COUNT == 0
            next_trigger = REPORT_TRIGGER_COUNT - ((current_count + 1) % REPORT_TRIGGER_COUNT)

            print(f"    Would trigger on next trade: {would_trigger}")
            print(f"    Trades until next trigger: {next_trigger}")

            # Test metrics gathering
            try:
                metrics = reporter._gather_window_metrics_stream(src, sym)
                total_trades = int(metrics.get("total_trades", 0))
                print(f"    Current trades in window: {total_trades}")
            except Exception as e:
                print(f"    ❌ Metrics gathering failed: {e}")

    except Exception as e:
        print(f"  ❌ Simulation error: {e}")

def main():
    print("🔬 Periodic Reporter Diagnostics")
    print("=" * 50)

    check_configuration()
    check_redis_data()
    test_filtering_logic()
    simulate_trigger()

    print("\n" + "=" * 50)
    print("📋 Recommendations:")
    print("  1. If counters are 0: No trades are being processed")
    print("  2. If trades exist but counters low: Check REPORT_TRIGGER_COUNT (default 30)")
    print("  3. If filtering fails: Check source/symbol normalization")
    print("  4. If metrics empty: Check Redis stream data structure")
    print("  5. Set PERIODIC_REPORT_SEND_EMPTY=true to debug empty reports")
    print("  6. Set REPORT_TRIGGER_COUNT=1 for testing")

if __name__ == "__main__":
    main()
