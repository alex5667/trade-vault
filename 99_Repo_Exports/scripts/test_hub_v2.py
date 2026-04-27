#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Тестовый скрипт для проверки работы Aggregated Signal Hub V2.

Использование:
    python3 scripts/test_hub_v2.py --mode=mock      # Тест с моковыми данными
    python3 scripts/test_hub_v2.py --mode=redis     # Тест чтения из Redis
    python3 scripts/test_hub_v2.py --mode=step      # Тест manual step
"""

import sys
import os
import time
import json
from pathlib import Path

# Добавляем пути к модулям
project_root = Path(__file__).parent.parent

try:
    from aggregated_signal_hub_v2 import AggregatedSignalHubV2, HubConfig
    import redis
except ImportError as e:
    print(f"❌ Import error: {e}")
    print("Убедитесь, что находитесь в правильной директории и установлены зависимости")
    print(f"\nProject root: {project_root}")
    print(f"Python path: {sys.path[:3]}")
    sys.exit(1)


def test_mock_data():
    """Тест с моковыми данными (без Redis)."""
    print("=" * 80)
    print("TEST: Mock Data Mode")
    print("=" * 80)

    # Конфигурация для теста (без Redis streams)
    cfg = HubConfig(
        symbol="XAUUSD",
        confidence_threshold=0.55,  # Низкий порог для тестирования
        min_signal_interval_sec=5,   # Короткий интервал для тестов
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        tick_stream=None,   # Отключаем streams
        prints_stream=None
    )

    try:
        hub = AggregatedSignalHubV2(cfg)
    except Exception as e:
        print(f"❌ Hub initialization failed: {e}")
        return False

    print("\n✅ Hub V2 initialized successfully")
    print(f"   - Pro detector: {'✅' if hub.det_pro else '❌'}")
    print(f"   - Legacy detector: {'✅' if hub.det_legacy else '❌'}")
    print(f"   - Cluster analyzer: {'✅' if hub.cluster else '❌'}")
    print(f"   - Writer: {'✅' if hub.writer else '❌'}")

    # Генерируем моковые данные
    print("\n" + "=" * 80)
    print("Generating mock trades and ticks...")
    print("=" * 80)

    base_price = 2045.0
    base_ts = int(time.time() * 1000)

    signals_generated = 0

    for i in range(100):
        ts_ms = base_ts + (i * 1000)  # 1 секунда между тиками

        # Симулируем волатильность
        price_delta = (i % 20 - 10) * 0.1  # Zigzag
        bid = base_price + price_delta
        ask = bid + 0.05
        atr = 2.5

        # Feed принты (имитация агрессивных покупок/продаж)
        if i % 5 == 0:
            side = "buy" if i % 10 == 0 else "sell"
            qty = 0.5 + (i % 3) * 0.3
            hub.on_trade(price=bid if side == "buy" else ask, qty=qty, side=side, ts_ms=ts_ms)
            print(f"  [{i:3d}] Trade: {side.upper():4s} {qty:.2f} @ {bid:.2f}")

        # Обрабатываем тик
        snap = {
            "ts": ts_ms,
            "bid": bid,
            "ask": ask,
            "atr": atr,
            "mid": (bid + ask) / 2.0
        }

        result = hub.step(snap)

        if result:
            signals_generated += 1
            print("\n" + "🔔" * 40)
            print(f"✅ SIGNAL #{signals_generated}")
            print(f"   Side: {result['side']}")
            print(f"   Confidence: {result['confidence']:.2%}")
            print(f"   Reason: {result['reason']}")
            print("🔔" * 40 + "\n")

    print("\n" + "=" * 80)
    print("TEST COMPLETE")
    print("  Ticks processed: 100")
    print(f"  Signals generated: {signals_generated}")
    print("=" * 80)

    return True


def test_redis_connection():
    """Тест подключения к Redis и чтения данных."""
    print("=" * 80)
    print("TEST: Redis Connection")
    print("=" * 80)

    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    print(f"\nConnecting to: {redis_url}")

    try:
        r = redis.Redis.from_url(redis_url, decode_responses=True)
        r.ping()
        print("✅ Redis connection OK")
    except Exception as e:
        print(f"❌ Redis connection failed: {e}")
        return False

    # Проверяем наличие streams
    symbol = os.getenv("SYMBOL", "XAUUSD")
    tick_stream = os.getenv("TICK_STREAM", f"ticks:{symbol}")
    prints_stream = os.getenv("PRINTS_STREAM", f"prints:{symbol}")

    print("\nChecking streams:")
    print(f"  - Tick stream: {tick_stream}")

    try:
        info = r.xinfo_stream(tick_stream)
        print(f"    ✅ Exists: {info['length']} messages")
    except Exception as e:
        print(f"    ⚠️  Not found or empty: {e}")

    print(f"  - Prints stream: {prints_stream}")
    try:
        info = r.xinfo_stream(prints_stream)
        print(f"    ✅ Exists: {info['length']} messages")
    except Exception as e:
        print(f"    ⚠️  Not found or empty: {e}")

    # Проверяем DOM key
    book_key = f"book:levels:{symbol}"
    print(f"\nChecking DOM key: {book_key}")
    try:
        dom_data = r.get(book_key)
        if dom_data:
            levels = json.loads(dom_data)
            print(f"    ✅ DOM data available: {len(levels)} levels")
        else:
            print("    ⚠️  No DOM data")
    except Exception as e:
        print(f"    ⚠️  Error reading DOM: {e}")

    print("\n" + "=" * 80)
    print("Redis check complete")
    print("=" * 80)

    return True


def test_manual_step():
    """Тест ручной обработки шагов."""
    print("=" * 80)
    print("TEST: Manual Step Processing")
    print("=" * 80)

    cfg = HubConfig(
        symbol="XAUUSD",
        confidence_threshold=0.50,
        min_signal_interval_sec=1,
        redis_url=os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        tick_stream=None,
        prints_stream=None
    )

    try:
        hub = AggregatedSignalHubV2(cfg)
    except Exception as e:
        print(f"❌ Hub initialization failed: {e}")
        return False

    print("✅ Hub initialized")

    # Тестовые сценарии
    scenarios = [
        {
            "name": "Bullish momentum",
            "trades": [
                {"side": "buy", "price": 2045.0, "qty": 0.5},
                {"side": "buy", "price": 2045.1, "qty": 0.8},
                {"side": "buy", "price": 2045.2, "qty": 1.2},
            ],
            "snap": {"bid": 2045.3, "ask": 2045.35, "atr": 2.5}
        },
        {
            "name": "Bearish momentum",
            "trades": [
                {"side": "sell", "price": 2045.0, "qty": 0.6},
                {"side": "sell", "price": 2044.9, "qty": 0.9},
                {"side": "sell", "price": 2044.8, "qty": 1.0},
            ],
            "snap": {"bid": 2044.7, "ask": 2044.75, "atr": 2.5}
        },
        {
            "name": "Choppy market",
            "trades": [
                {"side": "buy", "price": 2045.0, "qty": 0.5},
                {"side": "sell", "price": 2045.0, "qty": 0.5},
                {"side": "buy", "price": 2045.0, "qty": 0.5},
            ],
            "snap": {"bid": 2045.0, "ask": 2045.05, "atr": 2.5}
        },
    ]

    for idx, scenario in enumerate(scenarios, 1):
        print(f"\n{'='*60}")
        print(f"Scenario {idx}: {scenario['name']}")
        print('='*60)

        ts_ms = int(time.time() * 1000) + (idx * 10000)

        # Feed trades
        for trade in scenario['trades']:
            hub.on_trade(
                price=trade['price'],
                qty=trade['qty'],
                side=trade['side'],
                ts_ms=ts_ms
            )
            print(f"  Trade: {trade['side'].upper():4s} {trade['qty']:.2f} @ {trade['price']:.2f}")
            ts_ms += 100

        # Process snapshot
        snap = scenario['snap']
        snap['ts'] = ts_ms
        snap['mid'] = (snap['bid'] + snap['ask']) / 2.0

        print(f"\n  Processing snapshot: bid={snap['bid']:.2f} ask={snap['ask']:.2f}")

        result = hub.step(snap)

        if result:
            print("\n  ✅ SIGNAL GENERATED:")
            print(f"     Side: {result['side']}")
            print(f"     Confidence: {result['confidence']:.2%}")
            print(f"     Reason: {result['reason'][:100]}")
        else:
            print("\n  ⚠️  No signal (filtered)")

        time.sleep(2)  # Пауза между сценариями

    print("\n" + "=" * 80)
    print("Manual step test complete")
    print("=" * 80)

    return True


def main():
    """Главная функция."""
    import argparse

    parser = argparse.ArgumentParser(description="Test Aggregated Signal Hub V2")
    parser.add_argument(
        "--mode",
        choices=["mock", "redis", "step", "all"],
        default="all",
        help="Test mode"
    )
    args = parser.parse_args()

    print("\n")
    print("╔" + "═" * 78 + "╗")
    print("║" + " " * 20 + "Aggregated Signal Hub V2 - Test Suite" + " " * 21 + "║")
    print("╚" + "═" * 78 + "╝")
    print()

    success = True

    if args.mode in ["mock", "all"]:
        if not test_mock_data():
            success = False
        print("\n")

    if args.mode in ["redis", "all"]:
        if not test_redis_connection():
            success = False
        print("\n")

    if args.mode in ["step", "all"]:
        if not test_manual_step():
            success = False
        print("\n")

    if success:
        print("✅ All tests passed!")
        return 0
    else:
        print("❌ Some tests failed")
        return 1


if __name__ == "__main__":
    sys.exit(main())

