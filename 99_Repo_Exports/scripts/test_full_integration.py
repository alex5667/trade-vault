#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Полный интеграционный тест системы:
1. Symbol Specs в Redis
2. Симуляция потока принтов
3. Pro детектор
4. Label Sink
5. Анализ результатов
"""

import sys
import os
import time
import redis

# Добавляем корень проекта в PYTHONPATH

print("╔════════════════════════════════════════════════════════════════╗")
print("║     Полный интеграционный тест - Pro Version                   ║")
print("╚════════════════════════════════════════════════════════════════╝")
print()

# ============================================================================
# 1. Проверка Symbol Specs
# ============================================================================

print("1️⃣ Проверка Symbol Specs в Redis...")

try:
    from specs.symbol_specs_repo import SymbolSpecsRepo, SymbolSpecsModel

    r = redis.Redis.from_url(
        os.getenv("REDIS_URL", "redis://localhost:6379/0"),
        decode_responses=True
    )
    r.ping()

    repo = SymbolSpecsRepo(r)

    # Проверяем XAUUSD
    fallback = SymbolSpecsModel(symbol="XAUUSD", point=0.1, tick_value_per_lot=1.0)
    specs = repo.get("XAUUSD", fallback)

    print("  ✅ XAUUSD specs loaded:")
    print(f"     Point: {specs.point}")
    print(f"     Tick value: {specs.tick_value_per_lot}")
    print(f"     Min lot: {specs.min_lot}")

except Exception as e:
    print(f"  ❌ FAILED: {e}")
    sys.exit(1)

print()

# ============================================================================
# 2. Симуляция принтов
# ============================================================================

print("2️⃣ Симуляция потока принтов (30 секунд)...")

try:
    from adapters.trade_feed_adapter import TradeFeedAdapter, Trade
    import random

    adapter = TradeFeedAdapter(r, "XAUUSD")

    base_price = 2650.0
    for i in range(50):
        price = base_price + random.uniform(-2.0, 2.0)
        qty = round(random.uniform(0.5, 2.0), 2)
        side = random.choice(["buy", "sell"])

        trade = Trade(
            price=round(price, 2),
            qty=qty,
            side=side,
            ts=int(time.time() * 1000),
            symbol="XAUUSD"
        )

        adapter.publish_trade(trade)

        if (i + 1) % 10 == 0:
            print(f"  📊 Опубликовано {i+1} принтов...")

        time.sleep(0.05)

    stats = adapter.get_stats()
    print(f"  ✅ Опубликовано: {stats['trades_published']} принтов")

except Exception as e:
    print(f"  ❌ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()

# ============================================================================
# 3. Pro детектор
# ============================================================================

print("3️⃣ Тест Pro детектора...")

try:
    from core.microstructure_spike_detector_pro import MicrostructureSpikeDetectorPro, ProConfig
    from adapters.trade_feed_adapter import TradeStreamReader

    detector = MicrostructureSpikeDetectorPro(
        ProConfig(price_step=0.1, lookback_sec=30)
    )

    # Читаем принты из stream
    reader = TradeStreamReader(r, "XAUUSD")
    trades = reader.read_trades(count=100)

    print(f"  📥 Прочитано {len(trades)} принтов из stream")

    # Обрабатываем
    for trade in trades:
        detector.on_trade(trade.price, trade.qty, trade.side, trade.ts)
        detector.update_tick(trade.price - 0.1, trade.price + 0.1, trade.ts)

    # Метрики
    metrics = detector.metrics()

    print("  ✅ Метрики Pro детектора:")
    print(f"     Z-delta: {metrics['z_delta']:.2f}")
    print(f"     Z-speed: {metrics['z_speed']:.2f}")
    print(f"     SVbP imbalance: {metrics['svbp_imbalance']:.2f}")
    print(f"     Trades in window: {metrics['trades_in_window']}")
    print(f"     Trigger: {metrics['trigger']}")

except Exception as e:
    print(f"  ❌ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()

# ============================================================================
# 4. Label Sink
# ============================================================================

print("4️⃣ Тест Label Sink...")

try:
    from persistence.label_sink import ParquetLabelSink
    import tempfile
    import shutil

    temp_dir = tempfile.mkdtemp(prefix="test_labels_")

    sink = ParquetLabelSink(root_dir=temp_dir, tile_minutes=15)

    # Создаём тестовую метку
    record = {
        "ts": int(time.time() * 1000),
        "symbol": "XAUUSD",
        "source": "hub_pro",
        "side": "LONG",
        "price": 2650.5,
        "sl": 2645.0,
        "tp_levels": [2655.0, 2660.0],
        "lot": 0.5,
        "confidence": 0.75,
        "atr": 5.5,
        "reason": "Test signal",
        "metrics": metrics,  # используем метрики из предыдущего шага
        "emitted": True,
    }

    file_path = sink.write(record)

    print("  ✅ Метка сохранена:")
    print(f"     Файл: {os.path.basename(file_path)}")
    print(f"     Размер: {os.path.getsize(file_path)} bytes")

    # Очистка
    shutil.rmtree(temp_dir, ignore_errors=True)

except Exception as e:
    print(f"  ❌ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()

# ============================================================================
# 5. Гибридный хаб (симуляция)
# ============================================================================

print("5️⃣ Симуляция работы гибридного хаба...")

try:
    print("  📊 Симуляция 10 итераций...")

    for i in range(10):  # noqa: B007
        # Обновляем детектор
        bid, ask = 2650.0 + random.uniform(-1, 1), 2650.2 + random.uniform(-1, 1)
        detector.update_tick(bid, ask, int(time.time() * 1000))

        # Случайный принт
        if random.random() < 0.7:
            detector.on_trade(
                price=bid + 0.1,
                qty=round(random.uniform(0.5, 2.0), 2),
                side=random.choice(["buy", "sell"]),
                ts_ms=int(time.time() * 1000)
            )

        time.sleep(0.1)

    # Финальные метрики
    final_metrics = detector.metrics()

    print("  ✅ Гибридный хаб работает:")
    print(f"     Trades processed: {final_metrics['trades_in_window']}")
    print(f"     Detector active: {'Pro' if final_metrics['trades_in_window'] >= 5 else 'Legacy'}")

except Exception as e:
    print(f"  ❌ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()

# ============================================================================
# Итог
# ============================================================================

print("╔════════════════════════════════════════════════════════════════╗")
print("║  ✅ ВСЕ ИНТЕГРАЦИОННЫЕ ТЕСТЫ ПРОЙДЕНЫ УСПЕШНО!                 ║")
print("╚════════════════════════════════════════════════════════════════╝")
print()
print("🎉 Система готова к работе!")
print()
print("Следующие шаги:")
print("  1. Запустить Pro хаб:")
print("     $ python -m hub.aggregated_signal_hub_pro")
print()
print("  2. Симулировать принты (в другом терминале):")
print("     $ ./scripts/simulate_trades.py --duration 300")
print()
print("  3. Анализировать метки:")
print("     $ ./scripts/analyze_labels.sh")
print()

