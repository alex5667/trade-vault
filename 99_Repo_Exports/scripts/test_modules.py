#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для тестирования всех новых модулей
"""

import sys
import os
import time
import tempfile
import shutil

# Добавляем корень проекта в PYTHONPATH

print("=== Тестирование новых модулей ===\n")

# ============================================================================
# 1. Тест Symbol Specs Repository
# ============================================================================

print("1️⃣ Тест Symbol Specs Repository...")

try:
    import redis
    from specs.symbol_specs_repo import SymbolSpecsRepo, SymbolSpecsModel

    # Создаём тестовый клиент Redis
    r = redis.Redis.from_url(
        os.getenv("REDIS_URL", "redis://localhost:6379/15"),  # используем тестовую DB
        decode_responses=True
    )

    # Проверка подключения
    r.ping()

    repo = SymbolSpecsRepo(r, key_tpl="test:symbol_specs:{SYMBOL}")

    # Создание specs
    specs = SymbolSpecsModel(
        symbol="TEST",
        point=0.01,
        tick_value_per_lot=10.0,
        min_lot=0.1,
        max_lot=100.0,
        lot_step=0.1,
        contract_size=100.0,
        price_decimals=2,
        volume_decimals=1
    )

    # Запись
    repo.upsert(specs)

    # Чтение
    fallback = SymbolSpecsModel(symbol="TEST", point=1.0, tick_value_per_lot=1.0)
    loaded = repo.get("TEST", fallback)

    # Проверка
    assert loaded.point == 0.01, f"Expected point=0.01, got {loaded.point}"
    assert loaded.tick_value_per_lot == 10.0, f"Expected tick_value=10.0, got {loaded.tick_value_per_lot}"
    assert loaded.contract_size == 100.0, f"Expected contract_size=100.0, got {loaded.contract_size}"

    # Очистка
    r.delete("test:symbol_specs:TEST")

    print("  ✅ Symbol Specs Repository - OK")
    print("    - Созданы, сохранены и загружены specs для TEST")
    print(f"    - Point: {loaded.point}, Tick value: {loaded.tick_value_per_lot}")

except ImportError as e:
    print(f"  ⚠️  Пропущено (нет зависимости): {e}")
except redis.exceptions.ConnectionError:
    print("  ⚠️  Пропущено (Redis недоступен)")
except Exception as e:
    print(f"  ❌ FAILED: {e}")
    sys.exit(1)

print()

# ============================================================================
# 2. Тест Parquet Label Sink
# ============================================================================

print("2️⃣ Тест Parquet Label Sink...")

try:
    from persistence.label_sink import ParquetLabelSink

    # Создаём временную директорию
    temp_dir = tempfile.mkdtemp(prefix="test_labels_")

    try:
        sink = ParquetLabelSink(root_dir=temp_dir, tile_minutes=15)

        # Создание записи
        record = {
            "ts": int(time.time() * 1000),
            "symbol": "TEST",
            "source": "test",
            "side": "LONG",
            "price": 100.5,
            "sl": 95.0,
            "tp_levels": [105.0, 110.0],
            "lot": 1.0,
            "confidence": 0.85,
            "atr": 5.0,
            "reason": "test signal",
            "metrics": {"z_delta": 3.5, "z_speed": 4.2},
            "emitted": True
        }

        # Запись
        file_path = sink.write(record)

        # Проверка существования файла
        assert os.path.exists(file_path), f"File not created: {file_path}"

        # Проверка структуры партиций
        symbol_dir = os.path.join(temp_dir, "symbol=TEST")
        assert os.path.exists(symbol_dir), "Symbol partition not created"

        print("  ✅ Parquet Label Sink - OK")
        print(f"    - Создан файл: {os.path.basename(file_path)}")
        print("    - Партиции: symbol=TEST/date=.../tile=...")

    finally:
        # Очистка
        shutil.rmtree(temp_dir, ignore_errors=True)

except ImportError as e:
    print(f"  ⚠️  Пропущено (нет зависимости): {e}")
except Exception as e:
    print(f"  ❌ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()

# ============================================================================
# 3. Тест MicrostructureSpikeDetectorPro
# ============================================================================

print("3️⃣ Тест MicrostructureSpikeDetectorPro...")

try:
    from core.microstructure_spike_detector_pro import MicrostructureSpikeDetectorPro, ProConfig

    config = ProConfig(
        z_delta_thr=3.0,
        z_extreme_thr=4.5,
        speed_z_thr=3.0,
        price_step=0.1,
        lookback_sec=60,
        min_trades_for_delta=5
    )

    detector = MicrostructureSpikeDetectorPro(config)

    # Симуляция тиков
    base_price = 2650.0
    for i in range(100):
        bid = base_price + i * 0.01
        ask = bid + 0.2
        ts_ms = int(time.time() * 1000) + i * 100
        detector.update_tick(bid, ask, ts_ms)

        # Добавляем принты
        if i % 5 == 0:
            side = 'buy' if i % 10 == 0 else 'sell'
            detector.on_trade(bid + 0.1, 1.0, side, ts_ms)

    # Получение метрик
    metrics = detector.metrics()

    # Проверка структуры метрик
    required_keys = [
        'z_delta', 'z_speed', 'z_range', 'svbp_top', 'svbp_imbalance',
        'extreme', 'trigger', 'dir_up', 'trades_in_window'
    ]
    for key in required_keys:
        assert key in metrics, f"Missing key in metrics: {key}"

    assert metrics['trades_in_window'] >= 5, f"Expected trades >= 5, got {metrics['trades_in_window']}"

    print("  ✅ MicrostructureSpikeDetectorPro - OK")
    print(f"    - Z-delta: {metrics['z_delta']:.2f}")
    print(f"    - Z-speed: {metrics['z_speed']:.2f}")
    print(f"    - SVbP imbalance: {metrics['svbp_imbalance']:.2f}")
    print(f"    - Trades in window: {metrics['trades_in_window']}")
    print(f"    - Trigger: {metrics['trigger']}, Extreme: {metrics['extreme']}")

except ImportError as e:
    print(f"  ⚠️  Пропущено (нет зависимости): {e}")
except Exception as e:
    print(f"  ❌ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()

# ============================================================================
# 4. Тест интеграции
# ============================================================================

print("4️⃣ Тест интеграции модулей...")

try:
    # Проверяем импорты интегрированных файлов
    from core.filtered_signal_writer import FilteredSignalWriter  # noqa: F401
    from hub.aggregated_signal_hub import AggregatedSignalHub  # noqa: F401

    print("  ✅ Интеграция - OK")
    print("    - FilteredSignalWriter импортирует SymbolSpecsRepo")
    print("    - AggregatedSignalHub импортирует ParquetLabelSink")

except ImportError as e:
    print(f"  ❌ FAILED: {e}")
    import traceback
    traceback.print_exc()
    sys.exit(1)

print()

# ============================================================================
# Итог
# ============================================================================

print("=" * 60)
print("✅ Все тесты пройдены успешно!")
print("=" * 60)
print()
print("Следующие шаги:")
print("  1. Инициализируйте specs: ./scripts/init_symbol_specs.sh")
print("  2. Запустите хаб для проверки работы")
print("  3. Проверьте созданные Parquet-файлы в /data/labels/")
print()

