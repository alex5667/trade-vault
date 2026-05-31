#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Диагностика Pro Hub - проверка всех компонентов системы
"""

import sys
import os
import redis
import json
from datetime import datetime, timezone


print("╔════════════════════════════════════════════════════════════════╗")
print("║          Диагностика AggregatedSignalHubPro                    ║")
print("╚════════════════════════════════════════════════════════════════╝")
print()

issues = []
warnings = []

# ============================================================================
# 1. Проверка Redis
# ============================================================================
print("1️⃣ Проверка Redis...")
try:
    r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=True)
    r.ping()
    print("  ✅ Redis доступен")
except Exception as e:
    print(f"  ❌ Redis недоступен: {e}")
    issues.append("Redis не подключён")
    sys.exit(1)

print()

# ============================================================================
# 2. Проверка Symbol Specs
# ============================================================================
print("2️⃣ Проверка Symbol Specs...")
try:
    specs = r.get("symbol_specs:XAUUSD")
    if specs:
        data = json.loads(specs)
        print("  ✅ Specs найдены:")
        print(f"     Point: {data.get('point')}")
        print(f"     Tick value: {data.get('tick_value_per_lot')}")
        print(f"     Min lot: {data.get('min_lot')}")
    else:
        print("  ⚠️  Specs не найдены в Redis")
        warnings.append("Symbol specs отсутствуют - используется config")
except Exception as e:
    print(f"  ⚠️  Ошибка чтения specs: {e}")
    warnings.append("Specs недоступны")

print()

# ============================================================================
# 3. Проверка потока принтов
# ============================================================================
print("3️⃣ Проверка потока принтов...")
try:
    trades_stream = os.getenv("TRADES_STREAM", "trades:XAUUSD")
    stream_len = r.xlen(trades_stream)

    if stream_len > 0:
        print(f"  ✅ Stream найден: {trades_stream}")
        print(f"     Принтов в stream: {stream_len}")

        # Последние 5 принтов
        recent = r.xrevrange(trades_stream, count=5)
        if recent:
            print("     Последний принт:")
            last_id, last_data = recent[0]
            print(f"       ID: {last_id}")
            print(f"       Price: {last_data.get('price', 'N/A')}")
            print(f"       Qty: {last_data.get('qty', 'N/A')}")
            print(f"       Side: {last_data.get('side', 'N/A')}")
    else:
        print(f"  ⚠️  Stream пуст: {trades_stream}")
        warnings.append("Нет принтов - Pro детектор будет использовать legacy mode")
        print("     💡 Запустите: ./scripts/simulate_trades.py --duration 300")
except Exception as e:
    print(f"  ⚠️  Ошибка чтения stream: {e}")
    warnings.append("Trade stream недоступен")

print()

# ============================================================================
# 4. Проверка тиков (market data)
# ============================================================================
print("4️⃣ Проверка market data...")
try:
    # Проверяем последний тик
    tick_key = "tick:XAUUSD"
    tick_data = r.hgetall(tick_key)

    if tick_data:
        print(f"  ✅ Тики найдены: {tick_key}")
        print(f"     Bid: {tick_data.get('bid', 'N/A')}")
        print(f"     Ask: {tick_data.get('ask', 'N/A')}")
        print(f"     Last: {tick_data.get('last', 'N/A')}")

        # Проверяем свежесть
        ts = tick_data.get('ts')
        if ts:
            tick_ts = int(ts) / 1000
            age = datetime.now(timezone.utc).timestamp() - tick_ts
            if age < 60:
                print(f"     ⏱️  Свежесть: {age:.1f}s (OK)")
            else:
                print(f"     ⚠️  Свежесть: {age:.1f}s (устарели!)")
                warnings.append(f"Тики устарели ({age:.0f}s)")
    else:
        print(f"  ❌ Тики не найдены: {tick_key}")
        issues.append("Нет market data - хаб не может работать")
except Exception as e:
    print(f"  ❌ Ошибка чтения тиков: {e}")
    issues.append("Market data недоступна")

print()

# ============================================================================
# 5. Проверка ATR
# ============================================================================
print("5️⃣ Проверка ATR...")
try:
    atr_key = "atr:XAUUSD"
    atr = r.get(atr_key)

    if atr:
        atr_val = float(atr)
        print(f"  ✅ ATR найден: {atr_val:.2f}")
        if atr_val < 0.1:
            warnings.append(f"ATR слишком мал ({atr_val}) - возможны проблемы с sizing")
    else:
        print("  ⚠️  ATR не найден")
        warnings.append("ATR отсутствует - sizing может быть некорректным")
except Exception as e:
    print(f"  ⚠️  Ошибка чтения ATR: {e}")

print()

# ============================================================================
# 6. Проверка DOM
# ============================================================================
print("6️⃣ Проверка DOM...")
try:
    dom_key = "dom:XAUUSD"
    dom_data = r.get(dom_key)

    if dom_data:
        dom = json.loads(dom_data)
        print("  ✅ DOM найден")
        print(f"     Bid levels: {len([x for x in dom if x.get('side') == 'bid'])}")
        print(f"     Ask levels: {len([x for x in dom if x.get('side') == 'ask'])}")
    else:
        print("  ⚠️  DOM не найден")
        warnings.append("DOM отсутствует - cluster analysis недоступен")
except Exception as e:
    print(f"  ⚠️  Ошибка чтения DOM: {e}")

print()

# ============================================================================
# 7. Проверка конфигурации
# ============================================================================
print("7️⃣ Проверка конфигурации...")
try:
    from infra.config import load_config
    cfg = load_config()

    print("  ✅ Config загружен:")
    print(f"     Symbol: {cfg.symbol}")
    print(f"     Poll interval: {cfg.poll_ms}ms")
    print(f"     Z delta threshold: {cfg.z_delta_thr}")
    print(f"     Cooldown: {cfg.cooldown_sec}s")

    # Проверяем критические параметры
    if cfg.point <= 0:
        issues.append(f"Point некорректен: {cfg.point}")
    if cfg.tick_value_per_lot <= 0:
        issues.append(f"Tick value некорректен: {cfg.tick_value_per_lot}")

except Exception as e:
    print(f"  ❌ Ошибка загрузки config: {e}")
    issues.append("Config не загружается")

print()

# ============================================================================
# 8. Проверка недавних сигналов
# ============================================================================
print("8️⃣ Проверка недавних сигналов...")
try:
    notify_stream = os.getenv("NOTIFY_STREAM", "signals:notify")
    recent_signals = r.xrevrange(notify_stream, count=5)

    if recent_signals:
        print(f"  ✅ Найдено сигналов: {len(recent_signals)}")
        for msg_id, data in recent_signals[:3]:
            print(f"     Signal ID: {msg_id}")
            print(f"       Text: {data.get('text', 'N/A')[:60]}...")
    else:
        print("  ℹ️  Недавних сигналов нет")
        warnings.append("Сигналы не генерируются - см. ниже возможные причины")
except Exception as e:
    print(f"  ⚠️  Ошибка чтения сигналов: {e}")

print()

# ============================================================================
# 9. Проверка директории labels
# ============================================================================
print("9️⃣ Проверка директории labels...")
try:
    labels_dir = os.getenv("LABEL_PARQUET_DIR", "/data/labels")
    if os.path.exists(labels_dir):
        print(f"  ✅ Директория существует: {labels_dir}")

        # Считаем файлы
        import subprocess
        result = subprocess.run(
            f"find {labels_dir} -name '*.parquet' -o -name '*.csv' 2>/dev/null | wc -l",
            shell=True,
            capture_output=True,
            text=True
        )
        count = int(result.stdout.strip())
        print(f"     Меток сохранено: {count}")
    else:
        print(f"  ⚠️  Директория не существует: {labels_dir}")
        warnings.append("Label directory не создана - автоматически создастся при первом сигнале")
except Exception as e:
    print(f"  ⚠️  Ошибка проверки labels: {e}")

print()

# ============================================================================
# Итоги
# ============================================================================
print("═" * 70)
print()

if issues:
    print("❌ КРИТИЧЕСКИЕ ПРОБЛЕМЫ:")
    for i, issue in enumerate(issues, 1):
        print(f"   {i}. {issue}")
    print()

if warnings:
    print("⚠️  ПРЕДУПРЕЖДЕНИЯ:")
    for i, warning in enumerate(warnings, 1):
        print(f"   {i}. {warning}")
    print()

if not issues and not warnings:
    print("✅ ВСЁ В ПОРЯДКЕ!")
    print()
    print("Хаб должен генерировать сигналы.")
    print("Если сигналов всё равно нет, проверьте:")
    print("  - Confidence >= 0.6 (порог эмиссии)")
    print("  - Cooldown не блокирует новые сигналы")
    print("  - Логи хаба на наличие ошибок")
else:
    print()
    print("🔧 РЕКОМЕНДАЦИИ:")
    print()

    if "Redis не подключён" in issues:
        print("1. Запустите Redis:")
        print("   $ redis-server")

    if "Нет market data" in issues:
        print("2. Запустите источник market data (тики, DOM):")
        print("   $ python -m signal-generator/signal_generator.py")

    if "Нет принтов" in [w for w in warnings if "принтов" in w]:
        print("3. Симулируйте принты для Pro детектора:")
        print("   $ ./scripts/simulate_trades.py --duration 300")

    if "Symbol specs отсутствуют" in warnings:
        print("4. Инициализируйте specs:")
        print("   $ ./scripts/init_symbol_specs.sh")

    print()
    print("После устранения проблем запустите хаб:")
    print("  $ python -m hub.aggregated_signal_hub_pro")

print()
print("═" * 70)


