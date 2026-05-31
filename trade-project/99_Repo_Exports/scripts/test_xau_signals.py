#!/usr/bin/env python3
"""
Тестовый скрипт для проверки генерации сигналов XAU OrderFlow Handler.
"""

import json
import time
from dataclasses import dataclass

import redis
from signals.pivots import compute_daily_pivots, is_near_level_atr

def get_redis():
    return redis.from_url("redis://localhost:6379/0", decode_responses=True)

def get_dual_signals_redis():
    return get_redis()  # Используем тот же клиент для простоты


@dataclass
class TestTick:
    """Тестовая структура данных тика."""
    ts: int
    bid: float
    ask: float
    last: float
    volume: float
    flags: int


def test_pivot_initialization():
    """Тест инициализации pivot данных."""
    print("🧪 Тест 1: Инициализация pivot данных")

    # Подключение к Redis
    redis_client = get_redis()

    # Проверяем pivot данные в Redis
    pivot_json = redis_client.get("pivots:latest")
    print(f"   pivots:latest в Redis: {pivot_json}")

    if pivot_json:
        hlc = json.loads(pivot_json)
        print(f"   Parsed HLC: {hlc}")

        # Вычисляем pivot уровни
        pivots = compute_daily_pivots(hlc)
        print(f"   Calculated pivots: {pivots}")
        return pivots
    else:
        print("   ❌ Нет pivot данных в Redis")
        return None


def test_signal_conditions(pivots):
    """Тест условий генерации сигналов."""
    print("\n🧪 Тест 2: Условия генерации сигналов")

    if not pivots:
        print("   ❌ Нет pivot данных - сигналы не будут генерироваться")
        return False

    # Тестовые значения
    test_price = 3977.5  # Между нашими test pivot'ами
    test_atr = 3.0
    test_z_delta = 4.0  # Выше threshold (0.5)
    test_weak_progress = True

    print(f"   Тестовая цена: {test_price}")
    print(f"   ATR: {test_atr}")
    print(f"   Z-delta: {test_z_delta}")
    print(f"   Weak progress: {test_weak_progress}")

    # Проверяем близость к уровню
    near_level = is_near_level_atr(test_price, pivots, test_atr, 0.5)
    print(f"   Близко к уровню: {near_level}")

    # Условие для ABSORPTION сигнала
    absorption_condition = (
        test_weak_progress and
        abs(test_z_delta) >= 0.5 and  # delta_z_threshold
        near_level
    )
    print(f"   Условие ABSORPTION: {absorption_condition}")

    # Условие для BREAKOUT сигнала
    breakout_condition = abs(test_z_delta) >= 0.5
    print(f"   Условие BREAKOUT: {breakout_condition}")

    return absorption_condition or breakout_condition


def test_signal_publishing():
    """Тест публикации сигнала."""
    print("\n🧪 Тест 3: Публикация тестового сигнала")

    dual_redis = get_dual_signals_redis()

    # Формируем тестовый сигнал
    test_signal = {
        "text": "🧪 TEST XAUUSD LONG @ 3977.50, Volume 0.10 lot. Manual test signal",
        "sid": f"{int(time.time() * 1000)}:LONG:397750",
        "side": "LONG",
        "price": "3977.50",
        "lot": "0.10",
        "note": "Manual test signal",
        "risk": {
            "sl": 3974.50,
            "tp_levels": [3980.50, 3983.50],
            "rr": [1.0, 2.0],
            "atr": 3.0,
            "stop_dist": 3.0,
            "mode": "ATR"
        }
    }

    # Конвертируем для Redis
    redis_payload = {}
    for key, value in test_signal.items():
        if isinstance(value, dict):
            redis_payload[key] = json.dumps(value)
        else:
            redis_payload[key] = str(value)

    try:
        # Публикуем тестовый сигнал
        message_id = dual_redis.xadd(
            "notify:telegram",
            redis_payload,
            maxlen=500,
            approximate=True
        )
        print(f"   ✅ Тестовый сигнал опубликован: {message_id}")
        print(f"   📤 Payload: {test_signal['text']}")
        return True

    except Exception as e:
        print(f"   ❌ Ошибка публикации: {e}")
        return False


def main():
    """Основная функция тестирования."""
    print("🚀 Запуск диагностики XAU OrderFlow Handler")
    print("=" * 60)

    # Тест 1: Pivot данные
    pivots = test_pivot_initialization()

    # Тест 2: Условия сигналов
    conditions_ok = test_signal_conditions(pivots)

    # Тест 3: Публикация сигнала
    publishing_ok = test_signal_publishing()

    print("\n" + "=" * 60)
    print("📋 РЕЗУЛЬТАТЫ ДИАГНОСТИКИ:")
    print(f"   Pivot данные: {'✅' if pivots else '❌'}")
    print(f"   Условия сигналов: {'✅' if conditions_ok else '❌'}")
    print(f"   Публикация: {'✅' if publishing_ok else '❌'}")

    if pivots and conditions_ok and publishing_ok:
        print("\n🎉 Все тесты пройдены! Проблема может быть в другом месте.")
        print("\n💡 РЕКОМЕНДАЦИИ:")
        print("   1. Проверьте notify-worker логи: docker logs scanner-notify-worker")
        print("   2. Проверьте telegram-worker логи: docker logs scanner-telegram-worker")
        print("   3. Проверьте настройки Telegram бота")
    else:
        print("\n❌ Найдены проблемы. Исправьте их для работы сигналов.")


if __name__ == "__main__":
    main()


