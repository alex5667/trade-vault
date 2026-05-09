from utils.time_utils import get_ny_time_millis

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Simple Market Data Generator - генерирует тики и ATR для Hub Pro.

Запуск:
    python3 services/simple_market_data_generator.py

ENV:
    REDIS_URL - URL Redis (default: redis://localhost:6379/0)
    SYMBOL - символ (default: XAUUSD)
    BASE_PRICE - базовая цена (default: 2650.0)
    VOLATILITY - волатильность (default: 1.0)
    UPDATE_INTERVAL_MS - интервал обновления в мс (default: 500)
"""

import os
import random
import sys
import time
from collections import deque

import redis

# Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
SYMBOL = os.getenv("SYMBOL", "XAUUSD")
BASE_PRICE = float(os.getenv("BASE_PRICE", "2650.0"))
VOLATILITY = float(os.getenv("VOLATILITY", "1.0"))
UPDATE_INTERVAL_MS = int(os.getenv("UPDATE_INTERVAL_MS", "500"))
ATR_PERIOD = int(os.getenv("ATR_PERIOD", "14"))

print("╔════════════════════════════════════════════════════════════════╗")
print("║        Simple Market Data Generator                            ║")
print("╚════════════════════════════════════════════════════════════════╝")
print()
print(f"Symbol: {SYMBOL}")
print(f"Base price: {BASE_PRICE}")
print(f"Volatility: {VOLATILITY}")
print(f"Update interval: {UPDATE_INTERVAL_MS}ms")
print(f"ATR period: {ATR_PERIOD}")
print()


class SimpleATR:
    """Простой расчет ATR для генерации реалистичного значения"""

    def __init__(self, period: int = 14):
        self.period = period
        self.high_low_ranges = deque(maxlen=period)
        self.value: float | None = None

    def update(self, high: float, low: float) -> float:
        """Обновить ATR"""
        hl_range = high - low
        self.high_low_ranges.append(hl_range)

        if len(self.high_low_ranges) >= self.period:
            self.value = sum(self.high_low_ranges) / len(self.high_low_ranges)
        elif self.value is None:
            # Начальное значение
            self.value = sum(self.high_low_ranges) / len(self.high_low_ranges)
        else:
            # Сглаживание
            alpha = 1.0 / self.period
            self.value = (1 - alpha) * self.value + alpha * hl_range

        return self.value


class MarketDataGenerator:
    """Генератор market data для Hub Pro"""

    def __init__(self):
        self.r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
        self.symbol = SYMBOL
        self.price = BASE_PRICE
        self.volatility = VOLATILITY
        self.atr_calc = SimpleATR(ATR_PERIOD)

        # Проверка подключения
        self.r.ping()
        print("✅ Redis подключён")
        print()

    def generate_tick(self) -> dict:
        """Генерирует один тик"""
        # Случайное изменение цены
        change = random.gauss(0, self.volatility)
        self.price += change

        # Bid/Ask spread
        spread = random.uniform(0.1, 0.3)
        bid = round(self.price - spread / 2, 2)
        ask = round(self.price + spread / 2, 2)
        last = round(self.price, 2)

        # Для ATR нужны High/Low
        high = round(last + random.uniform(0, 0.5), 2)
        low = round(last - random.uniform(0, 0.5), 2)

        # Обновляем ATR
        atr = self.atr_calc.update(high, low)

        ts = get_ny_time_millis()

        return {
            "bid": bid,
            "ask": ask,
            "last": last,
            "high": high,
            "low": low,
            "ts": ts,
            "atr": atr if atr else 5.0  # Фолбэк значение
        }

    def publish_tick(self, tick: dict):
        """Публикует тик в Redis"""
        try:
            # Сохраняем тик
            tick_key = f"tick:{self.symbol}"
            self.r.hset(tick_key, mapping={
                "bid": str(tick["bid"]),
                "ask": str(tick["ask"]),
                "last": str(tick["last"]),
                "ts": str(tick["ts"])
            })

            # Сохраняем ATR
            atr_key = f"atr:{self.symbol}"
            self.r.set(atr_key, str(round(tick["atr"], 4)))

            # Также сохраняем в простой ключ для Hub Pro
            # (Hub Pro ожидает именно такой формат)
            self.r.set("atr:XAUUSD", str(round(tick["atr"], 4)))

        except Exception as e:
            print(f"❌ Ошибка публикации: {e}")

    def run(self):
        """Основной цикл генерации"""
        print("🚀 Генерация market data запущена...")
        print(f"   Keys: tick:{self.symbol}, atr:{self.symbol}")
        print()

        stats = {"ticks": 0, "start_time": time.time()}

        try:
            while True:
                tick = self.generate_tick()
                self.publish_tick(tick)

                stats["ticks"] += 1

                # Логируем каждые 20 тиков
                if stats["ticks"] % 20 == 0:
                    elapsed = time.time() - stats["start_time"]
                    rate = stats["ticks"] / elapsed if elapsed > 0 else 0

                    print(f"📊 Tick #{stats['ticks']}: "
                          f"Bid={tick['bid']:.2f}, Ask={tick['ask']:.2f}, "
                          f"Last={tick['last']:.2f}, ATR={tick['atr']:.4f} "
                          f"({rate:.1f} t/s)")

                # Задержка
                time.sleep(UPDATE_INTERVAL_MS / 1000.0)

        except KeyboardInterrupt:
            print("\n⚠️  Остановлено пользователем")
            elapsed = time.time() - stats["start_time"]
            print("\n📊 Статистика:")
            print(f"   Тиков сгенерировано: {stats['ticks']}")
            print(f"   Время работы: {elapsed:.1f}s")
            print(f"   Средняя скорость: {stats['ticks']/elapsed:.1f} ticks/sec")


if __name__ == "__main__":
    try:
        generator = MarketDataGenerator()
        generator.run()
    except Exception as e:
        print(f"❌ Критическая ошибка: {e}")
        import traceback
        traceback.print_exc()
        sys.exit(1)


