#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Скрипт для симуляции потока принтов.
Используется для тестирования Pro детектора.
"""

import redis
import time
import random
import argparse
import os
from adapters.trade_feed_adapter import TradeFeedAdapter, Trade


def simulate_trades(
    symbol: str = "XAUUSD",
    duration_sec: int = 60,
    trades_per_sec: float = 5.0,
    base_price: float = 2650.0,
    volatility: float = 0.5,
    redis_url: str = "redis://localhost:6379/0"
):
    """
    Симулирует поток принтов для тестирования.

    Args:
        symbol: символ инструмента
        duration_sec: длительность симуляции в секундах
        trades_per_sec: количество принтов в секунду
        base_price: базовая цена
        volatility: волатильность (шаг цены)
        redis_url: URL Redis
    """
    print(f"🚀 Симуляция принтов для {symbol}")
    print(f"   Длительность: {duration_sec}s")
    print(f"   Скорость: {trades_per_sec} trades/sec")
    print(f"   Базовая цена: {base_price}")
    print(f"   Волатильность: {volatility}")
    print()

    # Подключение к Redis
    r = redis.Redis.from_url(redis_url)
    adapter = TradeFeedAdapter(r, symbol)

    # Параметры симуляции
    delay = 1.0 / trades_per_sec
    start_time = time.time()
    end_time = start_time + duration_sec

    price = base_price
    trades_count = 0

    print("Генерация принтов...\n")

    try:
        while time.time() < end_time:
            # Случайное изменение цены
            price_change = random.uniform(-volatility, volatility)
            price += price_change
            price = round(price, 2)

            # Случайный объём
            qty = round(random.uniform(0.5, 3.0), 2)

            # Случайная сторона (с лёгким смещением в зависимости от направления)
            if price_change > 0:
                side = "buy" if random.random() < 0.6 else "sell"
            elif price_change < 0:
                side = "sell" if random.random() < 0.6 else "buy"
            else:
                side = random.choice(["buy", "sell"])

            # Создаём принт
            trade = Trade(
                price=price,
                qty=qty,
                side=side,
                ts=int(time.time() * 1000),
                symbol=symbol
            )

            # Публикуем
            success = adapter.publish_trade(trade)

            if success:
                trades_count += 1
                emoji = "🟢" if side == "buy" else "🔴"
                print(f"{emoji} Trade {trades_count}: {side.upper():4s} {qty:.2f}@{price:.2f}")

            time.sleep(delay)

    except KeyboardInterrupt:
        print("\n⚠️  Остановлено пользователем")

    # Статистика
    stats = adapter.get_stats()
    elapsed = time.time() - start_time

    print("\n📊 Статистика:")
    print(f"   Опубликовано: {stats['trades_published']} принтов")
    print(f"   Ошибок: {stats['errors']}")
    print(f"   Время работы: {elapsed:.1f}s")
    print(f"   Средняя скорость: {stats['trades_published']/elapsed:.1f} trades/sec")
    print("\n✅ Симуляция завершена!")


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Simulate trade feed for testing")
    parser.add_argument("--symbol", default="XAUUSD", help="Symbol")
    parser.add_argument("--duration", type=int, default=60, help="Duration in seconds")
    parser.add_argument("--trades-per-sec", type=float, default=5.0, help="Trades per second")
    parser.add_argument("--base-price", type=float, default=2650.0, help="Base price")
    parser.add_argument("--volatility", type=float, default=0.5, help="Price volatility")
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))

    args = parser.parse_args()

    simulate_trades(
        symbol=args.symbol,
        duration_sec=args.duration,
        trades_per_sec=args.trades_per_sec,
        base_price=args.base_price,
        volatility=args.volatility,
        redis_url=args.redis_url
    )

