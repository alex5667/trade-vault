# -*- coding: utf-8 -*-
"""
Утилита для заливки спецификаций инструментов в Redis.
Используется для инициализации specs из фида/адаптера.
"""

import redis
import json
import os


def get_redis_client() -> redis.Redis:
    """Создает Redis-клиент из ENV."""
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    return redis.Redis.from_url(redis_url, decode_responses=True)


def push_specs(
    symbol: str,
    point: float,
    tick_value_per_lot: float,
    min_lot: float = 0.01,
    max_lot: float = 10.0,
    lot_step: float = 0.01,
    **extra
) -> str:
    """
    Записывает спецификации инструмента в Redis.
    
    Args:
        symbol: Символ инструмента (например, "XAUUSD")
        point: Размер минимального шага цены
        tick_value_per_lot: Стоимость одного тика на лот
        min_lot: Минимальный размер лота
        max_lot: Максимальный размер лота
        lot_step: Шаг изменения лота
        **extra: Дополнительные поля (contract_size, price_decimals, volume_decimals и т.д.)
    
    Returns:
        str: Ключ в Redis, куда записаны данные
    """
    r = get_redis_client()
    key = f"symbol_specs:{symbol}"
    
    payload = {
        "point": point,
        "tick_value_per_lot": tick_value_per_lot,
        "min_lot": min_lot,
        "max_lot": max_lot,
        "lot_step": lot_step,
    }
    payload.update(extra or {})
    
    r.set(key, json.dumps(payload, ensure_ascii=False))
    return key


if __name__ == "__main__":
    # Пример использования
    import sys
    
    if len(sys.argv) < 4:
        print("Usage: python push_specs.py <symbol> <point> <tick_value_per_lot> [min_lot] [max_lot] [lot_step]")
        print("Example: python push_specs.py XAUUSD 0.1 1.0 0.01 10.0 0.01")
        sys.exit(1)
    
    symbol = sys.argv[1]
    point = float(sys.argv[2])
    tick_value = float(sys.argv[3])
    min_lot = float(sys.argv[4]) if len(sys.argv) > 4 else 0.01
    max_lot = float(sys.argv[5]) if len(sys.argv) > 5 else 10.0
    lot_step = float(sys.argv[6]) if len(sys.argv) > 6 else 0.01
    
    key = push_specs(symbol, point, tick_value, min_lot, max_lot, lot_step)
    print(f"✓ Specs pushed to Redis: {key}")

