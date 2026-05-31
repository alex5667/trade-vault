from __future__ import annotations

"""
RedisDeduper - защита от повторной обработки сообщений.

Использует Redis SET NX EX для атомарной проверки и установки ключа.
- True  => "первый раз", можно выполнять сайд-эффекты
- False => дубликат, сайд-эффекты запрещены, но msg нужно ACK-нуть

Применение:
- Signals: защита от повторного открытия позиций
- Events: защита от повторного применения трейлинга
- Report triggers: защита от спама отчетами

Senior Developer + Trading Analyst (40 years exp)
"""

import os

import redis


class RedisDeduper:
    """
    Дедупликация по ключу: SET key value NX EX ttl
    - True  => "первый раз", можно выполнять сайд-эффекты
    - False => дубликат, сайд-эффекты запрещены, но msg нужно ACK-нуть
    """

    def __init__(self, r: redis.Redis, prefix: str = "dedup"):
        """
        Инициализация deduper.
        
        Args:
            r: Redis клиент
            prefix: префикс для ключей дедупликации
        """
        self.r = r
        self.prefix = prefix

    def key(self, *parts: str) -> str:
        """
        Формирует ключ дедупликации из частей.
        
        Args:
            *parts: части ключа
            
        Returns:
            Полный ключ дедупликации
        """
        safe = [p.replace(" ", "_") for p in parts if p]
        return f"{self.prefix}:" + ":".join(safe)

    def acquire(self, key: str, ttl_s: int, value: str = "1") -> bool:
        """
        Атомарно проверяет и устанавливает ключ дедупликации.
        
        Args:
            key: ключ дедупликации
            ttl_s: время жизни ключа в секундах
            value: значение ключа (обычно timestamp)
            
        Returns:
            True если ключ установлен (первая обработка)
            False если ключ уже существует (дубликат)
        """
        # redis-py: set(name, value, nx=True, ex=ttl)
        return bool(self.r.set(key, value, nx=True, ex=ttl_s))


def env_int(name: str, default: int) -> int:
    """
    Получает целочисленное значение из переменной окружения.
    
    Args:
        name: имя переменной окружения
        default: значение по умолчанию
        
    Returns:
        Целочисленное значение
    """
    try:
        return int(os.getenv(name, str(default)))
    except Exception:
        return default

