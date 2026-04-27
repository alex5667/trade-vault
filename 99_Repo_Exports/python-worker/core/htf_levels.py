# core/htf_levels.py
"""
HTF (Higher Time Frame) levels provider.

Заглушки для совместимости с существующим кодом.
"""

from typing import Dict, List, Optional, Any
from dataclasses import dataclass


@dataclass
class HTFLevels:
    """HTF уровни для символа."""
    symbol: str
    levels: Dict[str, List[float]] = None

    def __post_init__(self):
        if self.levels is None:
            self.levels = {}


class HTFLevelsProvider:
    """Провайдер HTF уровней."""

    def __init__(self):
        self._cache: Dict[str, HTFLevels] = {}

    def get_levels(self, symbol: str) -> Optional[HTFLevels]:
        """
        Получить HTF уровни для символа.

        Пока возвращает пустые уровни для совместимости.
        """
        if symbol not in self._cache:
            self._cache[symbol] = HTFLevels(symbol=symbol)

        return self._cache[symbol]


class RedisHTFLevelsProvider:
    """Провайдер HTF уровней из Redis."""

    def __init__(self, redis_client):
        self.redis = redis_client
        self._cache: Dict[str, HTFLevels] = {}

    def get_levels(self, symbol: str) -> Optional[HTFLevels]:
        """
        Получить HTF уровни для символа из Redis.

        Ключ формата: htf:{symbol}:levels
        """
        try:
            # Попытка получить из кэша
            if symbol in self._cache:
                return self._cache[symbol]

            # Попытка получить из Redis
            key = f"htf:{symbol}:levels"
            levels_data = self.redis.hgetall(key)

            if levels_data:
                # Преобразование данных из Redis в HTFLevels
                levels = {}
                for k, v in levels_data.items():
                    if isinstance(v, bytes):
                        v = v.decode('utf-8')
                    try:
                        levels[k] = float(v)
                    except (ValueError, TypeError):
                        continue

                htf_levels = HTFLevels(symbol=symbol, levels=levels)
                self._cache[symbol] = htf_levels
                return htf_levels
            else:
                # Возвращаем пустые уровни если нет данных в Redis
                htf_levels = HTFLevels(symbol=symbol)
                self._cache[symbol] = htf_levels
                return htf_levels

        except Exception:
            # В случае ошибки возвращаем пустые уровни
            return HTFLevels(symbol=symbol)

