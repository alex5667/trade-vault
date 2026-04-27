# symbol_specs_store.py
"""
Symbol Specs Store - хранение спецификаций торговых инструментов в Redis.
"""
from __future__ import annotations
from dataclasses import dataclass, asdict
from typing import Optional
import json
import os

try:
    import redis
except ImportError:
    redis = None

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")

@dataclass
class SymbolSpecs:
    """Спецификации торгового инструмента."""
    symbol: str = "XAUUSD"
    point: float = 1e-8                 # Default to high precision (was 0.1)
    tick_value_per_lot: float = 1.0    # $ за 1 point на 1.0 lot (CFD/фьюч — разные!)
    lot_step: float = 0.01
    min_lot: float = 0.01
    max_lot: float = 10.0
    min_stop_points: float = 10.0       # Минимальное расстояние до SL/TP в пунктах (point)

class SymbolSpecsStore:
    """Хранилище спецификаций инструментов в Redis."""
    
    def __init__(self, r=None):
        if r is not None:
            self.r = r
        else:
            if not redis:
                raise RuntimeError("redis-py не установлен")
            self.r = redis.Redis.from_url(REDIS_URL, decode_responses=True)

    def _key(self, symbol: str) -> str:
        """Генерирует ключ Redis для символа."""
        return f"symbol_specs:{symbol}"

    def get(self, symbol: str, fallback: Optional[SymbolSpecs] = None) -> SymbolSpecs:
        """Получить спецификации символа из Redis."""
        s = self.r.get(self._key(symbol))
        if not s:
            return fallback or SymbolSpecs(symbol=symbol)
        try:
            data = json.loads(s)
            # Мержим с дефолтными значениями
            default_dict = asdict(SymbolSpecs(symbol=symbol))
            return SymbolSpecs(**{**default_dict, **data})
        except Exception:
            return fallback or SymbolSpecs(symbol=symbol)

    def set(self, specs: SymbolSpecs) -> None:
        """Сохранить спецификации символа в Redis."""
        self.r.set(self._key(specs.symbol), json.dumps(asdict(specs), ensure_ascii=False))

    def ensure_default(self, symbol: str = "XAUUSD") -> SymbolSpecs:
        """Убедиться что спецификации существуют (создать если нет)."""
        key = self._key(symbol)
        if not self.r.get(key):
            self.set(SymbolSpecs(symbol=symbol))
        return self.get(symbol)

