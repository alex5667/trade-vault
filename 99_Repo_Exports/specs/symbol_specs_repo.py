# -*- coding: utf-8 -*-
"""
SymbolSpecsRepo — хранит/читает спецификации инструмента в Redis:
  ключ: symbol_specs:{SYMBOL}
  JSON: {
    "point": 0.1,
    "tick_value_per_lot": 1.0,
    "min_lot": 0.01,
    "max_lot": 10.0,
    "lot_step": 0.01,
    "contract_size": 100.0,          # опционально
    "price_decimals": 1,              # опционально
    "volume_decimals": 2              # опционально
  }

Безопасно мержит неполные payload'ы c ENV-фолбэком.
"""

from __future__ import annotations

import json
import logging
from dataclasses import dataclass
from typing import Any, Dict, Optional

import redis

log = logging.getLogger(__name__)


@dataclass
class SymbolSpecsModel:
    symbol: str
    point: float
    tick_value_per_lot: float
    min_lot: float = 0.01
    max_lot: float = 10.0
    lot_step: float = 0.01
    contract_size: float = 0.0
    price_decimals: int = 1
    volume_decimals: int = 2

    @staticmethod
    def from_dict(
        symbol: str,
        d: Dict[str, Any],
        fallback: SymbolSpecsModel,
    ) -> SymbolSpecsModel:
        def g(name: str, default: Any) -> Any:
            v = d.get(name, None)
            return default if v is None else v

        return SymbolSpecsModel(
            symbol=symbol,
            point=float(g("point", fallback.point)),
            tick_value_per_lot=float(
                g("tick_value_per_lot", fallback.tick_value_per_lot)
            ),
            min_lot=float(g("min_lot", fallback.min_lot)),
            max_lot=float(g("max_lot", fallback.max_lot)),
            lot_step=float(g("lot_step", fallback.lot_step)),
            contract_size=float(g("contract_size", fallback.contract_size)),
            price_decimals=int(g("price_decimals", fallback.price_decimals)),
            volume_decimals=int(g("volume_decimals", fallback.volume_decimals)),
        )

    def to_dict(self) -> Dict[str, Any]:
        return {
            "point": self.point,
            "tick_value_per_lot": self.tick_value_per_lot,
            "min_lot": self.min_lot,
            "max_lot": self.max_lot,
            "lot_step": self.lot_step,
            "contract_size": self.contract_size,
            "price_decimals": self.price_decimals,
            "volume_decimals": self.volume_decimals,
        }


class SymbolSpecsRepo:
    def __init__(self, r: redis.Redis, key_tpl: str = "symbol_specs:{SYMBOL}"):
        self.r = r
        self.key_tpl = key_tpl

    def _key(self, symbol: str) -> str:
        return self.key_tpl.replace("{SYMBOL}", symbol)

    def get(self, symbol: str, fallback: SymbolSpecsModel) -> SymbolSpecsModel:
        key = self._key(symbol)
        v = self.r.get(key)
        if not v:
            return fallback
        try:
            d = json.loads(v)
            return SymbolSpecsModel.from_dict(symbol, d, fallback)
        except (json.JSONDecodeError, ValueError, KeyError, TypeError) as exc:
            log.warning("Failed to parse symbol specs for %s: %s", symbol, exc)
            return fallback

    def upsert(self, specs: SymbolSpecsModel, ttl_sec: Optional[int] = None) -> None:
        key = self._key(specs.symbol)
        j = json.dumps(specs.to_dict(), ensure_ascii=False)
        if ttl_sec and ttl_sec > 0:
            self.r.setex(key, ttl_sec, j)
        else:
            self.r.set(key, j)
