# core/contracts.py
from __future__ import annotations
"""
Strict data contracts using msgspec for Redis -> Python validation.
"""

import msgspec
from typing import Optional

class SymbolSpecs(msgspec.Struct):
    """Спецификации торгового инструмента."""
    symbol=""
    point: float = 1e-8                 # Default to high precision
    tick_value_per_lot: float = 1.0     # $ за 1 point на 1.0 lot
    lot_step: float = 0.01
    min_lot: float = 0.01
    max_lot: float = 10.0
    min_stop_points: float = 10.0       # Минимальное расстояние до SL/TP в пунктах
    contract_size: float = 100000.0     # Размер контракта (от SpecExporter.mq5)
