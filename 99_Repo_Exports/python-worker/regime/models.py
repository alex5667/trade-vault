from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from typing import Optional


@dataclass
class SignalExecRow:
    """Строка из таблицы signal_exec_summary"""
    signal_id: int
    symbol: str
    family: str
    opened_at: datetime
    result_r: float


@dataclass
class BaselineQuantiles:
    """Квантили для одной метрики"""
    p05: Optional[float]
    p10: Optional[float]
    p25: Optional[float]
    p50: Optional[float]
    p75: Optional[float]
    p90: Optional[float]
    p95: Optional[float]
    sample_size: int  # сколько окон


@dataclass
class FamilyBaseline:
    """Baseline для одной пары symbol+family"""
    symbol: str
    family: str
    window_size: int
    horizon_days: int
    hit_rate: BaselineQuantiles
    expectancy_r: BaselineQuantiles
    computed_at: datetime
