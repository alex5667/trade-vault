from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime


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
    p05: float | None
    p10: float | None
    p25: float | None
    p50: float | None
    p75: float | None
    p90: float | None
    p95: float | None
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
