from __future__ import annotations

"""
Единая санитизация чисел для входных данных (ticks/ctx/L2/L3).

Цель:
  - NaN/Inf не должны "пробивать" пайплайн и ломать сравнения/метрики/скоринг.
  - Все вероятностные/score-поля держим в строгих диапазонах (0..1, 0..100).
"""

import math
from typing import Any, Optional


def is_finite_number(x: Any) -> bool:
    try:
        return isinstance(x, (int, float)) and math.isfinite(float(x))
    except Exception:
        return False


def finite_float(x: Any, default: Optional[float] = None) -> Optional[float]:
    """
    Приводит к float и отбрасывает NaN/Inf.
    Возвращает default (обычно None) если значение непригодно.
    """
    try:
        v = float(x)
    except Exception:
        return default
    if not math.isfinite(v):
        return default
    return v


def clamp(v: float, lo: float, hi: float) -> float:
    if v < lo:
        return lo
    if v > hi:
        return hi
    return v


def clamp01(x: Any, default: float = 0.0) -> float:
    v = finite_float(x, default=default)
    if v is None:
        v = default
    return clamp(float(v), 0.0, 1.0)


def clamp100(x: Any, default: float = 0.0) -> float:
    v = finite_float(x, default=default)
    if v is None:
        v = default
    return clamp(float(v), 0.0, 100.0)


def safe_div(num: Any, den: Any, default: float = 0.0) -> float:
    n = finite_float(num, default=None)
    d = finite_float(den, default=None)
    if n is None or d is None or d == 0.0:
        return float(default)
    out = n / d
    if not math.isfinite(out):
        return float(default)
    return float(out)
