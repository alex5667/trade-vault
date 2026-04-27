from __future__ import annotations

import math
from typing import Any


def _is_finite(x: float) -> bool:
    return isinstance(x, (int, float)) and math.isfinite(float(x))


def clamp(x: float, lo: float, hi: float) -> float:
    v = float(x)
    if v < lo:
        return lo
    if v > hi:
        return hi
    return v


def normalize_confidence_pct(x: Any) -> float:
    """
    Canonical representation across the codebase:
      confidence_pct ∈ [0..100]

    Backward-compat heuristic:
      - if 0 <= x <= 1.0001 -> treat as ratio and convert to pct (x*100)
      - else treat as pct
    """
    try:
        v = float(x)
    except Exception:
        return 0.0
    if not _is_finite(v):
        return 0.0

    if 0.0 <= v <= 1.0001:
        v = v * 100.0
    return clamp(v, 0.0, 100.0)


def confidence_pct_to_ratio(pct: Any) -> float:
    """Convert confidence_pct [0..100] to ratio [0..1]."""
    p = normalize_confidence_pct(pct)
    return clamp(p / 100.0, 0.0, 1.0)
