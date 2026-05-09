from __future__ import annotations

import math
from typing import Any


def f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return default
        return v
    except Exception:
        return default


def nz(x: Any) -> bool:
    try:
        return bool(x)
    except Exception:
        return False


def infer_side(ctx: Any, fallback: int = 0) -> int:
    s = getattr(ctx, "side", None)
    if isinstance(s, (int, float)):
        sv = float(s)
        return 1 if sv > 0 else (-1 if sv < 0 else 0)
    d = f(getattr(ctx, "direction", None), 0.0)
    if d > 0:
        return 1
    if d < 0:
        return -1
    return int(fallback)
