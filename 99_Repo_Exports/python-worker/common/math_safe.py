from __future__ import annotations

import math
from typing import Any

# NOTE:
# - Hot-path safe helpers: MUST be cheap.
# - Never raise on bad inputs.
# - Always return finite numbers (or None).


def safe_float(x: Any, default: float | None = None) -> float | None:
    """
    Convert to float and ensure math.isfinite().
    Returns default (None by default) when conversion fails or value is NaN/Inf.
    """
    try:
        v = float(x)
    except Exception:
        return default
    return v if math.isfinite(v) else default


def clamp(v: float, lo: float, hi: float) -> float:
    """Clamp finite float into [lo..hi]. Assumes v is finite."""
    if v < lo:
        return lo
    if v > hi:
        return hi
    return v


def clamp01(v: float) -> float:
    """Clamp into [0..1]. Assumes v is finite."""
    if v <= 0.0:
        return 0.0
    if v >= 1.0:
        return 1.0
    return v


def safe_div(num: Any, den: Any, *, default: float = 0.0, eps: float = 1e-12) -> float:
    """
    Safe division returning finite float.
    - If den is 0/None/NaN/Inf -> default.
    - If result becomes NaN/Inf -> default.
    """
    n = safe_float(num, None)
    d = safe_float(den, None)
    if n is None or d is None:
        return default
    if abs(d) <= eps:
        return default
    out = n / d
    return out if math.isfinite(out) else default


def safe_bps_dist(a: Any, b: Any, *, base: Any, default: float | None = None) -> float | None:
    """
    Return abs(a-b)/base*10000 in bps.
    If any part is invalid -> default.
    """
    fa = safe_float(a, None)
    fb = safe_float(b, None)
    fbase = safe_float(base, None)
    if fa is None or fb is None or fbase is None or fbase <= 0:
        return default
    out = abs(fa - fb) / fbase * 10_000.0
    return out if math.isfinite(out) else default


def finite_or(v: Any, fallback: float) -> float:
    """
    Return float(v) if finite else fallback.
    Use this at the very end of pipelines (wire format).
    """
    fv = safe_float(v, None)
    return float(fv) if fv is not None else float(fallback)
