from __future__ import annotations

"""
signal_scoring/_helpers.py
===========================
Shared pure-Python utility functions used across the signal_scoring package.
All functions are stateless, deterministic, and importable without side-effects.
"""

import math
from typing import Any


def is_finite(x: Any) -> bool:
    """Return True if x can be converted to a finite float."""
    try:
        return x is not None and math.isfinite(float(x))
    except Exception:
        return False


def safe_float(x: Any, default: float = 0.0) -> float:
    """Safe cast to float; returns *default* on any failure or non-finite value."""
    try:
        v = float(x)
    except Exception:
        return default
    if not math.isfinite(v):
        return default
    return v


def clamp(x: float, lo: float, hi: float) -> float:
    """Clamp *x* to the closed interval [lo, hi]."""
    if x < lo:
        return lo
    if x > hi:
        return hi
    return x


def clamp01(x: float) -> float:
    """Clamp *x* to [0.0, 1.0] — convenience alias for clamp(x, 0.0, 1.0)."""
    return clamp(x, 0.0, 1.0)


# ---------------------------------------------------------------------------
# Private aliases used by submodules that previously defined local copies.
# Keeping the underscore-prefixed names for backward compatibility inside the
# package (nothing outside the package should call these directly).
# ---------------------------------------------------------------------------
_is_finite = is_finite
_safe_float = safe_float
_clamp = clamp
_clamp01 = clamp01
