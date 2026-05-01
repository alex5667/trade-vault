from __future__ import annotations
"""utils.helpers — Shared low-level type-coercion helpers.

These tiny helpers are intentionally fail-open (return default on any error)
so upstream callers never crash due to bad Redis values.
"""

from typing import Any


def _f(x: Any, d: float = 0.0) -> float:  # noqa: ANN401
    """Coerce *x* to float, returning default *d* on failure or None."""
    if x is None:
        return d
    try:
        return float(x)
    except (TypeError, ValueError):
        return d



def _i(x: Any, d: int = 0) -> int:  # noqa: ANN401
    """Coerce *x* to int, returning default *d* on failure or None."""
    if x is None:
        return d
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return d


def _sf(x: Any, d: float = 0.0) -> float:
    """Canonical name for safe_float."""
    return _f(x, d)


def _si(x: Any, d: int = 0) -> int:
    """Canonical name for safe_int."""
    return _i(x, d)


def _sb(v: Any, d: bool = False) -> bool:
    """Coerce value to bool (False for 0, '0', 'false', 'none', empty)."""
    if v is None:
        return d
    s = str(v).strip().lower()
    if s in ("1", "true", "yes", "on", "ok"):
        return True
    if s in ("0", "false", "no", "off", "none", ""):
        return False
    return d


def _norm_map(d: dict[Any, Any]) -> dict[str, str]:
    """Convert a Redis hgetall dict to a flat dict of str keys/values."""
    if not d:
        return {}
    res = {}
    for k, v in d.items():
        ks = k.decode() if isinstance(k, bytes) else str(k)
        vs = v.decode() if isinstance(v, bytes) else str(v)
        res[ks] = vs
    return res
