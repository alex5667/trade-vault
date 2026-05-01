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
    """Coerce *x* to int (via float), returning default *d* on failure or None."""
    if x is None:
        return d
    try:
        return int(float(x))
    except (TypeError, ValueError):
        return d
