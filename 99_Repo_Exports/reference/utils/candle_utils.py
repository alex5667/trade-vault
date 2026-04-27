"""utils.candle_utils — Lightweight candle/kline computation helpers.

All functions are pure (no I/O) and safe for use in hot paths.
"""
from __future__ import annotations


def calc_volatility(kline: dict[str, str]) -> float:
    """Return candle volatility as ``(high - low) / open * 100`` (percent).

    Args:
        kline: Dict with Binance-style kline fields (``'h'``, ``'l'``, ``'o'``).

    Returns:
        Volatility in percent. Returns 0.0 when *open* is zero.

    Formula: ``(high - low) / open * 100``
    """
    high = float(kline["h"])
    low = float(kline["l"])
    open_price = float(kline["o"])
    if open_price == 0.0:
        return 0.0
    return (high - low) / open_price * 100


def average(values: list[float]) -> float:
    """Return the arithmetic mean of *values*.

    Args:
        values: List of numeric values.

    Returns:
        Mean value, or ``0.0`` for an empty list.
    """
    if not values:
        return 0.0
    return sum(values) / len(values)
