from __future__ import annotations


def dist_bps(price: float, level: float) -> float:
    """
    Basis points distance between current price and a reference level.

    bps = |price - level| / price * 10_000

    Why normalize by price:
    - stable and intuitive for trading thresholds,
    - avoids surprises if 'level' is far away or (theoretically) near zero.
    """
    if price <= 0:
        return float("inf")
    return abs(price - level) / price * 10_000.0
