from __future__ import annotations

"""Derived flow features (A4).

We derive two low-cost, low-cardinality features from already available
L3-lite EMA rates and top-of-book depth aggregates:

1) liquidity_pressure
   (taker_buy_rate_ema + taker_sell_rate_ema) / depth_total_10

   Intuition: how quickly aggressive flow can consume current visible depth.
   Units: 1/sec (since numerator is qty/sec, denominator is qty).

2) info_flow (toxicity proxy, VPIN-like)
   |taker_buy_rate_ema - taker_sell_rate_ema| / (taker_buy_rate_ema + taker_sell_rate_ema)

   Range: [0..1], where values closer to 1 indicate highly one-sided aggressive flow.

Guard / determinism rules:
- Negative rates are clamped to 0 (rates should be >=0; this is defensive).
- If depth_total_10 is missing or too small -> liquidity_pressure = 0 (fail-open).
- If sum rates is too small -> info_flow = 0 (no directional dominance).
- All outputs are finite and bounded.
"""

import math


def compute_liquidity_pressure_and_info_flow(
    *,
    taker_buy_rate_ema: float,
    taker_sell_rate_ema: float,
    depth_total_10: float,
    eps_depth: float = 1e-6,
    eps_rate: float = 1e-12,
    clip_liquidity_pressure: float = 1e6,
) -> tuple[float, float]:
    """Return (liquidity_pressure, info_flow)."""
    try:
        buy = float(taker_buy_rate_ema or 0.0)
        sell = float(taker_sell_rate_ema or 0.0)
        depth = float(depth_total_10 or 0.0)
    except Exception:
        return 0.0, 0.0

    if not math.isfinite(buy):
        buy = 0.0
    if not math.isfinite(sell):
        sell = 0.0
    if not math.isfinite(depth):
        depth = 0.0

    if buy < 0.0:
        buy = 0.0
    if sell < 0.0:
        sell = 0.0
    if depth < 0.0:
        depth = 0.0

    s = buy + sell

    # Info flow (toxicity proxy) is always in [0..1]
    if s <= eps_rate:
        info = 0.0
    else:
        info = abs(buy - sell) / s
        if info < 0.0:
            info = 0.0
        elif info > 1.0:
            info = 1.0

    # Liquidity pressure is non-negative; treat missing depth as no-data -> 0.
    if depth <= eps_depth:
        lp = 0.0
    else:
        lp = s / depth
        if not math.isfinite(lp) or lp < 0.0:
            lp = 0.0
        elif lp > clip_liquidity_pressure:
            lp = clip_liquidity_pressure

    return float(lp), float(info)
