from __future__ import annotations

"""TCA math primitives (Phase B).

We isolate formulas from IO so they are:
  - unit-testable
  - deterministic
  - reusable in both online gates and post-trade workers

All functions return values in **basis points (bps)**.
"""

import math
from typing import Optional


def _finite(x: Optional[float]) -> Optional[float]:
    if x is None:
        return None
    try:
        f = float(x)
    except Exception:
        return None
    if not math.isfinite(f):
        return None
    return float(f)


def side_sign(side: str) -> int:
    """Map side/direction to sign.

    +1: BUY/LONG
    -1: SELL/SHORT
    """
    s = (side or "").strip().upper()
    if s in {"LONG", "BUY"}:
        return +1
    if s in {"SHORT", "SELL"}:
        return -1
    # Unknown side -> neutral; caller should handle.
    return 0


def effective_spread_bps(*, trade_px: float, mid_t: float, side: str) -> Optional[float]:
    """Effective spread in bps.

    Standard microstructure proxy: 2*(P_trade - mid_t)/mid_t for buys;
    2*(mid_t - P_trade)/mid_t for sells.
    """
    px = _finite(trade_px)
    mid = _finite(mid_t)
    ss = side_sign(side)
    if px is None or mid is None or mid <= 0 or ss == 0:
        return None
    return float(2.0 * ss * (px - mid) / mid * 10_000.0)


def realized_spread_bps(*, trade_px: float, mid_t: float, mid_t_delta: float, side: str) -> Optional[float]:
    """Realized spread in bps for horizon Δ.

    Uses mid at t+Δ; denominator is mid_t to keep scale consistent.
    """
    px = _finite(trade_px)
    mid = _finite(mid_t)
    mid_d = _finite(mid_t_delta)
    ss = side_sign(side)
    if px is None or mid is None or mid <= 0 or mid_d is None or ss == 0:
        return None
    return float(2.0 * ss * (px - mid_d) / mid * 10_000.0)


def permanent_impact_bps(*, mid_t: float, mid_t_delta: float, side: str) -> Optional[float]:
    """Permanent (price) impact proxy in bps.

    Positive means mid moved in the direction of the trade and persisted.
    """
    mid = _finite(mid_t)
    mid_d = _finite(mid_t_delta)
    ss = side_sign(side)
    if mid is None or mid <= 0 or mid_d is None or ss == 0:
        return None
    return float(ss * (mid_d - mid) / mid * 10_000.0)


def implementation_shortfall_bps(
    *
    vwap_fill_px: float
    decision_mid: float
    side: str
    fee_bps: float = 0.0
) -> Optional[float]:
    """Implementation Shortfall (IS) in bps.

    Simplified version (full fill):
      IS = side_sign * (VWAP_fill - decision_mid) / decision_mid * 1e4 + fee_bps

    fee_bps should reflect *execution fees* for the fill (best-effort).
    """
    px = _finite(vwap_fill_px)
    mid0 = _finite(decision_mid)
    fee = _finite(fee_bps)
    ss = side_sign(side)
    if px is None or mid0 is None or mid0 <= 0 or ss == 0:
        return None
    fee = fee if fee is not None else 0.0
    return float(ss * (px - mid0) / mid0 * 10_000.0 + float(fee))
