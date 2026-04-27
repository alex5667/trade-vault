"""Book geometry features (Phase C / P2).

Adds bounded microstructure liquidity geometry on top of spread/depth:
- book slope (liquidity gradient): USD per bps on each side
- depth-weighted spread (DWS): VWAP(asks)-VWAP(bids) within X bps, in bps
- notional within X bps: reachable notional (USD) within a price band

Hot-path constraints:
- deterministic, bounded CPU/memory
- uses top-N levels only (this project keeps top5 in BookSnapshot)
- fail-open: returns 0.0 on bad inputs
"""

from __future__ import annotations

import math
from typing import Any, Iterable, List, Optional, Sequence, Tuple

Level = Tuple[float, float]  # (price, qty)


def _safe_f(v: Any) -> Optional[float]:
    try:
        x = float(v)
    except Exception:
        return None
    if not math.isfinite(x):
        return None
    return float(x)


def _iter_levels(levels: Any) -> Iterable[Level]:
    if not levels or not isinstance(levels, (list, tuple)):
        return
    for it in levels:
        try:
            px = _safe_f(it[0])
            qty = _safe_f(it[1])
        except Exception:
            continue
        if px is None or qty is None or px <= 0 or qty <= 0:
            continue
        yield (float(px), float(qty))


def _mid_from_bbo(best_bid: float, best_ask: float) -> float:
    if best_bid > 0 and best_ask > 0:
        return 0.5 * (best_bid + best_ask)
    return 0.0


def calc_cost_to_cross(levels: Sequence[Level], mid: float, *, xbps: float) -> float:
    """Total notional (USD) available within X bps from mid on ONE side."""
    mid = float(mid or 0.0)
    if mid <= 0 or xbps <= 0:
        return 0.0
    band = float(xbps)
    out = 0.0
    for px, qty in _iter_levels(levels):
        dist_bps = abs(px - mid) / mid * 10_000.0
        if dist_bps <= band + 1e-9:
            out += px * qty
    if not math.isfinite(out) or out < 0:
        return 0.0
    return float(out)


def calc_book_slope(bids: Sequence[Level], asks: Sequence[Level], mid: float, *, eps_bps: float = 0.10) -> Tuple[float, float]:
    """Liquidity gradient (USD per bps) for each side."""
    mid = float(mid or 0.0)
    if mid <= 0:
        return (0.0, 0.0)

    def _side(levels: Sequence[Level]) -> float:
        s = 0.0
        for px, qty in _iter_levels(levels):
            dist_bps = abs(px - mid) / mid * 10_000.0
            dist_bps = max(float(dist_bps), float(eps_bps))
            s += (px * qty) / dist_bps
        if not math.isfinite(s) or s < 0:
            return 0.0
        return float(s)

    return (_side(bids), _side(asks))


def calc_depth_weighted_spread(bids: Sequence[Level], asks: Sequence[Level], mid: float, *, xbps: float = 5.0) -> float:
    """Depth-weighted spread (DWS) within X bps around mid, returned in bps."""
    mid = float(mid or 0.0)
    if mid <= 0 or xbps <= 0:
        return 0.0

    def _vwap(levels: Sequence[Level]) -> Optional[float]:
        num = 0.0
        den = 0.0
        for px, qty in _iter_levels(levels):
            dist_bps = abs(px - mid) / mid * 10_000.0
            if dist_bps <= float(xbps) + 1e-9:
                num += px * qty
                den += qty
        if den <= 0:
            return None
        v = num / den
        if not math.isfinite(v) or v <= 0:
            return None
        return float(v)

    vb = _vwap(bids)
    va = _vwap(asks)
    if vb is None or va is None:
        return 0.0

    dws = (va - vb) / mid * 10_000.0
    if not math.isfinite(dws):
        return 0.0
    return float(max(0.0, min(float(dws), 50_000.0)))


def extract_levels_from_runtime(runtime: Any) -> Tuple[List[Level], List[Level], float]:
    """Best-effort extraction of (bids, asks, mid) from SymbolRuntime."""
    bids: List[Level] = []
    asks: List[Level] = []
    mid = 0.0

    try:
        snap = getattr(runtime, "last_book", None)
        if snap is not None:
            bids = list(getattr(snap, "top5_bids", None) or getattr(snap, "bids", None) or [])
            asks = list(getattr(snap, "top5_asks", None) or getattr(snap, "asks", None) or [])
            bb = float(getattr(snap, "best_bid_px", 0.0) or 0.0)
            ba = float(getattr(snap, "best_ask_px", 0.0) or 0.0)
            mid = _mid_from_bbo(bb, ba)
    except Exception:
        pass

    if mid <= 0:
        try:
            bs = getattr(runtime, "book_state", None)
            snap = getattr(bs, "snap", None) if bs is not None else None
            if snap is not None:
                bids = list(getattr(snap, "top5_bids", None) or getattr(snap, "bids", None) or bids)
                asks = list(getattr(snap, "top5_asks", None) or getattr(snap, "asks", None) or asks)
                bb = float(getattr(snap, "best_bid_px", 0.0) or 0.0)
                ba = float(getattr(snap, "best_ask_px", 0.0) or 0.0)
                mid = _mid_from_bbo(bb, ba)
        except Exception:
            pass

    return (bids, asks, float(mid))
