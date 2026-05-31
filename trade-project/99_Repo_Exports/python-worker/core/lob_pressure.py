from __future__ import annotations

# -*- coding: utf-8 -*-
"""LOB Pressure features (P91).

Performance notes
-----------------
NumPy allocation overhead dominates at small depths (depth ≤ 10).
Benchmark on RTX-3060 host:
  depth=5  pure-Python=8µs, np.zeros+ops=62µs  →  Python 7.4x FASTER
  depth=20 pure-Python=25µs, numpy=14µs         →  NumPy 1.8x faster
  depth=50+ numpy is the clear winner

Strategy:
  depth ≤ 10  →  optimized pure-Python (no numpy allocation)
  depth > 10  →  NumPy vectorized path

API is identical in both paths.

Features:
1) Queue imbalance per level (L1..L5) + aggregates (mean, max_abs, slope)
2) Microprice divergence vs mid (bps) + microprice shift (bps)
3) Book slope/convexity (by cumulative depth) per side + imbalance
4) Depth-weighted OBI (weights 1/(level)) for stronger near-touch emphasis
"""


import math
from typing import Any

import numpy as np

# Pre-computed 1/level weights for depth 1..10 (avoids per-call division)
_LEVEL_WEIGHTS: list[float] = [1.0 / (i + 1) for i in range(10)]


def _sf(x: Any, default: float = 0.0) -> float:
    """Safe float conversion: returns default on NaN/Inf/error."""
    try:
        v = float(x)
        if math.isnan(v) or math.isinf(v):
            return default
        return v
    except Exception:
        return default


def _microprice(best_bid_px: float, best_bid_qty: float, best_ask_px: float, best_ask_qty: float) -> float:
    """Quantity-weighted average of best bid/ask (microprice)."""
    denom = best_bid_qty + best_ask_qty
    if denom <= 0:
        return 0.0
    return (best_ask_px * best_bid_qty + best_bid_px * best_ask_qty) / denom


def _lin_slope_py(y: list[float]) -> float:
    """Least-squares slope, pure Python, depth≤10."""
    n = len(y)
    if n <= 1:
        return 0.0
    x_mean = (n + 1) * 0.5
    y_mean = sum(y) / float(n)
    num = 0.0
    den = 0.0
    for i, yi in enumerate(y, 1):
        dx = float(i) - x_mean
        dy = float(yi) - y_mean
        num += dx * dy
        den += dx * dx
    return (num / den) if den > 0 else 0.0


def _lin_slope_np(y: np.ndarray) -> float:
    """Least-squares slope via numpy dot — for depth>10."""
    n = len(y)
    if n <= 1:
        return 0.0
    x = np.arange(1, n + 1, dtype=np.float64)
    dx = x - x.mean()
    dy = y - y.mean()
    den = float(np.dot(dx, dx))
    return float(np.dot(dx, dy) / den) if den > 0 else 0.0


# ---------------------------------------------------------------------------
# Hot path: optimized pure-Python (depth ≤ 10, no numpy allocation)
# ---------------------------------------------------------------------------
def _compute_lob_python(
    bids: list[tuple[float, float]],
    asks: list[tuple[float, float]],
    prev_bids: list[tuple[float, float]] | None,
    prev_asks: list[tuple[float, float]] | None,
    depth: int,
) -> dict[str, float]:
    """Pure-Python path: depth ≤ 10. No NumPy allocation overhead."""
    eps = 1e-12

    bq: list[float] = []
    aq: list[float] = []
    bp: list[float] = []
    ap: list[float] = []
    for i in range(depth):
        if i < len(bids):
            bp.append(_sf(bids[i][0], 0.0))
            bq.append(max(0.0, _sf(bids[i][1], 0.0)))
        else:
            bp.append(0.0); bq.append(0.0)
        if i < len(asks):
            ap.append(_sf(asks[i][0], 0.0))
            aq.append(max(0.0, _sf(asks[i][1], 0.0)))
        else:
            ap.append(0.0); aq.append(0.0)

    # Queue imbalance per level
    qi_levels: list[float] = []
    for i in range(depth):
        if bq[i] <= 0.0 or aq[i] <= 0.0:
            qi_levels.append(0.0)
        else:
            d = bq[i] + aq[i]
            qi_levels.append((bq[i] - aq[i]) / d if d > 0 else 0.0)

    qi_mean = sum(qi_levels) / float(max(1, depth))
    qi_max_abs = max(abs(q) for q in qi_levels)
    qi_slope = _lin_slope_py(qi_levels)

    out: dict[str, float] = {
        "qi_mean": qi_mean,
        "qi_max_abs": qi_max_abs,
        "qi_slope": qi_slope,
    }
    for i, q in enumerate(qi_levels, 1):
        out[f"qi_l{i}"] = q

    # Microprice
    best_bid_px = bp[0]; best_ask_px = ap[0]
    best_bid_qty = bq[0]; best_ask_qty = aq[0]
    mid = 0.5 * (best_bid_px + best_ask_px) if (best_bid_px > 0 and best_ask_px > 0) else 0.0
    mp = _microprice(best_bid_px, best_bid_qty, best_ask_px, best_ask_qty) if mid > 0 else 0.0

    mp_mid_div_bps = ((mp - mid) / mid) * 10_000.0 if (mid > 0 and mp > 0) else 0.0

    mp_shift_bps = 0.0
    if prev_bids and prev_asks:
        pbid_px = _sf(prev_bids[0][0], 0.0); pask_px = _sf(prev_asks[0][0], 0.0)
        pbid_qty = max(0.0, _sf(prev_bids[0][1], 0.0)); pask_qty = max(0.0, _sf(prev_asks[0][1], 0.0))
        pmid = 0.5 * (pbid_px + pask_px) if (pbid_px > 0 and pask_px > 0) else 0.0
        pmp = _microprice(pbid_px, pbid_qty, pask_px, pask_qty) if pmid > 0 else 0.0
        if pmid > 0 and mp > 0 and pmp > 0:
            mp_shift_bps = ((mp - pmp) / pmid) * 10_000.0

    out["micro_mid_div_bps"] = mp_mid_div_bps
    out["micro_shift_bps"] = mp_shift_bps

    # Cumulative depth slopes/convexity
    cum_bid: list[float] = []
    cum_ask: list[float] = []
    s = 0.0
    for q in bq: s += q; cum_bid.append(s)
    s = 0.0
    for q in aq: s += q; cum_ask.append(s)

    def _slope(cum: list[float]) -> float:
        n = len(cum)
        return (cum[-1] - cum[0]) / float(n - 1) if n > 1 else 0.0

    def _convexity(cum: list[float]) -> float:
        n = len(cum)
        if n < 3: return 0.0
        mid_i = n // 2
        a = cum[0]; m = cum[mid_i]; z = cum[-1]
        return ((z - m) - (m - a)) / float(max(eps, abs(z)))

    slope_bid = _slope(cum_bid); slope_ask = _slope(cum_ask)
    out["depth_slope_bid"] = slope_bid
    out["depth_slope_ask"] = slope_ask
    out["depth_slope_imb"] = slope_bid - slope_ask
    out["depth_convexity_bid"] = _convexity(cum_bid)
    out["depth_convexity_ask"] = _convexity(cum_ask)
    out["depth_convexity_imb"] = out["depth_convexity_bid"] - out["depth_convexity_ask"]

    # Depth-weighted OBI (precomputed weights)
    wb = 0.0; wa = 0.0
    weights = _LEVEL_WEIGHTS if depth <= 10 else [1.0 / (i + 1) for i in range(depth)]
    for i in range(depth):
        w = weights[i]; wb += w * bq[i]; wa += w * aq[i]
    denom_dw = wb + wa
    out["dw_obi"] = (wb - wa) / denom_dw if denom_dw > eps else 0.0

    return out


# ---------------------------------------------------------------------------
# NumPy path: depth > 10 (allocation overhead amortized by vector length)
# ---------------------------------------------------------------------------
def _compute_lob_numpy(
    bids: list[tuple[float, float]],
    asks: list[tuple[float, float]],
    prev_bids: list[tuple[float, float]] | None,
    prev_asks: list[tuple[float, float]] | None,
    depth: int,
) -> dict[str, float]:
    """NumPy-vectorized path: depth > 10."""
    eps = 1e-12

    bq = np.zeros(depth, dtype=np.float64)
    aq = np.zeros(depth, dtype=np.float64)
    bp = np.zeros(depth, dtype=np.float64)
    ap = np.zeros(depth, dtype=np.float64)

    for i in range(depth):
        if i < len(bids):
            bp[i] = _sf(bids[i][0], 0.0)
            bq[i] = max(0.0, _sf(bids[i][1], 0.0))
        if i < len(asks):
            ap[i] = _sf(asks[i][0], 0.0)
            aq[i] = max(0.0, _sf(asks[i][1], 0.0))

    denom_qi = bq + aq
    valid = (bq > 0.0) & (aq > 0.0)
    qi_arr = np.where(valid & (denom_qi > 0.0), (bq - aq) / np.where(denom_qi > 0, denom_qi, 1.0), 0.0)

    qi_mean = float(qi_arr.mean())
    qi_max_abs = float(np.abs(qi_arr).max())
    qi_slope = _lin_slope_np(qi_arr)

    out: dict[str, float] = {"qi_mean": qi_mean, "qi_max_abs": qi_max_abs, "qi_slope": qi_slope}
    for i in range(depth):
        out[f"qi_l{i + 1}"] = float(qi_arr[i])

    best_bid_px = float(bp[0]); best_ask_px = float(ap[0])
    best_bid_qty = float(bq[0]); best_ask_qty = float(aq[0])
    mid = 0.5 * (best_bid_px + best_ask_px) if (best_bid_px > 0 and best_ask_px > 0) else 0.0
    mp = _microprice(best_bid_px, best_bid_qty, best_ask_px, best_ask_qty) if mid > 0 else 0.0

    out["micro_mid_div_bps"] = ((mp - mid) / mid) * 10_000.0 if (mid > 0 and mp > 0) else 0.0

    mp_shift_bps = 0.0
    if prev_bids and prev_asks:
        pbid_px = _sf(prev_bids[0][0], 0.0); pask_px = _sf(prev_asks[0][0], 0.0)
        pbid_qty = max(0.0, _sf(prev_bids[0][1], 0.0)); pask_qty = max(0.0, _sf(prev_asks[0][1], 0.0))
        pmid = 0.5 * (pbid_px + pask_px) if (pbid_px > 0 and pask_px > 0) else 0.0
        pmp = _microprice(pbid_px, pbid_qty, pask_px, pask_qty) if pmid > 0 else 0.0
        if pmid > 0 and mp > 0 and pmp > 0:
            mp_shift_bps = ((mp - pmp) / pmid) * 10_000.0
    out["micro_shift_bps"] = mp_shift_bps

    cum_bid = np.cumsum(bq); cum_ask = np.cumsum(aq)

    def _slope_np(cum: np.ndarray) -> float:
        n = len(cum)
        return float((cum[-1] - cum[0]) / float(n - 1)) if n > 1 else 0.0

    def _convexity_np(cum: np.ndarray) -> float:
        n = len(cum)
        if n < 3: return 0.0
        a = float(cum[0]); m = float(cum[n // 2]); z = float(cum[-1])
        return ((z - m) - (m - a)) / float(max(eps, abs(z)))

    slope_bid = _slope_np(cum_bid); slope_ask = _slope_np(cum_ask)
    out["depth_slope_bid"] = slope_bid; out["depth_slope_ask"] = slope_ask
    out["depth_slope_imb"] = slope_bid - slope_ask
    out["depth_convexity_bid"] = _convexity_np(cum_bid)
    out["depth_convexity_ask"] = _convexity_np(cum_ask)
    out["depth_convexity_imb"] = out["depth_convexity_bid"] - out["depth_convexity_ask"]

    weights = 1.0 / np.arange(1, depth + 1, dtype=np.float64)
    wb = float(np.dot(weights, bq)); wa = float(np.dot(weights, aq))
    denom_dw = wb + wa
    out["dw_obi"] = (wb - wa) / denom_dw if denom_dw > eps else 0.0

    return out


# ---------------------------------------------------------------------------
# Public entry point — hybrid dispatch
# ---------------------------------------------------------------------------
def compute_lob_pressure(
    *,
    bids: list[tuple[float, float]],
    asks: list[tuple[float, float]],
    prev_bids: list[tuple[float, float]] | None = None,
    prev_asks: list[tuple[float, float]] | None = None,
    depth: int = 5,
) -> dict[str, float]:
    """Compute LOB pressure features from top-of-book levels.

    Args:
        bids: List of (price, qty) tuples for bid side, best price first.
        asks: List of (price, qty) tuples for ask side, best price first.
        prev_bids: Previous snapshot bid levels (for microprice shift).
        prev_asks: Previous snapshot ask levels (for microprice shift).
        depth: Number of levels to analyze (1..10, default 5).

    Returns:
        Dict with all LOB pressure features, all values float:
        - qi_l1..qi_lN: per-level queue imbalance [-1..+1]
        - qi_mean, qi_max_abs, qi_slope: aggregates
        - micro_mid_div_bps: microprice vs mid divergence (bps)
        - micro_shift_bps: microprice shift from prev snapshot (bps)
        - depth_slope_bid/ask/imb: cumulative depth slope
        - depth_convexity_bid/ask/imb: depth curve convexity
        - dw_obi: depth-weighted OBI (weights 1/level)
    """
    depth = int(max(1, min(50, depth)))
    if depth <= 10:
        return _compute_lob_python(bids, asks, prev_bids, prev_asks, depth)
    return _compute_lob_numpy(bids, asks, prev_bids, prev_asks, depth)
