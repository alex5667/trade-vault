from __future__ import annotations

"""
Order Book L2 Metrics - Optimized Vectorized Implementation.

ФУНКЦИОНАЛ:
- Расчёт глубины на разных уровнях (depth_5, depth_20)
- Order Book Imbalance (OBI)
- Slope (эластичность)
- Microprice (взвешенная цена)
- Wall detection
- Vectorized operations using numpy for performance
"""

from dataclasses import dataclass
from typing import Any

import numpy as np

EPS = 1e-12

@dataclass
class L2Metrics:
    ts: int
    best_bid: float
    best_ask: float
    mid: float
    spread_bps: float

    depth_bid_5: float
    depth_ask_5: float
    depth_bid_20: float
    depth_ask_20: float

    obi_5: float
    obi_20: float

    slope_bid_20: float
    slope_ask_20: float

    microprice_20: float
    microprice_shift_bps_20: float

    wall_bid: bool
    wall_ask: bool
    wall_bid_dist_bps: float
    wall_ask_dist_bps: float

    bid_top3: float
    ask_top3: float
    bid_top5: float
    ask_top5: float


def _prepare_levels(levels: Any, is_bid: bool, max_depth: int) -> tuple[np.ndarray, np.ndarray]:
    """
    Convert levels to sorted numpy arrays of (price, volume).
    """
    if not levels:
        return np.array([]), np.array([])

    # Fast path for list of lists/tuples
    try:
        arr = np.array(levels, dtype=np.float64)
    except Exception:
        # Fallback for mixed types (str/float)
        # Slower but robust
        clean = []
        for row in levels:
            try:
                if len(row) >= 2:
                    p, v = float(row[0]), float(row[1])
                    if p > 0 and v >= 0:
                        clean.append((p, v))
            except (ValueError, TypeError):
                continue
        arr = np.array(clean, dtype=np.float64)

    if arr.ndim != 2 or arr.shape[1] < 2:
        return np.array([]), np.array([])

    # Filter bad values if not done above
    if arr.shape[0] > 0:
        mask = (arr[:, 0] > 0) & (arr[:, 1] >= 0)
        arr = arr[mask]

    # Sort
    # Bids: Descending (highest price first)
    # Asks: Ascending (lowest price first)
    if arr.shape[0] > 0:
        if is_bid:
            # Sort desc by price
            arr = arr[np.argsort(arr[:, 0])[::-1]]
        else:
            # Sort asc by price
            arr = arr[np.argsort(arr[:, 0])]

    # Slice to max_depth
    arr = arr[:max_depth]

    if arr.shape[0] == 0:
        return np.array([]), np.array([])

    return arr[:, 0], arr[:, 1]


def compute_l2_metrics(
    book: dict[str, Any],
    *,
    k_small: int = 5,
    k_large: int = 20,
    wall_mult: float = 3.0,
    wall_max_dist_bps: float = 15.0,
) -> L2Metrics | None:
    """
    Vectorized computation of L2 metrics.
    Uses numpy for 10-100x speedup over vanilla python on large books.
    """
    if not book:
        return None

    # Use max depth needed
    limit = max(k_large, 50)

    bp, bv = _prepare_levels(book.get("bids"), True, limit)
    ap, av = _prepare_levels(book.get("asks"), False, limit)

    if bp.size == 0 or ap.size == 0:
        return None

    best_bid = float(bp[0])
    best_ask = float(ap[0])

    if best_bid <= 0 or best_ask <= 0:
        return None

    mid = 0.5 * (best_bid + best_ask)
    if mid <= EPS:
        return None

    spread_bps = (best_ask - best_bid) / mid * 10_000.0

    # Function dispatch for depth
    # sum(volume[:k])

    def get_depth(v: np.ndarray, k: int) -> float:
        k = min(k, v.size)
        if k <= 0: return 0.0
        return float(np.sum(v[:k]))

    depth_bid_5 = get_depth(bv, k_small)
    depth_ask_5 = get_depth(av, k_small)
    depth_bid_20 = get_depth(bv, k_large)
    depth_ask_20 = get_depth(av, k_large)

    # OBI
    def calc_obi(bd: float, ad: float) -> float:
        den = bd + ad
        if den <= EPS: return 0.0
        return (bd - ad) / den

    obi_5 = calc_obi(depth_bid_5, depth_ask_5)
    obi_20 = calc_obi(depth_bid_20, depth_ask_20)

    # Slope
    # cum_depth / dist_bps
    def calc_slope(p: np.ndarray, v: np.ndarray, k: int) -> float:
        k = min(k, p.size)
        if k == 0: return 0.0
        cum = np.sum(v[:k])
        pk = p[k-1]
        dist = abs(pk - mid) / mid * 10_000.0
        if dist <= EPS: return 0.0 # prevent div by zero, effectively infinite slope
        return float(cum / dist)

    slope_bid_20 = calc_slope(bp, bv, k_large)
    slope_ask_20 = calc_slope(ap, av, k_large)

    # Microprice
    # sum(p * v / (d+1)) / sum(v / (d+1))
    def calc_microprice(p: np.ndarray, v: np.ndarray, k: int) -> tuple[float, float]:
        k = min(k, p.size)
        if k == 0: return 0.0, 0.0

        pp = p[:k]
        vv = v[:k]

        # dist bps
        d = np.abs(pp - mid) / mid * 10_000.0
        w = vv / (d + 1.0)

        w_sum = np.sum(w)
        if w_sum <= EPS: return 0.0, 0.0

        weighted_price_sum = np.sum(w * pp)
        return float(weighted_price_sum), float(w_sum)

    mp_num_b, mp_den_b = calc_microprice(bp, bv, k_large)
    mp_num_a, mp_den_a = calc_microprice(ap, av, k_large)

    mp_total_num = mp_num_b + mp_num_a
    mp_total_den = mp_den_b + mp_den_a

    if mp_total_den > EPS:
        mp20 = float(mp_total_num / mp_total_den)
    else:
        mp20 = mid

    mp_shift_bps = (mp20 - mid) / mid * 10_000.0

    # Wall detection
    # Wall = vol > mult * median
    def detect_wall(p: np.ndarray, v: np.ndarray, k: int) -> tuple[bool, float]:
        k = min(k, p.size)
        if k == 0: return False, 0.0

        vv = v[:k]
        pp = p[:k]

        med = float(np.median(vv))
        if med <= EPS: return False, 0.0

        threshold = med * wall_mult

        # Find indices where volume > threshold
        candidates_mask = vv >= threshold

        if not np.any(candidates_mask):
            return False, 0.0

        # Calculate distances for all candidates
        dists = np.abs(pp[candidates_mask] - mid) / mid * 10_000.0

        # Check if any distance is within max
        valid_mask = dists <= wall_max_dist_bps

        if np.any(valid_mask):
            # Return true and the distance of the CLOSEST valid wall
            valid_dists = dists[valid_mask]
            return True, float(np.min(valid_dists))

        return False, 0.0

    wall_bid, wall_bid_dist = detect_wall(bp, bv, k_small)
    wall_ask, wall_ask_dist = detect_wall(ap, av, k_small)

    ts = int(book.get("ts", 0)) if book.get("ts") is not None else 0

    return L2Metrics(
        ts=ts,
        best_bid=best_bid,
        best_ask=best_ask,
        mid=mid,
        spread_bps=float(spread_bps),

        depth_bid_5=depth_bid_5,
        depth_ask_5=depth_ask_5,
        depth_bid_20=depth_bid_20,
        depth_ask_20=depth_ask_20,

        obi_5=obi_5,
        obi_20=obi_20,

        slope_bid_20=slope_bid_20,
        slope_ask_20=slope_ask_20,

        microprice_20=mp20,
        microprice_shift_bps_20=mp_shift_bps,

        wall_bid=wall_bid,
        wall_ask=wall_ask,
        wall_bid_dist_bps=wall_bid_dist,
        wall_ask_dist_bps=wall_ask_dist,

        bid_top3=get_depth(bv, 3),
        ask_top3=get_depth(av, 3),
        bid_top5=depth_bid_5,
        ask_top5=depth_ask_5,
    )

