from __future__ import annotations

import math
from typing import Any, Dict, List, Optional, Tuple

from core.book_microstructure_v2 import (
    compute_ofi_multilevel_topn,
    compute_queue_imbalance_topn,
)

Level = Tuple[float, float]  # (price, qty)

def _is_finite(x: float) -> bool:
    return isinstance(x, (int, float)) and math.isfinite(float(x))

def _to_f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return float(default)
        return v
    except Exception:
        return float(default)

def _get_levels(snap: Any, side: str) -> List[Level]:
    """
    Supports:
      - object with attributes .bids/.asks as list[[px,qty],...]
      - dict with keys 'bids'/'asks'
    """
    if snap is None:
        return []
    try:
        if isinstance(snap, dict):
            key = side if side in snap else ("bid" if side == "bids" else "ask")
            levels = snap.get(key, []) or []
        else:
            levels = getattr(snap, side, None)
            if levels is None:
                levels = getattr(snap, "bid" if side == "bids" else "ask", []) or []
        out: List[Level] = []
        for it in levels:
            if it is None:
                continue
            if isinstance(it, (list, tuple)) and len(it) >= 2:
                out.append((_to_f(it[0], 0.0), _to_f(it[1], 0.0)))
            elif isinstance(it, dict):
                px = _to_f(it.get("px") or it.get("price"), 0.0)
                qty = _to_f(it.get("qty") or it.get("q") or it.get("size"), 0.0)
                out.append((px, qty))
        return out
    except Exception:
        return []


def _gini(values: List[float]) -> float:
    """Gini coefficient for non-negative values.

    Returns:
        0.0 for empty/degenerate input, and is bounded to [0, 1].

    Notes:
        - We clamp negatives to 0.0 (bad book data should not explode features).
        - Complexity: O(n log n) due to sorting (n here is small: <= 20 for top-10 both sides).
    """
    try:
        if not values:
            return 0.0
        xs = [max(float(x or 0.0), 0.0) for x in values]
        n = len(xs)
        if n <= 1:
            return 0.0
        s = sum(xs)
        if s <= 0.0:
            return 0.0
        xs.sort()
        num = 0.0
        for i, x in enumerate(xs, start=1):
            num += i * x
        g = (2.0 * num) / (n * s) - (n + 1.0) / n
        if not _is_finite(g):
            return 0.0
        if g < 0.0:
            return 0.0
        if g > 1.0:
            return 1.0
        return float(g)
    except Exception:
        return 0.0

def compute_microstructure_v4(snap: Any, prev_snap: Any, levels: int = 5) -> Dict[str, float]:
    """
    Computes V4 microstructure features (Microprice + Slope + Convexity + Depth-Weighted OBI).
    
    Returns:
        mp_mid_bps: (microprice - mid) in bps
        mp_shift_bps: shift of microprice relative to prev_snap in bps (if prev_snap avail)
        depth_bid_5, depth_ask_5: cumulative depth (L1..L5)
        book_slope_bid, book_slope_ask: log(cum5/cum1)/4
        book_convex_bid, book_convex_ask: simple convexity measure
        obi_dw: depth-weighted imbalance

        # v4.1 extras (cheap, high-ROI for ML / diagnostics)
        qimb_wmean: weighted mean queue imbalance (L1..L5)
        qimb_l1/qimb_l5/qimb_slope: queue imbalance shape proxies
        ofi_ml_norm/ofi_ml_wsum: multi-level OFI proxy (normalized + weighted)
        depth_top5_sum/depth_total_5/depth_imbalance_5: depth aggregates
        depth_total_10/depth_imbalance_10/gini_depth_10: depth aggregates (top-10, cross-side)
        micro_price/micro_price_diff_bps: absolute microprice and its diff to mid (bps)
    """
    out: Dict[str, float] = {}

    # Defaults
    out["mp_mid_bps"] = 0.0
    out["mp_shift_bps"] = 0.0
    out["depth_bid_5"] = 0.0
    out["depth_ask_5"] = 0.0
    out["book_slope_bid"] = 0.0
    out["book_slope_ask"] = 0.0
    out["book_convex_bid"] = 0.0
    out["book_convex_ask"] = 0.0
    out["obi_dw"] = 0.0

    # v4.1 extras defaults
    out["qimb_wmean"] = 0.0
    out["qimb_l1"] = 0.0
    out["qimb_l5"] = 0.0
    out["qimb_slope"] = 0.0
    out["ofi_ml_norm"] = 0.0
    out["ofi_ml_wsum"] = 0.0
    out["depth_top5_sum"] = 0.0
    out["depth_total_5"] = 0.0
    out["depth_imbalance_5"] = 0.0
    out["micro_price"] = 0.0
    out["micro_price_diff_bps"] = 0.0
    out["depth_total_10"] = 0.0
    out["depth_imbalance_10"] = 0.0
    out["gini_depth_10"] = 0.0

    if snap is None:
        return out

    # 1. Parse current snapshot
    bids = _get_levels(snap, "bids")
    asks = _get_levels(snap, "asks")

    # Need at least L1 for basic calc
    if not bids or not asks:
        return out

    # --- Top-10 depth aggregates (bid/ask) + Gini of depth distribution ---
    # These features are purely snapshot-based and are safe even when the book has <10 levels.
    # We clamp negative quantities to 0.0 to avoid bad data exploding the ML feature vector.
    try:
        n10 = 10
        bid10 = [max(float(q or 0.0), 0.0) for _, q in bids[:n10]]
        ask10 = [max(float(q or 0.0), 0.0) for _, q in asks[:n10]]
        s_bid10 = float(sum(bid10))
        s_ask10 = float(sum(ask10))
        out["depth_total_10"] = s_bid10 + s_ask10
        if out["depth_total_10"] > 0.0:
            out["depth_imbalance_10"] = (s_bid10 - s_ask10) / out["depth_total_10"]
        else:
            out["depth_imbalance_10"] = 0.0
        out["gini_depth_10"] = _gini(bid10 + ask10)
    except Exception:
        pass

    best_bid_px, best_bid_qty = bids[0]
    best_ask_px, best_ask_qty = asks[0]
    mid = 0.5 * (best_bid_px + best_ask_px)
    
    if mid <= 1e-9:
        return out

    # --- Microprice (Stoikov) ---
    # mp = (vb*ask_px + va*bid_px) / (vb + va)
    # where vb = bid volume, va = ask volume used for weighting.
    # We use L1 qty for standard microprice or sum up to N? 
    # Usually standard microprice uses L1 imbalance.
    # mp = (qty_b * ask_px + qty_a * bid_px) / (qty_b + qty_a)
    
    denom_mp = best_bid_qty + best_ask_qty
    if denom_mp > 0:
        mp = (best_bid_qty * best_ask_px + best_ask_qty * best_bid_px) / denom_mp
        mp_mid_diff = mp - mid
        out["mp_mid_bps"] = (mp_mid_diff / mid) * 10000.0
        out["micro_price"] = float(mp)
        out["micro_price_diff_bps"] = float(out.get("mp_mid_bps", 0.0) or 0.0)
    else:
        mp = mid  # fallback
        out["micro_price"] = float(mp)
        out["micro_price_diff_bps"] = 0.0

    # --- Microprice Shift (vs prev) ---
    # Needs prev snapshot microprice
    if prev_snap is not None:
        bids_p = _get_levels(prev_snap, "bids")
        asks_p = _get_levels(prev_snap, "asks")
        if bids_p and asks_p:
            bb_p, bq_p = bids_p[0]
            ba_p, aq_p = asks_p[0]
            d_p = bq_p + aq_p
            if d_p > 0:
                mp_prev = (bq_p * ba_p + aq_p * bb_p) / d_p
                # Shift in bps relative to current mid? Or prev mid? 
                # Usually we want absolute shift normalized by price level.
                shift = mp - mp_prev
                out["mp_shift_bps"] = (shift / mid) * 10000.0
    
    # --- Depth & Slope & Convexity ---
    # Slope logic: log(cum_vol_L5 / cum_vol_L1) / (levels - 1)
    # We use k=5 levels max, or whatever available
    # Convexity: difference between actual depth profile and linear? 
    # Or simplified: (cum3 / cum1) vs (cum5 / cum3)?
    # Description says: "simple convexity of depth profile". 
    # Often: convex ~ (d2 - d1) - (d1 - d0) logic on logs?
    # Let's verify prompt: "book_convex_bid/ask — simple convexity of depth profile"
    # A common simple proxy: convexity = (Vol_L1 + Vol_L5) / (2 * Vol_L3) - 1 ?
    # Or (Vol_L3 / Vol_L1) / (Vol_L5 / Vol_L3)?
    
    # Let's iterate up to N=5
    lim = min(len(bids), int(levels))
    cum_bid = 0.0
    
    bid_vols = []
    
    for i in range(lim):
        cum_bid += bids[i][1]
        bid_vols.append(cum_bid)
    
    lim = min(len(asks), int(levels))
    cum_ask = 0.0
    ask_vols = []
    for i in range(lim):
        cum_ask += asks[i][1]
        ask_vols.append(cum_ask)

    # Fill up to 5 with last value if needed (cumulative)
    # But slope formula is specific: log(cum5/cum1)/4
    # We need exactly defined levels 1 and 5.
    
    # Bids
    if len(bid_vols) >= 1:
        v1 = bid_vols[0]
        v5 = bid_vols[min(4, len(bid_vols)-1)] # Use last available if < 5
        out["depth_bid_5"] = v5
        
        if v1 > 0 and v5 > 0:
             # slope over 4 steps (1->5)
             out["book_slope_bid"] = math.log(v5 / v1) / 4.0
        
        # Convexity: let's use a 3-point check if possible (1, 3, 5)
        # If we have at least 3 levels
        if len(bid_vols) >= 3:
            v3 = bid_vols[min(2, len(bid_vols)-1)]
            # If linear growth in log-space (constant slope), then v3^2 approx v1*v5
            # Convexity > 0 means "tends to grow faster than exponential" (or slower?)
            # Let's define as numerical 2nd deriv proxy or ratio of slopes.
            # Slope1_3 = log(v3/v1)/2
            # Slope3_5 = log(v5/v3)/2
            # Convexity = Slope3_5 - Slope1_3
            if v1 > 0 and v3 > 0 and v5 > 0:
                 s13 = math.log(v3/v1)/2.0
                 s35 = math.log(v5/v3)/2.0
                 out["book_convex_bid"] = s35 - s13
    
    # Asks
    if len(ask_vols) >= 1:
        v1 = ask_vols[0]
        v5 = ask_vols[min(4, len(ask_vols)-1)]
        out["depth_ask_5"] = v5
        
        if v1 > 0 and v5 > 0:
            out["book_slope_ask"] = math.log(v5 / v1) / 4.0
            
        if len(ask_vols) >= 3:
            v3 = ask_vols[min(2, len(ask_vols)-1)]
            if v1 > 0 and v3 > 0 and v5 > 0:
                 s13 = math.log(v3/v1)/2.0
                 s35 = math.log(v5/v3)/2.0
                 out["book_convex_ask"] = s35 - s13

    # --- Depth-Weighted OBI (obi_dw) ---
    # Standard OBI: (qb - qa) / (qb + qa) at L1.
    # Multilevel OBI is often flow-based. 
    # Here "obi_dw" likely refers to static imbalance weighted by depth.
    # sum( w_k * (qb_k - qa_k) ) / sum( w_k * (qb_k + qa_k) ) ?
    # Or just sum(w_k * qimb_k)?
    # Prompt says: "obi_dw — depth-weighted imbalance (w=1/i)"
    # Usually it means sum( (qb_i - qa_i) / (qb_i + qa_i) * w_i ) / sum(w_i) is the weighted MEAN imbalance.
    # Let's implement weighted mean of imbalances.
    
    num = 0.0
    den = 0.0
    
    # We iterate up to levels=5
    L = max(len(bids), len(asks), int(levels))
    
    for k in range(1, L + 1):
        if k > int(levels):
            break
            
        bq = bids[k-1][1] if k-1 < len(bids) else 0.0
        aq = asks[k-1][1] if k-1 < len(asks) else 0.0
        
        w = 1.0 / float(k)
        
        vol_sum = bq + aq
        if vol_sum > 0:
            imb = (bq - aq) / vol_sum
        else:
            imb = 0.0
        
        num += w * imb
        den += w
        
    if den > 0:
        out["obi_dw"] = num / den
    else:
        out["obi_dw"] = 0.0

    # --- v4.1 extras: depth aggregates / qimb / multilevel OFI ---
    try:
        out["depth_total_5"] = float(out.get("depth_bid_5", 0.0) or 0.0) + float(out.get("depth_ask_5", 0.0) or 0.0)
        denom_depth = float(out["depth_total_5"])
        if denom_depth > 0.0:
            out["depth_imbalance_5"] = (
                (float(out.get("depth_bid_5", 0.0) or 0.0) - float(out.get("depth_ask_5", 0.0) or 0.0)) / denom_depth
            )
    except Exception:
        pass

    try:
        q = compute_queue_imbalance_topn(snap, levels=int(levels))
        out["qimb_wmean"] = float(q.get("qimb_wmean", 0.0) or 0.0)
        out["qimb_l1"] = float(q.get("qimb_l1", 0.0) or 0.0)
        out["qimb_l5"] = float(q.get("qimb_l5", 0.0) or 0.0)
        out["qimb_slope"] = float(out["qimb_l1"] - out["qimb_l5"])
    except Exception:
        pass

    try:
        if prev_snap is not None:
            ofi_ml = compute_ofi_multilevel_topn(prev_snap, snap, levels=int(levels))
            out["ofi_ml_norm"] = float(ofi_ml.get("ofi_ml_norm", 0.0) or 0.0)
            out["ofi_ml_wsum"] = float(ofi_ml.get("ofi_ml_wsum", 0.0) or 0.0)
            out["depth_top5_sum"] = float(ofi_ml.get("depth_top5_sum", out.get("depth_total_5", 0.0)) or 0.0)
        else:
            out["depth_top5_sum"] = float(out.get("depth_total_5", 0.0) or 0.0)
    except Exception:
        pass

    return out
