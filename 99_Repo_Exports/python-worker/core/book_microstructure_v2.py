from __future__ import annotations

import math
from typing import Any

Level = tuple[float, float]  # (price, qty)

def _is_finite(x: float) -> bool:
    return isinstance(x, (int, float)) and math.isfinite(float(x))

def _to_f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return default
        return v
    except Exception:
        return default

def _get_levels(snap: Any, side: str) -> list[Level]:
    """
    Supports:
      - object with attributes .bids/.asks as list[[px,qty],...]
      - dict with keys 'bids'/'asks' (or 'bid'/'ask')
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
        out: list[Level] = []
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

def compute_queue_imbalance_topn(snap: Any, levels: int = 5) -> dict[str, float]:
    """
    qimb_lk = (bid_qty_k - ask_qty_k)/(bid_qty_k + ask_qty_k), 0 if denom==0
    qimb_wmean: weights 1/k
    """
    if snap is None:
        return {}
    bids = _get_levels(snap, "bids")[: max(0, int(levels))]
    asks = _get_levels(snap, "asks")[: max(0, int(levels))]
    if not bids and not asks:
        return {}
    L = max(len(bids), len(asks))
    if L <= 0:
        return {}
    out: dict[str, float] = {}
    num_w = 0.0
    den_w = 0.0
    for k in range(1, L + 1):
        bq = bids[k - 1][1] if k - 1 < len(bids) else 0.0
        aq = asks[k - 1][1] if k - 1 < len(asks) else 0.0
        denom = bq + aq
        qimb = 0.0 if denom <= 0.0 else (bq - aq) / denom
        if not _is_finite(qimb):
            qimb = 0.0
        out[f"qimb_l{k}"] = float(qimb)
        w = 1.0 / float(k)
        num_w += w * qimb
        den_w += w
    out["qimb_wmean"] = float(num_w / den_w) if den_w > 0 else 0.0
    return out

def compute_ofi_multilevel_topn(prev_snap: Any, snap: Any, levels: int = 5) -> dict[str, float]:
    """
    Proxy multi-level OFI:
      ofi_k = (bid_qty_k - prev_bid_qty_k) - (ask_qty_k - prev_ask_qty_k)
    Aggregates:
      ofi_ml, ofi_ml_wsum (weights 1/k), ofi_ml_norm (divide by topN depth)
    """
    if prev_snap is None or snap is None:
        return {}
    bids0 = _get_levels(prev_snap, "bids")[: max(0, int(levels))]
    asks0 = _get_levels(prev_snap, "asks")[: max(0, int(levels))]
    bids1 = _get_levels(snap, "bids")[: max(0, int(levels))]
    asks1 = _get_levels(snap, "asks")[: max(0, int(levels))]
    L = max(len(bids0), len(asks0), len(bids1), len(asks1), int(levels))
    if L <= 0:
        return {}
    ofi_sum = 0.0
    ofi_wsum = 0.0
    depth_sum = 0.0
    for k in range(1, L + 1):
        b0 = bids0[k - 1][1] if k - 1 < len(bids0) else 0.0
        a0 = asks0[k - 1][1] if k - 1 < len(asks0) else 0.0
        b1 = bids1[k - 1][1] if k - 1 < len(bids1) else 0.0
        a1 = asks1[k - 1][1] if k - 1 < len(asks1) else 0.0
        depth_sum += max(0.0, b1) + max(0.0, a1)
        ofi_k = (b1 - b0) - (a1 - a0)
        if not _is_finite(ofi_k):
            ofi_k = 0.0
        ofi_sum += ofi_k
        ofi_wsum += ofi_k / float(k)
    eps = 1e-9
    ofi_norm = ofi_sum / (depth_sum + eps) if depth_sum > 0 else 0.0
    return {
        "ofi_ml": float(ofi_sum),
        "ofi_ml_wsum": float(ofi_wsum),
        "ofi_ml_norm": float(ofi_norm),
        "depth_top5_sum": float(depth_sum),
    }
