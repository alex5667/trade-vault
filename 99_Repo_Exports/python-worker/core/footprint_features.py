from __future__ import annotations

from typing import Dict, Tuple, Any, List
import math


def _f(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else d
    except Exception:
        return d


def compute_bucket_stats(
    m: Dict[int, Tuple[float, float]],
    eps: float,
) -> Tuple[List[int], Dict[int, Dict[str, float]]]:
    """
    m: bucket_id -> (buy, sell)
    returns:
      keys_sorted
      stats[bucket_id] = {buy, sell, total, delta, imb_frac, imb_ratio, dom}
        dom: +1 if buy>sell, -1 if sell>buy, 0 if equal
    """
    keys = sorted(int(k) for k in m.keys())
    st: Dict[int, Dict[str, float]] = {}
    for k in keys:
        buy = _f(m.get(k, (0.0, 0.0))[0], 0.0)
        sell = _f(m.get(k, (0.0, 0.0))[1], 0.0)
        total = buy + sell
        if total <= eps:
            continue
        delta = buy - sell
        imb_frac = abs(delta) / (total + eps)              # 0..1
        mn = min(buy, sell)
        mx = max(buy, sell)
        imb_ratio = mx / (mn + eps)                        # >=1, corresponds to 3:1 etc.
        dom = 1.0 if buy > sell else (-1.0 if sell > buy else 0.0)
        st[k] = {
            "buy": buy,
            "sell": sell,
            "total": total,
            "delta": delta,
            "imb_frac": imb_frac,
            "imb_ratio": imb_ratio,
            "dom": dom,
        }
    keys2 = [k for k in keys if k in st]
    return keys2, st


def compute_edge_ladders(
    keys: List[int],
    st: Dict[int, Dict[str, float]],
    *,
    ratio_th: float,
    edge_buckets: int,
) -> Tuple[int, int]:
    """
    Edge ladders:
      low edge: consecutive SELL-dominant buckets from the very low (dom=-1) with imb_ratio>=ratio_th
      high edge: consecutive BUY-dominant buckets from the very high (dom=+1) with imb_ratio>=ratio_th
    Returns: (ladder_low_len_sell, ladder_high_len_buy)
    """
    if not keys:
        return 0, 0

    edge_n = max(1, min(int(edge_buckets), len(keys)))
    lo_keys = keys[:edge_n]
    hi_keys = list(reversed(keys[-edge_n:]))

    # low: walk upward from min bucket
    low_len = 0
    prev = None
    for k in lo_keys:
        if prev is not None and k != prev + 1:
            break
        s = st[k]
        if s["dom"] == -1.0 and s["imb_ratio"] >= ratio_th:
            low_len += 1
            prev = k
        else:
            break

    # high: walk downward from max bucket
    high_len = 0
    prev = None
    for k in hi_keys:
        if prev is not None and k != prev - 1:
            break
        s = st[k]
        if s["dom"] == 1.0 and s["imb_ratio"] >= ratio_th:
            high_len += 1
            prev = k
        else:
            break

    return low_len, high_len


def compute_poc(
    keys: List[int],
    st: Dict[int, Dict[str, float]],
) -> Tuple[int, float]:
    """
    POC bucket = bucket with max total volume.
    Returns: (poc_bucket_id, poc_total)
    """
    if not keys:
        return 0, 0.0
    best_k = 0
    best_total = 0.0
    for k in keys:
        t = st[k]["total"]
        if t > best_total:
            best_total = t
            best_k = k
    return best_k, float(best_total)


def poc_on_edge(
    *,
    poc_bucket: int,
    keys: List[int],
    edge_tol_buckets: int,
) -> Tuple[int, str]:
    """
    Returns: (poc_on_edge(0/1), edge_side "LOW"/"HIGH"/"NONE")
    """
    if not keys or poc_bucket == 0:
        return 0, "NONE"
    lo = keys[0]
    hi = keys[-1]
    tol = max(0, int(edge_tol_buckets))
    if abs(poc_bucket - lo) <= tol:
        return 1, "LOW"
    if abs(hi - poc_bucket) <= tol:
        return 1, "HIGH"
    return 0, "NONE"


def compute_eff_delta(
    *,
    bar_open: float,
    bar_close: float,
    bucket_px: float,
    bar_delta_sum: float,
    eps: float,
) -> float:
    """
    eff_delta = ticks_moved / |delta_sum|
    Use bucket_px as tick proxy (since true tick_size isn't present).
    """
    bp = max(eps, float(bucket_px))
    ticks = abs(float(bar_close) - float(bar_open)) / bp
    denom = max(eps, abs(float(bar_delta_sum)))
    return float(ticks / denom)
