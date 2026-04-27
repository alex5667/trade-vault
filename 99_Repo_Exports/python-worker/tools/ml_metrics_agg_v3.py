from __future__ import annotations
from typing import Any, Dict, List, Tuple
import math


def _f(x: Any, d: float = 0.0) -> float:
    """Convert to float with default."""
    try:
        return float(x)
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    """Convert to int with default."""
    try:
        return int(float(x))
    except Exception:
        return d


def pctl(xs: List[float], q: float) -> float:
    """Compute percentile of sorted list.
    
    Args:
        xs: List of floats (will be sorted)
        q: Quantile (0.0..1.0)
        
    Returns:
        Percentile value
    """
    if not xs:
        return 0.0
    xs = sorted(xs)
    i = int(round((len(xs) - 1) * q))
    i = max(0, min(len(xs) - 1, i))
    return float(xs[i])


def agg_health_ml_confirm(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate health metrics from metrics:ml_confirm stream.
    
    Computes:
    - n: sample size
    - missing_rate: fraction of rows with missing=1
    - err_rate: fraction of rows with non-empty err
    - lat_p99_ms: 99th percentile latency in milliseconds
    
    Args:
        rows: List of message field dicts from metrics:ml_confirm
        
    Returns:
        Dict with n, missing_rate, err_rate, lat_p99_ms
    """
    n = len(rows)
    if n == 0:
        return {"n": 0}
    missing = 0
    err = 0
    lat = []
    for r in rows:
        missing += 1 if _i(r.get("missing", 0), 0) == 1 else 0
        err += 1 if (str(r.get("err", "")) or "").strip() != "" else 0
        lat.append(_f(r.get("latency_ms", 0.0), 0.0))
    return {
        "n": n,
        "missing_rate": missing / n,
        "err_rate": err / n,
        "lat_p99_ms": pctl(lat, 0.99),
    }


def agg_exec_risk(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate exec_risk_norm metrics.
    
    Args:
        rows: List of message field dicts with exec_risk_norm
        
    Returns:
        Dict with n, exec_p90 (90th percentile)
    """
    xs = [_f(r.get("exec_risk_norm", 0.0), 0.0) for r in rows]
    return {"n": len(xs), "exec_p90": pctl(xs, 0.90) if xs else 0.0}


def agg_selected(rows: List[Dict[str, Any]], t: float) -> Dict[str, Any]:
    """Aggregate metrics for rows with p_edge >= threshold.
    
    Computes:
    - n: sample size (selected rows)
    - meanR: mean r_mult
    - tail_rate: fraction of r_mult <= -1.0
    - es05: Expected Shortfall (mean of worst 5% R)
    - win_rate: fraction with y=1
    
    Args:
        rows: List of message field dicts from metrics:ml_outcome (must have p_edge, r_mult, y)
        t: Threshold (p_edge >= t)
        
    Returns:
        Dict with n, meanR, tail_rate, es05, win_rate
    """
    sel = []
    for r in rows:
        p = _f(r.get("p_edge", 0.0), 0.0)
        if p >= t:
            sel.append(r)
    n = len(sel)
    if n == 0:
        return {"n": 0}
    rm = [_f(r.get("r_mult", 0.0), 0.0) for r in sel]
    y = [int(float(r.get("y", 0) or 0)) for r in sel]
    meanR = sum(rm) / n
    tail = sum(1 for x in rm if x <= -1.0) / n
    # ES05: mean of worst 5% R
    k = max(1, int(round(0.05 * n)))
    es05 = sum(sorted(rm)[:k]) / k
    win = sum(y) / n
    return {
        "n": n,
        "meanR": float(meanR),
        "tail_rate": float(tail),
        "es05": float(es05),
        "win_rate": float(win),
    }


def pick_threshold(
    rows_short: List[Dict[str, Any]],
    rows_long: List[Dict[str, Any]],
    *,
    grid: List[float],
    min_n_short: int,
    min_n_long: int,
    tail_max: float,
    meanR_min: float,
    es05_min: float,
) -> Tuple[float, Dict[str, Any], Dict[str, Any]]:
    """Pick smallest threshold that satisfies constraints on both windows.
    
    Primary strategy: find smallest threshold where both windows satisfy:
    - n >= min_n
    - tail_rate <= tail_max
    - meanR >= meanR_min
    - es05 >= es05_min
    
    Fallback: if no threshold satisfies all constraints, pick threshold that maximizes
    conservative score (meanR - 0.5*tail_rate) on long window with min_n_long.
    
    Args:
        rows_short: Short window outcomes (e.g., 24h)
        rows_long: Long window outcomes (e.g., 168h)
        grid: List of thresholds to try (sorted ascending)
        min_n_short: Minimum sample size for short window
        min_n_long: Minimum sample size for long window
        tail_max: Maximum allowed tail_rate
        meanR_min: Minimum required meanR
        es05_min: Minimum required ES05
        
    Returns:
        Tuple of (best_threshold, short_stats, long_stats)
        If no valid threshold found, returns (0.0, {"n":0}, {"n":0})
    """
    best = None
    best_stats = None
    best_long = None
    for t in grid:
        s = agg_selected(rows_short, t)
        l = agg_selected(rows_long, t)
        if s["n"] < min_n_short or l["n"] < min_n_long:
            continue
        if not (s["tail_rate"] <= tail_max and l["tail_rate"] <= tail_max):
            continue
        if not (s["meanR"] >= meanR_min and l["meanR"] >= meanR_min):
            continue
        if not (s["es05"] >= es05_min and l["es05"] >= es05_min):
            continue
        best = t
        best_stats = s
        best_long = l
        break
    if best is None:
        # fallback: pick threshold that maximizes meanR_long with min_n_long and tail constraint relaxed
        best = -1.0
        best_stats = {"n": 0}
        best_long = {"n": 0}
        best_score = -1e9
        for t in grid:
            l = agg_selected(rows_long, t)
            if l["n"] < min_n_long:
                continue
            score = l["meanR"] - 0.5 * l["tail_rate"]  # conservative
            if score > best_score:
                best_score = score
                best = t
                best_long = l
                best_stats = agg_selected(rows_short, t)
        if best < 0:
            return 0.0, {"n": 0}, {"n": 0}
    return float(best), best_stats, best_long

