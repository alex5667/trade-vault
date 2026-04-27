from __future__ import annotations

from typing import Any, Dict, List, Tuple
import math


def _f(x: Any, d: float = 0.0) -> float:
    """Safe float conversion."""
    try:
        return float(x)
    except Exception:
        return d


def pctl(xs: List[float], q: float) -> float:
    """Compute percentile (0.0 to 1.0)."""
    if not xs:
        return 0.0
    xs = sorted(xs)
    i = int(round((len(xs) - 1) * q))
    i = max(0, min(len(xs) - 1, i))
    return float(xs[i])


def ece_bin(p: float, y: int, n_bins: int = 10) -> int:
    """Returns bin index for ECE (Expected Calibration Error) calculation."""
    b = int(min(n_bins - 1, max(0, math.floor(p * n_bins))))
    return b


def agg_outcomes(rows: List[Dict[str, Any]], *, n_bins: int = 10) -> Dict[str, Any]:
    """Aggregate outcome metrics: Brier, ECE, win rate, R-mult stats.
    
    Computes:
    - Brier score (mean squared error)
    - ECE (Expected Calibration Error) via binning
    - Win rate (y=1 rate)
    - R-mult percentiles (p05, p50, mean)
    - Challenger metrics if present
    
    Args:
        rows: List of outcome dicts (from metrics:ml_outcome)
        n_bins: Number of bins for ECE calculation (default: 10)
        
    Returns:
        Dict with n, win_rate, brier, ece, r_mean, r_p05, r_p50, and optional challenger metrics
    """
    n = 0
    wins = 0
    briers = []
    rm = []

    bins_n = [0] * n_bins
    bins_p = [0.0] * n_bins
    bins_y = [0.0] * n_bins

    # challenger
    briers_ch = []
    rm_ch = []
    bins_n_ch = [0] * n_bins
    bins_p_ch = [0.0] * n_bins
    bins_y_ch = [0.0] * n_bins

    for r in rows:
        y = int(float(r.get("y", 0) or 0))
        p = _f(r.get("p_edge", 0.0), 0.0)
        briers.append((p - float(y)) ** 2)
        rm.append(_f(r.get("r_mult", 0.0), 0.0))
        wins += y
        n += 1

        bi = ece_bin(p, y, n_bins)
        bins_n[bi] += 1
        bins_p[bi] += p
        bins_y[bi] += float(y)

        if "p_edge_chal" in r and "brier_chal" in r:
            pc = _f(r.get("p_edge_chal", 0.0), 0.0)
            briers_ch.append((pc - float(y)) ** 2)
            rm_ch.append(_f(r.get("r_mult", 0.0), 0.0))
            bic = ece_bin(pc, y, n_bins)
            bins_n_ch[bic] += 1
            bins_p_ch[bic] += pc
            bins_y_ch[bic] += float(y)

    def _ece(bn, bp, by) -> float:
        """Compute ECE from bin counts."""
        total = sum(bn)
        if total == 0:
            return 0.0
        e = 0.0
        for i in range(len(bn)):
            if bn[i] == 0:
                continue
            avg_p = bp[i] / bn[i]
            avg_y = by[i] / bn[i]
            e += (bn[i] / total) * abs(avg_p - avg_y)
        return float(e)

    # Utility metrics: meanR, tail_rate, ES05 (mean of worst 5%)
    meanR = (sum(rm) / len(rm)) if rm else 0.0
    tail = sum(1 for rv in rm if rv <= -1.0)
    tail_rate = (tail / n) if n else 0.0
    es05 = 0.0
    if rm:
        k = max(1, int(round(0.05 * len(rm))))
        worst = sorted(rm)[:k]
        es05 = sum(worst) / len(worst)

    out = {
        "n": n,
        "win_rate": (wins / n) if n else 0.0,
        "brier": (sum(briers) / len(briers)) if briers else 0.0,
        "ece": _ece(bins_n, bins_p, bins_y),
        "meanR": float(meanR),
        "tail_rate": float(tail_rate),
        "es05": float(es05),
        "r_p05": pctl(rm, 0.05) if rm else 0.0,
        "r_p50": pctl(rm, 0.50) if rm else 0.0,
    }
    # Backward compatibility: keep r_mean
    out["r_mean"] = float(meanR)
    if briers_ch:
        out.update({
            "n_ch": len(briers_ch),
            "brier_ch": (sum(briers_ch) / len(briers_ch)),
            "ece_ch": _ece(bins_n_ch, bins_p_ch, bins_y_ch),
            "r_mean_ch": (sum(rm_ch) / len(rm_ch)) if rm_ch else 0.0,
        })
    return out


def agg_health_ml_confirm(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate health metrics from ml_confirm stream.
    
    Computes:
    - Missing rate (missing=1 count / total)
    - Error rate (err != "" count / total)
    - Latency p99 (99th percentile)
    - p_edge p50 (median prediction)
    
    Args:
        rows: List of ml_confirm dicts (from metrics:ml_confirm)
        
    Returns:
        Dict with n, missing_rate, err_rate, lat_p99_ms, p_edge_p50
    """
    n = len(rows)
    if n == 0:
        return {"n": 0}
    missing = 0
    err = 0
    lat = []
    p = []
    for r in rows:
        missing += 1 if int(float(r.get("missing", 0) or 0)) == 1 else 0
        err += 1 if (str(r.get("err", "")) or "").strip() != "" else 0
        lat.append(_f(r.get("latency_ms", 0.0), 0.0))
        p.append(_f(r.get("p_edge", 0.0), 0.0))
    return {
        "n": n,
        "missing_rate": missing / n,
        "err_rate": err / n,
        "lat_p99_ms": pctl(lat, 0.99),
        "p_edge_p50": pctl(p, 0.50),
    }


def agg_exec_risk(rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    """Aggregate exec_risk_norm metrics (for range exec-risk segment veto).
    
    Computes:
    - exec_p90: 90th percentile of exec_risk_norm (for range promotion veto)
    
    Args:
        rows: List of ml_confirm dicts (from metrics:ml_confirm, filtered by bucket=range and symbol)
        
    Returns:
        Dict with n, exec_p90
    """
    xs = []
    for r in rows:
        xs.append(_f(r.get("exec_risk_norm", 0.0), 0.0))
    return {"n": len(xs), "exec_p90": pctl(xs, 0.90) if xs else 0.0}

