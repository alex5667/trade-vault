from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, Optional, Tuple, Union


def compute_daily_pivots(hlc: Optional[Dict[str, float]]) -> Dict[str, float]:
    """
    Вычисляет Classic Pivot Points (Floor Pivots) по HLC предыдущего дня.
    
    Args:
        hlc: Словарь с ключами high, low, close для предыдущего дня.
        
    Returns:
        Словарь с ключами P, R1..R3, S1..S3.
    """
    if not hlc or not all(k in hlc for k in ("high", "low", "close")):
        return {}
        
    try:
        high = float(hlc["high"])
        low = float(hlc["low"])
        close = float(hlc["close"])
    except (ValueError, TypeError):
        return {}
        
    p = (high + low + close) / 3.0
    
    r1 = 2 * p - low
    s1 = 2 * p - high
    
    r2 = p + (high - low)
    s2 = p - (high - low)
    
    r3 = high + 2 * (p - low)
    s3 = low - 2 * (high - p)
    
    return {
        "P": p,
        "R1": r1,
        "S1": s1,
        "R2": r2,
        "S2": s2,
        "R3": r3,
        "S3": s3,
    }

def _dist_bps(price: float, level: float) -> float:
    """
    Basis points distance between price and level:
      bps = |price - level| / price * 10_000
    """
    if price <= 0:
        return float("inf")
    return abs(price - level) / price * 10_000.0


@dataclass(frozen=True)
class PivotProximityCfg:
    dist_atr_threshold: float = 0.5
    dist_bp_threshold: Optional[float] = None
    dist_mode: str = "or"
    key_levels: Tuple[str, ...] = ("P", "R1", "S1", "R2", "S2", "R3", "S3")


def check_pivot_proximity(
    price: float,
    pivots: Dict[str, Any],
    atr: Optional[float],
    cfg: PivotProximityCfg,
    *,
    return_details: bool = False,
) -> Union[bool, Tuple[bool, Dict[str, Any]]]:
    """
    Pivot proximity check (ATR + optional BPS) with rich details.
    Intended usage:
      - return_details=True on signal/veto paths (telemetry),
      - return_details=False in hot loops.
    """
    if price <= 0:
        if return_details:
            return False, {"reason": "bad_price", "price": price}
        return False

    closest_key: Optional[str] = None
    closest_level: Optional[float] = None
    min_distance = float("inf")

    # Find closest level
    for k in cfg.key_levels:
        lv = pivots.get(k)
        if lv is None:
            continue
        try:
            level_f = float(lv)
        except Exception:
            continue
        d = abs(price - level_f)
        if d < min_distance:
            min_distance = d
            closest_key = k
            closest_level = level_f

    if closest_level is None or closest_key is None:
        if return_details:
            return False, {"reason": "no_levels"}
        return False

    # ATR distance
    near_atr = False
    dist_atr = float("inf")
    if atr is not None and atr > 0:
        dist_atr = min_distance / atr
        near_atr = dist_atr <= cfg.dist_atr_threshold
    else:
        # Backward-compatible fail-closed if BPS is not enabled
        if cfg.dist_bp_threshold is None:
            if return_details:
                return False, {"reason": "bad_atr", "atr": atr, "closest_key": closest_key, "closest_level": closest_level}
            return False

    # BPS distance (optional)
    dist_bps = _dist_bps(price, closest_level)
    thr_bp = cfg.dist_bp_threshold
    near_bps = False
    if thr_bp is not None:
        near_bps = dist_bps <= float(thr_bp)
        mode = (cfg.dist_mode or "or").strip().lower()
        if mode not in ("or", "and"):
            mode = "or"
        passed = (near_atr or near_bps) if mode == "or" else (near_atr and near_bps)
        mode_used = mode
    else:
        passed = near_atr
        mode_used = "atr_only"

    if return_details:
        return passed, {
            "closest_key": closest_key,
            "closest_level": float(closest_level),
            "price": float(price),
            "min_distance": float(min_distance),
            "atr": float(atr) if atr is not None else None,
            "dist_atr": float(dist_atr),
            "dist_bps": float(dist_bps),
            "thr_atr": float(cfg.dist_atr_threshold),
            "thr_bp": float(thr_bp) if thr_bp is not None else None,
            "near_atr": bool(near_atr),
            "near_bps": bool(near_bps),
            "mode": mode_used,
        }
    return passed


def is_near_level_atr(price: float, pivots: dict, atr: float, threshold: float = 0.5) -> bool:
    """
    Backward-compatible ATR-only wrapper (keeps old signature and behavior).
    """
    cfg = PivotProximityCfg(dist_atr_threshold=threshold, dist_bp_threshold=None, dist_mode="or")
    return bool(check_pivot_proximity(price, pivots, atr, cfg, return_details=False))
