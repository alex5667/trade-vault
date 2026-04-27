from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict, List, Tuple
import math


def _f(x: Any, d: float = 0.0) -> float:
    """Safe float conversion with default."""
    try:
        if x is None:
            return d
        v = float(x)
        return v if math.isfinite(v) else d
    except Exception:
        return d


@dataclass(frozen=True)
class Barriers:
    """Triple-barrier configuration: TP/SL in bps and scale reference."""
    tp_bps: float
    sl_bps: float
    scale_bps: float  # stop_bps or atr_bps (for R-multiple calculation)


def infer_tp_sl_bps(
    indicators: Dict[str, Any],
    *,
    tp_k_atr: float,
    sl_k_atr: float,
    fallback_tp_bps: float,
    fallback_sl_bps: float,
) -> Barriers:
    """
    Infer TP/SL barriers from indicators.
    
    Priority: stop_bps > atr_bps > fallback.
    
    Args:
        indicators: Dict with stop_bps, atr_bps, etc.
        tp_k_atr: Multiplier for TP (e.g., 1.0 = 1x stop/atr)
        sl_k_atr: Multiplier for SL (e.g., 1.0 = 1x stop/atr)
        fallback_tp_bps: Default TP if no stop/atr available
        fallback_sl_bps: Default SL if no stop/atr available
    
    Returns:
        Barriers with tp_bps, sl_bps, scale_bps
    """
    stop_bps = _f(indicators.get("stop_bps", 0.0), 0.0)
    atr_bps = _f(indicators.get("atr_bps", 0.0), 0.0)

    if stop_bps > 1e-6:
        return Barriers(tp_bps=tp_k_atr * stop_bps, sl_bps=sl_k_atr * stop_bps, scale_bps=stop_bps)
    if atr_bps > 1e-6:
        return Barriers(tp_bps=tp_k_atr * atr_bps, sl_bps=sl_k_atr * atr_bps, scale_bps=atr_bps)
    return Barriers(tp_bps=fallback_tp_bps, sl_bps=fallback_sl_bps, scale_bps=0.0)


def signed_ret_bps(direction: str, entry_px: float, px: float) -> float:
    """
    Compute signed return in bps (positive for favorable move).
    
    For LONG: positive if px > entry_px
    For SHORT: positive if px < entry_px
    """
    if entry_px <= 0.0 or px <= 0.0:
        return 0.0
    ret = 10000.0 * (px - entry_px) / entry_px
    return ret if (direction or "").upper() == "LONG" else -ret


def barrier_stats(
    *,
    ts0: int,
    direction: str,
    entry_px: float,
    path: List[Tuple[int, float]],  # (ts, px) ascending
    b: Barriers,
    h_ms: int,
    adv_max: float,
) -> Dict[str, Any]:
    """
    Compute triple-barrier outcome + MAE/MFE + adverse_proxy.

    Returns:
        label: TP|SL|TIMEOUT|NO_TICKS
        hit_ms: timestamp when barrier hit (or timeout)
        ret_bps: signed return at hit/timeout
        r_mult: ret_bps/scale_bps (R-multiple)
        mae_bps: max adverse move magnitude in bps
        mfe_bps: max favorable move in bps
        adverse_proxy: mae/mfe if mfe>0 else mae (risk-adjusted quality)
        y_edge: 1 if TP hit AND adverse_proxy<=adv_max else 0
    """
    ts1 = ts0 + int(h_ms)
    if entry_px <= 0.0 or not path:
        return {
            "h_ms": int(h_ms),
            "label": "NO_TICKS",
            "hit_ms": int(ts1),
            "ret_bps": 0.0,
            "r_mult": 0.0,
            "mae_bps": 0.0,
            "mfe_bps": 0.0,
            "adverse_proxy": 0.0,
            "y_edge": 0,
        }

    tp = float(b.tp_bps)
    sl = float(b.sl_bps)

    label = "TIMEOUT"
    hit_ms = ts1
    ret_bps = 0.0

    mae = 0.0  # most negative (adverse)
    mfe = 0.0  # most positive (favorable)

    for ts, px in path:
        if ts < ts0:
            continue
        if ts > ts1:
            break
        r = signed_ret_bps(direction, entry_px, px)
        ret_bps = r
        if r > mfe:
            mfe = r
        if r < mae:
            mae = r

        if tp > 0.0 and r >= tp:
            label = "TP"
            hit_ms = int(ts)
            break
        if sl > 0.0 and r <= -sl:
            label = "SL"
            hit_ms = int(ts)
            break

    mae_mag = abs(mae)
    mfe_mag = max(0.0, mfe)
    adverse_proxy = (mae_mag / mfe_mag) if mfe_mag > 1e-9 else mae_mag

    r_mult = float(ret_bps / b.scale_bps) if b.scale_bps > 1e-9 else 0.0
    y_edge = 1 if (label == "TP" and adverse_proxy <= float(adv_max)) else 0

    return {
        "h_ms": int(h_ms),
        "label": label,
        "hit_ms": int(hit_ms),
        "ret_bps": float(ret_bps),
        "r_mult": float(r_mult),
        "mae_bps": float(mae_mag),
        "mfe_bps": float(mfe_mag),
        "adverse_proxy": float(adverse_proxy),
        "y_edge": int(y_edge),
    }


def exec_cost_r(indicators: Dict[str, Any], scale_bps: float) -> float:
    """
    Compute execution cost in R-multiples (spread + slippage normalized by scale).
    
    Returns:
        (spread_bps + expected_slippage_bps) / scale_bps
    """
    if scale_bps <= 1e-9:
        return 0.0
    spread = _f(indicators.get("spread_bps", 0.0), 0.0)
    slip = _f(indicators.get("expected_slippage_bps", 0.0), 0.0)
    return float(max(0.0, spread + slip) / scale_bps)

