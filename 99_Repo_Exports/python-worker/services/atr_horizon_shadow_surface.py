from __future__ import annotations

import math
import os
from dataclasses import dataclass, asdict
from typing import Any, Dict


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        return x if math.isfinite(x) else default
    except Exception:
        return default


def _ensure_dict(v: Any) -> Dict[str, Any]:
    return dict(v) if isinstance(v, dict) else {}


@dataclass(frozen=True)
class RiskSurfaceShadow:
    mode: str
    atr_tf_ms: int
    atr_value: float
    atr_pct: float
    hold_target_ms: int
    alpha_half_life_ms: int
    max_signal_age_ms: int
    sl_atr_mult: float
    tp1_atr_mult: float
    selected_stop_dist_px: float
    selected_tp1_dist_px: float
    selected_sl_price_shadow: float
    selected_tp1_price_shadow: float
    entry_ttl_ms_shadow: int
    risk_reason_code: str


def build_risk_surface_shadow(signal: Dict[str, Any]) -> Dict[str, Any]:
    """
    Phase 2.2: compute shadow stop/entry risk surface from selected ATR.

    Reads meta.atr_profile (Phase 2) and meta.horizon for context.
    Does NOT modify sl_price / tp1_price — returns a separate shadow-only surface.

    Fail-open: missing fields produce RS_SHADOW_INCOMPLETE with zero prices.
    """
    signal = _ensure_dict(signal)
    meta = _ensure_dict(signal.get("meta"))
    horizon = _ensure_dict(meta.get("horizon"))
    atr_profile = _ensure_dict(meta.get("atr_profile"))

    side = str(signal.get("side") or "").upper()
    # Normalise BUY/LONG aliases
    if side in ("LONG",):
        side = "BUY"
    elif side in ("SHORT",):
        side = "SELL"

    entry_price = _safe_float(
        signal.get("entry_price")
        or signal.get("entry")
        or signal.get("price")
        0.0
    )

    # Selected ATR from atr_profile (Phase 2 selector output)
    atr_value = _safe_float(atr_profile.get("atr_value"), 0.0)
    atr_tf_ms = _safe_int(atr_profile.get("atr_tf_ms"), 0)
    atr_pct = _safe_float(atr_profile.get("atr_pct"), 0.0)

    hold_target_ms = _safe_int(horizon.get("hold_target_ms"), 0)
    alpha_half_life_ms = _safe_int(horizon.get("alpha_half_life_ms"), 0)
    max_signal_age_ms = _safe_int(horizon.get("max_signal_age_ms"), 0)

    # Multipliers: meta > signal > ENV defaults
    sl_atr_mult = _safe_float(
        meta.get("sl_atr_mult")
        or signal.get("sl_atr_mult")
        or os.getenv("ATR_HORIZON_SHADOW_SL_ATR_MULT", "1.5")
        1.5
    )
    tp1_atr_mult = _safe_float(
        meta.get("tp1_atr_mult")
        or signal.get("tp1_atr_mult")
        or os.getenv("ATR_HORIZON_SHADOW_TP1_ATR_MULT", "2.0")
        2.0
    )

    stop_dist = atr_value * sl_atr_mult if atr_value > 0.0 else 0.0
    tp1_dist = atr_value * tp1_atr_mult if atr_value > 0.0 else 0.0

    if side == "BUY":
        sl_shadow = entry_price - stop_dist if entry_price > 0.0 else 0.0
        tp1_shadow = entry_price + tp1_dist if entry_price > 0.0 else 0.0
    elif side == "SELL":
        sl_shadow = entry_price + stop_dist if entry_price > 0.0 else 0.0
        tp1_shadow = entry_price - tp1_dist if entry_price > 0.0 else 0.0
    else:
        sl_shadow = 0.0
        tp1_shadow = 0.0

    reason = "RS_SHADOW_OK" if (atr_value > 0.0 and entry_price > 0.0) else "RS_SHADOW_INCOMPLETE"

    return asdict(RiskSurfaceShadow(
        mode="shadow"
        atr_tf_ms=atr_tf_ms
        atr_value=atr_value
        atr_pct=atr_pct
        hold_target_ms=hold_target_ms
        alpha_half_life_ms=alpha_half_life_ms
        max_signal_age_ms=max_signal_age_ms
        sl_atr_mult=sl_atr_mult
        tp1_atr_mult=tp1_atr_mult
        selected_stop_dist_px=stop_dist
        selected_tp1_dist_px=tp1_dist
        selected_sl_price_shadow=sl_shadow
        selected_tp1_price_shadow=tp1_shadow
        entry_ttl_ms_shadow=max_signal_age_ms
        risk_reason_code=reason
    ))
