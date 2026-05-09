from __future__ import annotations

"""atr_horizon_live_surface.py — Phase 2.4B: live risk surface builder from selected ATR.

Computes live sl_price / tp1_price / max_signal_age_ms from meta.atr_profile.
Does NOT touch trailing — that stays on get_atr(pos.symbol) path downstream.

Fail-open: missing/invalid fields → reason_code=LIVE_SURFACE_INCOMPLETE, zero prices.
"""

import math
import os
from dataclasses import asdict, dataclass
from typing import Any


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


def _ensure_dict(v: Any) -> dict[str, Any]:
    return dict(v) if isinstance(v, dict) else {}


@dataclass(frozen=True)
class LiveRiskSurface:
    mode: str
    atr_tf_ms: int
    atr_value: float
    atr_pct: float
    sl_atr_mult: float
    tp1_atr_mult: float
    selected_sl_price: float
    selected_tp1_price: float
    selected_stop_dist_px: float
    selected_tp1_dist_px: float
    selected_max_signal_age_ms: int
    reason_code: str


def build_live_risk_surface(signal: dict[str, Any]) -> dict[str, Any]:
    """Phase 2.4B: compute live stop/entry/TTL surface from selected ATR (meta.atr_profile).

    Side aliases: LONG → BUY, SHORT → SELL.
    Returns a flat dict (via asdict) — safe for JSON serialisation and meta enrichment.

    Caller decides whether to apply (canary router controls that separately).
    Fail-open: always returns a valid dict; reason_code communicates completeness.
    """
    signal = _ensure_dict(signal)
    meta = _ensure_dict(signal.get("meta"))
    horizon = _ensure_dict(meta.get("horizon"))
    atr_profile = _ensure_dict(meta.get("atr_profile"))

    # Side normalisation (support LONG/SHORT aliases)
    side = (signal.get("side") or "").upper()
    if side == "LONG":
        side = "BUY"
    elif side == "SHORT":
        side = "SELL"

    entry_price = _safe_float(
        signal.get("entry_price")
        or signal.get("entry")
        or signal.get("price"),
        0.0,
    )

    # Selected ATR from Phase 2 atr_profile
    atr_value = _safe_float(atr_profile.get("atr_value"), 0.0)
    atr_tf_ms = _safe_int(atr_profile.get("atr_tf_ms"), 0)
    atr_pct = _safe_float(atr_profile.get("atr_pct"), 0.0)

    # TTL from horizon contract (Phase 0)
    max_signal_age_ms = _safe_int(horizon.get("max_signal_age_ms"), 0)

    # Multipliers: meta > signal > ENV defaults (same precedence as shadow surface)
    sl_atr_mult = _safe_float(
        meta.get("sl_atr_mult")
        or signal.get("sl_atr_mult")
        or os.getenv("ATR_HORIZON_LIVE_SL_ATR_MULT", "1.5"),
        1.5,
    )
    tp1_atr_mult = _safe_float(
        meta.get("tp1_atr_mult")
        or signal.get("tp1_atr_mult")
        or os.getenv("ATR_HORIZON_LIVE_TP1_ATR_MULT", "2.0"),
        2.0,
    )

    stop_dist = atr_value * sl_atr_mult if atr_value > 0.0 else 0.0
    tp1_dist = atr_value * tp1_atr_mult if atr_value > 0.0 else 0.0

    if side == "BUY":
        sl_price = entry_price - stop_dist if entry_price > 0.0 else 0.0
        tp1_price = entry_price + tp1_dist if entry_price > 0.0 else 0.0
    elif side == "SELL":
        sl_price = entry_price + stop_dist if entry_price > 0.0 else 0.0
        tp1_price = entry_price - tp1_dist if entry_price > 0.0 else 0.0
    else:
        sl_price = 0.0
        tp1_price = 0.0

    reason = (
        "LIVE_SURFACE_OK"
        if (atr_value > 0.0 and entry_price > 0.0 and side in ("BUY", "SELL"))
        else "LIVE_SURFACE_INCOMPLETE"
    )

    return asdict(LiveRiskSurface(
        mode="live_canary_candidate",
        atr_tf_ms=atr_tf_ms,
        atr_value=atr_value,
        atr_pct=atr_pct,
        sl_atr_mult=sl_atr_mult,
        tp1_atr_mult=tp1_atr_mult,
        selected_sl_price=sl_price,
        selected_tp1_price=tp1_price,
        selected_stop_dist_px=stop_dist,
        selected_tp1_dist_px=tp1_dist,
        selected_max_signal_age_ms=max_signal_age_ms,
        reason_code=reason,
    ))
