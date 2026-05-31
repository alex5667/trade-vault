"""kelly_sizer_v2.py — Quarter-Kelly position-size scaler (P2.6).

Shadow mode (default):  computes Kelly multiplier, writes it to indicators,
                        does NOT change effective_risk_pct.
Enforce mode:           multiplies effective_risk_pct by the Kelly multiplier.

Activation priority (highest first):
  1. ENV KELLY_SIZING_ENABLED=1  — manual override, always respected.
  2. Autopilot flag `kelly_sizing_enabled` in Redis hash
     calibration:autopilot:state — activated by calibration_autopilot_v1
     after meta_label_gate has been active >= AUTOPILOT_KELLY_GATE_MIN_HOURS (48h)
     AND model roc_auc_oos >= AUTOPILOT_KELLY_MIN_AUC (0.58).
  3. Default: shadow (no sizing applied).

ENV:
  KELLY_SIZING_ENABLED  1 = enforce, 0 = shadow (default 0)
  KELLY_FRACTION        Kelly fraction, default 0.25 (quarter-Kelly)
  KELLY_MIN_SCALE       Floor multiplier, default 0.50
  KELLY_MAX_SCALE       Cap multiplier,   default 1.50
  KELLY_AUTOPILOT_REDIS_URL  Redis URL to read autopilot state (default REDIS_URL)

P_Edge comes from of_confirm.evidence.ml.p_edge (calibrated win prob).
RR ratio (b) = tp1_target_r / 1.0  (SL always = 1R by definition).

Kelly formula: f* = p - (1-p)/b,  scaled_f = f* × KELLY_FRACTION,
               multiplier = clamp(scaled_f / baseline_f, MIN, MAX)
               where baseline_f = 0.25 × (0.50 - 0.50/1.0) = 0 →
               we use a simpler ratio: multiplier = clamp(scaled_f / 0.25, MIN, MAX).
"""
from __future__ import annotations

import logging
import math
import os
from typing import Any

logger = logging.getLogger(__name__)

_KELLY_FRACTION: float = float(os.getenv("KELLY_FRACTION", "0.25"))
_KELLY_MIN_SCALE: float = float(os.getenv("KELLY_MIN_SCALE", "0.50"))
_KELLY_MAX_SCALE: float = float(os.getenv("KELLY_MAX_SCALE", "1.50"))

# Reference full-Kelly at p=0.55, b=1.5 → f*=0.18 → quarter-Kelly=0.045
# We normalise to fraction 0.25 as baseline so scale=1.0 means "normal".
_BASELINE_KELLY: float = 0.25  # normalisation anchor

# ── Autopilot state cache ─────────────────────────────────────────────────────
# Cached so we don't hit Redis on every signal. TTL = 300s (same as model gate).

_autopilot_enforce: bool = os.getenv("KELLY_SIZING_ENABLED", "0").strip() == "1"
_autopilot_rc: Any = None
_autopilot_checked_at: float = 0.0
_AUTOPILOT_TTL_SEC: float = 300.0


def _ensure_rc() -> Any:
    global _autopilot_rc
    if _autopilot_rc is None:
        try:
            import redis as _redis  # type: ignore
            url = os.getenv("KELLY_AUTOPILOT_REDIS_URL", os.getenv("REDIS_URL", ""))
            if url:
                _autopilot_rc = _redis.from_url(url, decode_responses=True)
        except Exception:
            pass
    return _autopilot_rc


def _kelly_enforce() -> bool:
    """Return True if Kelly enforcement is active (ENV or autopilot flag)."""
    global _autopilot_enforce, _autopilot_checked_at
    if _autopilot_enforce:
        return True
    now = os.times().elapsed if hasattr(os, "times") else 0.0
    import time as _time
    now = _time.monotonic()
    if now - _autopilot_checked_at < _AUTOPILOT_TTL_SEC:
        return _autopilot_enforce
    rc = _ensure_rc()
    if rc is not None:
        try:
            from orderflow_services.calibration_autopilot_v1 import read_autopilot_flag
            _autopilot_enforce = read_autopilot_flag(rc, "kelly_sizing_enabled")
        except Exception:
            pass
    _autopilot_checked_at = now
    return _autopilot_enforce


def compute_kelly_scale(
    p_edge: float,
    tp1_target_r: float,
    *,
    kelly_fraction: float = _KELLY_FRACTION,
    min_scale: float = _KELLY_MIN_SCALE,
    max_scale: float = _KELLY_MAX_SCALE,
) -> float:
    """Return position-size multiplier in [min_scale, max_scale].

    Returns 1.0 on degenerate inputs (p_edge=0, bad rr) so caller is safe.
    """
    if p_edge <= 0.0:
        return 1.0
    if p_edge >= 1.0:
        return max_scale
    b = tp1_target_r if tp1_target_r and tp1_target_r > 0 else 1.0
    q = 1.0 - p_edge
    full_kelly = p_edge - q / b
    if full_kelly <= 0:
        return min_scale
    frac_kelly = full_kelly * kelly_fraction
    # Normalise: scale = frac_kelly / (baseline_kelly × kelly_fraction)
    # baseline_kelly for "fair coin at 1:1" = 0.0 → degenerate; use fixed anchor.
    anchor = _BASELINE_KELLY * kelly_fraction
    scale = frac_kelly / anchor if anchor > 0 else 1.0
    return max(min_scale, min(max_scale, scale))


def apply_kelly_sizing(
    indicators: dict[str, Any],
    effective_risk_pct: float,
    *,
    enforce: bool,
    symbol: str = "",
    kind: str = "",
) -> float:
    """Compute Kelly scale from indicators, write shadow metrics, optionally apply.

    Returns (possibly modified) effective_risk_pct.
    """
    p_edge = float(indicators.get("p_edge", 0.0) or 0.0)
    tp1_target_r = float(indicators.get("tp1_target_r", 1.5) or 1.5)

    scale = compute_kelly_scale(p_edge, tp1_target_r)

    indicators["kelly_scale_shadow"] = round(scale, 4)
    indicators["kelly_p_edge_input"] = round(p_edge, 4)

    effective_enforce = enforce or _kelly_enforce()
    if effective_enforce and math.isfinite(scale) and scale > 0:
        new_risk = effective_risk_pct * scale
        logger.info(
            "[KELLY] %s %s p_edge=%.3f tp1r=%.2f scale=%.3f risk %.2f→%.2f enforce=%s",
            symbol, kind, p_edge, tp1_target_r, scale, effective_risk_pct, new_risk,
            "env" if enforce else "autopilot",
        )
        return new_risk

    return effective_risk_pct
