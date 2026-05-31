"""LiqMap hard-risk gate.

Philosophy:
  - never widen SL to "fit" structure
  - if liquidation clusters imply stop would sit inside a dense adverse zone -> veto
  - gate supports SHADOW (observe only) and ENFORCE (hard veto)

The gate consumes already-computed liqmap_* features (see liqmap_features_v1).
"""

from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Dict


def _f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if v != v or v == float("inf") or v == float("-inf"):
            return float(default)
        return v
    except Exception:
        return float(default)


def _i(x: Any, default: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        try:
            return int(float(x))
        except Exception:
            return int(default)


@dataclass(frozen=True)
class LiqMapGateDecision:
    mode: str
    window: str
    shadow_veto: int
    veto: int
    reason: str
    risk_bps: float
    reward_bps: float
    rr: float
    adverse_peak_usd: float
    favorable_peak_usd: float


def evaluate_liqmap_gate_v1(
    *,
    direction: str,
    indicators: Dict[str, Any],
    cfg2: Dict[str, Any],
) -> LiqMapGateDecision:
    """Evaluate liqmap hard-risk gate.

    Required inputs:
      - indicators should include atr_bps_exec or atr_bps (bps units)
      - indicators must include liqmap_<window>_dist_up_bps / dist_dn_bps / peak_up1_usd / peak_dn1_usd

    Config keys (cfg2):
      - liqmap_gate_enable (0/1)
      - liqmap_gate_mode: OFF/SHADOW/ENFORCE
      - liqmap_gate_window: e.g. '5m'
      - liqmap_gate_sl_atr_mult: use stop_atr_mult if absent
      - liqmap_gate_sl_band_mult: multiply SL band for "adverse inside stop" check
      - liqmap_gate_min_peak_usd: ignore peaks smaller than this
      - liqmap_gate_min_rr: veto if rr < this (optional)
      - liqmap_gate_require_reward: if 1, missing favorable peak => veto
    """
    mode = str(cfg2.get("liqmap_gate_mode", "OFF") or "OFF").upper()
    # Backward/forward compatibility:
    # - legacy: explicit liqmap_gate_enable (0/1)
    # - current: mode alone enables the gate (SHADOW/ENFORCE)
    enable_raw = cfg2.get("liqmap_gate_enable", None)
    if enable_raw is None:
        enable = (mode != "OFF")
    else:
        enable = bool(_i(enable_raw, 0))
    window = str(cfg2.get("liqmap_gate_window", "5m") or "5m")

    if not enable or mode == "OFF":
        return LiqMapGateDecision(
            mode="OFF",
            window=window,
            shadow_veto=0,
            veto=0,
            reason="off",
            risk_bps=0.0,
            reward_bps=0.0,
            rr=0.0,
            adverse_peak_usd=0.0,
            favorable_peak_usd=0.0,
        )

    d = str(direction or "").upper()
    if d not in ("LONG", "SHORT"):
        d = "LONG"

    atr_bps = _f(indicators.get("atr_bps_exec", indicators.get("atr_bps", 0.0)), 0.0)
    stop_atr_mult = _f(cfg2.get("liqmap_gate_sl_atr_mult", cfg2.get("stop_atr_mult", 0.6)), 0.6)
    sl_band_mult = _f(cfg2.get("liqmap_gate_sl_band_mult", 1.0), 1.0)
    sl_band_bps = max(0.0, atr_bps * stop_atr_mult * sl_band_mult)

    dist_up = _f(indicators.get(f"liqmap_{window}_dist_up_bps", 0.0), 0.0)
    dist_dn = _f(indicators.get(f"liqmap_{window}_dist_dn_bps", 0.0), 0.0)
    peak_up_usd = _f(indicators.get(f"liqmap_{window}_peak_up1_usd", 0.0), 0.0)
    peak_dn_usd = _f(indicators.get(f"liqmap_{window}_peak_dn1_usd", 0.0), 0.0)

    if d == "LONG":
        risk_bps = float(dist_dn)
        reward_bps = float(dist_up)
        adverse_usd = float(peak_dn_usd)
        favorable_usd = float(peak_up_usd)
    else:
        risk_bps = float(dist_up)
        reward_bps = float(dist_dn)
        adverse_usd = float(peak_up_usd)
        favorable_usd = float(peak_dn_usd)

    # Backward-compatible alias: liqmap_gate_peak_min_usd
    min_peak_usd = _f(cfg2.get("liqmap_gate_min_peak_usd", cfg2.get("liqmap_gate_peak_min_usd", 0.0)), 0.0)
    min_rr = _f(cfg2.get("liqmap_gate_min_rr", 0.0), 0.0)
    require_reward = bool(_i(cfg2.get("liqmap_gate_require_reward", 0), 0))

    rr = 0.0
    if risk_bps > 0.0:
        rr = float(reward_bps / max(risk_bps, 1e-9))
    elif reward_bps > 0.0:
        rr = 999.0

    reason = "ok"
    would_veto = False

    if sl_band_bps > 0.0 and risk_bps > 0.0 and risk_bps <= sl_band_bps and adverse_usd >= min_peak_usd:
        would_veto = True
        reason = "adverse_peak_in_sl"

    if (not would_veto) and min_rr > 0.0:
        if require_reward and reward_bps <= 0.0:
            would_veto = True
            reason = "missing_reward_peak"
        elif risk_bps > 0.0 and rr < min_rr:
            would_veto = True
            reason = "rr_low"

    shadow_veto = 1 if would_veto else 0
    veto = 1 if (would_veto and mode == "ENFORCE") else 0

    return LiqMapGateDecision(
        mode=mode,
        window=window,
        shadow_veto=int(shadow_veto),
        veto=int(veto),
        reason=str(reason),
        risk_bps=float(risk_bps),
        reward_bps=float(reward_bps),
        rr=float(rr),
        adverse_peak_usd=float(adverse_usd),
        favorable_peak_usd=float(favorable_usd),
    )
