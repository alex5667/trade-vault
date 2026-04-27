from __future__ import annotations

from dataclasses import dataclass

from core.bucket_allowlist_v1 import bucket_allowed
from typing import Any, Dict


@dataclass
class TakerFlowGateResult:
    """Result of the taker-flow contra gate evaluation.

    Fields:
        veto        – 1 if enforce mode and contra signal detected (hard block)
        shadow_veto – 1 if shadow mode and contra signal detected (would-have-vetoed)
        soft        – 1 if contra signal detected in any mode (soft awareness flag)
        reason      – short reason string ("ok" / "low_rate" / "contra")
    """
    veto: int
    shadow_veto: int
    soft: int
    reason: str


def _f(x: Any, d: float = 0.0) -> float:
    """Safe float cast with default."""
    try:
        return float(x)
    except Exception:
        return float(d)


def eval_taker_flow_gate(
    direction: str,
    indicators: Dict[str, Any],
    cfg2: Dict[str, Any],
) -> TakerFlowGateResult:
    """Isolated signed taker-flow imbalance contra gate.

    Trigger (contra):
      LONG  and imb <= -imb_thr and z <= -z_thr
      SHORT and imb >= +imb_thr and z >= +z_thr

    mode (cfg2["taker_flow_gate_mode"]):
      - "shadow" (default): shadow_veto=1, soft=1  – observe only, no enforcement
      - "enforce":          veto=1                  – hard block in of_confirm_engine

    Optional guard: ignore when total taker rate is too small
    (cfg2["taker_flow_gate_min_abs_rate"], default 0.0).
    """
    mode = str(cfg2.get("taker_flow_gate_mode", "shadow") or "shadow").strip().lower()    # Enforce only on selected execution-regime buckets (default: HIGH_VOL_LOW_LIQ).
    # If mode is "enforce" but bucket is not allowed -> behave as shadow.
    bucket = str(indicators.get("exec_regime_bucket", "NORMAL") or "NORMAL")
    enforce_buckets_raw = str(cfg2.get("taker_flow_gate_enforce_buckets", "HIGH_VOL_LOW_LIQ") or "HIGH_VOL_LOW_LIQ")
    enforce_allowed = bucket_allowed(bucket, enforce_buckets_raw, default_bucket="HIGH_VOL_LOW_LIQ")
    z_thr       = _f(cfg2.get("taker_flow_contra_z_hard", 2.5), 2.5)
    imb_thr     = _f(cfg2.get("taker_flow_contra_imb_hard", 0.25), 0.25)
    min_abs_rate = _f(cfg2.get("taker_flow_gate_min_abs_rate", 0.0), 0.0)

    imb  = _f(indicators.get("taker_flow_imb",       0.0), 0.0)
    z    = _f(indicators.get("taker_flow_imb_z",     0.0), 0.0)
    br   = _f(indicators.get("taker_buy_rate_ema",   0.0), 0.0)
    sr   = _f(indicators.get("taker_sell_rate_ema",  0.0), 0.0)
    abs_rate = max(0.0, br) + max(0.0, sr)

    # Guard: skip gate when absolute taker flow is too low (avoids noise on thin alts)
    if abs_rate < min_abs_rate:
        return TakerFlowGateResult(veto=0, shadow_veto=0, soft=0, reason="low_rate")

    d = str(direction or "").upper()
    contra = False
    if d == "LONG":
        # contra: heavy sell flow against long entry
        contra = (imb <= -abs(imb_thr)) and (z <= -abs(z_thr))
    elif d == "SHORT":
        # contra: heavy buy flow against short entry
        contra = (imb >= abs(imb_thr)) and (z >= abs(z_thr))

    if not contra:
        return TakerFlowGateResult(veto=0, shadow_veto=0, soft=0, reason="ok")

    # Contra detected — enforce or shadow
    if mode == "enforce" and enforce_allowed:
        return TakerFlowGateResult(veto=1, shadow_veto=0, soft=0, reason="contra")

    # Shadow for all other buckets (or when mode=shadow)
    return TakerFlowGateResult(veto=0, shadow_veto=1, soft=1, reason="contra")
