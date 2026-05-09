from __future__ import annotations

import math
from typing import Any


def _to_f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        return v if math.isfinite(v) else default
    except Exception:
        return default

def eval_liq_pressure_gate(
    direction: str,
    qimb_wmean: float,
    ofi_ml_norm: float,
    cfg2: dict[str, Any],
    obi_dw: float = 0.0,
    res_recovered: int = 0,
    res_recovery_ms: int = 0,
) -> tuple[float, float, int, str, int, int]:
    """
    Liquidity Pressure Gate (P2d) with extra “world practice” signals:
      - Depth-weighted OBI (obi_dw)
      - Resilience (fast recovery after sweep => penalize directional pressure confidence)

    Base signals:
      - Queue Imbalance (qimb_wmean): weighted mean of top-5 levels
      - Multi-level OFI (ofi_ml_norm): normalized OFI across top-5 levels
      
    Logics:
      - Alignment:
         LONG: qimb > 0 (bids > asks), ofi > 0 (buying pressure)
         SHORT: qimb < 0 (asks > bids), ofi < 0 (selling pressure)
         
    Modes (liq_pressure_gate_mode):
      - "off": returns 0 boost, 0 penalty, 0 veto
      - "boost": can add up to liq_pressure_boost_max
      - "penalty": can subtract up to liq_pressure_penalty_max
      - "both": applies both boost and penalty
      - "enforce": like "both" + optional HARD VETO if misalignment is extreme
      
    Returns:
      (liq_boost, liq_pen, liq_veto, liq_reason, q_align, ofi_align)
    """

    # 1. Config & Thresholds
    mode = (cfg2.get("liq_pressure_gate_mode", "off")).lower()
    if mode == "off":
        return 0.0, 0.0, 0, "", 0, 0

    q_thr = float(cfg2.get("liq_pressure_qimb_thr", 0.12) or 0.12)
    ofi_thr = float(cfg2.get("liq_pressure_ofi_thr", 0.02) or 0.02)
    obi_thr = float(cfg2.get("liq_pressure_obi_dw_thr", 0.06) or 0.06)

    boost_max = float(cfg2.get("liq_pressure_boost_max", 0.05) or 0.05)
    pen_max = float(cfg2.get("liq_pressure_pen_max", 0.10) or 0.10)
    veto_mult = float(cfg2.get("liq_pressure_veto_mult", 2.0) or 2.0)

    res_fast_ms = int(cfg2.get("liq_pressure_res_fast_ms", 1200) or 1200)
    res_pen_max = float(cfg2.get("liq_pressure_res_pen_max", 0.05) or 0.05)

    # 2. Alignment Check
    # qimb: >0 means Bid heavier (support for LONG), <0 means Ask heavier (support for SHORT)
    # ofi: >0 means Bid add/Ask remove (support for LONG), <0 means Ask add/Bid remove (support for SHORT)

    q_val = _to_f(qimb_wmean, 0.0)
    ofi_val = _to_f(ofi_ml_norm, 0.0)
    obi_val = _to_f(obi_dw, 0.0)

    q_align = 0
    if direction == "LONG":
        if q_val > q_thr: q_align = 1
        elif q_val < -q_thr: q_align = -1 # Contradiction
    else: # SHORT
        if q_val < -q_thr: q_align = 1
        elif q_val > q_thr: q_align = -1 # Contradiction

    ofi_align = 0
    if direction == "LONG":
        if ofi_val > ofi_thr: ofi_align = 1
        elif ofi_val < -ofi_thr: ofi_align = -1
    else: # SHORT
        if ofi_val < -ofi_thr: ofi_align = 1
        elif ofi_val > ofi_thr: ofi_align = -1

    obi_align = 0
    if direction == "LONG":
        if obi_val > obi_thr: obi_align = 1
        elif obi_val < -obi_thr: obi_align = -1
    else: # SHORT
        if obi_val < -obi_thr: obi_align = 1
        elif obi_val > obi_thr: obi_align = -1

    # 3. Compute Boost/Penalty
    # Boost: both align=1
    # Penalty: either align=-1

    boost_score = 0.0
    pen_score = 0.0
    reasons = []

    # Boost Logic
    if mode in ["boost", "both", "enforce"]:
        # allow any 2-of-3 (qimb/ofi/obi) alignments
        if (q_align == 1 and ofi_align == 1) or (obi_align == 1 and ofi_align == 1) or (q_align == 1 and obi_align == 1):
            boost_score = boost_max
            # scale by intensity? Keep simple step function for v1
            reasons.append("bst")

    # Penalty Logic
    if mode in ["penalty", "both", "enforce"]:
        # Penalty if SIGNIFICANT misalignment (split across 3 sources)
        p = 0.0
        if q_align == -1:
            p += (1.0 / 3.0) * pen_max
            reasons.append("bad_q")
        if ofi_align == -1:
            p += (1.0 / 3.0) * pen_max
            reasons.append("bad_ofi")
        if obi_align == -1:
            p += (1.0 / 3.0) * pen_max
            reasons.append("bad_obi")
        pen_score = p

    # 3b. Resilience adjustment: fast recovery after sweep => more likely fake impulse
    try:
        if int(res_recovered or 0) == 1 and int(res_recovery_ms or 0) > 0 and int(res_recovery_ms) <= res_fast_ms:
            pen_score += float(res_pen_max)
            reasons.append("fast_recover")
    except Exception:
        pass

    # 4. Veto Logic (Enforce only)
    veto = 0
    if mode == "enforce":
        # Hard Veto if BOTH are contradicting strongly
        # or if one is contradicting strongly (> veto_mult * thr)

        q_bad_severe = False
        if direction == "LONG" and q_val < (-q_thr * veto_mult): q_bad_severe = True
        if direction == "SHORT" and q_val > (q_thr * veto_mult): q_bad_severe = True

        ofi_bad_severe = False
        if direction == "LONG" and ofi_val < (-ofi_thr * veto_mult): ofi_bad_severe = True
        if direction == "SHORT" and ofi_val > (ofi_thr * veto_mult): ofi_bad_severe = True

        obi_bad_severe = False
        if direction == "LONG" and obi_val < (-obi_thr * veto_mult): obi_bad_severe = True
        if direction == "SHORT" and obi_val > (obi_thr * veto_mult): obi_bad_severe = True

        if q_bad_severe or ofi_bad_severe or obi_bad_severe:
            veto = 1
            reasons.append("VETO")

    return boost_score, pen_score, veto, ",".join(reasons), q_align, ofi_align
