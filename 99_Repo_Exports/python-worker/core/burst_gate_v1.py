from __future__ import annotations

import math
from typing import Any

# -------------------------------------------------------------------------
# BURST GATE / Hawkes Proxy V1
# -------------------------------------------------------------------------

def _f(x: Any, d: float = 0.0) -> float:
    try:
        if x is None: return d
        v = float(x)
        return v if math.isfinite(v) else d
    except Exception:
        return d

def _i(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return d

def eval_burst_gate(
    indicators: dict[str, Any],
    cfg: dict[str, Any],
) -> tuple[float, int, str, dict[str, float]]:
    """
    Evaluates burst activity penalties (Hawkes proxy, Cancel/Trade bursts).
    
    Returns:
        (burst_pen, burst_veto, burst_reason, burst_snapshot)
        
    - burst_pen: score penalty [0.0 ... burst_pen_max]
    - burst_veto: 0 or 1 (only if mode=enforce/veto)
    - burst_reason: string explanation
    - burst_snapshot: dict of computed metrics for evidence/logging
    """

    # 1. Config
    mode = (cfg.get("burst_gate_mode", "penalty")).lower().strip() # penalty | enforce | veto | off
    if mode == "off" or int(cfg.get("burst_gate_enable", 1)) == 0:
        return 0.0, 0, "ok", {}

    pen_w = _f(cfg.get("burst_pen_w", 0.08))
    pen_max = _f(cfg.get("burst_pen_max", 0.25))
    veto_mult = _f(cfg.get("burst_veto_mult", 1.6))

    # Defaults for excess ratio baseline (mu)
    mu_t = _f(cfg.get("hawkes_mu_t", 5.0))   # trade rate baseline
    mu_c = _f(cfg.get("hawkes_mu_c", 10.0))  # cancel rate baseline
    mu_h = _f(cfg.get("hawkes_mu_h", 15.0))  # combined baseline

    if mu_t < 0.1: mu_t = 0.1
    if mu_c < 0.1: mu_c = 0.1
    if mu_h < 0.1: mu_h = 0.1

    # Thresholds
    thr_ctr = _f(cfg.get("burst_ctr_thr", 4.0))           # Cancel-to-Trade Ratio
    thr_excess = _f(cfg.get("burst_churn_excess_thr", 2.5)) # Hawkes excess (lam/mu)
    thr_score = _f(cfg.get("burst_churn_score_thr", 3.0))   # Multi-factor score

    # 2. Extract Metrics

    # Rates (EMA/SMA from indicators)
    # Support multiple keys for robustness
    tr_ema = _f(indicators.get("taker_rate_ema", indicators.get("trade_rate_ema", 0.0)))
    cr_ema = _f(indicators.get("cancel_rate_ema", 0.0))
    ha_lam = _f(indicators.get("hawkes_combined_lam", indicators.get("hawkes_lam", 0.0)))

    # Orderbook Churn / Pressure (if available)
    book_churn = _f(indicators.get("book_churn_score", 0.0))
    book_z = _f(indicators.get("book_rate_z", 0.0))
    pressure_sps = _f(indicators.get("pressure_sps", 0.0))

    # 3. Derived Metrics

    # Cancel-to-Trade Ratio (CTR)
    # Cap at reasonable max to avoid inf
    ctr = 0.0
    if tr_ema > 1e-6:
        ctr = cr_ema / tr_ema
    elif cr_ema > 0:
        ctr = 99.0 # Very high cancel rate with no trades

    # Hawkes Excess (Lambda / Mu)
    # We use combined lambda usually, but could check t/c separately
    # Simple proxy: how much higher is current activity vs baseline?

    # Try to get specific lambdas if available
    lam_t = _f(indicators.get("hawkes_trade_lam", 0.0))
    lam_c = _f(indicators.get("hawkes_cancel_lam", 0.0))

    exc_t = lam_t / mu_t if mu_t > 0 else 0.0
    exc_c = lam_c / mu_c if mu_c > 0 else 0.0
    exc_h = ha_lam / mu_h if mu_h > 0 else 0.0

    # Composite Excess (max of components to catch single-sided bursts)
    excess_max = max(exc_t, exc_c, exc_h)

    # 4. Penalty Calculation

    pen = 0.0
    reasons = []

    # A. CTR Penalty
    # If CTR is high, it means spoofing/layering likely (lots of cancels per trade)
    if ctr > thr_ctr:
        # Linear ramp penalty: (CTR - THR) * W
        p = (ctr - thr_ctr) * pen_w
        # Cap contribution
        if p > pen_max: p = pen_max
        pen += p
        reasons.append(f"ctr={ctr:.1f}")

    # B. Excess Activity Penalty
    # Sudden burst of activity (trades or cancels)
    if excess_max > thr_excess:
        p = (excess_max - thr_excess) * pen_w
        if p > pen_max: p = pen_max
        pen += p
        reasons.append(f"exc={excess_max:.1f}")

    # C. Churn Score Penalty (External complexity metric)
    if book_churn > thr_score:
        p = (book_churn - thr_score) * pen_w
        if p > pen_max: p = pen_max
        pen += p
        reasons.append(f"churn={book_churn:.1f}")

    # Clamp total penalty
    if pen > pen_max:
        pen = pen_max

    # 5. Veto Logic
    veto = 0
    veto_reason = ""

    # Veto only if strict mode enabled
    if mode in ("enforce", "veto", "hard"):
         # Condition: Severe burst (Penalty maxed out AND exceed higher thresholds)
         # Simple heuristic: if metrics exceed threshold * multiplier

         is_veto = False

         if ctr > (thr_ctr * veto_mult):
             is_veto = True
             veto_reason = "veto_ctr"
         elif excess_max > (thr_excess * veto_mult):
             is_veto = True
             veto_reason = "veto_excess"
         elif book_churn > (thr_score * veto_mult):
             is_veto = True
             veto_reason = "veto_churn"

         if is_veto:
             veto = 1
             reasons.append(str(veto_reason) if veto_reason else "VETO")

    # Snapshot for evidence/logging
    snap = {
        "burst_pen": float(pen),
        "burst_veto": int(veto),
        "burst_ctr": float(ctr),
        "burst_exc": float(excess_max),
        "burst_churn": float(book_churn),
        "burst_z": float(book_z),
        "burst_tr_ema": float(tr_ema),
        "burst_cr_ema": float(cr_ema),
        "burst_ha_lam": float(ha_lam),
    }

    full_reason = ",".join(reasons) if reasons else "ok"
    if not full_reason: full_reason = "ok"

    return pen, veto, full_reason, snap
