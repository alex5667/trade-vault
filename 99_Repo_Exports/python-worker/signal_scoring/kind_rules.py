from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Any, Dict, List, Optional, Tuple


def _f(x: Any, default: Optional[float] = None) -> Optional[float]:
    try:
        v = float(x)
    except Exception:
        return default
    if not math.isfinite(v):
        return default
    return v


def _b(x: Any, default: bool = False) -> bool:
    if x is None:
        return default
    return bool(x)


def _clamp01(x: float) -> float:
    if x < 0.0:
        return 0.0
    if x > 1.0:
        return 1.0
    return x


@dataclass(frozen=True)
class KindRuleResult:
    veto: bool
    conf_mult01: float
    flags: Dict[str, Any]
    reasons: List[str]
    labels: Dict[str, Any]


# -----------------------------
# Breakout rules
# -----------------------------
BREAKOUT_MIN_MPS_BPS = 0.8              # microprice_shift bps threshold to treat as "shift exists"
BREAKOUT_MIN_TAKER_CONT = 0.22          # taker_rate_ema low -> no continuation
BREAKOUT_VETO_C2T = 2.6                 # cancel_to_trade too high -> spoof-like
BREAKOUT_VETO_TAKER = 0.16              # and taker_rate_ema low -> veto
BREAKOUT_NO_CONT_PENALTY = 0.65         # conf multiplier when shift exists but no taker continuation
BREAKOUT_POST_ACCEPT_HORIZON_S = 20     # "post-breakout acceptance" probe horizon (label only)


# -----------------------------
# Absorption rules
# -----------------------------
ABS_MIN_TAKER_RATE = 0.18               # "абсорбить нечего" if taker_rate too low


# -----------------------------
# OBI spike rules
# -----------------------------
OBI_VETO_L3_C2T = 3.2                   # high cancel-to-trade
OBI_VETO_IF_NOT_SUSTAINED = True        # veto if (not sustained) and l3_c2t high
OBI_NOT_SUSTAINED_PENALTY = 0.70        # downscale if not sustained (but no veto)
OBI_L3_C2T_PENALTY = 0.60               # additional penalty for elevated l3 c2t (soft)


# -----------------------------
# Spread / Impact scaling
# -----------------------------
SPREAD_SCALE_BPS = 6.0                  # bigger spread -> smaller conf
SPREAD_FLOOR = 0.55


def _spread_scale(ctx: Any) -> float:
    spread_bps = _f(getattr(ctx, "spread_bps", None), None)
    if spread_bps is None:
        spread_bps = _f(getattr(ctx, "spread", None), None)
    if spread_bps is None:
        return 1.0
    # linear scaling with floor
    mult = 1.0 - (spread_bps / max(SPREAD_SCALE_BPS, 1e-9))
    if mult < SPREAD_FLOOR:
        mult = SPREAD_FLOOR
    if mult > 1.0:
        mult = 1.0
    return float(mult)


def apply_kind_rules(kind: str, ctx: Any, quality_flags: Optional[Dict[str, Any]] = None) -> KindRuleResult:
    """
    Kind-specific real-world improvements (3.3):
      - Breakout: fake breakout, taker continuation, cancel-to-trade veto, post-acceptance probe label
      - Absorption: require 2 independent sources + min taker rate
      - Extreme: unify under candidate + require micro/book quality (when provided)
      - OBI spike: anti-spoof (sustained + l3 cancel-to-trade), spread-dependent scaling
    Contract:
      - conf_mult01 in [0..1]
      - veto indicates hard reject
      - flags/reasons explain decisions for audit + downstream calibration
      - labels are non-online training probes (e.g., post-breakout acceptance)
    """
    qf: Dict[str, Any] = dict(quality_flags or {})
    reasons: List[str] = []
    labels: Dict[str, Any] = {}
    veto = False
    conf_mult = 1.0

    k = (kind or "").lower()

    # Always apply spread scaling as soft factor (all kinds)
    spread_mult = _spread_scale(ctx)
    if spread_mult < 1.0:
        qf["spread_scale"] = spread_mult
        reasons.append("spread_scale")
        conf_mult *= spread_mult

    # ---------------- Breakout ----------------
    if k in ("breakout", "breakout_up", "breakout_down"):
        mps_bps = _f(getattr(ctx, "microprice_shift_bps", None), None)
        if mps_bps is None:
            mps_bps = _f(getattr(ctx, "microprice_shift", None), 0.0)  # some pipelines store already in bps
        taker_rate = _f(getattr(ctx, "taker_rate_ema", None), None)
        c2t = _f(getattr(ctx, "cancel_to_trade", None), None)

        if (mps_bps is not None and mps_bps >= BREAKOUT_MIN_MPS_BPS) and (taker_rate is not None and taker_rate < BREAKOUT_MIN_TAKER_CONT):
            qf["no_taker_continuation"] = True
            reasons.append("no_taker_continuation")
            conf_mult *= BREAKOUT_NO_CONT_PENALTY

        if c2t is not None and taker_rate is not None:
            if c2t >= BREAKOUT_VETO_C2T and taker_rate <= BREAKOUT_VETO_TAKER:
                veto = True
                qf["veto_spoof_like"] = True
                reasons.append("veto_spoof_like")

        # Non-online training probe: post-breakout acceptance
        level = getattr(ctx, "level_price", None) or getattr(ctx, "breakout_level", None)
        if level is not None:
            labels["post_acceptance_probe"] = {
                "horizon_s": BREAKOUT_POST_ACCEPT_HORIZON_S,
                "level_price": level,
            }

    # ---------------- Absorption ----------------
    elif k in ("absorption", "absorb", "absorption_l2"):
        wall_here = _b(qf.get("wall_here", getattr(ctx, "wall_here", None)), False)
        refill = _b(qf.get("refill", getattr(ctx, "refill", None)), False)
        mp_contra = _b(qf.get("mp_contra", getattr(ctx, "mp_contra", None)), False)
        micro_proxy = _b(qf.get("micro_proxy", getattr(ctx, "micro_proxy", None)), False)
        taker_rate = _f(getattr(ctx, "taker_rate_ema", None), None)

        # Two independent sources: (wall or refill) + (mp_contra or micro_proxy)
        cond_a = (wall_here or refill)
        cond_b = (mp_contra or micro_proxy)
        if not cond_a:
            veto = True
            qf["veto_no_wall_or_refill"] = True
            reasons.append("veto_no_wall_or_refill")
        if not cond_b:
            veto = True
            qf["veto_no_micro_contra_or_proxy"] = True
            reasons.append("veto_no_micro_contra_or_proxy")
        if taker_rate is not None and taker_rate < ABS_MIN_TAKER_RATE:
            veto = True
            qf["veto_low_taker_rate"] = True
            reasons.append("veto_low_taker_rate")

    # ---------------- Extreme ----------------
    elif k in ("extreme", "extreme_move", "extreme_reversal"):
        # Must pass micro_quality + book_quality (when present); otherwise soft-penalize.
        micro_ok = getattr(ctx, "micro_quality_ok", None)
        book_ok = getattr(ctx, "book_quality_ok", None)
        micro_q = _f(getattr(ctx, "micro_quality", None), None)
        book_q = _f(getattr(ctx, "book_quality", None), None)

        if micro_ok is False or book_ok is False:
            veto = True
            qf["veto_quality"] = True
            reasons.append("veto_quality")
        if micro_q is not None and micro_q < 0.5:
            veto = True
            qf["veto_micro_quality"] = micro_q
            reasons.append("veto_micro_quality")
        if book_q is not None and book_q < 0.5:
            veto = True
            qf["veto_book_quality"] = book_q
            reasons.append("veto_book_quality")
        # If metrics absent, keep working but slightly downscale to avoid overconfident extremes.
        if (micro_ok is None and micro_q is None) or (book_ok is None and book_q is None):
            qf["quality_missing_soft_penalty"] = True
            reasons.append("quality_missing_soft_penalty")
            conf_mult *= 0.85

    # ---------------- OBI spike ----------------
    elif k in ("obi", "obi_spike", "obi_spike_up", "obi_spike_down"):
        obi_sustained = _b(getattr(ctx, "obi_sustained", None), False) or _b(qf.get("obi_sustained", None), False)
        l3_c2t = _f(getattr(ctx, "l3_cancel_to_trade", None), None)
        if l3_c2t is None:
            l3_c2t = _f(getattr(ctx, "cancel_to_trade", None), None)  # fallback

        if not obi_sustained:
            qf["obi_not_sustained"] = True
            reasons.append("obi_not_sustained")
            conf_mult *= OBI_NOT_SUSTAINED_PENALTY

        if l3_c2t is not None:
            if (not obi_sustained) and OBI_VETO_IF_NOT_SUSTAINED and l3_c2t >= OBI_VETO_L3_C2T:
                veto = True
                qf["veto_obi_spoof"] = True
                reasons.append("veto_obi_spoof")
            elif l3_c2t >= OBI_VETO_L3_C2T:
                qf["obi_l3_c2t_high_soft"] = l3_c2t
                reasons.append("obi_l3_c2t_high_soft")
                conf_mult *= OBI_L3_C2T_PENALTY

    # Unknown kind: nothing special

    conf_mult = _clamp01(float(conf_mult))
    return KindRuleResult(veto=bool(veto), conf_mult01=conf_mult, flags=qf, reasons=reasons, labels=labels)
