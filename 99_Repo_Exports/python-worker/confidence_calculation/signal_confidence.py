# -*- coding: utf-8 -*-
from __future__ import annotations
"""
Signal confidence scorer (0..1) for BaseOrderFlowHandler / CryptoOrderFlowHandler.

Confidence = качество сигнала * согласованность подтверждений * (1 - штрафы за микро).
Работает даже при частично отсутствующих полях (getattr с дефолтами).
"""

import os
from dataclasses import dataclass
from typing import Any, Dict, Tuple
import math

try:
    from prometheus_client import Counter as _PromCounter
except Exception:  # pragma: no cover
    _PromCounter = None  # type: ignore


def _make_counter(name: str, doc: str, labels: Tuple[str, ...]):
    if _PromCounter is None:
        return None
    try:
        return _PromCounter(name, doc, list(labels))
    except ValueError:
        # already registered (process re-import)
        return None


# ── Shadow/Enforce decision diff counter (Phase 2.5) ────────────────────────
# Every ML-scored signal is compared against the rule-based accept/reject
# decision at a configurable threshold. The counter is incremented with label
# `diff`:
#   same            — rule and ML agree on accept/reject
#   rule_only       — rule would ACCEPT, ML would REJECT
#   ml_only         — ML would ACCEPT, rule would REJECT
#   ml_unavailable  — ML returned None (fail-open path)
#
# Safe to read in both shadow and enforce modes: counts are informational,
# no behaviour change. Used to compare shadow population with what would
# happen under enforce before promoting.
SHADOW_VS_ENFORCE_DIFF = _make_counter(
    "shadow_vs_enforce_decision_diff_total",
    "ML scorer shadow vs rule-based decision agreement. "
    "diff=same|rule_only|ml_only|ml_unavailable, mode=shadow|enforce.",
    ("diff", "mode"),
)


_conf_debug_counter = 0


def _record_shadow_enforce_diff(
    *, rule_conf: Any, ml_conf: Any, mode: str, threshold: float = 0.5
) -> None:
    """Increment shadow_vs_enforce_decision_diff_total at the acceptance
    threshold. Accept = conf >= SHADOW_ENFORCE_DECISION_THRESHOLD (default 0.5).

    Any exception in metric emission is silently swallowed — this is a
    telemetry-only path and must not affect scoring."""
    if SHADOW_VS_ENFORCE_DIFF is None:
        return
    try:
        if ml_conf is None:
            SHADOW_VS_ENFORCE_DIFF.labels(diff="ml_unavailable", mode=mode).inc()
            return

        try:
            rule_f = float(rule_conf) if rule_conf is not None else 0.0
            ml_f = float(ml_conf)
        except Exception:
            return

        rule_accept = rule_f >= threshold
        ml_accept = ml_f >= threshold

        if rule_accept == ml_accept:
            label = "same"
        elif rule_accept and not ml_accept:
            label = "rule_only"
        else:
            label = "ml_only"

        SHADOW_VS_ENFORCE_DIFF.labels(diff=label, mode=mode).inc()
    except Exception:
        # never raise from telemetry path
        pass

def _f(obj: Any, name: str, default: float = 0.0) -> float:
    try:
        v = getattr(obj, name, default)
        if v is None:
            return float(default)
        x = float(v)
        return x if math.isfinite(x) else float(default)
    except Exception:
        return float(default)


def _b(obj: Any, name: str, default: bool = False) -> bool:
    try:
        v = getattr(obj, name, default)
        if v is None:
            return bool(default)
        return bool(v)
    except Exception:
        return bool(default)


def _f_any(obj: Any, *names: str, default: float = 0.0) -> float:
    """Return first available finite float attribute among names."""
    for n in names:
        try:
            v = getattr(obj, n)
            if v is None:
                continue
            x = float(v)
            if math.isfinite(x):
                return x
        except Exception:
            continue
    return float(default)


def _b_any(obj: Any, *names: str, default: bool = False) -> bool:
    """Return first available bool attribute among names."""
    for n in names:
        try:
            v = getattr(obj, n)
            if v is None:
                continue
            return bool(v)
        except Exception:
            continue
    return bool(default)


def _clamp(x: float, lo: float = 0.0, hi: float = 1.0) -> float:
    try:
        v = float(x)
    except Exception:
        return lo
    if not math.isfinite(v):
        return lo
    if v < lo:
        return lo
    if v > hi:
        return hi
    return v


def _ramp(x: float, lo: float, hi: float) -> float:
    """0 при x<=lo, 1 при x>=hi, линейно между."""
    if hi <= lo:
        return 0.0
    return _clamp((x - lo) / (hi - lo), 0.0, 1.0)


def _sign(x: float) -> int:
    if x > 0:
        return 1
    if x < 0:
        return -1
    return 0


def _dir_sign_from_side(side: str) -> int:
    s = (side or "").upper()
    if s == "LONG":
        return 1
    if s == "SHORT":
        return -1
    return 0


def _confirm_value(confirmations: list[str], key: str) -> float:
    """
    confirmations содержат строки вида:
      - fp_absorb=1.23
      - fp_imb=0.82
      - obi=0.45
    """
    k0 = (key or "").strip().lower()
    for c in confirmations or []:
        try:
            k, v = c.split("=", 1)
            if k.strip().lower() == k0:
                return float(v)
        except Exception:
            continue
    return 0.0


def _ctx_confirm_value(ctx: Any, key: str) -> float:
    """
    Phase 2: Structured Evidence Access.
    1. Try ctx.evidence[key] (if available)
    2. Fallback to parsing ctx.confirmations via _confirm_value
    """
    # 1. Evidence dict (fast & structured)
    try:
        ev = getattr(ctx, "evidence", None)
        if ev and isinstance(ev, dict):
            if key in ev:
                return float(ev[key])
            # Case-insensitive fallback
            k_lower = key.lower()
            if k_lower in ev:
                return float(ev[k_lower])
    except Exception:
        pass

    # 2. Legacy string parsing (fallback)
    confs = getattr(ctx, "confirmations", [])
    return _confirm_value(confs, key)


def _sigmoid(x: float) -> float:
    # bounded smooth mapping
    try:
        return 1.0 / (1.0 + math.exp(-x))
    except Exception:
        return 0.0


@dataclass
class ConfidenceConfig:
    # output clamp
    min_conf: float = 0.05
    max_conf: float = 0.98

    # z saturation: z_abs>=thr*z_saturation_mult -> score ~1
    z_saturation_mult: float = 1.6

    # L2 staleness penalties (ms)
    l2_stale_soft_ms: int = 900
    l2_stale_hard_ms: int = 2500

    # spread penalties (bps)
    spread_soft_bps: float = 8.0
    spread_hard_bps: float = 20.0

    # impact penalties (unitless proxy)
    impact_soft: float = 0.25
    impact_hard: float = 0.70

    # L3-lite “badness”
    ctr_soft: float = 2.5
    ctr_hard: float = 7.0
    eta_soft_sec: float = 1.8
    eta_hard_sec: float = 8.0

    # generic OBI threshold
    obi_thr_default: float = 0.20

    # how much penalties reduce final confidence
    penalty_power: float = 0.85


class ConfidenceScorer:
    def __init__(
        self,
        *,
        cfg: ConfidenceConfig | None = None,
        main_z_thr: float = 3.0,
        breakout_z_thr: float = 3.5,
        absorption_z_thr: float = 3.5,
        extreme_z_thr: float = 5.0,
        obi_thr: float | None = None,
        obi20_thr: float | None = None,
        calibrator: Any = None,
        ml_scoring_gate: Any = None,
        **kwargs: Any,
    ):
        self.cfg = cfg or ConfidenceConfig()
        self.calibrator = calibrator

        self.main_z_thr = float(main_z_thr)
        self.breakout_z_thr = float(breakout_z_thr)
        self.absorption_z_thr = float(absorption_z_thr)
        self.extreme_z_thr = float(extreme_z_thr)

        self.obi_thr = float(obi_thr) if (obi_thr is not None) else self.cfg.obi_thr_default
        self.obi20_thr = float(obi20_thr) if (obi20_thr is not None) else self.cfg.obi_thr_default

        # [REMEDIATION P4.1] Cache ENV variables for hot-path scoring
        self._ml_mode = os.getenv("ML_SCORER_MODE", "shadow").strip().lower()
        self._shadow_enforce_threshold = float(os.getenv("SHADOW_ENFORCE_DECISION_THRESHOLD", "0.5"))

        # ML Scoring V2: lazy-initialized gate for USE_UNIFIED_SCORING flag
        self._ml_gate = ml_scoring_gate
        if self._ml_gate is None:
            try:
                if os.getenv("ML_SCORER_V2_ENABLED", "0").strip().lower() in ("1", "true", "yes"):
                    from services.ml_scoring_gate import MLScoringGate
                    self._ml_gate = MLScoringGate()
            except Exception:
                self._ml_gate = None

    # ---------- primitives ----------
    def _z_score(self, z_abs: float, thr: float) -> float:
        sat = max(thr * self.cfg.z_saturation_mult, thr + 1e-9)
        return _ramp(z_abs, thr, sat)

    def _obi_score(self, obi_avg: float, obi_sustained: bool, dir_sign: int, thr: float) -> float:
        if dir_sign == 0:
            return 0.0
        aligned = (obi_avg * dir_sign) > 0.0
        strength = _ramp(abs(obi_avg), thr, 1.0)
        s = strength * (1.0 if aligned else 0.0)
        return s if obi_sustained else 0.5 * s

    def _microprice_score(self, mp_shift_bps_20: float, dir_sign: int, min_bps: float = 0.2) -> float:
        if dir_sign == 0:
            return 0.0
        ok = mp_shift_bps_20 * dir_sign
        return _ramp(ok, min_bps, min_bps * 6.0)

    def _wall_penalty_breakout(self, ctx: Any, dir_sign: int) -> float:
        wall_max = 10.0
        if dir_sign > 0 and _b(ctx, "wall_ask", False):
            d = _f(ctx, "wall_ask_dist_bps", 1e9)
            return _ramp(wall_max - d, 0.0, wall_max)
        if dir_sign < 0 and _b(ctx, "wall_bid", False):
            d = _f(ctx, "wall_bid_dist_bps", 1e9)
            return _ramp(wall_max - d, 0.0, wall_max)
        return 0.0

    def _absorption_support_score(self, ctx: Any, impulse_sign: int) -> float:
        refill = _f(ctx, "refill_score", 0.0)
        s_refill = _ramp(refill, 0.05, 0.25)

        wall_max = 12.0
        s_wall = 0.0
        if impulse_sign > 0 and _b(ctx, "wall_ask", False):
            s_wall = _ramp(wall_max - _f(ctx, "wall_ask_dist_bps", 1e9), 0.0, wall_max)
        elif impulse_sign < 0 and _b(ctx, "wall_bid", False):
            s_wall = _ramp(wall_max - _f(ctx, "wall_bid_dist_bps", 1e9), 0.0, wall_max)

        mp = _f(ctx, "microprice_shift_bps_20", 0.0)
        s_mp_contra = _ramp((-mp * impulse_sign), 0.05, 1.0)

        s_wp = 1.0 if _b(ctx, "weak_progress", False) else 0.0

        combined = 1.0 - (1.0 - s_refill) * (1.0 - s_wall) * (1.0 - s_mp_contra) * (1.0 - s_wp)
        return _clamp(combined, 0.0, 1.0)

    def _l3_quality_score(self, ctx: Any, dir_sign: int) -> float:
        if dir_sign == 0:
            return 0.0
        if dir_sign > 0:
            rate = _f(ctx, "taker_buy_rate_ema", 0.0)
            ctr = _f(ctx, "cancel_to_trade_ask", 0.0)
            eta = _f(ctx, "eta_fill_ask_sec", 0.0)
        else:
            rate = _f(ctx, "taker_sell_rate_ema", 0.0)
            ctr = _f(ctx, "cancel_to_trade_bid", 0.0)
            eta = _f(ctx, "eta_fill_bid_sec", 0.0)

        s_rate = 0.30 if rate <= 0 else _ramp(rate, 0.5, 6.0)
        pen_ctr = _ramp(ctr, self.cfg.ctr_soft, self.cfg.ctr_hard)
        pen_eta = _ramp(eta, self.cfg.eta_soft_sec, self.cfg.eta_hard_sec)

        quality = s_rate * (1.0 - 0.75 * pen_ctr) * (1.0 - 0.45 * pen_eta)
        return _clamp(quality, 0.0, 1.0)

    def _penalties(self, ctx: Any) -> Dict[str, float]:
        l2_age = _f(ctx, "l2_age_ms", 0.0)
        l2_is_stale = _b(ctx, "l2_is_stale", False) or (l2_age > 0 and l2_age >= self.cfg.l2_stale_soft_ms)
        pen_l2 = _ramp(l2_age, float(self.cfg.l2_stale_soft_ms), float(self.cfg.l2_stale_hard_ms)) if l2_is_stale else 0.0

        spread = _f(ctx, "spread_bps", 0.0)
        pen_spread = _ramp(spread, self.cfg.spread_soft_bps, self.cfg.spread_hard_bps)

        impact = _f(ctx, "impact_proxy", 0.0)
        pen_impact = _ramp(impact, self.cfg.impact_soft, self.cfg.impact_hard)

        return {
            "pen_l2_stale": pen_l2,
            "pen_spread": pen_spread,
            "pen_impact": pen_impact,
        }

    # ---------- public ----------
    def score(self, *args, kind: str = None, side: str = None, ctx: Any = None, ff: Any = None, **kwargs) -> Tuple[float, Dict[str, float]]:
        # Fallback for positional arguments
        if args:
            import logging
            logging.getLogger("crypto_orderflow_service").error(
                "❌ ConfidenceScorer.score called with %d positional args: %r. kwargs=%r", len(args), args, kwargs
            )
            # Try to recover if we have 3 args
            if len(args) >= 3:
                kind = args[0]
                side = args[1]
                ctx = args[2]

        kind = (kind or "custom").lower()

        # ── ML Scoring V2: USE_UNIFIED_SCORING gate ──────────────────────
        # ff.use_unified_scoring=True → run ML scorer
        #   ML_SCORER_MODE=shadow  → both paths, return rule-based, log ML in parts
        #   ML_SCORER_MODE=enforce → return ML result if available
        # Fail-open: if ML unavailable → always return rule-based
        _use_ml = bool(getattr(ff, "use_unified_scoring", False)) if ff else False
        if _use_ml and self._ml_gate is not None:
            ml_conf01, ml_parts = self._ml_gate.score(kind=kind, side=side, ctx=ctx)

            if self._ml_mode == "enforce" and ml_conf01 is not None:
                # Enforce: use ML result directly. Still record the diff
                # counter with mode="enforce" so the acceptance population
                # is observable post-promote.
                ml_parts["scorer_mode"] = "ml_enforce"
                try:
                    rule_conf_for_cmp, _rule_parts_cmp = self._score_rule_based(
                        kind=kind, side=side, ctx=ctx, **kwargs
                    )
                    _record_shadow_enforce_diff(
                        rule_conf=rule_conf_for_cmp,
                        ml_conf=ml_conf01,
                        mode="enforce",
                        threshold=self._shadow_enforce_threshold,
                    )
                except Exception:
                    pass
                return ml_conf01, ml_parts
            else:
                # Shadow (default): compute rule-based, attach ML as shadow fields
                rule_conf, rule_parts = self._score_rule_based(
                    kind=kind, side=side, ctx=ctx, **kwargs
                )
                if ml_conf01 is not None:
                    rule_parts["ml_shadow_conf01"] = ml_conf01
                    rule_parts["ml_shadow_predicted_r"] = ml_parts.get("ml_predicted_r", 0.0)
                    rule_parts["ml_shadow_status"] = "ok"
                else:
                    rule_parts["ml_shadow_status"] = ml_parts.get("ml_status", "unknown")
                rule_parts["scorer_mode"] = "shadow"
                _record_shadow_enforce_diff(
                    rule_conf=rule_conf,
                    ml_conf=ml_conf01,
                    mode="shadow",
                    threshold=self._shadow_enforce_threshold,
                )
                return rule_conf, rule_parts
        # ── End ML Scoring V2 ────────────────────────────────────────────

        return self._score_rule_based(kind=kind, side=side, ctx=ctx, **kwargs)

    def _score_rule_based(self, *, kind: str = "custom", side: str = None, ctx: Any = None, **kwargs) -> Tuple[float, Dict[str, float]]:
        """Original rule-based scoring logic (extracted for ML shadow)."""
        dir_sign = _dir_sign_from_side(side)

        parts: Dict[str, Any] = {"kind": kind, "side": side}
        
        # Helper for safer float access (allows 0.0)
        def _get_f(name, default):
            try:
                val = getattr(ctx, name, None)
                if val is None: return float(default)
                return float(val)
            except Exception:
                return float(default)

        # Helper for safer string access
        def _get_s(name, default):
            try:
                val = getattr(ctx, name, None)
                if val is None: return str(default)
                return str(val)
            except Exception:
                return str(default)

        z = _f_any(ctx, "delta_z", "z_delta", default=0.0)
        z_abs = abs(z)
        impulse_sign = _sign(z)
        
        # Phase 3: Regime-Awareness
        # Normalize market mode to trend/range/mixed
        raw_mode = _get_s("market_mode", "mixed").lower()
        from common.market_mode import is_range_regime as _is_range, is_trend_regime as _is_trend
        if _is_trend(raw_mode):
            regime = "trend"
        elif _is_range(raw_mode):
            regime = "range"
        else:
            regime = "mixed"
        parts["regime_class_raw"] = regime
        
        freeze = bool(int(_get_f("confidence_score_freeze", 0)))
        parts["confidence_score_freeze"] = 1 if freeze else 0
        
        if freeze:
            # Fail-closed: disable regime shaping when guardrails request freeze
            regime = "mixed"
            
        parts["regime"] = regime

        if kind == "breakout":
            thr = self.breakout_z_thr
            s_z = self._z_score(z_abs, thr)
            obi20 = _f_any(ctx, "obi_avg_20", "obi_avg", "obi", default=0.0)
            obi_sustained_20 = _b_any(ctx, "obi_sustained_20", "obi_sustained", default=False)
            s_obi20 = self._obi_score(obi20, obi_sustained_20, dir_sign, self.obi20_thr)
            s_mp = self._microprice_score(_f(ctx, "microprice_shift_bps_20", 0.0), dir_sign, min_bps=0.2)
            dep = _f(ctx, "depletion_score", 0.0)
            ref = _f(ctx, "refill_score", 0.0)
            s_dep = _ramp(dep, 0.05, 0.25)
            s_ref_good = 1.0 - _ramp(ref, 0.05, 0.25)
            s_l3 = self._l3_quality_score(ctx, dir_sign)
            pen_wall = self._wall_penalty_breakout(ctx, dir_sign)
            s_mode = 1.0 if regime in ("trend", "mixed") else 0.55

            base = (
                0.40 * s_z
                + 0.18 * s_obi20
                + 0.12 * s_mp
                + 0.10 * s_dep
                + 0.08 * s_ref_good
                + 0.07 * s_l3
                + 0.05 * s_mode
            )
            base = base * (1.0 - 0.45 * pen_wall)

            parts.update({
                "s_z": s_z,
                "s_obi20": s_obi20,
                "s_microprice": s_mp,
                "s_depletion": s_dep,
                "s_refill_good": s_ref_good,
                "s_l3": s_l3,
                "s_mode": s_mode,
                "pen_wall": pen_wall,
                "base": _clamp(base),
            })

        elif kind == "absorption":
            thr = self.absorption_z_thr
            s_z = self._z_score(z_abs, thr)

            support_dir = impulse_sign if impulse_sign != 0 else (-dir_sign or 1)
            s_support = self._absorption_support_score(ctx, support_dir)

            obi_avg = _f_any(ctx, "obi_avg", "obi", default=0.0)
            obi_sus = _b(ctx, "obi_sustained", False)
            obi_confirms_impulse = (obi_avg * (impulse_sign if impulse_sign != 0 else 1)) > 0.0 and obi_sus
            s_obi_not_confirm = 1.0 - _ramp(abs(obi_avg), self.obi_thr, 1.0) if obi_confirms_impulse else 1.0

            adv = _f(ctx, "adverse_ratio_ema", 0.0)
            rema = _f(ctx, "realized_ema_bps", 0.0)
            s_micro_block = _clamp(
                0.6 * _ramp(adv, 0.55, 0.85) + 0.4 * _ramp(-rema, 0.20, 2.00),
                0.0, 1.0
            )

            l3_dir = impulse_sign if impulse_sign != 0 else (-dir_sign or 1)
            s_l3 = self._l3_quality_score(ctx, l3_dir)

            s_mode = 1.0 if regime in ("range", "mixed") else 0.55

            base = (
                0.42 * s_z
                + 0.22 * s_support
                + 0.12 * s_micro_block
                + 0.10 * s_l3
                + 0.09 * s_obi_not_confirm
                + 0.05 * s_mode
            )

            # ------------------------------------------------------------
            # Phase 2: Structural Evidence Access (Refactored)
            # ------------------------------------------------------------
            fp_abs_score = _ctx_confirm_value(ctx, "fp_absorb")
            fp_imb = _ctx_confirm_value(ctx, "fp_imb")

            thr_abs = _get_f("fp_absorb_min_score", 1.0)
            s_fp_abs = _sigmoid((fp_abs_score - thr_abs) / 0.35) if fp_abs_score > 0 else 0.0
            s_fp_imb = _clamp(fp_imb)

            # bounded bonus
            w_abs = _get_f("fp_absorb_bonus_w", 0.06)
            w_imb = _get_f("fp_imb_bonus_w", 0.03)
            bonus_cap = _get_f("fp_bonus_cap", 0.08)
            bonus = min(bonus_cap, (w_abs * s_fp_abs + w_imb * s_fp_imb))

            base = _clamp(base + bonus)

            parts.update({
                "s_z": s_z,
                "s_support(L2)": s_support,
                "s_micro_block": s_micro_block,
                "s_l3_pressure": s_l3,
                "s_obi_not_confirm_impulse": s_obi_not_confirm,
                "s_mode": s_mode,
                "base": _clamp(base),
            })

        elif kind == "extreme":
            thr = self.extreme_z_thr
            s_z = self._z_score(z_abs, thr)
            obi_avg = _f_any(ctx, "obi_avg", "obi", default=0.0)
            obi_sustained = _b(ctx, "obi_sustained", False)
            s_obi = self._obi_score(obi_avg, obi_sustained, dir_sign, self.obi_thr)
            s_l3 = self._l3_quality_score(ctx, dir_sign)
            s_mode = 1.0 if regime != "range" else 0.65
            base = 0.60 * s_z + 0.15 * s_obi + 0.10 * s_l3 + 0.15 * s_mode

            parts.update({
                "s_z": s_z,
                "s_obi": s_obi,
                "s_l3": s_l3,
                "s_mode": s_mode,
                "base": _clamp(base),
            })

        elif kind == "obi_spike":
            obi_avg = _f_any(ctx, "obi_avg", "obi", default=0.0)
            obi_sustained = _b(ctx, "obi_sustained", False)
            s_obi = _ramp(abs(obi_avg), 0.70, 0.95) * (1.0 if obi_sustained else 0.6)
            s_mode = 1.0 if regime != "range" else 0.80
            base = 0.78 * s_obi + 0.22 * s_mode
            parts.update({"s_obi_spike": s_obi, "s_mode": s_mode, "base": _clamp(base)})

        else:
            thr = self.main_z_thr
            s_z = self._z_score(z_abs, thr)
            obi_avg = _f_any(ctx, "obi_avg", "obi", default=0.0)
            obi_sustained = _b(ctx, "obi_sustained", False)
            s_obi = self._obi_score(obi_avg, obi_sustained, dir_sign, self.obi_thr)
            base = 0.75 * s_z + 0.25 * s_obi
            parts.update({"s_z": s_z, "s_obi": s_obi, "base": _clamp(base)})

        base = float(parts.get("base", 0.0))

        # ------------------------------------------------------------
        # Phase E: Generic confirmations (RSI / Divergence / Sweep)
        # Phase 3: Regime-Aware Weights
        # ------------------------------------------------------------
        try:
            cap_e = _get_f("phaseE_bonus_cap", 0.08)
            bonus_gen = 0.0

            # Default multipliers (1.0 = neutral rollout)
            rsi_mult = 1.0
            div_mult = 1.0
            sweep_mult = 1.0

            # Phase 3: Regime Multipliers Config
            if (not freeze) and regime == "trend":
                rsi_mult = _get_f("rsi_bonus_trend_mult", 1.35)
                div_mult = _get_f("div_bonus_trend_mult", 0.65)
                sweep_mult = _get_f("sweep_bonus_trend_mult", 0.90)
            elif (not freeze) and regime == "range":
                rsi_mult = _get_f("rsi_bonus_range_mult", 0.80)
                div_mult = _get_f("div_bonus_range_mult", 1.35)
                sweep_mult = _get_f("sweep_bonus_range_mult", 1.15)

            # RSI confirm
            rsi_val = _ctx_confirm_value(ctx, "rsi")
            if rsi_val > 0:
                bonus_gen += (0.03 * rsi_mult) * _clamp(rsi_val, 0, 1)

            # Div confirm
            div_val = _ctx_confirm_value(ctx, "div")
            if div_val > 0:
                 bonus_gen += (0.04 * div_mult) * _clamp(div_val, 0, 1)
            
            # Phase 3: Counter-Trend Divergence Penalty
            # (only penalize if NO divergence confirmation but divergence signal exists in wrong direction)
            # Actually, the requirement says "add penalty for counter-trend divergence".
            # Typically this means: if we are Long, and we see Bearish Div -> penalty.
            # We check div_kind and div_strength (passed via evidence or indicators)
            pen_div_ct = 0.0
            if (not freeze) and regime == "trend":
                div_kind = _get_s("div_kind", "").lower()
                div_str = _get_f("div_strength", 0.0)
                is_counter = False
                if dir_sign > 0 and "bear" in div_kind: is_counter = True
                if dir_sign < 0 and "bull" in div_kind: is_counter = True
                
                if is_counter:
                    # Configurable penalty strength
                    pen_w = _get_f("div_countertrend_pen", 0.04)
                    lo = _get_f("div_strength_lo", 0.35)
                    hi = _get_f("div_strength_hi", 0.75)
                    pen_div_ct = pen_w * _ramp(div_str, lo, hi)
                    if pen_div_ct > 0:
                        parts["pen_div_countertrend"] = pen_div_ct

            # Sweep confirm
            s_sweep = 0.0
            if _ctx_confirm_value(ctx, "sweep_eqh") > 0 or _ctx_confirm_value(ctx, "sweep_eql") > 0:
                s_sweep = 0.8
            elif _ctx_confirm_value(ctx, "sweep") > 0:
                s_sweep = _get_f("sweep_simple_strength", 0.5)
            
            if s_sweep > 0:
                bonus_gen += (0.03 * sweep_mult) * _clamp(s_sweep, 0, 1)
            
            # Apply generic bonuses/penalties to base
            if bonus_gen > 0:
                bonus_gen = min(bonus_gen, cap_e)
                base += bonus_gen
                parts["bonus_generic"] = float(bonus_gen)
            
            if pen_div_ct > 0:
                base -= pen_div_ct

            base = _clamp(base, 0.0, 1.0)
            
        except Exception:
            pass

        # ------------------------------------------------------------
        # Phase F: Microstructure bonuses (OBI stability / OFI / CVD reclaim)
        # Phase 2: Structural Evidence Access (Refactored)
        # ------------------------------------------------------------
        micro_cap = _get_f("micro_bonus_cap", 0.10)
        micro_bonus = 0.0

        # --- OBI stability ---
        obi_secs = max(_ctx_confirm_value(ctx, "obi_stable"), _f_any(ctx, "obi_stable_secs", "obi_stable_sec", default=0.0))
        obi_q = _ctx_confirm_value(ctx, "obi_q")
        if obi_q <= 0.0:
            obi_q = _f_any(ctx, "obi_stability_score", default=1.0)
        q_floor = _get_f("obi_stable_bonus_q_floor", 0.35)
        obi_q = _clamp(max(float(q_floor), float(obi_q)), 0.0, 1.0)

        obi_val = _f_any(ctx, "obi_avg", "obi", default=0.0)
        obi_dir = _get_s("obi_dir", "").upper()
        obi_aligned = True
        if dir_sign != 0:
            if obi_dir in ("LONG", "SHORT"):
                obi_aligned = (obi_dir == ("LONG" if dir_sign > 0 else "SHORT"))
            elif obi_val != 0.0:
                obi_aligned = (obi_val * dir_sign) > 0.0

        if obi_secs > 0.0 and obi_aligned:
            min_secs = _get_f("obi_stable_bonus_min_secs", 1.5)
            s_secs = _ramp(obi_secs, min_secs, min_secs * 2.5)
            w = _get_f("obi_stable_bonus_w", 0.04)
            micro_bonus += w * (s_secs * obi_q)
            parts.update({
                "obi_stable_secs": float(obi_secs),
                "obi_stability_q": float(obi_q),
                "s_obi_stable": float(_clamp(s_secs * obi_q, 0.0, 1.0)),
            })

        # --- OFI stability ---
        ofi_secs = max(_ctx_confirm_value(ctx, "ofi_stable"), _f_any(ctx, "ofi_stable_secs", "ofi_stable_sec", default=0.0))
        ofi_q = _ctx_confirm_value(ctx, "ofi_q")
        if ofi_q <= 0.0:
            ofi_q = _f_any(ctx, "ofi_stability_score", default=1.0)
        ofi_q_floor = _get_f("ofi_bonus_q_floor", 0.35)
        ofi_q = _clamp(max(float(ofi_q_floor), float(ofi_q)), 0.0, 1.0)

        ofi_aligned = True
        try:
            ofi_dir_ok = getattr(ctx, "ofi_dir_ok")
            ofi_aligned = bool(int(ofi_dir_ok))
        except Exception:
            ofi_val = _f_any(ctx, "ofi", "ofi_best_norm", default=0.0)
            if dir_sign != 0 and ofi_val != 0.0:
                ofi_aligned = (ofi_val * dir_sign) > 0.0

        if ofi_secs > 0.0 and ofi_aligned:
            min_secs = _get_f("ofi_bonus_min_secs", 1.0)
            s_secs = _ramp(ofi_secs, min_secs, min_secs * 2.5)
            w = _get_f("ofi_bonus_w", 0.03)
            micro_bonus += w * (s_secs * ofi_q)
            parts.update({
                "ofi_stable_secs": float(ofi_secs),
                "ofi_stability_q": float(ofi_q),
                "s_ofi_stable": float(_clamp(s_secs * ofi_q, 0.0, 1.0)),
            })

        # --- CVD reclaim ---
        cvdR = _ctx_confirm_value(ctx, "cvdR")
        if cvdR > 0.0:
            lo = _get_f("cvd_reclaim_bonus_lo", _get_f("cvdR_lo", 1.0))
            hi = _get_f("cvd_reclaim_bonus_hi", _get_f("cvdR_hi", 1.8))
            s_cvd = _ramp(cvdR, lo, hi)
            w = _get_f("cvd_reclaim_bonus_w", 0.02)
            cap = _get_f("cvd_reclaim_bonus_cap", 0.03)
            micro_bonus += min(cap, w * s_cvd)
            parts.update({
                "cvdR": float(cvdR),
                "s_cvdR": float(_clamp(s_cvd, 0.0, 1.0)),
            })
        else:
            cvd_q = _ctx_confirm_value(ctx, "cvd_reclaim")
            if cvd_q > 0.0:
                cvd_min = _get_f("cvd_reclaim_min_score", 0.35)
                s_cvd = _ramp(cvd_q, cvd_min, 1.0)
                w = _get_f("cvd_reclaim_bonus_w", 0.02)
                cap = _get_f("cvd_reclaim_bonus_cap", 0.03)
                micro_bonus += min(cap, w * s_cvd)
                parts.update({
                    "cvd_reclaim_q": float(cvd_q),
                    "s_cvd_reclaim": float(_clamp(s_cvd, 0.0, 1.0)),
                })

        applied_micro = 0.0
        if micro_bonus > 0.0:
            applied_micro = min(micro_cap, micro_bonus)
            base = _clamp(base + applied_micro, 0.0, 1.0)

        parts.update({
            "micro_bonus_raw": float(micro_bonus),
            "micro_bonus_applied": float(applied_micro),
            "micro_bonus_cap": float(micro_cap),
        })

        pens = self._penalties(ctx)
        parts.update(pens)

        pen_total = (
            0.45 * pens["pen_spread"]
            + 0.35 * pens["pen_impact"]
            + 0.20 * pens["pen_l2_stale"]
        )
        pen_total = _clamp(pen_total, 0.0, 1.0)

        mult = 1.0 - (self.cfg.penalty_power * pen_total)
        mult = _clamp(mult, 0.20, 1.00)
        parts["pen_total"] = pen_total
        parts["mult"] = mult
        
        # ------------------------------------------------------------
        # Phase 3: DataHealth Calibration
        # conf01 = base * mult * data_health_mult
        # ------------------------------------------------------------
        data_health_mult = 1.0
        try:
            dh = _get_f("data_health", 1.0)
            dh_power = _get_f("data_health_power", 1.0)
            dh_floor = _get_f("data_health_floor", 0.0)
            
            # Formula: (dh^power + floor) clamped 0..1
            data_health_mult = _clamp((dh ** dh_power) + dh_floor, 0.0, 1.0)
            parts["data_health"] = dh
            parts["data_health_mult"] = data_health_mult
        except Exception:
            pass

        conf01 = base * mult * data_health_mult

        # Optional global scale (guardrail-controlled). Applied to conf01 before min/max mapping.
        scale = float(_get_f("confidence_score_scale", 1.0))
        scale = _clamp(scale, 0.05, 1.50)
        parts["confidence_score_scale"] = scale
        
        conf01_scaled = _clamp(conf01 * scale, 0.0, 1.0)
        parts["confidence01_scaled"] = conf01_scaled

        conf = self.cfg.min_conf + (self.cfg.max_conf - self.cfg.min_conf) * conf01_scaled
        conf = float(_clamp(conf, self.cfg.min_conf, self.cfg.max_conf))
        parts["confidence01"] = conf01
        parts["confidence"] = conf

        # DEBUG: Temporary logging
        if z_abs > 2.0:
            import logging
            logger = logging.getLogger("crypto_orderflow_service")
            # Try to get thr from logic match
            thr_val = self.main_z_thr
            if kind == "breakout":
                thr_val = self.breakout_z_thr
            elif kind == "absorption":
                thr_val = self.absorption_z_thr
            elif kind == "extreme":
                thr_val = self.extreme_z_thr
            
            s_z_val = parts.get("s_z", "???")
            # For logging:
            if isinstance(s_z_val, float):
                s_z_str = f"{s_z_val:.3f}"
            else:
                s_z_str = str(s_z_val)
                
            global _conf_debug_counter
            _conf_debug_counter += 1
            if _conf_debug_counter % 1000 == 0:
                logger.info(f"🔍 [CONF-DEBUG] kind={kind} z={z:.3f} thr={thr_val} s_z={s_z_str}")

        return conf, parts

