"""
slq_risk_adjust.py — Stop-Loss Quantile (SLQ) Risk Adjustment

Stop-Risk Contract: SLQ может только РАСШИРЯТЬ стоп, никогда не сужать.
Если расширенный стоп слишком широк или EV < min_ev → DENY (cfgd["sizing_ok"]=False),
а не "ставим ближе".

Bucket hierarchy (fallback cascade, most → least specific). Primary cascade
is scoped by ATR timeframe so 1m/5m/15m samples never get mixed; legacy
non-scoped keys are still consulted as a final fallback during the migration
window so this reader keeps working against older snapshots.
  slq:tf={atr_tf}:{sym}:{side}:{scenario}:{regime}:{session}:{vol_bucket}:{liq_bucket}
  slq:tf={atr_tf}:{sym}:{side}:{scenario}:{regime}
  slq:tf={atr_tf}:{sym}:{side}:{regime}
  slq:tf={atr_tf}:{sym}:{side}
  slq:{sym}:{side}:{scenario}:{regime}:{session}:{vol_bucket}:{liq_bucket}   (legacy)
  slq:{sym}:{side}:{scenario}:{regime}                                       (legacy)
  slq:{sym}:{side}:{regime}                                                  (legacy)
  slq:{sym}:{side}                                                           (legacy)
"""
from __future__ import annotations

import os
from typing import Any

from common.math_safe import clamp
from services.slq_store import SlqSnapshot, fetch_slq
from utils.time_utils import get_ny_time_millis


# ---------------------------------------------------------------------------
# ENV helpers
# ---------------------------------------------------------------------------

def _envf(name: str, default: str) -> float:
    try:
        return float(os.getenv(name, default) or default)
    except Exception:
        return float(default)


def _envi(name: str, default: str) -> int:
    try:
        return int(float(os.getenv(name, default) or default))
    except Exception:
        return int(float(default))


def _env_on(name: str, default: str = "0") -> bool:
    v = (os.getenv(name, default) or "").strip().lower()
    return v in {"1", "true", "yes", "on"}


def _sym_envf(name: str, symbol: str, default: float) -> float:
    """
    Symbol-specific ENV override.
    Checks both full symbol (canonical, e.g. BTCUSDT) and stripped base (BTC).
    Priority: NAME__BTCUSDT > NAME__BTC > NAME (global).
    """
    sym_full = symbol.upper().replace("-", "").replace("/", "")
    sym_base = sym_full.replace("USDT", "").replace("USDC", "").replace("BUSD", "")

    for variant in (sym_full, sym_base):
        v = os.getenv(f"{name}__{variant}")
        if v:
            try:
                return float(v)
            except Exception:
                pass
    return _envf(name, str(default))


# ---------------------------------------------------------------------------
# Side / bucket helpers
# ---------------------------------------------------------------------------

def _side_str(side: Any) -> str:
    try:
        s = int(side)
        return "LONG" if s > 0 else "SHORT"
    except Exception:
        ss = (side or "").strip().lower()
        if ss in {"long", "buy", "1"}:
            return "LONG"
        if ss in {"short", "sell", "-1"}:
            return "SHORT"
        return "NA"


def _ctx_str(ctx: Any, *attrs: str, default: str = "na") -> str:
    for a in attrs:
        try:
            v = getattr(ctx, a, None)
            if v:
                return str(v).lower().strip()
        except Exception:
            pass
    return default


_ATR_TF_LABELS = {
    60_000: "1m",
    180_000: "3m",
    300_000: "5m",
    900_000: "15m",
    1_800_000: "30m",
    3_600_000: "1h",
    14_400_000: "4h",
    86_400_000: "1d",
}


def _atr_tf_label_from_ctx(ctx: Any) -> str:
    """
    Resolve ATR timeframe label from ctx (atr_tf_ms primary, atr_tf string fallback).
    Returns "na" when unknown so we don't silently collapse multi-scale buckets.
    """
    raw_ms = 0
    for attr in ("atr_tf_ms", "atr_selected_tf_ms"):
        try:
            v = getattr(ctx, attr, None)
            if v:
                raw_ms = int(float(v))
                break
        except Exception:
            pass
    if raw_ms > 0:
        return _ATR_TF_LABELS.get(raw_ms, f"{raw_ms}ms")
    # String fallback: ctx.atr_tf already in "1m"/"5m" form
    s = _ctx_str(ctx, "atr_tf", "atr_tf_selected", default="")
    return s or "na"


def _build_slq_key_cascade(symbol: str, side_s: str, ctx: Any) -> list[tuple[str, str]]:
    """
    Returns list of (redis_key, bucket_level_label) from most to least specific.
    Caller tries each in order and stops at first hit with N >= min_n.

    tf-scoped keys come first (primary), legacy non-scoped keys are appended
    as a transitional fallback while SLQ aggregator still emits both layouts.
    """
    sym = symbol.upper()
    scenario = _ctx_str(ctx, "scenario")
    regime   = _ctx_str(ctx, "regime")
    session  = _ctx_str(ctx, "session")
    vol_b    = _ctx_str(ctx, "vol_bucket", "catr_bucket")
    liq_b    = _ctx_str(ctx, "liq_bucket")
    atr_tf   = _atr_tf_label_from_ctx(ctx)

    tf = f"tf={atr_tf}:"
    return [
        (f"slq:{tf}{sym}:{side_s}:{scenario}:{regime}:{session}:{vol_b}:{liq_b}", "exact"),
        (f"slq:{tf}{sym}:{side_s}:{scenario}:{regime}", "sym_side_scenario_regime"),
        (f"slq:{tf}{sym}:{side_s}:{regime}", "sym_side_regime"),
        (f"slq:{tf}{sym}:{side_s}", "sym_side"),
        # Legacy (no atr_tf scope) — kept until aggregator drops SLQ_WRITE_LEGACY_KEYS.
        (f"slq:{sym}:{side_s}:{scenario}:{regime}:{session}:{vol_b}:{liq_b}", "legacy_exact"),
        (f"slq:{sym}:{side_s}:{scenario}:{regime}", "legacy_sym_side_scenario_regime"),
        (f"slq:{sym}:{side_s}:{regime}", "legacy_sym_side_regime"),
        (f"slq:{sym}:{side_s}", "legacy_sym_side"),
    ]


def _fetch_slq_with_fallback(
    redis: Any,
    symbol: str,
    side_s: str,
    ctx: Any,
    min_n: int,
    max_age_sec: int,
    now_ms: int,
) -> tuple[SlqSnapshot | None, str]:
    """
    Try cascade from most specific bucket to least.
    Returns (snapshot, bucket_level) or (None, "").
    """
    cascade = _build_slq_key_cascade(symbol, side_s, ctx)
    for key, level in cascade:
        snap = fetch_slq(redis, key=key)
        if snap is None:
            continue
        if snap.n < min_n:
            continue
        if max_age_sec > 0:
            age = (now_ms - int(snap.ts_ms)) / 1000.0
            if age > float(max_age_sec):
                continue
        return snap, level
    return None, ""


# ---------------------------------------------------------------------------
# EV computation after SLQ
# ---------------------------------------------------------------------------

def _compute_ev_after_slq(ctx: Any, cfgd: dict[str, Any], new_stop_mult: float) -> float:
    """
    Quick EV estimate (bps) after SLQ widening:
        EV = p_tp1 * tp1_bps - (1 - p_tp1) * stop_bps - cost_bps
    """
    p_tp1 = 0.0
    try:
        p_tp1 = float(getattr(ctx, "tp1_hit_prob", 0.0) or 0.0)
    except Exception:
        pass

    atr_bps = 0.0
    try:
        atr_bps = float(getattr(ctx, "atr_bps", 0.0) or 0.0)
        if atr_bps <= 0:
            atr_price = float(getattr(ctx, "atr", 0.0) or 0.0)
            entry = float(getattr(ctx, "entry_price", 1.0) or 1.0)
            if entry > 0 and atr_price > 0:
                atr_bps = (atr_price / entry) * 10_000.0
    except Exception:
        pass

    rr = float(cfgd.get("TP1_RR") or cfgd.get("tp1_rr") or 1.3)
    cost_bps = _envf("COST_TOTAL_BPS", "8.0")

    stop_bps = new_stop_mult * atr_bps
    tp1_bps  = stop_bps * rr
    ev = p_tp1 * tp1_bps - (1.0 - p_tp1) * stop_bps - cost_bps
    return ev


# ---------------------------------------------------------------------------
# Stop bps from ctx (for DENY width check)
# ---------------------------------------------------------------------------

def _stop_dist_to_bps(ctx: Any, stop_mult: float) -> float:
    """Convert ATR-based stop to bps. Returns 0.0 if not computable."""
    try:
        atr_bps = float(getattr(ctx, "atr_bps", 0.0) or 0.0)
        if atr_bps > 0:
            return atr_bps * stop_mult
        atr_price = float(getattr(ctx, "atr", 0.0) or 0.0)
        entry = float(getattr(ctx, "entry_price", 1.0) or 1.0)
        if entry > 0 and atr_price > 0:
            return (atr_price / entry) * 10_000.0 * stop_mult
    except Exception:
        pass
    return 0.0


# ---------------------------------------------------------------------------
# TP1 scaling after SLQ widen
# ---------------------------------------------------------------------------

def _scale_tp1(cfgd: dict[str, Any], symbol: str, ratio: float) -> None:
    """Proportionally scale TP1 mult to preserve R:R after SL widen."""
    base_tp1 = float(cfgd.get("ROCKET_TP1_ATR_MULT") or 0.0)

    if base_tp1 <= 0:
        sym_prefix = symbol.split("USDT")[0].split("USDC")[0].upper()
        base_tp1 = _envf(f"{sym_prefix}_ROCKET_TP1_ATR_MULT", "0.0")

    if base_tp1 <= 0:
        base_tp1 = _envf("ROCKET_TP1_ATR_MULT", "0.78")

    new_tp1 = clamp(base_tp1 * ratio, 0.5, 3.0)
    cfgd["ROCKET_TP1_ATR_MULT"] = new_tp1
    cfgd["slq_tp1_mult"] = new_tp1
    cfgd["slq_tp1_ratio"] = ratio


# ---------------------------------------------------------------------------
# Main entry point
# ---------------------------------------------------------------------------

def maybe_apply_slq_to_risk_cfg(
    *,
    redis: Any,
    ctx: Any,
    symbol: str,
    side: Any,
    cfg: dict[str, Any],
) -> dict[str, Any]:
    """
    Apply SLQ adjustment to risk config.

    Contract:
      - SLQ can only WIDEN the stop, never tighten it.
      - If widened stop is too wide → DENY (cfgd["sizing_ok"] = False).
      - If EV after widening < min_ev → DENY.
      - All decisions carry a reason-code in cfgd["slq_decision"].

    Fail-open: returns original cfg on any unexpected error.
    Idempotent: if cfg["slq_used"] == 1 → returns as-is.
    """
    cfgd = dict(cfg or {})
    if cfgd.get("slq_used") == 1:
        return cfgd
    if not _env_on("SLQ_ENABLE", "0"):
        return cfgd

    shadow_only = _env_on("SLQ_SHADOW_ONLY", "0")

    # --- knobs ---
    min_n        = _envi("SLQ_MIN_N", "300")
    max_age_sec  = _envi("SLQ_MAX_AGE_SEC", "1800")
    k            = _envf("SLQ_K", "0.50")
    bump_cap_atr = _envf("SLQ_BUMP_ATR_CAP", "0.30")
    stop_atr_min = _sym_envf("SLQ_STOP_ATR_MIN", symbol, 0.80)
    stop_atr_max = _sym_envf("SLQ_STOP_ATR_MAX", symbol, 2.20)
    tp1_prob_min = _envf("SLQ_TP1_PROB_MIN", "0.58")
    postsl_tp1_min = _envf("SLQ_POSTSL_TP1_MIN", "0.35")
    max_stop_bps = _sym_envf("SLQ_MAX_STOP_BPS", symbol, 150.0)
    min_ev_bps   = _envf("SLQ_MIN_EV_AFTER_BPS", "-50.0")

    side_s = _side_str(side)
    now_ms = get_ny_time_millis()

    try:
        # ── 1. Fetch snapshot via cascade ─────────────────────────────────
        snap, bucket_level = _fetch_slq_with_fallback(
            redis, symbol, side_s, ctx, min_n, max_age_sec, now_ms
        )

        if snap is None:
            cfgd["slq_decision"] = "slq_no_snapshot"
            return cfgd

        # ── 2. Quality gates ──────────────────────────────────────────────
        if float(snap.post_sl_tp1_hit_rate) < float(postsl_tp1_min):
            cfgd["slq_decision"] = "slq_low_postsl_tp1"
            return cfgd

        tp1_prob = 0.0
        try:
            tp1_prob = float(getattr(ctx, "tp1_hit_prob", 0.0) or 0.0)
        except Exception:
            pass
        if tp1_prob < float(tp1_prob_min):
            cfgd["slq_decision"] = "slq_low_tp1_prob"
            return cfgd

        # ── 3. ATR-mode only ──────────────────────────────────────────────
        stop_mode = str(
            cfgd.get("STOP_MODE") or cfgd.get("stop_mode") or "atr"
        ).lower()
        if stop_mode not in {"atr", "atr_mult", "atr-mult"}:
            cfgd["slq_decision"] = "slq_not_atr_mode"
            return cfgd

        base = float(cfgd.get("STOP_ATR_MULT") or cfgd.get("stop_atr_mult") or 0.0)
        if base <= 0:
            cfgd["slq_decision"] = "slq_no_base_mult"
            return cfgd

        # Preserve original mult
        if "STOP_ATR_MULT_BASE" not in cfgd:
            cfgd["STOP_ATR_MULT_BASE"] = base
            cfgd["stop_atr_mult_base"] = base

        # ── 4. Compute widened mult ───────────────────────────────────────
        bump = float(k) * float(snap.sl_buffer_atr_q90)
        bump = clamp(bump, 0.0, float(bump_cap_atr))

        # INVARIANT: val >= base (SLQ never tightens the stop)
        val = clamp(
            base + bump,
            max(float(stop_atr_min), base),  # floor is max(min, base)
            float(stop_atr_max),
        )

        # ── P1a: Shadow-only — record but do NOT mutate execution config ─
        # shadow_only=1 means compute + log only, never change STOP/TP.
        if shadow_only:
            widened_stop_bps_shadow = _stop_dist_to_bps(ctx, val)
            ev_shadow = _compute_ev_after_slq(ctx, cfgd, val)
            cfgd["slq_shadow_only"]         = True
            cfgd["slq_shadow_final_mult"]   = float(val)
            cfgd["slq_shadow_ev_after_bps"] = float(ev_shadow)
            cfgd["slq_shadow_widened_stop_bps"] = widened_stop_bps_shadow
            cfgd["slq_shadow_bucket_level"] = bucket_level
            cfgd["slq_shadow_n"]            = int(snap.n)
            cfgd["slq_shadow_q90"]          = float(snap.sl_buffer_atr_q90)
            cfgd["slq_shadow_postsl_tp1"]   = float(snap.post_sl_tp1_hit_rate)
            cfgd["slq_decision"]            = "shadow_computed"
            return cfgd
        # ─────────────────────────────────────────────────────────────────

        # ── 5. DENY: stop too wide ────────────────────────────────────────
        widened_stop_bps = _stop_dist_to_bps(ctx, val)
        if widened_stop_bps > 0 and widened_stop_bps > max_stop_bps:
            reason = "reject_too_wide"
            cfgd["slq_decision"] = reason
            cfgd["slq_widened_stop_bps"] = widened_stop_bps
            cfgd["slq_max_stop_bps"] = max_stop_bps
            if not shadow_only:
                cfgd["sizing_ok"] = False
            return cfgd

        # ── 6. DENY: EV negative after widening ──────────────────────────
        ev_after = _compute_ev_after_slq(ctx, cfgd, val)
        if ev_after < float(min_ev_bps):
            reason = "reject_ev_negative"
            cfgd["slq_decision"] = reason
            cfgd["slq_ev_after_bps"] = ev_after
            if not shadow_only:
                cfgd["sizing_ok"] = False
            return cfgd

        # ── 7. Apply ──────────────────────────────────────────────────────
        cfgd["STOP_ATR_MULT"] = float(val)
        cfgd["stop_atr_mult"] = float(val)

        if "STOP_MODE" in cfgd:
            cfgd["STOP_MODE"] = "ATR"
        cfgd["stop_mode"] = "atr"

        # Meta / observability
        cfgd["slq_used"]          = 1
        cfgd["slq_decision"]      = "applied"
        cfgd["slq_bucket_level"]  = bucket_level
        cfgd["slq_n"]             = int(snap.n)
        cfgd["slq_q90"]           = float(snap.sl_buffer_atr_q90)
        cfgd["slq_postsl_tp1"]    = float(snap.post_sl_tp1_hit_rate)
        cfgd["slq_tp1_prob"]      = float(tp1_prob)
        cfgd["slq_bump_atr"]      = float(bump)
        cfgd["slq_original_mult"] = float(base)
        cfgd["slq_final_mult"]    = float(val)
        cfgd["slq_ev_after_bps"]  = float(ev_after)
        cfgd["slq_widened_stop_bps"] = widened_stop_bps

        if shadow_only:
            cfgd["slq_shadow_only"] = True

        # ── 8. TP1 scaling (preserve R:R) ────────────────────────────────
        if val > base and base > 0:
            _scale_tp1(cfgd, symbol, ratio=val / base)

        return cfgd

    except Exception:
        # Fail-open: never block a trade due to SLQ logic error
        cfgd["slq_decision"] = "slq_exception"
        return cfgd
