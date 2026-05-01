from __future__ import annotations

import math
from typing import Any, Dict, Tuple


def _crypto_conf_factor(
    ctx: "SignalContext",
    signal_kind: str,
    *,
    side: str | None = None,
) -> Tuple[float, Dict[str, float] | None]:
    """
    Returns confidence in [0..1] + parts dict.

    Phases:
      - Base features → base score
      - Phase1: regime-aware multipliers, anti-correlation, sweep-type weights, synergy
      - Phase2: allow tuned bonus weights from ctx.conf_score_weight_tuning (dict)
      - Phase3: optional ML late-fusion blend using ctx.ml (from OFConfirmEngine)
    """

    # -----------------------------
    # helpers
    # -----------------------------
    def _clamp01(x: float) -> float:
        if x < 0.0:
            return 0.0
        if x > 1.0:
            return 1.0
        return x

    def _cfgf(name: str, default: float) -> float:
        try:
            v = getattr(ctx, name, default)
            if v is None:
                return float(default)
            return float(v)
        except Exception:
            return float(default)

    def _sat(raw: float, cap: float) -> float:
        # smooth saturation (diminishing returns)
        cap = max(float(cap), 1e-9)
        raw = max(float(raw), 0.0)
        return cap * (1.0 - math.exp(-raw / cap))

    parts: Dict[str, float] = {}

    # -----------------------------
    # 0) signal kind / market mode
    # -----------------------------
    st = (signal_kind or "").lower()
    is_breakout = "breakout" in st
    is_absorption = ("absorption" in st) or ("absorb" in st)
    is_meanrev = False
    from common.market_mode import is_range_regime as _is_rr
    if _is_rr(st) or ("revert" in st):
        is_meanrev = True

    micro_mode = str(getattr(ctx, "market_mode", "") or "mixed").lower()

    # Regime from RegimeDetector: trend/range/mixed.
    regime_raw = getattr(ctx, "market_regime", getattr(ctx, "regime", getattr(ctx, "regime_label", "")))
    regime_s = str(regime_raw).lower()
    if "trend" in regime_s:
        regime_ctx = "trend"
    elif "range" in regime_s:
        regime_ctx = "range"
    elif "mixed" in regime_s:
        regime_ctx = "mixed"
    else:
        regime_ctx = "neutral"

    regime_score = float(getattr(ctx, "market_regime_score", getattr(ctx, "regime_score", 0.0)) or 0.0)

    # Strong-momentum proxy for oscillator anticorrelation (uses price progress + low adverse pullback)
    realized_ema_bps = float(getattr(ctx, "realized_ema_bps", 0.0) or 0.0)
    adverse_ratio_ema = float(getattr(ctx, "adverse_ratio_ema", 0.0) or 0.0)
    momo_bps_thr = float(getattr(ctx, "conf_momo_bps_thr", 15.0) or 15.0)
    momo_adverse_thr = float(getattr(ctx, "conf_momo_adverse_thr", 0.25) or 0.25)
    strong_momentum = (abs(realized_ema_bps) >= momo_bps_thr) and (adverse_ratio_ema <= momo_adverse_thr)

    market_mode = str(getattr(ctx, "market_mode", "") or "").lower()
    is_momentum_mode = market_mode.startswith("momentum")
    is_meanrev_mode = market_mode.startswith("mean")

    # -----------------------------
    # 1) ATR quantile regime
    # -----------------------------
    atr_q = float(
        getattr(
            ctx,
            "atr_q_main",
            getattr(
                ctx,
                "atr_q_lookback",
                getattr(ctx, "atr_quantile", getattr(ctx, "atrQ", 0.5)),
            )
        )
        or 0.5
    )
    atr_q = _clamp01(atr_q)

    if atr_q <= 0.0 or atr_q >= 1.0:
        atr_regime = 0.0
    elif atr_q < 0.3:
        atr_regime = (atr_q - 0.05) / (0.3 - 0.05)
    elif atr_q > 0.7:
        atr_regime = (0.95 - atr_q) / (0.95 - 0.7)
    else:
        atr_regime = 1.0
    atr_regime = _clamp01(atr_regime)

    hard_penalty_atr = 0.0
    if atr_q < 0.02 or atr_q > 0.98:
        hard_penalty_atr = 0.40
    elif atr_q < 0.05 or atr_q > 0.95:
        hard_penalty_atr = 0.25

    parts["atr_q"] = atr_q
    parts["atr_regime"] = atr_regime
    parts["hard_penalty_atr"] = hard_penalty_atr

    # -----------------------------
    # 2) main z-score strength
    # -----------------------------
    main_z = float(
        abs(
            getattr(
                ctx,
                "main_z",
                getattr(ctx, "deltaSpikeZ", getattr(ctx, "cluster_z", 0.0)),
            )
            or 0.0
        )
    )

    if main_z <= 1.0:
        z_core = 0.0
    elif main_z >= 4.0:
        z_core = 1.0
    else:
        z_core = (main_z - 1.0) / (4.0 - 1.0)
    z_core = _clamp01(z_core)

    parts["main_z"] = main_z
    parts["z_core"] = z_core

    # -----------------------------
    # 3) OBI persistence
    # -----------------------------
    obi_levels = getattr(
        ctx,
        "OBI_windowLevels",
        getattr(ctx, "obi_windowLevels", getattr(ctx, "obi_window_levels", [])),
    )

    obi_persist_score = 0.0
    if isinstance(obi_levels, (list, tuple)) and len(obi_levels) > 0:
        levels = [abs(float(x)) for x in obi_levels]
        strong_cnt = sum(1 for v in levels if v >= 1.0)
        frac = strong_cnt / max(len(levels), 1)
        obi_persist_score = _clamp01((frac - 0.2) / (0.7 - 0.2))
    else:
        obi_z = float(abs(getattr(ctx, "obi_z", getattr(ctx, "obi_window_imbalance_z", 0.0)) or 0.0))
        if obi_z <= 0.5:
            obi_persist_score = 0.0
        elif obi_z >= 2.5:
            obi_persist_score = 1.0
        else:
            obi_persist_score = (obi_z - 0.5) / (2.5 - 0.5)
        obi_persist_score = _clamp01(obi_persist_score)

    parts["obi_persist"] = obi_persist_score

    # -----------------------------
    # 4) progress score (range_vs_atr)
    # -----------------------------
    weak_flag = bool(getattr(ctx, "weakProgress", False))
    weak_ratio = float(getattr(ctx, "weakProgress_ratio", getattr(ctx, "range_vs_atr", 1.0)) or 1.0)

    if weak_ratio <= 0.2:
        progress_score = 0.0
    elif weak_ratio >= 1.5:
        progress_score = max(0.0, (1.8 - weak_ratio) / (1.8 - 1.2))
    elif 0.4 <= weak_ratio <= 1.2:
        progress_score = 1.0
    else:
        if weak_ratio < 0.4:
            progress_score = (weak_ratio - 0.2) / (0.4 - 0.2)
        else:
            progress_score = (1.5 - weak_ratio) / (1.5 - 1.2)

    progress_score = _clamp01(progress_score)
    if weak_flag:
        progress_score *= 0.4

    parts["weak_ratio"] = weak_ratio
    parts["weak_flag"] = 1.0 if weak_flag else 0.0
    parts["progress_score"] = progress_score

    # -----------------------------
    # 5) book quality (spread/stale)
    # -----------------------------
    spread_bps = float(getattr(ctx, "spread_bps", 0.0) or 0.0)
    l2_is_stale = bool(getattr(ctx, "l2_is_stale_now", False))

    if spread_bps <= 2.0:
        book_quality = 1.0
    elif spread_bps >= 12.0:
        book_quality = 0.0
    else:
        book_quality = (12.0 - spread_bps) / (12.0 - 2.0)

    if l2_is_stale:
        book_quality *= 0.3
    book_quality = _clamp01(book_quality)

    parts["spread_bps"] = spread_bps
    parts["l2_stale"] = 1.0 if l2_is_stale else 0.0
    parts["book_quality"] = book_quality

    # -----------------------------
    # 6) regime
    # -----------------------------
    regime = regime_ctx
    if regime == "neutral":
        if is_momentum_mode or is_breakout:
            regime = "trend"
        elif is_meanrev_mode or is_meanrev:
            regime = "range"
    parts["regime_trend"] = 1.0 if regime == "trend" else 0.0
    parts["regime_range"] = 1.0 if regime == "range" else 0.0
    parts["regime_mixed"] = 1.0 if regime == "mixed" else 0.0
    parts["strong_momentum"] = 1.0 if strong_momentum else 0.0
    parts["regime_score"] = float(regime_score)

    # -----------------------------
    # 7) base weights (regime-aware multipliers)
    # -----------------------------
    w_z = 0.35
    w_obi = 0.25
    w_atr = 0.15
    w_progress = 0.15
    w_book = 0.10

    def _wm(name: str, default: float) -> float:
        return _cfgf(name, default)

    # Regime logic: trend / breakout / strong momentum emphasize Z+OBI; range emphasize progress+book
    if regime == 'trend' or is_breakout or strong_momentum:
        w_z *= _wm('z_w_trend_mult', 1.10)
        w_obi *= _wm('obi_w_trend_mult', 1.20)
        # In trends, 'overextended' progress is less predictive; downweight it
        w_progress *= _wm('prog_w_trend_mult', 0.60)
        w_book *= _wm('book_w_trend_mult', 0.95)
    elif regime == 'range' or is_meanrev:
        w_z *= _wm('z_w_range_mult', 0.90)
        w_progress *= _wm('prog_w_range_mult', 1.40)
        w_book *= _wm('book_w_range_mult', 1.20)
        w_obi *= _wm('obi_w_range_mult', 0.95)

    # Softly scale based on detector confidence (regime_score in [0..1])
    if regime in ('trend', 'range') and regime_score > 0.0:
        rs = _clamp01(regime_score)
        # Blend toward neutral weights when regime_score is low
        blend = 0.35 + 0.65 * rs
        w_z = w_z * blend + 0.35 * (1.0 - blend)
        w_obi = w_obi * blend + 0.25 * (1.0 - blend)
        w_atr = w_atr * blend + 0.15 * (1.0 - blend)
        w_progress = w_progress * blend + 0.15 * (1.0 - blend)
        w_book = w_book * blend + 0.10 * (1.0 - blend)

    w_sum = w_z + w_obi + w_atr + w_progress + w_book
    w_z /= w_sum
    w_obi /= w_sum
    w_atr /= w_sum
    w_progress /= w_sum
    w_book /= w_sum

    parts["w_z"] = w_z
    parts["w_obi"] = w_obi
    parts["w_atr"] = w_atr
    parts["w_progress"] = w_progress
    parts["w_book"] = w_book

    base = (w_z * z_core) + (w_obi * obi_persist_score) + (w_atr * atr_regime) + (w_progress * progress_score) + (w_book * book_quality)
    base = _clamp01(base)
    parts["base_pre_bonus"] = base

    # -----------------------------
    # 7.1) structured bonuses (allowlist + tuned weights)
    # -----------------------------
    _ALLOW = {
        "reclaim",
        "obi_stable",
        "ice_strict",
        "iceberg_strict",
        "fp_edge_absorb",
        "rsi_agree",
        "div_match",
        "sweep",
        "sweep_eqh",
        "sweep_eql",
    }

    flags: Dict[str, str] = {}

    def _put(k: str, v: str = "1") -> None:
        k = (k or "").strip()
        if k in _ALLOW:
            flags[k] = v or "1"
            if k == "ice_strict":
                flags.setdefault("iceberg_strict", flags[k])
            if k == "iceberg_strict":
                flags.setdefault("ice_strict", flags[k])

    evd = getattr(ctx, "evidence", None)
    if isinstance(evd, dict):
        for k in _ALLOW:
            if k in evd:
                vv = evd.get(k)
                try:
                    if vv is None:
                        _put(k, "1")
                    elif isinstance(vv, (bool, int, float)):
                        if float(vv) != 0.0:
                            _put(k, str(vv))
                    elif isinstance(vv, str):
                        s = vv.strip()
                        if s and s != "0":
                            _put(k, s)
                    else:
                        _put(k, "1")
                except Exception:
                    pass

    confs = getattr(ctx, "confirmations", []) or []
    for c in confs:
        if not isinstance(c, str):
            continue
        s = c.strip()
        if not s:
            continue
        if "=" in s:
            k, v = s.split("=", 1)
            _put(k, v.strip() or "1")
        else:
            _put(s, "1")

    def _has(k: str) -> bool:
        return k in flags

    # direction: prefer explicit kwarg (P1-4 fix); fall back to ctx attributes for
    # callers that still inject via ctx.side / ctx.evidence["side"].
    if side is None:
        side = str(getattr(ctx, "side", "") or getattr(ctx, "direction", "") or "").upper()
    else:
        side = str(side or "").upper()
    if side not in {"LONG", "SHORT"}:
        side = "UNKNOWN"
    parts["side_long"] = 1.0 if side == "LONG" else 0.0
    parts["side_short"] = 1.0 if side == "SHORT" else 0.0

    # optional Phase2 tuning table
    def _bonus(bkey: str, default: float) -> float:
        try:
            tuning = getattr(ctx, "conf_score_weight_tuning", None)
            if isinstance(tuning, dict) and tuning:
                base_map = tuning.get("by_regime") if isinstance(tuning.get("by_regime"), dict) else tuning
                reg_map = base_map.get(regime) if isinstance(base_map, dict) else None
                if isinstance(reg_map, dict):
                    sk_map = reg_map.get(signal_kind) if isinstance(reg_map.get(signal_kind), dict) else None
                    if isinstance(sk_map, dict) and bkey in sk_map:
                        return float(sk_map[bkey])
                    if bkey in reg_map:
                        return float(reg_map[bkey])
                if bkey in tuning:
                    return float(tuning[bkey])
        except Exception:
            pass
        return _cfgf(bkey, default)

    b = {
        "reclaim": _bonus("bonus_reclaim", 0.05),
        "obi_stable": _bonus("bonus_obi_stable", 0.04),
        "iceberg_strict": _bonus("bonus_iceberg_strict", 0.04),
        "fp_edge_absorb": _bonus("bonus_fp_edge_absorb", 0.03),
        "rsi_agree": _bonus("bonus_rsi_agree", 0.03),
        "div_match": _bonus("bonus_div_match", 0.04),
        "sweep": _bonus("bonus_sweep", 0.02),
        "sweep_eqh": _bonus("bonus_sweep_eqh", 0.02),
        "sweep_eql": _bonus("bonus_sweep_eql", 0.02),
    }

    # anti-correlation (oscillators in strong momentum)
    mom_strength = _cfgf("mom_strength", -1.0)
    if mom_strength < 0.0:
        mom_strength = _clamp01((main_z - 2.0) / 2.0) if regime == "trend" else 0.0
    parts["mom_strength"] = mom_strength

    micro_mult = _cfgf(f"bonus_micro_mult_{regime}", 1.0)
    struct_mult = _cfgf(f"bonus_struct_mult_{regime}", 1.0)
    osc_mult = _cfgf(f"bonus_osc_mult_{regime}", 1.0)

    if regime == "trend":
        osc_mult *= max(0.25, 1.0 - _cfgf("osc_trend_corr_k", 0.60) * mom_strength)

    # sweep alignment & type
    sweep_eqh = _has("sweep_eqh")
    sweep_eql = _has("sweep_eql")
    sweep_any = _has("sweep") or sweep_eqh or sweep_eql

    sweep_aligned = (
        (side == "LONG" and sweep_eql) or
        (side == "SHORT" and sweep_eqh) or
        (side == "UNKNOWN" and sweep_any)
    )

    div_kind = str(getattr(ctx, "div_kind", "") or "").lower()
    div_dir = str(getattr(ctx, "div_dir", "") or getattr(ctx, "divergence_direction", "") or "").upper()
    div_strength = 0.0
    try:
        div_strength = float(getattr(ctx, "div_strength", 0.0) or 0.0)
    except Exception:
        div_strength = 0.0
    div_strength_mult = 1.0
    if div_strength > 0:
        div_strength_mult = min(max(div_strength / _cfgf("div_strength_ref", 1.0), 0.8), 1.3)

    div_aligned = (
        (side == "LONG" and ("bull" in div_dir.lower() or "bullish" in div_kind)) or
        (side == "SHORT" and ("bear" in div_dir.lower() or "bearish" in div_kind)) or
        (side == "UNKNOWN" and ("bullish" in div_kind or "bearish" in div_kind))
    )

    # Phase-2 anti-correlation tuning
    # Supports Phase-2 tuning shapes:
    #   tuning = {
    #     "anti_corr": {"trend": {"rsi_agree": 0.55, ...}},
    #     "by_regime": {"trend": {"anti_corr": {...}}}
    #   }
    def _anti_corr_override(key: str, default_mult: float) -> float:
        if mom_strength <= 0.0:
            return default_mult
        try:
            tuning = getattr(ctx, "conf_score_weight_tuning", None)
            if isinstance(tuning, dict) and tuning:
                reg_block = tuning.get("by_regime", {}).get(regime, {})
                t_ac_map = reg_block.get("anti_corr", {})
                if not t_ac_map:
                    t_ac_map = tuning.get("anti_corr", {}).get(regime, {})
                if not t_ac_map:
                    # backward-compat: allow direct key->mult map
                    maybe = tuning.get("anti_corr")
                    if isinstance(maybe, dict):
                        t_ac_map = {k: v for k, v in maybe.items() if not isinstance(v, dict)}
                
                if key in t_ac_map:
                    m2 = float(t_ac_map[key])
                    return default_mult * max(0.0, min(m2, 1.0))
        except Exception:
            pass
        return default_mult

    # caps
    micro_cap = _cfgf("bonus_micro_cap", 0.10)
    struct_cap = _cfgf("bonus_struct_cap", 0.10)
    osc_cap = _cfgf("bonus_osc_cap", 0.06)
    synergy_cap = _cfgf("bonus_synergy_cap", 0.05)
    total_cap = _cfgf("bonus_total_cap", 0.14)
    total_floor = _cfgf("bonus_total_floor", -0.06)

    # micro group (OBI / Iceberg / FP)
    micro_raw = 0.0
    if _has("obi_stable"):
        micro_raw += b["obi_stable"]
    if _has("iceberg_strict") or _has("ice_strict"):
        micro_raw += b["iceberg_strict"]
    if _has("fp_edge_absorb"):
        micro_raw += b["fp_edge_absorb"]
    micro_raw *= micro_mult
    micro_applied = _sat(micro_raw, micro_cap)

    # structural group (reclaim + sweep)
    struct_raw = 0.0
    if _has("reclaim"):
        struct_raw += b["reclaim"] * _cfgf(f"bonus_reclaim_mult_{regime}", 1.0)

    # sweep weights by type + alignment
    if sweep_any:
        sweep_bonus = 0.0
        if sweep_eqh:
            sweep_bonus += b["sweep_eqh"] * (1.0 if (side in {"SHORT", "UNKNOWN"}) else 0.5) * _anti_corr_override("sweep_eqh", 1.0)
        if sweep_eql:
            sweep_bonus += b["sweep_eql"] * (1.0 if (side in {"LONG", "UNKNOWN"}) else 0.5) * _anti_corr_override("sweep_eql", 1.0)
        if not (sweep_eqh or sweep_eql):
            sweep_bonus += b["sweep"] * _anti_corr_override("sweep", 1.0)

        sweep_bonus *= _cfgf(f"bonus_sweep_mult_{regime}", 1.0)
        if not sweep_aligned and side != "UNKNOWN":
            sweep_bonus *= _cfgf("sweep_misaligned_mult", 0.40)
        struct_raw += sweep_bonus

    struct_raw *= struct_mult
    struct_applied = _sat(struct_raw, struct_cap)

    # oscillator group (RSI agree + divergence)
    osc_raw = 0.0
    if _has("rsi_agree"):
        mult = _anti_corr_override("rsi_agree", _cfgf(f"bonus_rsi_mult_{regime}", 1.0))
        osc_raw += b["rsi_agree"] * mult
    if _has("div_match") and div_aligned:
        mult = _anti_corr_override("div_match", _cfgf(f"bonus_div_mult_{regime}", 1.0))
        osc_raw += b["div_match"] * mult * div_strength_mult
    osc_raw *= osc_mult
    osc_applied = _sat(osc_raw, osc_cap)

    def _synergy_override(keys_tuple: str, default: float) -> float:
        try:
            tuning = getattr(ctx, "conf_score_weight_tuning", None)
            if isinstance(tuning, dict) and tuning:
                reg_block = tuning.get("by_regime", {}).get(regime, {})
                sbm = tuning.get("synergy_by_regime", {}).get(regime, {})
                if keys_tuple in sbm: return float(sbm[keys_tuple])
                if keys_tuple in reg_block.get("synergy", {}): return float(reg_block["synergy"][keys_tuple])
                sym = tuning.get("synergy", {})
                if keys_tuple in sym: return float(sym[keys_tuple])
                # flip check
                if "+" in keys_tuple:
                    a, b_key = keys_tuple.split("+", 1)
                    flip = f"{b_key}+{a}"
                    if flip in sbm: return float(sbm[flip])
                    if flip in sym: return float(sym[flip])
        except Exception:
            pass
        return _cfgf(f"synergy_{keys_tuple.replace('+', '_')}", default)

    # synergy
    synergy_raw = 0.0
    if sweep_aligned and _has("reclaim"):
        synergy_raw += _synergy_override("sweep+reclaim", _cfgf("synergy_sweep_reclaim", 0.015))
    if sweep_aligned and _has("obi_stable"):
        synergy_raw += _synergy_override("sweep+obi_stable", _cfgf("synergy_sweep_obi", 0.010))
    if (_has("iceberg_strict") or _has("ice_strict")) and _has("fp_edge_absorb"):
        synergy_raw += _synergy_override("iceberg_strict+fp_edge_absorb", _cfgf("synergy_ice_fp", 0.010))
    if _has("div_match") and sweep_aligned and div_aligned:
        synergy_raw += _synergy_override("sweep+div_match", _cfgf("synergy_div_sweep", 0.010))
    synergy_applied = min(max(synergy_raw, 0.0), synergy_cap)

    # conflicts/penalties
    conflict_pen = 0.0
    if side == "LONG" and sweep_eqh:
        conflict_pen += _cfgf("penalty_sweep_wrong_side", 0.020)
    if side == "SHORT" and sweep_eql:
        conflict_pen += _cfgf("penalty_sweep_wrong_side", 0.020)

    # P1-3: counter-trend divergence penalty — applies in ALL regimes.
    # Uses div_aligned (single source of truth) instead of duplicating direction logic.
    # The old check had a bug: SHORT + "bearish" in div_kind is ALIGNED, not counter-trend.
    # Magnitude is regime-aware: trend bears more risk from counter-trend divergence.
    if _has("div_match") and not div_aligned and side != "UNKNOWN":
        if regime == "trend":
            conflict_pen += _cfgf("penalty_div_counter_trend", 0.020)
        else:  # range / mixed / neutral
            conflict_pen += _cfgf("penalty_div_counter_range", 0.010)

    bonuses = micro_applied + struct_applied + osc_applied + synergy_applied - conflict_pen
    bonuses = min(max(bonuses, total_floor), total_cap)

    parts["bonus_micro_raw"] = micro_raw
    parts["bonus_micro_applied"] = micro_applied
    parts["bonus_struct_raw"] = struct_raw
    parts["bonus_struct_applied"] = struct_applied
    parts["bonus_osc_raw"] = osc_raw
    parts["bonus_osc_applied"] = osc_applied
    parts["bonus_synergy_raw"] = synergy_raw
    parts["bonus_synergy_applied"] = synergy_applied
    parts["bonus_conflict_pen"] = conflict_pen
    parts["bonus_total_applied"] = bonuses

    base = _clamp01(base + bonuses)

    # -----------------------------
    # 8) hard penalties + data health
    # -----------------------------
    hard_penalty = hard_penalty_atr
    if book_quality <= 0.1:
        hard_penalty += 0.30
    elif book_quality <= 0.3:
        hard_penalty += 0.15
    if main_z < 1.2:
        hard_penalty += 0.50
    hard_penalty = min(max(hard_penalty, 0.0), 0.90)
    parts["hard_penalty_total"] = hard_penalty

    score = base * (1.0 - hard_penalty)

    dh = float(getattr(ctx, "data_health", 1.0) or 1.0)
    dh_power = float(getattr(ctx, "data_health_power", 1.0) or 1.0)
    dh_floor = float(getattr(ctx, "data_health_floor", 0.0) or 0.0)

    dh_mult = max(pow(dh, dh_power), dh_floor)
    dh_mult = _clamp01(dh_mult)
    parts["data_health"] = dh
    parts["dh_mult"] = dh_mult

    score *= dh_mult
    score = _clamp01(score)
    parts["confidence_pre_ml"] = score

    # -----------------------------
    # 9) Phase3: optional ML blend (late fusion)
    # -----------------------------
    # If MLConfirmGate provides a calibrated probability (ml_p_cal), blend in logit space.
    ml_mode = str(getattr(ctx, "conf_ml_mode", "fuse") or "fuse").lower()
    ml_p = None
    ev = getattr(ctx, "evidence", None)
    if isinstance(ev, dict):
        for k in ("ml_p_cal", "ml_p_edge_cal", "p_edge_cal", "ml_p"):
            v = ev.get(k)
            if v is not None:
                try:
                    ml_p = float(v)
                    break
                except Exception:
                    pass

    if ml_p is None:
        for k in ("ml_p_cal", "ml_p_edge_cal", "p_edge_cal", "ml_p"):
            v = getattr(ctx, k, None)
            if v is not None:
                try:
                    ml_p = float(v)
                    break
                except Exception:
                    pass

    ml_state = str(getattr(ctx, "ml_state", "") or (ev.get("ml_state") if isinstance(ev, dict) else "") or "").lower()

    if ml_mode not in ("off", "0", "false", "no") and ml_p is not None and (ml_state in ("", "ok", "shadow")):
        alpha = float(getattr(ctx, "conf_ml_alpha", 0.35) or 0.35)
        if ml_mode == "primary":
            alpha = max(alpha, float(getattr(ctx, "conf_ml_alpha_primary", 0.70) or 0.70))

        # Reduce alpha when feature coverage is low.
        cov = float(getattr(ctx, "ml_feature_coverage", (ev.get("ml_feature_coverage") if isinstance(ev, dict) else 1.0)) or 1.0)
        cov_mult = _clamp01((cov - 0.50) / 0.50)
        alpha *= cov_mult

        # Reduce alpha when data health is low (dh_mult already applied, but keep conservative).
        alpha *= _clamp01(dh)

        # In range regime, ML tends to be less stable unless explicitly trained for it.
        if regime == "range":
            alpha *= float(getattr(ctx, "conf_ml_alpha_range_mult", 0.80) or 0.80)

        # Logit blend for numerical stability.
        eps = 1e-6
        def _logit(p: float) -> float:
            p = min(max(float(p), eps), 1.0 - eps)
            return math.log(p / (1.0 - p))
        def _sigmoid(z: float) -> float:
            return 1.0 / (1.0 + math.exp(-z))

        bias = float(getattr(ctx, "conf_ml_logit_bias", 0.0) or 0.0)
        z = (1.0 - alpha) * _logit(score) + alpha * _logit(ml_p) + bias
        score = _sigmoid(z)

        parts["ml_p"] = float(ml_p)
        parts["ml_alpha"] = float(alpha)
        parts["ml_cov"] = float(cov)
        parts["ml_cov_mult"] = float(cov_mult)
        parts["ml_mode_primary"] = 1.0 if ml_mode == "primary" else 0.0
        parts["ml_bias"] = float(bias)

    parts["confidence_0_1"] = score
    return float(score), parts


class ConfidenceScorer:
    """
    Adapter to be compatible with TickProcessor:
      score(kind=..., side=..., ctx=...) -> (confidence_0_1, parts)
    """
    def __init__(self, *args: Any, **kwargs: Any) -> None:
        pass

    def score(self, kind: str, side: str, ctx: Any) -> Tuple[float, Dict[str, float] | None]:
        # P1-4 fix: pass side as explicit kwarg — no monkey-patch dependency.
        # Still inject into ctx/evidence for downstream consumers (gates, diagnostics)
        # that may read ctx.side or evidence["side"] directly.
        try:
            ctx.side = side
            ctx.direction = side
        except Exception:
            pass
        if hasattr(ctx, "evidence") and isinstance(ctx.evidence, dict):
            ctx.evidence["side"] = side
            ctx.evidence["direction"] = side
        return _crypto_conf_factor(ctx, kind, side=side)
