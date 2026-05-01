from __future__ import annotations

import math
from typing import Any, Tuple

from ..utils.helpers import _f


def _crypto_conf_factor(
    ctx: "SignalContext",
    signal_kind: str,
) -> tuple[float, dict[str, float] | None]:
    """
    Возвращает conf_factor ∈ [0..1] (НЕ pct).
    Внутри может вести parts для дебага.
    """

    def _clamp01(x: float) -> float:
        if x < 0.0:
            return 0.0
        if x > 1.0:
            return 1.0
        return x

    parts: dict[str, float] = {}

    # -------------------------------------------------
    # 0) Базовая классификация сигнала
    # -------------------------------------------------
    st = (signal_kind or "").lower()
    is_breakout = "breakout" in st
    is_absorption = "absorption" in st or "absorb" in st
    is_meanrev = "meanrev" in st or "revert" in st

    market_mode = str(getattr(ctx, "market_mode", "")).lower()
    is_momentum_mode = market_mode.startswith("momentum")
    is_meanrev_mode = market_mode.startswith("mean")

    # -------------------------------------------------
    # 1) ATR-квантиль (волатильностный режим)
    #    ожидаем что-то типа 0..1
    # -------------------------------------------------
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
    )
    atr_q = _clamp01(atr_q)

    # идеальная зона 0.3–0.7, края 0 и 1 считаем плохими
    if atr_q <= 0.0 or atr_q >= 1.0:
        atr_regime = 0.0
    elif atr_q < 0.3:
        # слишком тихий рынок
        atr_regime = (atr_q - 0.05) / (0.3 - 0.05)  # 0 при 0.05, 1 при 0.3
    elif atr_q > 0.7:
        # слишком дикий рынок
        atr_regime = (0.95 - atr_q) / (0.95 - 0.7)  # 1 при 0.7, 0 при 0.95
    else:
        atr_regime = 1.0

    atr_regime = _clamp01(atr_regime)
    parts["atr_regime"] = atr_regime
    parts["atr_q"] = atr_q

    # Жёсткий hard-gate: совсем экстремальный режим сильно режет confidence
    hard_penalty_atr = 0.0
    if atr_q < 0.02 or atr_q > 0.98:
        hard_penalty_atr = 0.4
    elif atr_q < 0.05 or atr_q > 0.95:
        hard_penalty_atr = 0.25
    parts["hard_penalty_atr"] = hard_penalty_atr

    # -------------------------------------------------
    # 2) Основной z-score сигнала (дельта, кластер и т.п.)
    # -------------------------------------------------
    main_z = float(
        abs(
            getattr(
                ctx,
                "main_z",
                getattr(
                    ctx,
                    "deltaSpikeZ",
                    getattr(ctx, "cluster_z", 0.0),
                )
            )
        )
    )

    # 1σ → ~0, 2σ → ~0.5, 3σ → ~0.85, 4σ+ → 1.0
    if main_z <= 1.0:
        z_core = 0.0
    elif main_z >= 4.0:
        z_core = 1.0
    else:
        z_core = (main_z - 1.0) / (4.0 - 1.0)

    z_core = _clamp01(z_core)
    parts["z_core"] = z_core
    parts["main_z"] = main_z

    # -------------------------------------------------
    # 3) OBI_windowLevels / микроструктура (персистентность дисбаланса)
    # -------------------------------------------------
    obi_levels = getattr(
        ctx,
        "OBI_windowLevels",
        getattr(
            ctx,
            "obi_windowLevels",
            getattr(ctx, "obi_window_levels", []),
        )
    )

    obi_persist_score = 0.0
    if isinstance(obi_levels, (list, tuple)) and len(obi_levels) > 0:
        # ожидаем в obi_levels z-scores по разным окнам
        levels = [abs(float(x)) for x in obi_levels]
        # считаем долю "сильных" уровней
        strong_cnt = sum(1 for v in levels if v >= 1.0)
        obi_persist_frac = strong_cnt / max(len(levels), 1)
        # 0 при 0, 1 при доле >= 0.7
        obi_persist_score = _clamp01((obi_persist_frac - 0.2) / (0.7 - 0.2))
    else:
        # fallback: по одному obi_z / cvd_z
        obi_z = float(
            abs(
                getattr(
                    ctx,
                    "obi_z",
                    getattr(ctx, "obi_window_imbalance_z", 0.0),
                )
            )
        )
        if obi_z <= 0.5:
            obi_persist_score = 0.0
        elif obi_z >= 2.5:
            obi_persist_score = 1.0
        else:
            obi_persist_score = (obi_z - 0.5) / (2.5 - 0.5)

        obi_persist_score = _clamp01(obi_persist_score)

    parts["obi_persist"] = obi_persist_score

    # -------------------------------------------------
    # 4) weakProgress / range_vs_atr — прогресс движения
    # -------------------------------------------------
    # предполагаем:
    #   weakProgress=True, если |range|/ATR <= ~0.3
    #   weakProgress_ratio ~ |range|/ATR
    weak_flag = bool(getattr(ctx, "weakProgress", False))
    weak_ratio = float(
        getattr(
            ctx,
            "weakProgress_ratio",
            getattr(ctx, "range_vs_atr", 1.0),
        )
    )

    # хотим считать хорошим прогрессом что-то типа 0.4–1.2 ATR
    if weak_ratio <= 0.2:
        progress_score = 0.0
    elif weak_ratio >= 1.5:
        # уже может быть переудлинённый ход
        progress_score = max(0.0, (1.8 - weak_ratio) / (1.8 - 1.2))
    elif 0.4 <= weak_ratio <= 1.2:
        progress_score = 1.0
    else:
        # плавный переход
        if weak_ratio < 0.4:
            progress_score = (weak_ratio - 0.2) / (0.4 - 0.2)
        else:  # 1.2 < weak_ratio < 1.5
            progress_score = (1.5 - weak_ratio) / (1.5 - 1.2)

    progress_score = _clamp01(progress_score)

    # если есть явный weakProgress-флаг, дополнительно подрежем
    if weak_flag:
        progress_score *= 0.4

    parts["progress_score"] = progress_score
    parts["weak_ratio"] = weak_ratio
    parts["weak_flag"] = 1.0 if weak_flag else 0.0

    # -------------------------------------------------
    # 5) L2-качество: спред, stale-флаг
    # -------------------------------------------------
    spread_bps = float(getattr(ctx, "spread_bps", 0.0))
    l2_is_stale = bool(getattr(ctx, "l2_is_stale_now", False))

    # считаем "качество книги" 0..1
    if spread_bps <= 2.0:
        book_quality = 1.0
    elif spread_bps >= 12.0:
        book_quality = 0.0
    else:
        book_quality = (12.0 - spread_bps) / (12.0 - 2.0)

    if l2_is_stale:
        book_quality *= 0.3

    book_quality = _clamp01(book_quality)
    parts["book_quality"] = book_quality
    parts["spread_bps"] = spread_bps
    parts["l2_stale"] = 1.0 if l2_is_stale else 0.0

    # -------------------------------------------------
    # 6) Regime-aware Weighting (Phase 3)
    # -------------------------------------------------
    # Detect regime from market_mode or derived signals
    regime = "neutral"
    if is_momentum_mode or is_breakout:
        regime = "trend"
    elif is_meanrev_mode or is_meanrev:
        regime = "range"
    
    parts["regime"] = 1.0 if regime == "trend" else (0.5 if regime == "range" else 0.0)

    # Base weights
    w_z = 0.35
    w_obi = 0.25
    w_atr = 0.15
    w_progress = 0.15
    w_book = 0.10

    # Apply multipliers based on regime (defaults from config or fallback)
    # These allow tuning without code changes
    def _get_mult(name, default):
        val = getattr(ctx, name, default)
        return float(val)

    if regime == "trend":
        # In trend: explicit structure (RSI/Sweep/Div) and Z-score matter most
        w_z *= _get_mult("z_w_trend_mult", 1.1)
        w_obi *= _get_mult("obi_w_trend_mult", 1.2)
        w_progress *= _get_mult("prog_w_trend_mult", 0.6) # ignore "overextended" in trend
    elif regime == "range":
        # In range: mean reversion progress and book quality matter most
        w_z *= _get_mult("z_w_range_mult", 0.9)
        w_progress *= _get_mult("prog_w_range_mult", 1.4)
        w_book *= _get_mult("book_w_range_mult", 1.2)

    # Re-normalize
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

    # -------------------------------------------------
    # 7) Base Score & Bonuses
    # -------------------------------------------------
    base = (
        w_z * z_core
        + w_obi * obi_persist_score
        + w_atr * atr_regime
        + w_progress * progress_score
        + w_book * book_quality
    )

    # -------------------------------------------------
    # 7.1) Structured Bonuses (Phase 2/3)
    # -------------------------------------------------
    # Priority: check ctx.evidence (dict) -> fallback to ctx.confirmations (list)
    # We use a helper to check existence in either
    def _has(key_part):
        # Check evidence dict keys
        if hasattr(ctx, "evidence") and isinstance(ctx.evidence, dict):
            for k in ctx.evidence:
                 if key_part in k: return True
        # Check confirmations list strings
        confs = getattr(ctx, "confirmations", []) or []
        for c in confs:
            if key_part in c: return True
        return False

    bonuses = 0.0
    
    # Strong structural setups
    if _has("reclaim"): bonuses += 0.05
    if _has("obi_stable"): bonuses += 0.04
    if _has("iceberg_strict"): bonuses += 0.04
    if _has("fp_edge_absorb"): bonuses += 0.03
    if _has("rsi_agree"): bonuses += 0.03
    if _has("div_match"): bonuses += 0.04
    
    if _has("sweep"):
        bonuses += 0.02
        if _has("sweep_eq"): # High quality EQ sweep
             bonuses += 0.02

    # Trend-mode structural penalties
    # If we are in TREND mode, but have a Counter-Trend divergence signal?
    # e.g. Long Trend + Bearish Div -> that's bad context for a Long setup
    # Implementation: if div detected against our signal direction
    div_pen = 0.0
    if regime == "trend":
         # Check for counter-trend divergence
         # If we are LONG, and we see Bearish Divergence
         # This usually comes from 'div_kind' or 'div_dir' in evidence
         div_dir = str(getattr(ctx, "div_dir", "") or getattr(ctx, "divergence_direction", "")).upper()
         # ctx.evidence["div_dir"] might be set if div logic puts it there
         
         # Assuming signal direction is passed as 'signal_kind' or implicitly known? 
         # The function signature has 'signal_kind', but commonly direction is passed via side-channel or 
         # inferred. Wait, the caller passes `side=direction`.
         # But `_crypto_conf_factor` signature is `(ctx, signal_kind)`.
         # FIX: We need `side` (direction) here. 
         # The caller `confidence_scorer.score` calls `_crypto_conf_factor(ctx, kind)`.
         # It DOES NOT pass side! 
         # We can try to fetch `side` from ctx if available (ConfCtx usually has access to everything)
         # In tick_processor, we pass `ctx` which resolves to `runtime.config` etc. 
         # But direction is local var in process_tick.
         # Let's check if we can get it from ctx.ind["direction"] or similar.
         pass # penalty placeholder if we can't reliably get side

    parts["raw_bonus"] = bonuses
    bonuses = min(bonuses, 0.12)
    parts["applied_bonus"] = bonuses
    
    base += bonuses
    base = _clamp01(base)

    # -------------------------------------------------
    # 8) Penalties & Data Health Calibration (Phase 3)
    # -------------------------------------------------
    hard_penalty = hard_penalty_atr

    if book_quality <= 0.1:
        hard_penalty += 0.3
    elif book_quality <= 0.3:
        hard_penalty += 0.15

    if main_z < 1.2:
        hard_penalty += 0.5

    hard_penalty = min(max(hard_penalty, 0.0), 0.9)
    parts["hard_penalty_total"] = hard_penalty

    # Apply penalties
    final_score = base * (1.0 - hard_penalty)
    
    # Data Health Calibration
    # Multiplier = max( (health ^ power), floor )
    dh = float(getattr(ctx, "data_health", 1.0))
    dh_power = float(getattr(ctx, "data_health_power", 1.0))
    dh_floor = float(getattr(ctx, "data_health_floor", 0.0))
    
    dh_mult = max(pow(dh, dh_power), dh_floor)
    dh_mult = _clamp01(dh_mult)
    
    final_score *= dh_mult
    
    parts["data_health"] = dh
    parts["dh_mult"] = dh_mult
    
    final_score = _clamp01(final_score)

    parts["confidence_0_1"] = final_score

    return float(final_score), parts


class ConfidenceScorer:
    """
    Adapter class to make functional _crypto_conf_factor compatible with TickProcessor.
    Replaces services.signal_confidence.ConfidenceScorer.
    """
    def __init__(self, *args, **kwargs):
        # Accept any args to be drop-in replacement
        pass

    def score(self, kind: str, side: str, ctx: Any) -> tuple[float, dict[str, float] | None]:
        # Delegate to the functional implementation
        # We assume side is available in ctx or handled via evidence injection if needed for penalties.
        # But _crypto_conf_factor currently doesn't take 'side' explicit arg, 
        # it expects ctx to contain necessary info.
        # We can inject side into ctx if it's a dynamic wrapper, but ctx in TickProcessor 
        # is a ConfCtx wrapper around indicators/runtime.
        
        # If we need side for penalties (e.g. counter-trend), we should make sure it's in ctx.
        # TickProcessor passes 'side' (direction) to score(), but _crypto_conf_factor doesn't accept it.
        # We can try to monkey-patch ctx or assume 'direction' is in indicators.
        # In tick_processor.py, 'ctx' wraps 'indicators' which DOES NOT necessarily have 'direction' (it has 'side'?)
        # Let's check tick_processor.py again. 'direction' is a local variable.
        # It IS passed to score(..., side=direction, ...)
        
        # So we can pass it via ctx if we attach it.
        if hasattr(ctx, "evidence") and isinstance(ctx.evidence, dict):
             ctx.evidence["side"] = side
             ctx.evidence["direction"] = side
        
        return _crypto_conf_factor(ctx, kind)
