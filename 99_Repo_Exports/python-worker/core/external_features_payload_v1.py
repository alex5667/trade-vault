from __future__ import annotations

"""External features payload helper — flushes Phase 7.8/7.9/7.9b/8.1/8.2 keys
from the inference-time `indicators_with_v4` dict into the outbound
`indicators` dict that ships in `signals:of:inputs`.

Why this exists
---------------
of_confirm_engine.py populates derivative/external/composite features into
`indicators_with_v4` for inline ML scoring, but the outbound payload reads
from a separate `indicators` dict. Without an explicit hand-off, these
features vectorize to 0.0 in the offline dataset → train/serve skew.

This module mirrors the pattern of v14_of_features.build_og_payload: a pure
function that the engine calls once per signal alongside the og_* update.

Design
------
- Pure copy: no Redis, no I/O, no globals.
- Fail-open: every key defaults to 0.0; missing inputs never raise.
- Idempotent: identical inputs → identical output dict.
- Stable list: keys are explicitly enumerated so future schema bumps stay
  visible in code review.

When updating the key list
--------------------------
Keep this list in sync with the populate blocks in of_confirm_engine.py:
- Phase 7.8 (cross-context)        — anchor returns, TCA p95, PIT priors
- Phase 7.9  (derivatives context) — funding/OI/basis/liq/L-S/sector breadth
- Phase 7.9b (composites)          — taker_imb, force_order_imb, oi_confirm,
                                      squeeze_risk, liq_impulse
- Phase 8.1 (external joiners)     — breadth WS, Deribit IV, Fear & Greed
- Phase 8.2 (time/gate)           — sector_breadth_1m, prior_stale_ms,
                                      hour_sin/cos, dow_sin/cos, news_blackout,
                                      fill_prob_1s/3s/5s
"""

from typing import Any


# Numeric keys — copied from indicators_with_v4 with float() cast.
_NUM_KEYS: tuple[str, ...] = (
    # ── Phase 7.8: ADR-0006 anchor returns + cross-asset
    "btc_ret_30s", "btc_ret_1m", "btc_ret_5m",
    "eth_ret_30s", "eth_ret_1m", "eth_ret_5m",
    "rel_ret_1m_vs_btc", "rel_ret_5m_vs_btc",
    "leader_confidence",
    "market_risk_on_score",
    "rel_ofi_ml_norm_btc",
    "rel_lob_micro_shift_bps_btc",
    # ── Phase 7.8: ADR-0007 PIT priors (extended)
    "prior_winrate_symbol_kind_session",
    "prior_ev_r_symbol_kind_session",
    "prior_ev_r_median",
    "prior_sample_count_log",
    "prior_age_ms",
    "prior_profit_factor",
    "prior_sl_hit_rate",
    "prior_r_std",
    # ── Phase 7.8: ADR-0005 TCA EMA priors + p95
    "tca_eff_spread_bps_ema",
    "tca_realized_spread_1s_bps_ema",
    "tca_realized_spread_5s_bps_ema",
    "tca_perm_impact_1s_bps_ema",
    "tca_perm_impact_5s_bps_ema",
    "tca_is_bps_ema",
    "tca_samples",
    "tca_stale_ms",
    "spread_p95_bps_symbol_kind_session",
    "slippage_p95_bps_symbol_kind_session",
    # ── Phase 7.9: derivatives context (from ctx:deriv:{symbol})
    "funding_rate", "funding_rate_z",
    "oi_notional_usd", "open_interest_z",
    "oi_delta_5m", "oi_delta_1m", "oi_accel",
    "basis_bps", "premium_index_bps", "basis_pressure_score",
    "liq_long_notional_1m", "liq_short_notional_1m",
    "liq_long_notional_5m", "liq_short_notional_5m",
    "liq_imbalance_1m", "liq_imbalance_5m", "liq_imbalance_z",
    "long_short_ratio", "long_short_ratio_z",
    "leader_btc_eth_confirm",
    "leader_direction_conflict",
    "sector_breadth_ret_24h", "sector_breadth_vol_z",
    # ── Phase 7.9b: composite scores
    "taker_buy_sell_imbalance",
    "force_order_imbalance_1m",
    "oi_confirmation_score",
    "squeeze_risk_score",
    "liq_impulse_score",
    # ── Phase 8.1: live market breadth (from runtime:breadth)
    "market_breadth_ret_24h",
    "market_breadth_vol_z",
    "btc_leader_ret_breadth",
    "eth_leader_ret_breadth",
    "breadth_leader_confirm",
    # ── Phase breadth-v2: granular 1m/5m returns + segment breadth
    "market_breadth_ret_1m", "market_breadth_ret_5m",
    "major_breadth_1m", "major_ret_1m",
    "meme_breadth_1m", "meme_ret_1m",
    "alt_breadth_1m", "alt_ret_1m",
    "alt_breadth_5m", "alt_ret_5m",
    "sector_breadth_score",
    # ── Phase 8.1: Deribit vol-regime (from ctx:deribit:global)
    "deribit_btc_iv_proxy", "deribit_eth_iv_proxy",
    "deribit_btc_iv_z", "deribit_eth_iv_z",
    "deribit_btc_funding_8h", "deribit_eth_funding_8h",
    "deribit_vol_regime_code",
    # ── Phase 8.1: Fear & Greed (from ctx:sentiment:global)
    "fear_greed_index",
    # ── Phase 8.2: 1-min rolling breadth (from runtime:breadth HASH)
    "sector_breadth_1m",
    # ── Phase 8.2: PIT prior staleness as float ms
    "prior_stale_ms",
    # ── Phase 8.2: cyclical time encoding (from ctx_hour_utc / ctx_dow)
    "hour_sin", "hour_cos",
    "dow_sin", "dow_cos",
    # ── Phase 8.2: news gate as float (from news_gate_veto)
    "news_blackout",
    # ── Phase 8.2: fixed-horizon fill probability (1s / 3s / 5s max-wait)
    "fill_prob_1s", "fill_prob_3s", "fill_prob_5s",
    # ── Phase 8.3: taker ratio/z, top-trader L/S, forceOrder per-side notionals + composites
    "taker_buy_sell_ratio",
    "taker_buy_sell_ratio_z",
    "top_trader_long_short_ratio",
    "force_order_long_notional_1m",
    "force_order_short_notional_1m",
    "force_order_cluster_score",
    "futures_crowding_score",
    # ── Phase 8.4: Hawkes/VPIN raw intensities + derived composites
    "hawkes_dt_s",
    "hawkes_taker_buy_lam",
    "hawkes_taker_sell_lam",
    "hawkes_cancel_bid_lam",
    "hawkes_cancel_ask_lam",
    "hawkes_limit_add_lam",
    "hawkes_taker_lam",
    "hawkes_cancel_lam",
    "hawkes_churn_lam",
    "added_bid_rate_ema",
    "added_ask_rate_ema",
    "added_total_rate_ema",
    "vpin_tox_ema",
    "vpin_tox_z",
    "hawkes_S_taker_buy",
    "hawkes_S_taker_sell",
    "hawkes_S_cancel_bid",
    "hawkes_S_cancel_ask",
    "hawkes_S_limit_add",
    "hawkes_buy_sell_lam_ratio",
    "hawkes_cancel_imbalance",
    # ── Phase 8.4: OI delta z, premium z, 5m breadth, news remaining
    "oi_delta_z",
    "premium_index_z",
    "sector_breadth_5m",
    "news_until_ms_norm",
    # ── Phase 8.4: queue alias and gate trace
    "queue_ahead_qty_5",
    "of_confirm_scenario",
    "of_confirm_reason_group",
    "strong_need",
    "strong_have",
    # ── Phase 8.5: gate trace completeness
    "have_need_ratio",
    # ── Phase 8.5: cross-venue sanity (Group XV)
    "cross_venue_agree_score",
    "cross_venue_dislocation_bps",
    "cross_venue_dislocation_z",
    "binance_local_noise_score",
    # ── Phase 8.5: CoinGecko macro context (Group XVI)
    "cg_btc_dom_pct",
    "cg_stable_dom_pct",
    "cg_btc_dom_mom",
    "cg_global_turnover",
    "cg_symbol_rank",
    "cg_rel_strength_btc_1h",
    "cg_volume_mcap_ratio",
    # ── Phase 8.5: Deribit extended — options OI + per-symbol perp basis (Group XVII)
    "deribit_btc_options_oi_usd",
    "deribit_eth_options_oi_usd",
    "deribit_perp_basis_bps",
    # ── Phase P1: Deribit term structure (tenor-bucketed IV + put/call ratios)
    "deribit_btc_iv_7d", "deribit_btc_iv_30d",
    "deribit_eth_iv_7d", "deribit_eth_iv_30d",
    "deribit_iv_term_structure_7d_30d",
    "deribit_put_call_ratio",
    "deribit_options_oi_call_put_ratio",
    "deribit_event_vol_premium_score",
    # ── Phase P1: 5-min market breadth volume + z-score
    "market_breadth_vol_5m",
    "market_breadth_volume_z",
    # ── Phase P1: symbol relative strength vs market / BTC / sector
    "symbol_rel_strength_vs_btc_1m",
    "symbol_rel_strength_vs_market_1m",
    "symbol_rel_strength_vs_sector_1m",
    # ── Phase 8.5: DefiLlama slow-regime context (Group XVIII)
    "dl_stablecoin_mcap_usd",
    "dl_stablecoin_mcap_delta_1d",
    "dl_stablecoin_risk_regime_code",
    "dl_eth_tvl_usd",
    "dl_eth_dex_vol_delta_1d_pct",
    # ── Phase 4.10: rolling PIT priors (7d / 30d)
    "prior_winrate_symbol_kind_7d",
    "prior_ev_r_symbol_kind_7d",
    "prior_profit_factor_symbol_kind_7d",
    "prior_sl_hit_rate_symbol_kind_7d",
    "prior_tp1_hit_rate_symbol_kind_7d",
    "prior_samples_symbol_kind_7d",
    "prior_winrate_symbol_kind_session_7d",
    "prior_median_mae_r_winners_30d",
    "prior_p90_mae_r_winners_30d",
    "prior_median_mfe_r_30d",
    "prior_giveback_p75_30d",
    # ── Phase 4.12: macro event calendar proximity
    "macro_event_severity",
    "minutes_to_macro_event",
    "minutes_after_macro_event",
    # ── Phase 4.5: VPIN rolling + Hawkes limit_add bid/ask
    "vpin_tox_1m",
    "vpin_tox_5m",
    "vpin_tox_slope",
    "hawkes_limit_add_bid_lam",
    "hawkes_limit_add_ask_lam",
    "hawkes_limit_add_imbalance",
    # ── Phase 4.6: cross-symbol sector aggregation
    "sector_delta_z_median",
    "sector_obi_median",
    # ── Phase 4.7: liq heatmap aliases
    "liq_cluster_dist_above_bps",
    "liq_cluster_dist_below_bps",
    "liq_heatmap_density_above",
    "liq_heatmap_density_below",
    # ── Phase P3: Fear & Greed delta
    "fear_greed_delta_1d",
    # ── Phase P3: CoinPaprika fallback (Group XIX)
    "cp_btc_dom_pct",
    "cp_symbol_ret_7d",
    "cp_volume_mcap_ratio",
    "cp_market_cap_rank",
    # ── Phase P3: CoinMarketCap fallback (Group XX)
    "cmc_btc_dom_pct",
    "cmc_total_mcap_usd",
    "cmc_total_volume_usd",
    "cmc_active_cryptos",
    # ── Phase P3: DefiLlama extended (added to Group XVIII)
    "dl_dex_volume_spike_z",
    "dl_eth_fees_24h_usd",
    "dl_eth_fees_revenue_momentum",
    "dl_perps_oi_delta_1d_pct",
    # ── Phase P2: Bybit cross-venue (Group XXI)
    "bybit_funding_rate",
    "bybit_ret_1m",
    "bybit_oi_delta_5m",
    "bybit_taker_buy_sell_ratio",
    "binance_bybit_price_diff_bps",
    "binance_bybit_oi_divergence",
)

# Bool-like keys — encoded as 0/1 float (v13/v14 schemas have no bool block).
_BOOL_KEYS: tuple[str, ...] = (
    # ── Phase 7.8 PIT prior staleness flag
    "prior_stale",
    # ── Phase 8.1 Fear & Greed regime flags
    "fear_greed_regime_extreme_fear",
    "fear_greed_regime_extreme_greed",
)

# v12_of base keys whose producers exist in code (atr/liqmap/microbar/v12 features/
# decision engine) but live in dicts other than `indicators_with_v4`. Emitted
# only when found in either source — no key is forced to 0.0, so this never
# overrides an existing populated value in the caller's `indicators` dict.
_V12_BASE_OPTIONAL_KEYS: tuple[str, ...] = (
    # ATR pipeline (signal_pipeline / configuration / tick_decision_engine)
    "atr_bps_exec", "atr_candidates_n", "atr_cons_ok", "atr_consistency",
    "atr_fees_rocket_mult", "atr_fees_th_bps", "atr_fees_tp1_share",
    "atr_floor_t0_bps", "atr_floor_t1_bps", "atr_floor_t2_bps", "atr_floor_tier",
    "atr_percentile_rank_30d", "atr_sanity_ok", "atr_unified_th_bps",
    # Liqmap SL recommendation (signal_pipeline)
    "liqmap_sl_base_bps", "liqmap_sl_reco_bps",
    "liqmap_sl_widen_needed", "liqmap_sl_widen_ratio",
    # v12_of features (core/v12_of_features.py)
    "bid_ask_queue_imbalance", "calibration_age_ms", "cvd_divergence_from_price",
    "depth_migration_bps", "eth_btc_corr_5m", "large_trade_ratio",
    "last_trade_outcome_raw", "level2_wap_divergence",
    # Iceberg / decision-engine stats
    "iceberg_avg_qty",
    # Veto bookkeeping (of_confirm_engine)
    "book_health_veto_book_evidence", "data_health_veto_book_evidence",
    # Triple-barrier labels (only present when outcome is back-filled)
    "mae_r", "mfe_r",
)


def _is_present(src: dict[str, Any], k: str) -> bool:
    """True iff key exists with a non-None value (0/empty-string still counts)."""
    if k not in src:
        return False
    v = src[k]
    return v is not None


def _f(x: Any) -> float:
    """Coerce to float; non-numeric → 0.0."""
    try:
        if x is None:
            return 0.0
        if isinstance(x, bool):
            return 1.0 if x else 0.0
        return float(x)
    except (TypeError, ValueError):
        return 0.0


def build_external_features_payload(
    indicators_with_v4: dict[str, Any] | None,
    runtime_indicators: dict[str, Any] | None = None,
) -> dict[str, float]:
    """Return a dict of Phase 7.8/7.9/7.9b/8.1/8.2 feature keys + opt v12 base.

    Args:
        indicators_with_v4: inference-time ML-scoring dict (primary source).
            Pass None or {} → primary source treated as empty.
        runtime_indicators: outbound `indicators` dict the caller is about to
            ship in `signals:of:inputs`. Used as fall-back source for v12_of
            base keys whose producers (signal_pipeline, v12_of_features,
            tick_decision_engine, configuration) write here rather than into
            `indicators_with_v4`. Pass None to skip the fall-back.

    Bool fields emit as float 0/1 to match the no-bool-block design of
    v13_of/v14_of schemas.

    Semantics:
        - `_NUM_KEYS` / `_BOOL_KEYS` are *always* present in the output,
          defaulting to 0.0 when neither source has the key (preserves the
          historical contract callers depend on for fixed-shape payloads).
        - `_V12_BASE_OPTIONAL_KEYS` are emitted *only* when found in one of
          the sources. This prevents an over-eager 0.0 from overriding a
          legitimate populated value in the caller's `indicators` dict.

    Returns:
        dict[str, float] with len(_NUM_KEYS) + len(_BOOL_KEYS) +
        <0..len(_V12_BASE_OPTIONAL_KEYS)> entries.
    """
    src1 = indicators_with_v4 or {}
    src2 = runtime_indicators or {}
    out: dict[str, float] = {}

    def _pick(k: str) -> float:
        if _is_present(src1, k):
            return _f(src1[k])
        if _is_present(src2, k):
            return _f(src2[k])
        return 0.0

    for k in _NUM_KEYS:
        out[k] = _pick(k)
    for k in _BOOL_KEYS:
        out[k] = _pick(k)
    for k in _V12_BASE_OPTIONAL_KEYS:
        if _is_present(src1, k):
            out[k] = _f(src1[k])
        elif _is_present(src2, k):
            out[k] = _f(src2[k])
        # else: omit — caller's existing value (if any) is preserved.
    return out


def external_feature_keys() -> tuple[str, ...]:
    """Public accessor: all keys this helper can produce (num + bool + opt v12)."""
    return _NUM_KEYS + _BOOL_KEYS + _V12_BASE_OPTIONAL_KEYS
