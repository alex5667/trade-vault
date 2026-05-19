from __future__ import annotations

"""v14_of — v13_of (242 keys, academic) + v5_additions (execution/TCA/queue/
cross-symbol + OG rule-gate consensus + Phase 8.1 OE composites).

Known schema gap (2026-05-18 audit):
  core/external_features_payload_v1.py emits ~156 keys from later phases
  (Phase 8.2/8.3/8.4/8.5/P1/P2/P3/4.x — Hawkes, cross-venue, cg_*, dl_*,
  deribit term structure, macro calendar, etc.) that are NOT yet in
  V14_OF_NUMERIC_KEYS. Extending this schema requires bumping SCHEMA_HASH,
  invalidating Redis pins (cfg:feature_registry:edge_stack:v14_of) and
  retraining the v14_of canary model. Schedule as a coordinated op-event;
  do not patch piecemeal."""

SCHEMA_HASH = "v14_merged_20250517"

def get_v14_of_numeric_keys() -> list[str]:
    """v14_of numeric keys: v13_of base + v5_of additions."""
    from core.ml_feature_schema_v13_of import get_v13_of_numeric_keys
    v13_keys = set(get_v13_of_numeric_keys())

    v5_additions = [
        "exec_cost_to_tp1_ratio", "exec_cost_to_sl_ratio", "exec_cost_to_atr_ratio",
        "tca_eff_spread_bps_ema", "tca_realized_spread_1s_bps_ema", "tca_realized_spread_5s_bps_ema",
        "tca_perm_impact_1s_bps_ema", "tca_perm_impact_5s_bps_ema", "tca_is_bps_ema", "tca_samples", "tca_stale_ms",
        "spread_p95_bps_symbol_kind_session", "slippage_p95_bps_symbol_kind_session",
        "fill_prob_proxy", "eta_fill_sec", "eta_fill_sec_norm", "fill_prob_p_base", "fill_prob_p_wait", "exec_fill_pen",
        "queue_ahead_qty_l1", "queue_ahead_qty_l5", "queue_ahead_qty_5", "depth_to_taker_rate_ratio",
        "maker_fill_vs_taker_cost_edge", "fill_prob_1s", "fill_prob_3s", "fill_prob_5s",
        "obi_slope_1s", "obi_slope_3s", "obi_stability_decay", "qimb_slope_1s", "qimb_slope_3s",
        "depth_imbalance_5_delta_1s", "depth_imbalance_5_delta_3s", "spread_widen_velocity_bps_s",
        "fill_prob_decay_slope", "book_churn_delta_1s", "book_churn_z", "spread_mean_revert_score",
        "micro_mid_shift_vel_bps_s", "micro_mid_shift_accel_bps_s2",
        "btc_ret_30s", "btc_ret_1m", "btc_ret_5m", "eth_ret_30s", "eth_ret_1m", "eth_ret_5m",
        "rel_ret_1m_vs_btc", "rel_ret_5m_vs_btc", "leader_confidence", "market_risk_on_score",
        "rel_ofi_ml_norm_btc", "rel_lob_micro_shift_bps_btc",
        "signal_age_ms", "signal_age_to_half_life", "vol_expansion_score", "vol_compression_score",
        "atr_tf_ms", "atr_stop_pct", "atr_regime_pct", "atr_age_ms", "hold_target_ms_norm",
        "alpha_half_life_ms_norm", "max_signal_age_ratio", "vol_ratio_fast_slow",
        "dq_score", "dq_flag_count", "tick_lag_ms", "tick_lag_p95_1m", "tick_reorder_rate_1m",
        "tick_dedupe_rate_1m", "tick_gap_count_1m", "bad_time_streak", "book_age_ms", "book_gap_ms",
        "book_update_rate_hz", "book_staleness_z",
        "news_blackout", "news_until_ms_norm",
        "rule_have_need_gap", "missing_legs_count", "gate_pressure_score", "have_need_ratio",
        "of_confirm_scenario", "of_confirm_reason_group", "strong_need", "strong_have",
        # OrderFlow rule-Gate consensus (og_*) keys from v14_of_features
        "og_have", "og_need", "og_have_minus_need", "og_ok", "og_score_minus_threshold",
        "og_contrib_z", "og_contrib_wp", "og_contrib_reclaim", "og_contrib_obi",
        "og_contrib_iceberg", "og_contrib_absorption", "og_gate_bits_count",
        "og_strong_need_rev", "og_strong_need_cont", "og_weak_progress_any", "og_reason_code_id",
        # OE Phase 8.1 keys (deriv composites + breadth + Deribit + Fear&Greed)
        "taker_buy_sell_imbalance", "force_order_imbalance_1m",
        "oi_confirmation_score", "squeeze_risk_score", "liq_impulse_score",
        "market_breadth_ret_24h", "market_breadth_vol_z",
        "btc_leader_ret_breadth", "eth_leader_ret_breadth", "breadth_leader_confirm",
        "deribit_btc_iv_proxy", "deribit_eth_iv_proxy",
        "deribit_btc_iv_z", "deribit_eth_iv_z",
        "deribit_btc_funding_8h", "deribit_eth_funding_8h", "deribit_vol_regime_code",
        "fear_greed_index", "fear_greed_regime_extreme_fear", "fear_greed_regime_extreme_greed",
    ]
    return sorted(list(v13_keys) + [k for k in v5_additions if k not in v13_keys])

def v14_of_info() -> dict:
    keys = get_v14_of_numeric_keys()
    return {"ver": "v14_of", "n_numeric_keys": len(keys), "n_v13_base": 242, "n_v5_additions": len(keys) - 242}

# Export as module-level constant for compatibility
V14_OF_NUMERIC_KEYS = get_v14_of_numeric_keys()

# Hard invariant: count is pinned to current state (v13_of 242 + dedup'd
# v5_additions incl OG 16 + OE Phase 8.1 composites). Append-only contract:
# bump _EXPECTED_KEYS when intentionally adding keys.
# Catches accidental edits to v5_additions list or v13_of base drift.
_EXPECTED_KEYS = 359
assert len(V14_OF_NUMERIC_KEYS) == _EXPECTED_KEYS, (
    f"v14_of key count drift: got {len(V14_OF_NUMERIC_KEYS)}, expected {_EXPECTED_KEYS}. "
    f"If this is intentional, bump _EXPECTED_KEYS and update SCHEMA_HASH."
)
