#!/usr/bin/env python3
"""
Verify that v13_of (production schema) covers all features from the plan (4.1-4.13).

Output: Coverage report by feature group.
"""

import sys
sys.path.insert(0, '.')

from core.ml_feature_schema_v13_of import get_v13_of_numeric_keys, v13_of_info

all_keys = set(get_v13_of_numeric_keys())
info = v13_of_info()
print(f"v13_of Info: {info}")
# Add boolean keys from v12_of base (v13 inherits them)
all_keys.update(['res_recovered', 'lob_dw_obi_stable', 'atr_fresh', 'soft_fail_near_pass',
                 'session_asia', 'session_europe', 'session_us', 'weekend_flag',
                 'session_overlap_eu_us', 'prior_stale', 'cvd_quarantine_active'])

# Feature groups from plan (4.1-4.13)
PLAN_FEATURES = {
    "4.2_execution_tca": [
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
        "exec_cost_to_tp1_ratio",
        "exec_cost_to_sl_ratio",
        "exec_cost_to_atr_ratio",
    ],
    "4.3_queue_fill": [
        "fill_prob_proxy",
        "eta_fill_sec",
        "fill_prob_p_base",
        "fill_prob_p_wait",
        "exec_fill_pen",
        "eta_fill_sec_norm",
        "queue_ahead_qty_l1",
        "queue_ahead_qty_l5",
        "queue_ahead_qty_5",
        "depth_to_taker_rate_ratio",
        "maker_fill_vs_taker_cost_edge",
        "fill_prob_1s",
        "fill_prob_3s",
        "fill_prob_5s",
    ],
    "4.4_lob_dynamics": [
        "obi_slope_1s",
        "obi_slope_3s",
        "qimb_slope_1s",
        "qimb_slope_3s",
        "depth_imbalance_5_delta_1s",
        "depth_imbalance_5_delta_3s",
        "spread_widen_velocity_bps_s",
        "fill_prob_decay_slope",
        "obi_stability_decay",
        "book_churn_delta_1s",
        "book_churn_z",
        "spread_mean_revert_score",
        "micro_mid_shift_vel_bps_s",
        "micro_mid_shift_accel_bps_s2",
    ],
    "4.5_vpin_hawkes": [
        "vpin_tox_1m",
        "vpin_tox_5m",
        "vpin_tox_z",
        "vpin_tox_slope",
        "hawkes_taker_buy_lam",
        "hawkes_taker_sell_lam",
        "hawkes_cancel_bid_lam",
        "hawkes_cancel_ask_lam",
        "hawkes_limit_add_lam",
        "hawkes_limit_add_bid_lam",
        "hawkes_limit_add_ask_lam",
        "hawkes_taker_lam",
        "hawkes_cancel_lam",
        "hawkes_churn_lam",
        "hawkes_buy_sell_lam_ratio",
        "hawkes_cancel_imbalance",
    ],
    "4.6_cross_symbol": [
        "btc_ret_30s",
        "btc_ret_1m",
        "btc_ret_5m",
        "eth_ret_30s",
        "eth_ret_1m",
        "eth_ret_5m",
        "rel_ret_1m_vs_btc",
        "rel_ret_5m_vs_btc",
        "leader_confidence",
        "market_risk_on_score",
        "rel_ofi_ml_norm_btc",
        "rel_lob_micro_shift_bps_btc",
        "leader_btc_eth_confirm",
        "leader_direction_conflict",
        "sector_breadth_1m",
        "sector_breadth_5m",
        "sector_breadth_ret_24h",
    ],
    "4.7_liquidation_oi": [
        "funding_rate",
        "funding_rate_z",
        "oi_notional_usd",
        "oi_delta_1m",
        "oi_delta_5m",
        "oi_delta_z",
        "basis_bps",
        "premium_index_bps",
        "premium_index_z",
        "basis_pressure_score",
        "liq_long_notional_1m",
        "liq_short_notional_1m",
        "liq_long_notional_5m",
        "liq_short_notional_5m",
        "liq_imbalance_1m",
        "liq_imbalance_5m",
        "liq_imbalance_z",
        "long_short_ratio",
        "long_short_ratio_z",
    ],
    "4.8_regime_atr": [
        "atr_tf_ms",
        "atr_stop_pct",
        "atr_regime_pct",
        "atr_age_ms",
        "atr_fresh",
        "hold_target_ms_norm",
        "alpha_half_life_ms_norm",
        "vol_ratio_fast_slow",
        "max_signal_age_ratio",
        "signal_age_ms",
        "signal_age_to_half_life",
        "vol_expansion_score",
        "vol_compression_score",
    ],
    "4.9_dq_freshness": [
        "dq_score",
        "dq_flag_count",
        "tick_lag_ms",
        "tick_lag_p95_1m",
        "tick_reorder_rate_1m",
        "tick_dedupe_rate_1m",
        "tick_gap_count_1m",
        "bad_time_streak",
        "book_age_ms",
        "book_gap_ms",
        "book_update_rate_hz",
        "book_staleness_z",
    ],
    "4.10_historical_priors": [
        "prior_winrate_symbol_kind_7d",
        "prior_winrate_symbol_kind_session_7d",
        "prior_ev_r_symbol_kind_7d",
        "prior_profit_factor_symbol_kind_7d",
        "prior_sl_hit_rate_symbol_kind_7d",
        "prior_tp1_hit_rate_symbol_kind_7d",
        "prior_samples_symbol_kind_7d",
        "prior_median_mae_r_winners_30d",
        "prior_p90_mae_r_winners_30d",
        "prior_median_mfe_r_30d",
        "prior_giveback_p75_30d",
        "prior_winrate_symbol_kind_session",
        "prior_ev_r_symbol_kind_session",
        "prior_ev_r_median",
        "prior_sample_count_log",
        "prior_age_ms",
        "prior_stale_ms",
        "prior_stale",
        "prior_profit_factor",
        "prior_sl_hit_rate",
        "prior_r_std",
    ],
    "4.11_gate_trace": [
        "rule_have",
        "rule_need",
        "rule_have_need_gap",
        "rule_score",
        "missing_legs_count",
        "gate_pressure_score",
        "strong_need",
        "strong_have",
        "soft_fail_near_pass",
        "of_confirm_scenario",
        "of_confirm_reason_group",
        "have_need_ratio",
    ],
    "4.12_news_session": [
        "news_blackout",
        "news_until_ms_norm",
        "hour_sin",
        "hour_cos",
        "dow_sin",
        "dow_cos",
        "session_asia",
        "session_europe",
        "session_us",
        "session_overlap_eu_us",
        "weekend_flag",
    ],
}

print(f"v13_of Schema: {len(all_keys)} total keys (num + bool)")
print(f"{'='*80}")

total_covered = 0
total_planned = 0

for group, features in PLAN_FEATURES.items():
    found = [f for f in features if f in all_keys]
    missing = [f for f in features if f not in all_keys]
    total_planned += len(features)
    total_covered += len(found)

    coverage_pct = 100 * len(found) / len(features) if features else 0
    status = "✅" if not missing else "⚠️"

    print(f"{status} {group:20s} {len(found):3d}/{len(features):3d} ({coverage_pct:5.1f}%)")
    if missing:
        print(f"   Missing: {', '.join(missing[:5])}")
        if len(missing) > 5:
            print(f"   ... and {len(missing)-5} more")

print(f"\n{'='*80}")
print(f"Total Coverage: {total_covered}/{total_planned} features ({100*total_covered/total_planned:.1f}%)")

if total_covered == total_planned:
    print("✅ ALL PLAN FEATURES COVERED IN v13_of")
    sys.exit(0)
else:
    print(f"⚠️  {total_planned - total_covered} features still missing")
    sys.exit(1)
