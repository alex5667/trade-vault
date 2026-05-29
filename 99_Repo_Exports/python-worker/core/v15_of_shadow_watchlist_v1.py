"""core/v15_of_shadow_watchlist_v1.py — shadow features pending v15_of promotion.

The 48h coverage gate is the explicit promotion criterion for new features
into ``V15_OF_NUMERIC_KEYS`` (see `project_p2_phase1_shadow_emit_2026_05_28`
and the v15_of audit notes). To track when each feature clears the gate we
keep a single declarative watchlist:

    * Producers emit the features into `signals:of:inputs` already (see
      `core/external_features_payload_v1._V12_BASE_OPTIONAL_KEYS` —
      everything from "Phase 1 P1 shadow features" through the source-health
      block).
    * The coverage exporter
      (`orderflow_services/v15_of_coverage_exporter_v1.py`) reads this
      watchlist and emits dedicated per-group shadow coverage metrics so the
      gate is observable without bumping the prod schema.
    * Once a group's coverage stays ≥ 0.95 for 48h, the keys can be moved
      into `core/ml_feature_schema_v15_of.py` (or a v16 schema bump).

This module is **NOT** part of the v15_of schema. Importing it must not
expand `V15_OF_NUMERIC_KEYS` or affect any pin / contract check.
"""
from __future__ import annotations

from core.source_health_v1 import SOURCE_HEALTH_FEATURE_KEYS


# ── P1 Phase 1 shadow features (Groups A-J) ─────────────────────────────────
# Match `external_features_payload_v1._V12_BASE_OPTIONAL_KEYS` "Phase 1 P1
# shadow features" block. Keep in same order for grep parity.
_P1_GROUPS: dict[str, tuple[str, ...]] = {
    "p1_a_mlofi_microstructure": (
        "mlofi_accel_1s",
        "mlofi_flip_count_3s",
        "mlofi_same_dir_secs",
        "microprice_ret_1s",
        "microprice_reversion_3s",
    ),
    "p1_b_queue_adverse": (
        "queue_depletion_rate_l1",
        "queue_refill_rate_l1",
        "adverse_selection_1s_bps",
        "post_fill_reversion_prob",
        "limit_vs_market_entry_edge_bps",
    ),
    "p1_c_exec_ev": (
        "ev_after_slippage_bps",
        "net_edge_to_cost_ratio",
    ),
    "p1_d_cost_dynamics": (
        "cost_widening_5s_bps",
    ),
    "p1_e_regime_transitions": (
        "regime_transition_code",
        "failed_breakout_count_30m",
    ),
    "p1_f_liq_source_health": (
        # bybit_data_age_ms is now canonical via core.source_health_v1
        # (src_health group); kept liq_* here because liq feed has no
        # registered SourceSpec yet.
        "liq_source_available",
        "liq_source_age_ms",
    ),
    "p1_g_cross_venue_quality": (
        "cross_venue_lead_lag_ms",
        "venue_consensus_persistence_3s",
    ),
    "p1_h_liq_cascade": (
        "liq_cascade_risk_score",
    ),
    "p1_i_pit_priors_timeout": (
        "prior_timeout_rate_symbol_kind_session",
        "prior_tp1_before_timeout_rate",
    ),
    "p1_j_session_quality": (
        "session_liquidity_z",
        "session_signal_quality_prior",
    ),
}


# ── P2 Phase 1 shadow features ──────────────────────────────────────────────
_P2_GROUPS: dict[str, tuple[str, ...]] = {
    "p2_a_mlofi_subsecond": (
        "mlofi_1_3_5_slope",
        "mlofi_l1_l5_divergence",
        "mlofi_accel_500ms",
        "mlofi_exhaustion_score",
        "microprice_ret_250ms",
        "midprice_impact_per_1k_usd",
    ),
    "p2_b_queue_l5_adverse": (
        "queue_depletion_rate_l5",
        "queue_refill_rate_l5",
        "queue_position_risk_score",
        "adverse_selection_3s_bps",
        "fill_or_kill_edge_bps",
    ),
    "p2_c_cost_decomposition": (
        "ev_after_fee_bps",
        "ev_after_spread_bps",
        "ev_after_impact_bps",
        "tp1_net_after_cost_bps",
        "sl_net_after_cost_bps",
        "expected_hold_cost_bps",
        "cost_regime_z",
    ),
    "p2_d_regime_dynamics": (
        "regime_transition_age_ms",
        "trend_to_chop_prob",
        "chop_to_expansion_prob",
        "expansion_exhaustion_score",
        "vol_ofi_regime_agree",
        "vol_price_divergence_score",
        "range_break_attempt_count_30m",
    ),
    "p2_e_cross_venue_extended": (
        "bybit_book_age_ms",
        "bybit_trade_rate_hz",
        "cross_venue_latency_diff_ms",
        "binance_leads_bybit_score",
        "bybit_leads_binance_score",
        "venue_consensus_flip_count_10s",
        "cross_venue_spread_diff_bps",
    ),
    "p2_g_pit_priors_extended": (
        "prior_winrate_symbol_kind_regime_session",
        "prior_ev_r_symbol_kind_regime_session",
        "prior_timeout_loss_rate_session",
        "prior_mae_before_mfe_ratio",
        "prior_best_exit_policy_code",
        "prior_trailing_success_rate",
        "prior_be_stopout_rate",
        "prior_hold_time_p50_ms",
        "prior_hold_time_p90_ms",
    ),
    "p2_h_directional_change": (
        "dc_event_dir",
        "dc_event_age_ms",
        "dc_overshoot_bps",
        "dc_reversal_count_15m",
    ),
}


# ── Source health (canonical, from core.source_health_v1) ──────────────────
_SOURCE_HEALTH_GROUPS: dict[str, tuple[str, ...]] = {
    "src_health": SOURCE_HEALTH_FEATURE_KEYS,
}


# Combined: stable group → feature tuple map. Order matters for the exporter
# (Prometheus labels are stable across restarts).
SHADOW_WATCHLIST_GROUPS: dict[str, tuple[str, ...]] = {
    **_P1_GROUPS,
    **_P2_GROUPS,
    **_SOURCE_HEALTH_GROUPS,
}


SHADOW_WATCHLIST_KEYS: tuple[str, ...] = tuple(
    k for ks in SHADOW_WATCHLIST_GROUPS.values() for k in ks
)


def get_shadow_groups() -> dict[str, tuple[str, ...]]:
    """Return a copy of the watchlist groups so callers cannot mutate the
    module-level constant by accident."""
    return {g: tuple(ks) for g, ks in SHADOW_WATCHLIST_GROUPS.items()}


def get_shadow_key_to_group() -> dict[str, str]:
    """Flat ``feature_key → group_name`` map. First-match-wins on overlap."""
    out: dict[str, str] = {}
    for g, ks in SHADOW_WATCHLIST_GROUPS.items():
        for k in ks:
            out.setdefault(k, g)
    return out
