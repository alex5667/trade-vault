"""test_shadow_producers_full_emission.py — guard the full-schema contract.

Per the v15_of audit (2026-05-29): every P1 shadow producer must emit
*every* feature its enricher reader declares, even when the underlying
data is missing (defaults to 0.0). Coverage then tracks producer liveness
rather than the statistical sufficiency of any single rolling window.

These tests exercise the pure `_*State.compute(...)` paths so they run
without Redis / network.
"""
from __future__ import annotations


def test_queue_dynamics_emits_full_p1_p2_schema():
    from services.queue_dynamics_producer import _QueueState

    s = _QueueState()
    feats = s.compute()
    expected = {
        # P1 Group B
        "queue_depletion_rate_l1",
        "queue_refill_rate_l1",
        "adverse_selection_1s_bps",
        "post_fill_reversion_prob",
        "limit_vs_market_entry_edge_bps",
        # P2 Group B
        "queue_depletion_rate_l5",
        "queue_refill_rate_l5",
        "queue_position_risk_score",
        "adverse_selection_3s_bps",
        "fill_or_kill_edge_bps",
    }
    missing = expected - set(feats)
    assert missing == set(), f"queue_dynamics missing features: {missing}"
    for k in expected:
        assert feats[k] == 0.0, f"{k} should default to 0.0 with empty state, got {feats[k]}"


def test_cost_dynamics_emits_full_p1_p2_schema():
    from services.cost_dynamics_producer import _CostState

    s = _CostState()
    feats = s.compute(window_s=10.0, funding_bps_per_8h=0.0)
    expected = {
        # P1 Group D
        "cost_widening_5s_bps",
        # P2 Group C — cost decomposition
        "ev_after_fee_bps",
        "ev_after_spread_bps",
        "ev_after_impact_bps",
        "tp1_net_after_cost_bps",
        "sl_net_after_cost_bps",
        "expected_hold_cost_bps",
        "cost_regime_z",
    }
    missing = expected - set(feats)
    assert missing == set(), f"cost_dynamics missing features: {missing}"


def test_cost_dynamics_decomposition_with_data():
    """With a few observations, the producer must emit non-zero
    decomposition components reflecting half-spread + fee + impact."""
    from services.cost_dynamics_producer import (
        _CostState,
        _DEFAULT_FEE_BPS,
        _DEFAULT_IMPACT_BPS,
    )

    s = _CostState()
    for t, sp in [(0.0, 2.0), (1.0, 2.4), (2.0, 2.8), (3.0, 3.1)]:
        s.observe(sp, t)
    feats = s.compute(window_s=10.0, funding_bps_per_8h=4.0)
    # Half-spread ≈ 3.1 / 2 = 1.55
    assert feats["ev_after_spread_bps"] == -1.55
    assert feats["ev_after_fee_bps"] == -_DEFAULT_FEE_BPS
    assert feats["ev_after_impact_bps"] == -_DEFAULT_IMPACT_BPS
    # Funding cost positive when funding_bps_per_8h supplied
    assert feats["expected_hold_cost_bps"] > 0
    # tp1/sl net = R-target - sum(costs)
    assert "tp1_net_after_cost_bps" in feats
    assert "sl_net_after_cost_bps" in feats


def test_regime_transition_emits_full_p1_p2_schema():
    from services.regime_transition_producer import _RegimeState

    s = _RegimeState()
    feats = s.compute(now_ms=10_000)
    expected = {
        # P1 Group E
        "regime_transition_code",
        "failed_breakout_count_30m",
        # P2 Group D
        "regime_transition_age_ms",
        "trend_to_chop_prob",
        "chop_to_expansion_prob",
        "expansion_exhaustion_score",
        "range_break_attempt_count_30m",
        "vol_ofi_regime_agree",
        "vol_price_divergence_score",
    }
    missing = expected - set(feats)
    assert missing == set(), f"regime_transition missing features: {missing}"
    for k in expected:
        assert feats[k] == 0.0, f"{k} should default to 0.0 with empty state, got {feats[k]}"


def test_cross_venue_emits_full_p1_p2_schema():
    from services.cross_venue_health_producer import _CrossVenueState

    s = _CrossVenueState()
    feats = s.compute(bybit_price=0.0, bybit_ts_ms=0)
    expected = {
        # P1 Group G
        "cross_venue_lead_lag_ms",
        "venue_consensus_persistence_3s",
        # P2 Group E
        "bybit_book_age_ms",
        "bybit_trade_rate_hz",
        "cross_venue_latency_diff_ms",
        "binance_leads_bybit_score",
        "bybit_leads_binance_score",
        "venue_consensus_flip_count_10s",
        "cross_venue_spread_diff_bps",
    }
    missing = expected - set(feats)
    assert missing == set(), f"cross_venue missing features: {missing}"
    for k in expected:
        assert feats[k] == 0.0, f"{k} should default to 0.0 with empty state, got {feats[k]}"


def test_watchlist_groups_fully_covered_by_producers():
    """Cross-check: every P1/P2 group in the shadow watchlist (except
    src_health and p1_f legacy liq keys, which have separate origins)
    must be fully covered by one of the four producers above."""
    from core.v15_of_shadow_watchlist_v1 import SHADOW_WATCHLIST_GROUPS
    from services.queue_dynamics_producer import _QueueState
    from services.cost_dynamics_producer import _CostState
    from services.regime_transition_producer import _RegimeState
    from services.cross_venue_health_producer import _CrossVenueState

    producer_feats: set[str] = set()
    producer_feats |= set(_QueueState().compute().keys())
    producer_feats |= set(_CostState().compute(window_s=10.0).keys())
    producer_feats |= set(_RegimeState().compute(now_ms=0).keys())
    producer_feats |= set(_CrossVenueState().compute(bybit_price=0.0, bybit_ts_ms=0).keys())

    groups_to_cover = {
        "p1_b_queue_adverse",
        "p1_c_exec_ev",            # ev_after_slippage_bps / net_edge_to_cost_ratio — TODO (separate producer)
        "p1_d_cost_dynamics",
        "p1_e_regime_transitions",
        "p1_g_cross_venue_quality",
        "p2_b_queue_l5_adverse",
        "p2_c_cost_decomposition",
        "p2_d_regime_dynamics",
        "p2_e_cross_venue_extended",
    }
    for g in groups_to_cover:
        ks = set(SHADOW_WATCHLIST_GROUPS[g])
        # p1_c_exec_ev needs a different producer; skip until it lands.
        if g == "p1_c_exec_ev":
            continue
        missing = ks - producer_feats
        assert missing == set(), (
            f"group {g!r}: {len(missing)} declared features not emitted by any producer: {missing}"
        )
