"""test_derivatives_context_gate_v2.py — Tests for evaluate_derivatives_context_v2."""

import pytest

from services.orderflow.derivatives_context_gate import (
    DerivativesContextDecision
    evaluate_derivatives_context_v2
)


# ─── Helpers ──────────────────────────────────────────────────────────────────

def _v2(
    profile="monitor"
    side="BUY"
    funding_rate_z=0.0
    basis_bps=0.0
    oi_accel=0
    long_short_ratio_z=0.0
    taker_buy_sell_imbalance=0.0
    liq_imbalance_z=0.0
    market_breadth_ret_24h=0.0
    leader_btc_eth_confirm=0.0
    thr_funding_z=3.0
    thr_basis_bps=10.0
    require_oi_for_veto=False
    tighten_mult=1.0
    tighten_cap_bps=8.0
) -> DerivativesContextDecision:
    return evaluate_derivatives_context_v2(
        profile=profile
        side=side
        funding_rate_z=funding_rate_z
        basis_bps=basis_bps
        oi_accel=oi_accel
        long_short_ratio_z=long_short_ratio_z
        taker_buy_sell_imbalance=taker_buy_sell_imbalance
        liq_imbalance_z=liq_imbalance_z
        market_breadth_ret_24h=market_breadth_ret_24h
        leader_btc_eth_confirm=leader_btc_eth_confirm
        thr_funding_z=thr_funding_z
        thr_basis_bps=thr_basis_bps
        require_oi_for_veto=require_oi_for_veto
        tighten_mult=tighten_mult
        tighten_cap_bps=tighten_cap_bps
    )


# ─── Baseline: clean context ───────────────────────────────────────────────────

def test_clean_context_no_flags():
    dec = _v2()
    assert not dec.hit
    assert not dec.veto
    assert dec.flags == []
    assert dec.crowding_score == 0.0


# ─── Core flags (same as v1 logic) ────────────────────────────────────────────

def test_funding_extreme_flag():
    dec = _v2(funding_rate_z=3.5, thr_funding_z=3.0)
    assert "funding_extreme" in dec.flags
    assert dec.hit


def test_basis_extreme_flag():
    dec = _v2(basis_bps=15.0, thr_basis_bps=10.0)
    assert "basis_extreme" in dec.flags
    assert dec.hit


def test_oi_accel_flag():
    dec = _v2(oi_accel=1)
    assert "oi_accel" in dec.flags
    assert dec.hit


# ─── Crowding flags (side-aware) ──────────────────────────────────────────────

def test_long_crowded_buy_side():
    dec = _v2(side="BUY", long_short_ratio_z=3.0)
    assert "long_crowded" in dec.flags


def test_long_crowded_below_threshold():
    dec = _v2(side="BUY", long_short_ratio_z=2.4)
    assert "long_crowded" not in dec.flags


def test_short_crowded_sell_side():
    dec = _v2(side="SELL", long_short_ratio_z=-3.0)
    assert "short_crowded" in dec.flags


def test_long_crowded_wrong_side():
    """BUY-side crowding should not trigger on SELL signals."""
    dec = _v2(side="SELL", long_short_ratio_z=3.0)
    assert "long_crowded" not in dec.flags


# ─── Breadth flags ────────────────────────────────────────────────────────────

def test_breadth_against_long():
    dec = _v2(side="BUY", market_breadth_ret_24h=-0.02)
    assert "breadth_against_long" in dec.flags


def test_breadth_against_short():
    dec = _v2(side="SELL", market_breadth_ret_24h=0.02)
    assert "breadth_against_short" in dec.flags


def test_breadth_within_threshold_no_flag():
    dec = _v2(side="BUY", market_breadth_ret_24h=-0.005)
    assert "breadth_against_long" not in dec.flags


def test_breadth_against_wrong_side_no_flag():
    """Falling breadth should not flag SELL signals."""
    dec = _v2(side="SELL", market_breadth_ret_24h=-0.02)
    assert "breadth_against_long" not in dec.flags


# ─── Leader divergence ────────────────────────────────────────────────────────

def test_leader_diverged_flag():
    dec = _v2(leader_btc_eth_confirm=-0.5)
    assert "leader_diverged" in dec.flags


def test_leader_positive_no_flag():
    dec = _v2(leader_btc_eth_confirm=0.5)
    assert "leader_diverged" not in dec.flags


# ─── Liquidation stress ───────────────────────────────────────────────────────

def test_liq_stress_flag_positive():
    dec = _v2(liq_imbalance_z=3.5)
    assert "liq_stress" in dec.flags


def test_liq_stress_flag_negative():
    dec = _v2(liq_imbalance_z=-3.5)
    assert "liq_stress" in dec.flags


def test_liq_stress_below_threshold():
    dec = _v2(liq_imbalance_z=2.9)
    assert "liq_stress" not in dec.flags


# ─── Profile: monitor does not tighten ────────────────────────────────────────

def test_monitor_profile_no_tighten():
    dec = _v2(profile="monitor", funding_rate_z=4.0, basis_bps=12.0)
    assert dec.mode == "monitor"
    assert dec.tighten_add_bps == 0.0
    assert not dec.veto


# ─── Profile: tighten adds bps ────────────────────────────────────────────────

def test_tighten_profile_adds_bps():
    dec = _v2(profile="strict", funding_rate_z=4.0, thr_funding_z=3.0
               tighten_mult=1.0, tighten_cap_bps=8.0)
    assert dec.mode == "tighten"
    assert dec.tighten_add_bps > 0.0
    assert dec.tighten_add_bps <= 8.0


def test_tighten_respects_cap():
    dec = _v2(profile="strict", funding_rate_z=20.0, basis_bps=50.0
               thr_funding_z=3.0, thr_basis_bps=10.0
               tighten_mult=100.0, tighten_cap_bps=5.0)
    assert dec.tighten_add_bps <= 5.0


# ─── Profile: veto ────────────────────────────────────────────────────────────

def test_veto_requires_multi_core_flags():
    """Single breadth flag must NOT trigger veto."""
    dec = _v2(profile="hard", side="BUY"
               market_breadth_ret_24h=-0.05
               require_oi_for_veto=False)
    assert "breadth_against_long" in dec.flags
    assert not dec.veto, "Single breadth flag must not veto"


def test_veto_triggers_on_two_core_flags():
    dec = _v2(profile="hard"
               funding_rate_z=4.0, basis_bps=15.0
               require_oi_for_veto=False)
    assert dec.veto
    assert dec.veto_reason.startswith("deriv_ctx:")


def test_veto_triggers_on_crowding_plus_funding():
    dec = _v2(profile="hard", side="BUY"
               funding_rate_z=4.0
               long_short_ratio_z=3.5
               require_oi_for_veto=False)
    assert dec.veto


def test_veto_with_require_oi_three_flags():
    dec = _v2(profile="hard"
               funding_rate_z=4.0, basis_bps=15.0, oi_accel=1
               require_oi_for_veto=True)
    assert dec.veto
    assert "funding_extreme" in dec.flags
    assert "basis_extreme" in dec.flags
    assert "oi_accel" in dec.flags


def test_veto_with_require_oi_missing_oi_accel():
    dec = _v2(profile="hard"
               funding_rate_z=4.0, basis_bps=15.0
               oi_accel=0
               require_oi_for_veto=True)
    assert not dec.veto


# ─── Caution propagation ─────────────────────────────────────────────────────

def test_caution_set_on_any_flag():
    dec = _v2(profile="monitor", liq_imbalance_z=3.5)
    assert dec.caution


def test_caution_not_set_on_clean():
    dec = _v2()
    assert not dec.caution


# ─── Crowding score ──────────────────────────────────────────────────────────

def test_crowding_score_counts_all_flags():
    dec = _v2(
        profile="monitor"
        side="BUY"
        funding_rate_z=4.0
        basis_bps=15.0
        oi_accel=1
        long_short_ratio_z=3.0
        market_breadth_ret_24h=-0.03
        liq_imbalance_z=3.5
    )
    assert dec.crowding_score == pytest.approx(float(len(dec.flags)))


# ─── Edge cases ───────────────────────────────────────────────────────────────

def test_zero_inputs_clean():
    dec = _v2(
        profile="hard"
        side="BUY"
        funding_rate_z=0.0
        basis_bps=0.0
        oi_accel=0
    )
    assert not dec.hit
    assert not dec.veto


def test_side_case_insensitive():
    dec_upper = _v2(side="BUY", long_short_ratio_z=3.0)
    dec_lower = _v2(side="buy", long_short_ratio_z=3.0)
    assert dec_upper.flags == dec_lower.flags


def test_none_profile_defaults_to_monitor():
    dec = _v2(profile=None, funding_rate_z=4.0)
    assert dec.mode == "monitor"
    assert not dec.veto
