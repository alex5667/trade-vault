"""Tests for DefiLlama context gate policy."""
import pytest
from services.orderflow.defillama_context_gate import (
    DefiLlamaContextDecision,
    evaluate_defillama_context,
)


def _eval(**overrides):
    defaults = dict(
        profile="monitor",
        side="BUY",
        stablecoin_mcap_delta_1d=0.0,
        stablecoin_mcap_delta_7d=0.0,
        btc_dominance_momentum=0.0,
        chain_tvl_delta_1d_pct=0.0,
        dex_volume_spike_z=0.0,
        fees_revenue_momentum=0.0,
        tighten_mult=1.0,
        tighten_cap_bps=4.0,
    )
    defaults.update(overrides)
    return evaluate_defillama_context(**defaults)


def test_alt_risk_on_detected():
    dec = _eval(
        stablecoin_mcap_delta_1d=100_000_000,
        stablecoin_mcap_delta_7d=500_000_000,
        btc_dominance_momentum=-0.05,
        chain_tvl_delta_1d_pct=0.3,
        dex_volume_spike_z=2.5,
        fees_revenue_momentum=1.0,
    )
    assert "alt_risk_on" in dec.flags
    assert "dex_volume_spike" in dec.flags
    assert "ecosystem_activity_up" in dec.flags
    assert dec.veto is False  # monitor mode never vetos


def test_risk_off_detected():
    dec = _eval(
        stablecoin_mcap_delta_1d=-100_000_000,
        stablecoin_mcap_delta_7d=-500_000_000,
        btc_dominance_momentum=0.05,
    )
    assert "risk_off" in dec.flags


def test_risk_off_tightens_long():
    dec = _eval(
        profile="strict",
        side="BUY",
        stablecoin_mcap_delta_1d=-100_000_000,
        stablecoin_mcap_delta_7d=-500_000_000,
        btc_dominance_momentum=0.05,
        chain_tvl_delta_1d_pct=-1.5,
    )
    assert dec.tighten_add_bps > 0
    assert dec.veto is False  # strict doesn't veto


def test_chain_tvl_down_flag():
    dec = _eval(chain_tvl_delta_1d_pct=-2.0)
    assert "chain_tvl_down" in dec.flags


def test_dex_volume_spike_flag():
    dec = _eval(dex_volume_spike_z=2.5)
    assert "dex_volume_spike" in dec.flags


def test_ecosystem_activity_up_flag():
    dec = _eval(fees_revenue_momentum=0.5)
    assert "ecosystem_activity_up" in dec.flags


def test_monitor_no_veto():
    dec = _eval(
        profile="monitor",
        stablecoin_mcap_delta_1d=-100_000_000,
        stablecoin_mcap_delta_7d=-500_000_000,
        btc_dominance_momentum=0.05,
        chain_tvl_delta_1d_pct=-2.0,
    )
    assert dec.hit is True
    assert dec.veto is False
    assert dec.tighten_add_bps == 0.0


def test_hard_multiple_flags_veto():
    dec = _eval(
        profile="hard",
        side="BUY",
        stablecoin_mcap_delta_1d=-100_000_000,
        stablecoin_mcap_delta_7d=-500_000_000,
        btc_dominance_momentum=0.05,
        chain_tvl_delta_1d_pct=-2.0,
    )
    assert dec.veto is True
    assert "defillama_ctx:" in dec.veto_reason
    assert dec.tighten_add_bps > 0


def test_no_flags_clean_pass():
    dec = _eval()
    assert dec.hit is False
    assert dec.flags == []
    assert dec.veto is False
    assert dec.tighten_add_bps == 0.0
    assert dec.risk_score == 0.0


def test_sell_side_adverse_on_alt_risk_on():
    dec = _eval(
        profile="hard",
        side="SELL",
        stablecoin_mcap_delta_1d=100_000_000,
        stablecoin_mcap_delta_7d=500_000_000,
        btc_dominance_momentum=-0.05,
        dex_volume_spike_z=2.5,
    )
    assert "alt_risk_on" in dec.flags
    assert dec.veto is True  # hard + SELL + alt_risk_on + dex_volume_spike = 2+ flags


def test_tighten_cap_respected():
    dec = _eval(
        profile="tighten",
        side="BUY",
        stablecoin_mcap_delta_1d=-100_000_000,
        stablecoin_mcap_delta_7d=-500_000_000,
        btc_dominance_momentum=0.05,
        tighten_mult=100.0,
        tighten_cap_bps=4.0,
    )
    assert dec.tighten_add_bps <= 4.0
