"""
Tests for deribit_context_gate.py — DeribitContextDecision evaluation.
"""
import pytest

from services.orderflow.deribit_context_gate import (
    DeribitContextDecision,
    evaluate_deribit_context,
)


def _eval(**kwargs):
    defaults = dict(
        profile="monitor",
        side="BUY",
        vol_regime="normal",
        iv_z=0.0,
        funding_8h=0.0001,
        basis_bps=5.0,
        tighten_cap_bps=6.0,
    )
    defaults.update(kwargs)
    return evaluate_deribit_context(**defaults)  # type: ignore


# ─── No veto contract ─────────────────────────────────────────────────────────

def test_deribit_never_hard_veto_monitor():
    dec = _eval(profile="monitor", vol_regime="vol_stress", iv_z=5.0)
    assert dec.veto is False
    assert dec.veto_reason == ""


def test_deribit_never_hard_veto_strict():
    dec = _eval(profile="strict", vol_regime="vol_stress", iv_z=5.0)
    assert dec.veto is False


def test_deribit_never_hard_veto_hard_profile():
    dec = _eval(profile="hard", vol_regime="vol_stress", iv_z=10.0)
    assert dec.veto is False


# ─── Vol stress → risk reduced, tighten applied ───────────────────────────────

def test_deribit_vol_stress_reduces_risk_no_veto():
    dec = evaluate_deribit_context(
        profile="strict",
        side="BUY",
        vol_regime="vol_stress",
        iv_z=3.5,
        funding_8h=0.0002,
        basis_bps=5.0,
        tighten_cap_bps=6.0,
    )
    assert dec.veto is False
    assert dec.risk_multiplier == pytest.approx(0.60)
    assert dec.tighten_add_bps > 0
    assert "deribit_vol_stress" in dec.flags


def test_deribit_iv_extreme_triggers_stress_path():
    dec = _eval(profile="tighten", vol_regime="normal", iv_z=3.5)
    assert dec.risk_multiplier == pytest.approx(0.60)
    assert "deribit_iv_extreme" in dec.flags
    assert dec.tighten_add_bps > 0


def test_deribit_vol_stress_tighten_cap_respected():
    dec = _eval(profile="strict", vol_regime="vol_stress", iv_z=4.0, tighten_cap_bps=2.0)
    assert dec.tighten_add_bps <= 2.0


# ─── Vol expansion → moderate tighten ────────────────────────────────────────

def test_deribit_vol_expansion_tighten():
    dec = _eval(profile="tighten", vol_regime="vol_expansion", iv_z=0.5)
    assert dec.risk_multiplier == pytest.approx(0.80)
    assert dec.tighten_add_bps > 0
    assert "deribit_vol_expansion" in dec.flags


def test_deribit_iv_high_triggers_expansion_path():
    dec = _eval(profile="tighten", vol_regime="normal", iv_z=1.8)
    assert dec.risk_multiplier == pytest.approx(0.80)
    assert "deribit_iv_high" in dec.flags


def test_deribit_vol_expansion_tighten_cap_respected():
    dec = _eval(profile="strict", vol_regime="vol_expansion", tighten_cap_bps=1.5)
    assert dec.tighten_add_bps <= 1.5


# ─── Vol compression → no tighten, mark only ─────────────────────────────────

def test_deribit_vol_compression_no_tighten():
    dec = _eval(profile="strict", vol_regime="vol_compression")
    assert dec.tighten_add_bps == 0.0
    assert dec.risk_multiplier == pytest.approx(1.0)
    assert "deribit_vol_compression" in dec.flags


# ─── Monitor mode → flags but no tighten ─────────────────────────────────────

def test_deribit_monitor_mode_no_tighten():
    dec = _eval(profile="monitor", vol_regime="vol_stress", iv_z=5.0)
    assert dec.mode == "monitor"
    assert dec.tighten_add_bps == 0.0
    # risk_multiplier stays 1.0 in monitor mode
    assert dec.risk_multiplier == pytest.approx(1.0)
    # but flags are still set
    assert "deribit_vol_stress" in dec.flags


def test_deribit_soft_profile_treated_as_monitor():
    dec = _eval(profile="soft", vol_regime="vol_stress", iv_z=4.0)
    assert dec.mode == "monitor"
    assert dec.tighten_add_bps == 0.0


# ─── Normal regime → no flags, no action ─────────────────────────────────────

def test_deribit_normal_regime_no_flags():
    dec = _eval(profile="strict", vol_regime="normal", iv_z=0.3, funding_8h=0.0001, basis_bps=5.0)
    assert dec.hit is False
    assert dec.flags == []
    assert dec.tighten_add_bps == 0.0
    assert dec.risk_multiplier == pytest.approx(1.0)


# ─── Funding extreme flag ─────────────────────────────────────────────────────

def test_deribit_funding_extreme_flag():
    dec = _eval(funding_8h=0.002)
    assert "deribit_funding_extreme" in dec.flags


def test_deribit_funding_negative_extreme_flag():
    dec = _eval(funding_8h=-0.0015)
    assert "deribit_funding_extreme" in dec.flags


def test_deribit_funding_normal_no_flag():
    dec = _eval(funding_8h=0.0005)
    assert "deribit_funding_extreme" not in dec.flags


# ─── Basis wide flag ─────────────────────────────────────────────────────────

def test_deribit_basis_wide_flag():
    dec = _eval(basis_bps=25.0)
    assert "deribit_basis_wide" in dec.flags


def test_deribit_basis_negative_wide_flag():
    dec = _eval(basis_bps=-21.0)
    assert "deribit_basis_wide" in dec.flags


def test_deribit_basis_normal_no_flag():
    dec = _eval(basis_bps=10.0)
    assert "deribit_basis_wide" not in dec.flags


# ─── hit field ────────────────────────────────────────────────────────────────

def test_deribit_hit_true_when_flags():
    dec = _eval(vol_regime="vol_stress")
    assert dec.hit is True


def test_deribit_hit_false_no_flags():
    dec = _eval(vol_regime="normal", iv_z=0.0, funding_8h=0.0001, basis_bps=5.0)
    assert dec.hit is False


# ─── Unknown profile defaults to monitor ─────────────────────────────────────

def test_deribit_unknown_profile_defaults_to_monitor():
    dec = _eval(profile="garbage_value", vol_regime="vol_stress", iv_z=4.0)
    assert dec.mode == "monitor"
    assert dec.tighten_add_bps == 0.0


# ─── Decision is a dataclass (not frozen) ────────────────────────────────────

def test_deribit_decision_type():
    dec = _eval()
    assert isinstance(dec, DeribitContextDecision)
    assert isinstance(dec.flags, list)
    assert isinstance(dec.tighten_add_bps, float)
    assert isinstance(dec.risk_multiplier, float)
