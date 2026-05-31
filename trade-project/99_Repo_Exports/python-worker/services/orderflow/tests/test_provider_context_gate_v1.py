from __future__ import annotations

"""Tests for provider_context_gate.py — pure policy evaluation."""

import pytest

from services.orderflow.provider_context_gate import evaluate_provider_context

_BASE = dict(
    profile="monitor",
    side="BUY",
    provider_quality="ok",
    mcap_disagreement_bps=0.0,
    volume_disagreement_bps=0.0,
    btc_dom_disagreement_bps=0.0,
    provider_btc_dominance=54.0,
    provider_rel_strength_24h=1.0,
    provider_top_gainer=0,
    provider_top_loser=0,
    max_disagreement_bps=100.0,
    tighten_mult=1.0,
    tighten_cap_bps=4.0,
)


def _eval(**overrides):
    return evaluate_provider_context(**{**_BASE, **overrides})


# ─── No-op cases ──────────────────────────────────────────────────────────────

def test_ok_quality_no_flags():
    dec = _eval()
    assert dec["flags"] == []
    assert dec["veto"] is False
    assert dec["tighten_add_bps"] == 0.0


def test_never_veto():
    # Even with degraded + disagreement, veto must always be False
    dec = _eval(
        profile="hard",
        provider_quality="fallback",
        mcap_disagreement_bps=500,
        volume_disagreement_bps=500,
        btc_dom_disagreement_bps=500,
    )
    assert dec["veto"] is False
    assert dec["veto_reason"] == ""


# ─── Fallback flag ────────────────────────────────────────────────────────────

def test_provider_fallback_flag():
    dec = _eval(provider_quality="fallback")
    assert "provider_fallback_active" in dec["flags"]
    # fallback alone in monitor mode → no tighten
    assert dec["tighten_add_bps"] == 0.0


def test_provider_degraded_flag():
    dec = _eval(provider_quality="degraded")
    assert "provider_data_degraded" in dec["flags"]


def test_provider_unknown_flag():
    dec = _eval(provider_quality="unknown")
    assert "provider_quality_unknown" in dec["flags"]


# ─── Disagreement tighten ─────────────────────────────────────────────────────

def test_disagreement_tightens_in_strict():
    dec = _eval(
        profile="strict",
        provider_quality="degraded",
        mcap_disagreement_bps=150,
        volume_disagreement_bps=200,
        btc_dom_disagreement_bps=20,
        max_disagreement_bps=100,
    )
    assert dec["tighten_add_bps"] > 0
    assert dec["veto"] is False


def test_disagreement_no_tighten_in_monitor():
    dec = _eval(
        profile="monitor",
        provider_quality="degraded",
        mcap_disagreement_bps=150,
        max_disagreement_bps=100,
    )
    assert dec["tighten_add_bps"] == 0.0


def test_tighten_capped():
    dec = _eval(
        profile="strict",
        provider_quality="degraded",
        mcap_disagreement_bps=999,
        volume_disagreement_bps=999,
        btc_dom_disagreement_bps=999,
        max_disagreement_bps=100,
        tighten_mult=2.0,
        tighten_cap_bps=4.0,
    )
    assert dec["tighten_add_bps"] <= 4.0


def test_disagreement_tighten_proportional():
    # 2 adverse flags → 2 * mult
    dec = _eval(
        profile="strict",
        provider_quality="ok",
        mcap_disagreement_bps=150,
        volume_disagreement_bps=150,
        btc_dom_disagreement_bps=0,
        max_disagreement_bps=100,
        tighten_mult=1.0,
        tighten_cap_bps=10.0,
    )
    assert dec["tighten_add_bps"] == pytest.approx(2.0)


# ─── Side-specific universe flags ────────────────────────────────────────────

def test_top_loser_against_long():
    dec = _eval(side="BUY", provider_top_loser=1)
    assert "symbol_top_loser_against_long" in dec["flags"]
    # informational only — no tighten in monitor mode
    assert dec["tighten_add_bps"] == 0.0


def test_top_gainer_against_short():
    dec = _eval(side="SELL", provider_top_gainer=1)
    assert "symbol_top_gainer_against_short" in dec["flags"]


def test_top_loser_long_no_tighten_in_monitor():
    dec = _eval(side="BUY", provider_top_loser=1, profile="monitor")
    assert dec["tighten_add_bps"] == 0.0
    assert dec["veto"] is False


# ─── Key test from spec ───────────────────────────────────────────────────────

def test_provider_disagreement_tightens_not_veto():
    """Exact test case from spec §5."""
    dec = evaluate_provider_context(
        profile="strict",
        side="BUY",
        provider_quality="degraded",
        mcap_disagreement_bps=150,
        volume_disagreement_bps=200,
        btc_dom_disagreement_bps=20,
        provider_btc_dominance=55,
        provider_rel_strength_24h=1.2,
        provider_top_gainer=0,
        provider_top_loser=0,
        max_disagreement_bps=100,
        tighten_mult=1.0,
        tighten_cap_bps=4.0,
    )
    assert dec["tighten_add_bps"] > 0
    assert dec["veto"] is False
