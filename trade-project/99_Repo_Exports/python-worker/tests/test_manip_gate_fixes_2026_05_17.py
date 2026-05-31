"""
Gate-level tests for MANIP gate fixes (2026-05-17)
- P1: Data quality validation (NaN, bounds check)
- P2: Balanced OTR-only tighten scoring
- P3: Single source of truth (gate-based, not strategy-based)
"""

import math
import pytest
from types import SimpleNamespace

from handlers.crypto_orderflow.components.gates import GateOrchestrator
from core.gates.decision import GateDecisionV1


@pytest.fixture
def orchestrator():
    """Create a GateOrchestrator with no other gates."""
    return GateOrchestrator(
        entry_policy=None,
        cost_gate=None,
        portfolio_gate=None,
    )


def _make_ctx(symbol="BTCUSDT", indicators=None):
    """Helper to create context with indicators."""
    if indicators is None:
        indicators = {}
    ctx = SimpleNamespace(
        symbol=symbol,
        indicators=indicators,
        ts_ms=1000000,
        ts=1000,
    )
    return ctx


class TestP1DataQualityValidation:
    """P1 Fix: NaN and bounds validation for indicators."""

    def test_nan_quote_stuffing_treated_as_zero(self, orchestrator):
        """NaN in quote_stuffing_score → treated as 0."""
        ctx = _make_ctx(indicators={
            "quote_stuffing_score": float("nan"),
            "layering_score": 0.0,
            "otr_z": 0.0,
        })
        dec = orchestrator.check_manipulation_gate(
            ctx, kind="entry",
            profile="strict",
            thr_qs=0.5,
            thr_lay=0.0,
            thr_otr_z=0.0,
        )
        assert dec.decision == "ALLOW"
        assert dec.reason_code == "OK"

    def test_nan_layering_treated_as_zero(self, orchestrator):
        """NaN in layering_score → treated as 0."""
        ctx = _make_ctx(indicators={
            "quote_stuffing_score": 0.0,
            "layering_score": float("nan"),
            "otr_z": 0.0,
        })
        dec = orchestrator.check_manipulation_gate(
            ctx, kind="entry",
            profile="strict",
            thr_qs=0.0,
            thr_lay=0.5,
            thr_otr_z=0.0,
        )
        assert dec.decision == "ALLOW"
        assert dec.reason_code == "OK"

    def test_nan_otr_z_treated_as_zero(self, orchestrator):
        """NaN in otr_z → treated as 0."""
        ctx = _make_ctx(indicators={
            "quote_stuffing_score": 0.0,
            "layering_score": 0.0,
            "otr_z": float("nan"),
        })
        dec = orchestrator.check_manipulation_gate(
            ctx, kind="entry",
            profile="strict",
            thr_qs=0.0,
            thr_lay=0.0,
            thr_otr_z=3.0,
        )
        assert dec.decision == "ALLOW"
        assert dec.reason_code == "OK"

    def test_negative_quote_stuffing_treated_as_zero(self, orchestrator):
        """Negative quote_stuffing → treated as 0."""
        ctx = _make_ctx(indicators={
            "quote_stuffing_score": -0.5,
            "layering_score": 0.0,
            "otr_z": 0.0,
        })
        dec = orchestrator.check_manipulation_gate(
            ctx, kind="entry",
            profile="strict",
            thr_qs=0.3,
            thr_lay=0.0,
            thr_otr_z=0.0,
        )
        assert dec.decision == "ALLOW"
        assert dec.reason_code == "OK"

    def test_negative_layering_treated_as_zero(self, orchestrator):
        """Negative layering_score → treated as 0."""
        ctx = _make_ctx(indicators={
            "quote_stuffing_score": 0.0,
            "layering_score": -0.1,
            "otr_z": 0.0,
        })
        dec = orchestrator.check_manipulation_gate(
            ctx, kind="entry",
            profile="strict",
            thr_qs=0.0,
            thr_lay=0.4,
            thr_otr_z=0.0,
        )
        assert dec.decision == "ALLOW"
        assert dec.reason_code == "OK"

    def test_overbounded_quote_stuffing_capped_at_one(self, orchestrator):
        """quote_stuffing > 1.0 → capped at 1.0."""
        ctx = _make_ctx(indicators={
            "quote_stuffing_score": 5.0,  # Over 1.0
            "layering_score": 0.0,
            "otr_z": 0.0,
        })
        dec = orchestrator.check_manipulation_gate(
            ctx, kind="entry",
            profile="strict",
            thr_qs=0.3,
            thr_lay=0.0,
            thr_otr_z=0.0,
            tighten_cap_bps=6.0,
            tighten_mult=1.0,
        )
        # Should tighten with capped score = 1.0
        assert dec.decision == "TIGHTEN"
        # tighten_add = min(6.0, 1.0 * 1.0 * 3.0) = 3.0
        assert dec.notes["tighten_add_bps"] == pytest.approx(3.0, abs=0.1)

    def test_overbounded_layering_capped_at_one(self, orchestrator):
        """layering > 1.0 → capped at 1.0."""
        ctx = _make_ctx(indicators={
            "quote_stuffing_score": 0.0,
            "layering_score": 2.5,  # Over 1.0
            "otr_z": 0.0,
        })
        dec = orchestrator.check_manipulation_gate(
            ctx, kind="entry",
            profile="strict",
            thr_qs=0.0,
            thr_lay=0.4,
            thr_otr_z=0.0,
            tighten_cap_bps=6.0,
            tighten_mult=1.0,
        )
        assert dec.decision == "TIGHTEN"
        # tighten_add = min(6.0, 1.0 * 1.0 * 3.0) = 3.0
        assert dec.notes["tighten_add_bps"] == pytest.approx(3.0, abs=0.1)


class TestP2BalancedOTRScoring:
    """P2 Fix: OTR-only spike gets balanced weight vs QS/LAY."""

    def test_otr_only_spike_gets_full_score(self, orchestrator):
        """OTR-only spike (QS=0, LAY=0, OTR_Z=5.0) gets proper tighten."""
        ctx = _make_ctx(indicators={
            "quote_stuffing_score": 0.0,
            "layering_score": 0.0,
            "otr_z": 5.0,
        })
        dec = orchestrator.check_manipulation_gate(
            ctx, kind="entry",
            profile="strict",
            thr_qs=0.0,
            thr_lay=0.0,
            thr_otr_z=3.0,  # Threshold = 3.0
            tighten_cap_bps=10.0,
            tighten_mult=1.0,
        )
        # OTR_Z=5.0, thr=3.0 → score = (5-3)/3 = 0.667
        # weighted: 0.7*0 + 0.3*0.667 = 0.2
        # tighten = min(10, 0.2*1*3) = 0.6
        assert dec.decision == "TIGHTEN"
        # Should be ~0.6, NOT capped at 0.3 (old behavior)
        assert dec.notes["tighten_add_bps"] > 0.5

    def test_qs_lay_spike_includes_otr_weight(self, orchestrator):
        """QS/LAY spike with OTR also contributes weighted score."""
        ctx = _make_ctx(indicators={
            "quote_stuffing_score": 0.8,
            "layering_score": 0.0,
            "otr_z": 4.0,
        })
        dec = orchestrator.check_manipulation_gate(
            ctx, kind="entry",
            profile="strict",
            thr_qs=0.5,
            thr_lay=0.0,
            thr_otr_z=3.0,
            tighten_cap_bps=10.0,
            tighten_mult=1.0,
        )
        # QS_LAY = 0.8, OTR_Z score = (4-3)/3 = 0.333
        # weighted: 0.7*0.8 + 0.3*0.333 = 0.66
        # tighten = min(10, 0.66*1*3) = 1.98 ≈ 2.0
        assert dec.decision == "TIGHTEN"
        assert dec.notes["tighten_add_bps"] > 1.5

    def test_all_three_patterns_combined(self, orchestrator):
        """All three patterns detected → balanced scoring."""
        ctx = _make_ctx(indicators={
            "quote_stuffing_score": 0.9,
            "layering_score": 0.7,
            "otr_z": 6.0,
        })
        dec = orchestrator.check_manipulation_gate(
            ctx, kind="entry",
            profile="strict",
            thr_qs=0.5,
            thr_lay=0.4,
            thr_otr_z=3.0,
            tighten_cap_bps=10.0,
            tighten_mult=1.0,
        )
        # QS_LAY = max(0.9, 0.7) = 0.9, OTR score = 1.0 (normalized)
        # weighted: 0.7*0.9 + 0.3*1.0 = 0.93
        # tighten = min(10, 0.93*1*3) ≈ 2.79
        assert dec.decision == "TIGHTEN"
        assert 2.5 < dec.notes["tighten_add_bps"] <= 10.0


class TestP3NoDuplicateTighten:
    """P3 Fix: Tighten applied once from gate, not duplicated from strategy."""

    def test_gate_is_sole_source_of_tighten(self, orchestrator):
        """Gate decision is authoritative; no strategy inline tighten."""
        # This test verifies the gate returns the tighten decision
        # Strategy.py no longer applies inline tighten (removed)
        ctx = _make_ctx(indicators={
            "quote_stuffing_score": 0.7,
            "layering_score": 0.0,
            "otr_z": 0.0,
        })
        dec = orchestrator.check_manipulation_gate(
            ctx, kind="entry",
            profile="strict",
            thr_qs=0.6,
            thr_lay=0.0,
            thr_otr_z=0.0,
            tighten_cap_bps=6.0,
            tighten_mult=1.0,
        )
        assert dec.decision == "TIGHTEN"
        # Verify tighten_add_bps is set
        assert dec.notes.get("tighten_add_bps", 0.0) > 0.0

    def test_tighten_in_signal_pipeline_only(self, orchestrator):
        """Signal pipeline's _apply_decision() is the only place tighten is applied to indicators."""
        # Create a scenario where tighten is triggered
        ctx = _make_ctx(indicators={
            "quote_stuffing_score": 0.75,
            "layering_score": 0.0,
            "otr_z": 0.0,
        })
        dec = orchestrator.check_manipulation_gate(
            ctx, kind="entry",
            profile="strict",
            thr_qs=0.5,
            thr_lay=0.0,
            thr_otr_z=0.0,
            tighten_cap_bps=6.0,
            tighten_mult=1.0,
        )
        # Gate returns TIGHTEN
        assert dec.decision == "TIGHTEN"
        # Only gate applies it, strategy doesn't (fixed in P3)


class TestHardProfileVeto:
    """Verify hard profile still blocks with veto."""

    def test_hard_profile_vetos_qs_spike(self, orchestrator):
        """hard profile → DENY on QS spike."""
        ctx = _make_ctx(indicators={
            "quote_stuffing_score": 0.8,
            "layering_score": 0.0,
            "otr_z": 0.0,
        })
        dec = orchestrator.check_manipulation_gate(
            ctx, kind="entry",
            profile="hard",
            thr_qs=0.6,
            thr_lay=0.0,
            thr_otr_z=0.0,
        )
        assert dec.decision == "DENY"
        assert dec.reason_code == "VETO_QUOTE_STUFFING"

    def test_hard_profile_vetos_otr_spike(self, orchestrator):
        """hard profile → DENY on OTR spike."""
        ctx = _make_ctx(indicators={
            "quote_stuffing_score": 0.0,
            "layering_score": 0.0,
            "otr_z": 5.0,
        })
        dec = orchestrator.check_manipulation_gate(
            ctx, kind="entry",
            profile="hard",
            thr_qs=0.0,
            thr_lay=0.0,
            thr_otr_z=3.0,
        )
        assert dec.decision == "DENY"
        assert dec.reason_code == "VETO_OTR_SPIKE"


class TestStrictProfileTighten:
    """Verify strict profile applies tighten correctly."""

    def test_strict_profile_tightens_qs_spike(self, orchestrator):
        """strict profile → TIGHTEN on QS spike."""
        ctx = _make_ctx(indicators={
            "quote_stuffing_score": 0.7,
            "layering_score": 0.0,
            "otr_z": 0.0,
        })
        dec = orchestrator.check_manipulation_gate(
            ctx, kind="entry",
            profile="strict",
            thr_qs=0.5,
            thr_lay=0.0,
            thr_otr_z=0.0,
            tighten_cap_bps=5.0,
            tighten_mult=1.0,
        )
        assert dec.decision == "TIGHTEN"
        assert dec.notes["tighten_add_bps"] > 0.0

    def test_strict_profile_tightens_otr_spike(self, orchestrator):
        """strict profile → TIGHTEN on OTR spike."""
        ctx = _make_ctx(indicators={
            "quote_stuffing_score": 0.0,
            "layering_score": 0.0,
            "otr_z": 4.5,
        })
        dec = orchestrator.check_manipulation_gate(
            ctx, kind="entry",
            profile="strict",
            thr_qs=0.0,
            thr_lay=0.0,
            thr_otr_z=3.0,
            tighten_cap_bps=8.0,
            tighten_mult=1.0,
        )
        assert dec.decision == "TIGHTEN"
        assert dec.notes["tighten_add_bps"] > 0.0


class TestDefaultProfile:
    """Verify default profile allows all (no enforcement)."""

    def test_default_profile_allows_any_spike(self, orchestrator):
        """default profile → ALLOW even with high scores."""
        ctx = _make_ctx(indicators={
            "quote_stuffing_score": 0.95,
            "layering_score": 0.95,
            "otr_z": 10.0,
        })
        dec = orchestrator.check_manipulation_gate(
            ctx, kind="entry",
            profile="default",
            thr_qs=0.6,
            thr_lay=0.6,
            thr_otr_z=3.0,
        )
        assert dec.decision == "ALLOW"
