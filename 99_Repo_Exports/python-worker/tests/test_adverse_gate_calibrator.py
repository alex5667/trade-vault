# -*- coding: utf-8 -*-
"""
Unit tests for core/adverse_gate_calibrator.py.
"""
import pytest
from core.adverse_gate_calibrator import (
    AdverseOutcome,
    AdverseGateCalibResult,
    evaluate_adverse_gate,
    adv_mode_to_int,
    is_adv_enable,
    is_adv_disable,
)


def _make_reversal(*, vetoed: bool, loss: bool, symbol: str = "BTCUSDT") -> AdverseOutcome:
    """Helper to create a reversal outcome."""
    return AdverseOutcome(
        symbol=symbol,
        pnl_pct=-1.0 if loss else 1.0,
        is_loss=loss,
        scenario="reversal",
        direction="LONG",
        reversal_vetoed=vetoed,
        reversal_passed=not vetoed,
        has_evidence=not vetoed,
    )


def _make_continuation(*, confirmed: bool, loss: bool, symbol: str = "BTCUSDT") -> AdverseOutcome:
    """Helper to create a continuation outcome."""
    return AdverseOutcome(
        symbol=symbol,
        pnl_pct=-1.0 if loss else 1.0,
        is_loss=loss,
        scenario="continuation",
        direction="LONG",
        continuation_confirmed=confirmed,
        continuation_rejected=not confirmed,
    )


# =========================================================================
# Test: Insufficient Data
# =========================================================================

class TestInsufficientData:

    def test_empty_outcomes(self):
        r = evaluate_adverse_gate([], symbol="BTCUSDT", min_n_total=15)
        assert r.recommend == "hold"
        assert not r.data_sufficient
        assert r.proof_streak == 0

    def test_below_min_total(self):
        outcomes = [_make_reversal(vetoed=True, loss=True) for _ in range(10)]
        r = evaluate_adverse_gate(outcomes, min_n_total=15)
        assert r.recommend == "hold"
        assert not r.data_sufficient

    def test_below_min_reversals(self):
        """Enough total but too few reversals."""
        outcomes = (
            [_make_reversal(vetoed=True, loss=True) for _ in range(3)]
            + [_make_continuation(confirmed=True, loss=False) for _ in range(15)]
        )
        r = evaluate_adverse_gate(outcomes, min_n_total=10, min_n_reversals=5)
        assert r.recommend == "hold"
        assert "insufficient_reversals" in r.reason


# =========================================================================
# Test: Precision and Streak
# =========================================================================

class TestPrecisionAndStreak:

    def test_high_precision_increments_streak(self):
        """All vetoed reversals are losses → precision=1.0, should build streak."""
        outcomes = (
            [_make_reversal(vetoed=True, loss=True) for _ in range(8)]
            + [_make_reversal(vetoed=False, loss=False) for _ in range(10)]
        )
        r = evaluate_adverse_gate(outcomes, min_n_total=10, min_n_reversals=5)
        assert r.reversal_veto_precision == 1.0
        assert r.proof_streak == 1

    def test_streak_accumulates(self):
        """Streak should carry forward from previous runs."""
        outcomes = (
            [_make_reversal(vetoed=True, loss=True) for _ in range(8)]
            + [_make_reversal(vetoed=False, loss=False) for _ in range(10)]
        )
        r = evaluate_adverse_gate(outcomes, proof_streak=2, min_n_total=10, min_n_reversals=5)
        assert r.proof_streak == 3

    def test_low_precision_resets_streak(self):
        """Low precision resets proof streak to 0."""
        outcomes = (
            [_make_reversal(vetoed=True, loss=False) for _ in range(8)]  # Wrong vetoes → precision=0
            + [_make_reversal(vetoed=False, loss=True) for _ in range(10)]
        )
        r = evaluate_adverse_gate(outcomes, proof_streak=2, min_n_total=10, min_n_reversals=5)
        assert r.proof_streak == 0


# =========================================================================
# Test: Auto-Enable
# =========================================================================

class TestAutoEnable:

    def test_enable_to_shadow(self):
        """Once streak reaches required, should recommend enable."""
        outcomes = (
            [_make_reversal(vetoed=True, loss=True) for _ in range(8)]
            + [_make_reversal(vetoed=False, loss=False) for _ in range(10)]
        )
        r = evaluate_adverse_gate(
            outcomes, proof_streak=2, proof_streak_required=3,
            min_n_total=10, min_n_reversals=5, current_mode="disabled",
        )
        assert r.recommend == "enable"
        assert r.effective_mode == "shadow"
        assert "enable_shadow" in r.reason

    def test_no_auto_enforce(self):
        """shadow → enforce must be manual (Telegram only)."""
        outcomes = (
            [_make_reversal(vetoed=True, loss=True) for _ in range(8)]
            + [_make_reversal(vetoed=False, loss=False) for _ in range(10)]
        )
        r = evaluate_adverse_gate(
            outcomes, proof_streak=5, proof_streak_required=3,
            min_n_total=10, min_n_reversals=5, current_mode="shadow",
        )
        assert r.recommend == "hold"
        assert r.effective_mode == "shadow"


# =========================================================================
# Test: Rollback (Disable)
# =========================================================================

class TestRollback:

    def test_disable_on_degraded_precision(self):
        """Precision below rollback threshold should disable."""
        outcomes = (
            [_make_reversal(vetoed=True, loss=False) for _ in range(7)]  # Wrong vetoes → low precision
            + [_make_reversal(vetoed=True, loss=True) for _ in range(1)]
            + [_make_reversal(vetoed=False, loss=False) for _ in range(10)]
        )
        r = evaluate_adverse_gate(
            outcomes, current_mode="shadow",
            rollback_streak=1, rollback_streak_required=2,
            rollback_precision=0.35, min_n_total=10, min_n_reversals=5,
        )
        assert r.recommend == "disable"
        assert r.effective_mode == "disabled"

    def test_no_disable_when_disabled(self):
        """Can't disable what's already disabled."""
        outcomes = (
            [_make_reversal(vetoed=True, loss=False) for _ in range(8)]
            + [_make_reversal(vetoed=False, loss=True) for _ in range(10)]
        )
        r = evaluate_adverse_gate(
            outcomes, current_mode="disabled",
            rollback_streak=3, rollback_streak_required=2,
            min_n_total=10, min_n_reversals=5,
        )
        assert r.recommend == "hold"  # Not "disable" because already disabled

    def test_good_window_resets_rollback_streak(self):
        """A good window resets rollback streak."""
        outcomes = (
            [_make_reversal(vetoed=True, loss=True) for _ in range(8)]
            + [_make_reversal(vetoed=False, loss=False) for _ in range(10)]
        )
        r = evaluate_adverse_gate(
            outcomes, current_mode="enforce",
            rollback_streak=1, rollback_streak_required=2,
            min_n_total=10, min_n_reversals=5,
        )
        assert r.rollback_streak == 0  # Reset because window qualifies


# =========================================================================
# Test: Edge Cases
# =========================================================================

class TestEdgeCases:

    def test_no_reversals_vetoed(self):
        """All reversals passed → veto precision undefined (0.0)."""
        outcomes = (
            [_make_reversal(vetoed=False, loss=True) for _ in range(5)]
            + [_make_reversal(vetoed=False, loss=False) for _ in range(10)]
        )
        r = evaluate_adverse_gate(outcomes, min_n_total=10, min_n_reversals=5)
        assert r.reversal_veto_precision == 0.0
        assert r.n_rev_vetoed == 0

    def test_perfect_gate(self):
        """All vetoed are losses, all passed are wins."""
        outcomes = (
            [_make_reversal(vetoed=True, loss=True) for _ in range(10)]
            + [_make_reversal(vetoed=False, loss=False) for _ in range(10)]
        )
        r = evaluate_adverse_gate(outcomes, min_n_total=10, min_n_reversals=5)
        assert r.reversal_veto_precision == 1.0
        assert r.reversal_pass_loss_rate == 0.0
        assert r.reversal_veto_lift == 1.0

    def test_continuation_metrics(self):
        """Continuation sub-gate metrics calculated correctly."""
        outcomes = (
            [_make_reversal(vetoed=True, loss=True) for _ in range(6)]
            + [_make_reversal(vetoed=False, loss=False) for _ in range(6)]
            + [_make_continuation(confirmed=True, loss=False) for _ in range(5)]
            + [_make_continuation(confirmed=False, loss=True) for _ in range(3)]
        )
        r = evaluate_adverse_gate(outcomes, min_n_total=10, min_n_reversals=5)
        assert r.n_cont_confirmed == 5
        assert r.n_cont_confirmed_win == 5
        assert r.n_cont_rejected == 3
        assert r.n_cont_rejected_loss == 3
        assert r.continuation_confirm_wr == 1.0
        assert r.continuation_reject_loss == 1.0

    def test_counts_correctness(self):
        """Verify internal counts are consistent."""
        outcomes = (
            [_make_reversal(vetoed=True, loss=True) for _ in range(4)]
            + [_make_reversal(vetoed=True, loss=False) for _ in range(2)]
            + [_make_reversal(vetoed=False, loss=True) for _ in range(3)]
            + [_make_reversal(vetoed=False, loss=False) for _ in range(6)]
        )
        r = evaluate_adverse_gate(outcomes, min_n_total=10, min_n_reversals=5)
        assert r.n_total == 15
        assert r.n_reversals == 15
        assert r.n_rev_vetoed == 6
        assert r.n_rev_vetoed_loss == 4
        assert r.n_rev_vetoed_win == 2
        assert r.n_rev_passed == 9
        assert r.n_rev_passed_loss == 3
        assert r.n_rev_passed_win == 6
        assert abs(r.reversal_veto_precision - 4 / 6) < 1e-9
        assert abs(r.reversal_pass_loss_rate - 3 / 9) < 1e-9


# =========================================================================
# Test: Mode Helpers
# =========================================================================

class TestModeHelpers:

    def test_mode_to_int(self):
        assert adv_mode_to_int("disabled") == 0
        assert adv_mode_to_int("shadow") == 1
        assert adv_mode_to_int("enforce") == 2
        assert adv_mode_to_int("unknown") == 0

    def test_is_enable(self):
        assert is_adv_enable("disabled", "shadow")
        assert is_adv_enable("shadow", "enforce")
        assert not is_adv_enable("enforce", "shadow")
        assert not is_adv_enable("shadow", "disabled")

    def test_is_disable(self):
        assert is_adv_disable("enforce", "shadow")
        assert is_adv_disable("shadow", "disabled")
        assert not is_adv_disable("disabled", "shadow")
