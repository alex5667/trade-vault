# -*- coding: utf-8 -*-
"""
Tests for Strong Gate Calibrator — core evaluation logic.

Run:
    cd python-worker && python -m pytest tests/test_strong_gate_calibrator.py -v
"""
from __future__ import annotations

import pytest

from core.strong_gate_calibrator import (
    TradeOutcome,
    StrongGateCalibResult,
    evaluate_strong_gate,
    mode_to_int,
    is_promotion,
    is_rollback,
)


# ---------------------------------------------------------------------------
# Test helpers
# ---------------------------------------------------------------------------

def _make_outcome(
    pnl: float,
    shadow_vetoed: bool = False,
    ok: bool = True,
    symbol: str = "BTCUSDT",
    scenario: str = "reversal",
) -> TradeOutcome:
    return TradeOutcome(
        symbol=symbol,
        pnl_pct=pnl,
        is_loss=pnl < 0,
        shadow_vetoed=shadow_vetoed,
        ok=ok and not shadow_vetoed,
        scenario=scenario,
    )


def _batch(n: int, pnl: float, *, vetoed: bool = False) -> list[TradeOutcome]:
    """Create n outcomes with same PnL."""
    return [_make_outcome(pnl, shadow_vetoed=vetoed, ok=not vetoed) for _ in range(n)]


# ---------------------------------------------------------------------------
# Test: Insufficient data → hold
# ---------------------------------------------------------------------------

class TestInsufficientData:
    def test_empty_outcomes(self) -> None:
        result = evaluate_strong_gate([], min_n_total=30)
        assert result.recommend == "hold"
        assert result.effective_mode == "shadow"
        assert not result.data_sufficient
        assert "insufficient_data" in result.reason

    def test_below_min_total(self) -> None:
        outcomes = _batch(10, -0.5, vetoed=True) + _batch(5, 0.3)
        result = evaluate_strong_gate(outcomes, min_n_total=30)
        assert result.recommend == "hold"
        assert "insufficient_data" in result.reason
        assert result.proof_streak == 0

    def test_below_min_vetoed(self) -> None:
        # Enough total but not enough vetoed
        outcomes = _batch(3, -0.5, vetoed=True) + _batch(40, 0.3)
        result = evaluate_strong_gate(outcomes, min_n_total=30, min_n_vetoed=10)
        assert result.recommend == "hold"
        assert "insufficient_vetoes" in result.reason


# ---------------------------------------------------------------------------
# Test: High precision builds streak
# ---------------------------------------------------------------------------

class TestPrecisionAndStreak:
    def test_high_precision_increments_streak(self) -> None:
        """Vetoes that are mostly losers = high precision → streak++."""
        outcomes = (
            _batch(15, -0.8, vetoed=True)       # 15 vetoed losers → correct vetoes
            + _batch(2, 0.3, vetoed=True)        # 2 vetoed winners → false vetoes
            + _batch(30, 0.5)                    # 30 passed winners
            + _batch(5, -0.2)                    # 5 passed losers
        )
        result = evaluate_strong_gate(
            outcomes,
            min_n_total=30,
            min_n_vetoed=10,
            min_precision=0.55,
            min_lift=0.05,
            proof_streak=0,
            proof_streak_required=3,
        )
        # precision = 15/17 ≈ 0.88, pass_loss_rate = 5/35 ≈ 0.14
        # lift = 0.88 - 0.14 ≈ 0.74 → qualifies
        assert result.data_sufficient
        assert result.veto_precision > 0.55
        assert result.veto_lift > 0.05
        assert result.proof_streak == 1
        assert result.recommend == "hold"  # streak=1, need=3

    def test_streak_accumulates(self) -> None:
        """Previous streak + new qualifying → streak increases."""
        outcomes = (
            _batch(15, -0.8, vetoed=True)
            + _batch(2, 0.3, vetoed=True)
            + _batch(30, 0.5)
            + _batch(5, -0.2)
        )
        result = evaluate_strong_gate(
            outcomes,
            proof_streak=2,
            proof_streak_required=3,
            min_n_total=30,
            min_n_vetoed=10,
        )
        assert result.proof_streak == 3  # 2 + 1 = 3
        assert result.recommend == "promote"  # streak=3 = required
        assert result.effective_mode == "shadow_enforce"

    def test_low_precision_resets_streak(self) -> None:
        """Bad window resets proof streak to 0."""
        outcomes = (
            _batch(5, -0.3, vetoed=True)       # 5 vetoed losers
            + _batch(10, 0.5, vetoed=True)     # 10 vetoed winners → bad precision
            + _batch(30, 0.5)
            + _batch(5, -0.2)
        )
        result = evaluate_strong_gate(
            outcomes,
            proof_streak=2,
            proof_streak_required=3,
            min_n_total=30,
            min_n_vetoed=10,
            min_precision=0.55,
        )
        # precision = 5/15 ≈ 0.33 < 0.55 → reset
        assert result.proof_streak == 0
        assert result.recommend == "hold"


# ---------------------------------------------------------------------------
# Test: Promotion logic
# ---------------------------------------------------------------------------

class TestPromotion:
    def test_promote_to_shadow_enforce(self) -> None:
        outcomes = (
            _batch(20, -0.8, vetoed=True)
            + _batch(3, 0.3, vetoed=True)
            + _batch(40, 0.5)
            + _batch(5, -0.2)
        )
        result = evaluate_strong_gate(
            outcomes,
            proof_streak=2,
            proof_streak_required=3,
            min_n_total=30,
            min_n_vetoed=10,
            current_mode="shadow",
        )
        assert result.recommend == "promote"
        assert result.effective_mode == "shadow_enforce"
        assert "promote_to_shadow_enforce" in result.reason

    def test_no_auto_promote_from_shadow_enforce_to_full(self) -> None:
        """shadow_enforce → full_enforce is manual (Telegram Approve only)."""
        outcomes = (
            _batch(20, -0.8, vetoed=True)
            + _batch(3, 0.3, vetoed=True)
            + _batch(40, 0.5)
            + _batch(5, -0.2)
        )
        result = evaluate_strong_gate(
            outcomes,
            proof_streak=5,
            proof_streak_required=3,
            min_n_total=30,
            min_n_vetoed=10,
            current_mode="shadow_enforce",
        )
        # Should NOT auto-promote to full_enforce
        assert result.recommend == "hold"
        assert result.effective_mode == "shadow_enforce"


# ---------------------------------------------------------------------------
# Test: Rollback logic
# ---------------------------------------------------------------------------

class TestRollback:
    def test_rollback_on_degraded_precision(self) -> None:
        """In enforce mode, degraded precision → rollback."""
        outcomes = (
            _batch(3, -0.3, vetoed=True)       # 3 vetoed losers
            + _batch(12, 0.5, vetoed=True)     # 12 vetoed winners → precision 3/15=0.2
            + _batch(30, 0.5)
            + _batch(5, -0.2)
        )
        result = evaluate_strong_gate(
            outcomes,
            rollback_precision=0.40,
            rollback_streak=1,
            rollback_streak_required=2,
            min_n_total=30,
            min_n_vetoed=10,
            current_mode="shadow_enforce",
        )
        assert result.rollback_streak == 2  # 1 + 1 = 2
        assert result.recommend == "rollback"
        assert result.effective_mode == "shadow"
        assert "rollback" in result.reason

    def test_no_rollback_in_shadow_mode(self) -> None:
        """Rollback logic doesn't apply in shadow mode."""
        outcomes = (
            _batch(3, -0.3, vetoed=True)
            + _batch(12, 0.5, vetoed=True)
            + _batch(30, 0.5)
            + _batch(5, -0.2)
        )
        result = evaluate_strong_gate(
            outcomes,
            rollback_precision=0.40,
            rollback_streak=5,
            rollback_streak_required=2,
            min_n_total=30,
            min_n_vetoed=10,
            current_mode="shadow",
        )
        assert result.recommend != "rollback"

    def test_good_window_resets_rollback_streak(self) -> None:
        """Good precision resets rollback streak."""
        outcomes = (
            _batch(15, -0.8, vetoed=True)
            + _batch(2, 0.3, vetoed=True)
            + _batch(30, 0.5)
            + _batch(5, -0.2)
        )
        result = evaluate_strong_gate(
            outcomes,
            rollback_streak=1,
            rollback_streak_required=2,
            min_n_total=30,
            min_n_vetoed=10,
            current_mode="shadow_enforce",
        )
        assert result.rollback_streak == 0  # Reset on good window


# ---------------------------------------------------------------------------
# Test: Edge cases
# ---------------------------------------------------------------------------

class TestEdgeCases:
    def test_no_vetoed_trades(self) -> None:
        """All trades passed (none vetoed) → insufficient vetoes."""
        outcomes = _batch(50, 0.3) + _batch(10, -0.2)
        result = evaluate_strong_gate(outcomes, min_n_total=30, min_n_vetoed=10)
        assert result.n_vetoed == 0
        assert "insufficient_vetoes" in result.reason

    def test_all_vetoed_are_winners(self) -> None:
        """All vetoed trades were actually winners → zero precision → bad gate."""
        outcomes = (
            _batch(15, 0.5, vetoed=True)     # All vetoed are winners → 0% precision
            + _batch(30, 0.3)
            + _batch(5, -0.2)
        )
        result = evaluate_strong_gate(outcomes, min_n_total=30, min_n_vetoed=10)
        assert result.veto_precision == 0.0
        assert result.proof_streak == 0

    def test_perfect_gate(self) -> None:
        """All vetoed = losers, all passed = winners → perfect precision + lift."""
        outcomes = (
            _batch(20, -1.0, vetoed=True)    # All vetoed are losers
            + _batch(30, 0.5)                # All passed are winners
        )
        result = evaluate_strong_gate(
            outcomes,
            proof_streak=0,
            proof_streak_required=1,
            min_n_total=30,
            min_n_vetoed=10,
        )
        assert result.veto_precision == 1.0
        assert result.pass_loss_rate == 0.0
        assert result.veto_lift == 1.0
        # With required=1, should promote immediately
        assert result.recommend == "promote"

    def test_counts_correctness(self) -> None:
        """Verify all count fields are correct."""
        outcomes = (
            _batch(10, -0.5, vetoed=True)    # 10 vetoed losers
            + _batch(5, 0.3, vetoed=True)    # 5 vetoed winners
            + _batch(20, 0.4)                # 20 passed winners
            + _batch(8, -0.1)                # 8 passed losers
        )
        result = evaluate_strong_gate(outcomes, min_n_total=30, min_n_vetoed=10)
        assert result.n_total == 43
        assert result.n_vetoed == 15
        assert result.n_vetoed_loss == 10
        assert result.n_vetoed_win == 5
        assert result.n_passed == 28
        assert result.n_passed_loss == 8
        assert result.n_passed_win == 20
        assert abs(result.veto_precision - 10 / 15) < 1e-6
        assert abs(result.pass_loss_rate - 8 / 28) < 1e-6


# ---------------------------------------------------------------------------
# Test: Mode helpers
# ---------------------------------------------------------------------------

class TestModeHelpers:
    def test_mode_to_int(self) -> None:
        assert mode_to_int("shadow") == 0
        assert mode_to_int("shadow_enforce") == 1
        assert mode_to_int("full_enforce") == 2
        assert mode_to_int("unknown") == 0

    def test_is_promotion(self) -> None:
        assert is_promotion("shadow", "shadow_enforce")
        assert is_promotion("shadow", "full_enforce")
        assert is_promotion("shadow_enforce", "full_enforce")
        assert not is_promotion("shadow_enforce", "shadow")
        assert not is_promotion("full_enforce", "shadow")

    def test_is_rollback(self) -> None:
        assert is_rollback("shadow_enforce", "shadow")
        assert is_rollback("full_enforce", "shadow")
        assert is_rollback("full_enforce", "shadow_enforce")
        assert not is_rollback("shadow", "shadow_enforce")
