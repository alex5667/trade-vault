# -*- coding: utf-8 -*-
from __future__ import annotations
"""
Regression: Strong Gate Calibrator — boundary math conditions (merge-blocker).

Tests:
  - Exact precision threshold equality (must be >=, not >)
  - EXACT streak threshold equality
  - NaN PnL handling (should be skipped or treated as failure)
  - All-vetoed input dataset
  - Single trade dataset (far below minimums)
  - Zero required streak (immediate promotion)

Run:
    cd python-worker && python -m pytest tests/test_strong_gate_calibrator_boundary.py -v
"""

import math
import pytest

from core.strong_gate_calibrator import (
    TradeOutcome,
    evaluate_strong_gate,
)


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
        is_loss=pnl < 0 or math.isnan(pnl),
        shadow_vetoed=shadow_vetoed,
        ok=ok and not shadow_vetoed,
        scenario=scenario,
    )


def _batch(n: int, pnl: float, *, vetoed: bool = False) -> list[TradeOutcome]:
    return [_make_outcome(pnl, shadow_vetoed=vetoed, ok=not vetoed) for _ in range(n)]


class TestStrongGateBoundary:

    def test_precision_exactly_at_threshold(self) -> None:
        """Precision exactly matches min_precision → must pass (>=)."""
        # We need 10 vetoes. 6 right, 4 wrong → 6/10 = 0.60
        outcomes = (
            _batch(6, -0.5, vetoed=True)   # correct vetoes
            + _batch(4, 0.5, vetoed=True)  # incorrect vetoes
            + _batch(30, 0.5)              # enough total to pass min_n_total=40
        )
        result = evaluate_strong_gate(
            outcomes,
            min_n_total=40,
            min_n_vetoed=10,
            min_precision=0.60,
            min_lift=-1.0,  # ignore lift for this test
            proof_streak=0,
            proof_streak_required=1,
        )
        assert result.data_sufficient
        assert result.veto_precision == pytest.approx(0.60)
        assert result.recommend == "promote"

    def test_streak_exactly_at_required(self) -> None:
        """If proof_streak == required, but this batch is BAD → rollback overrides streak."""
        # Precision 0 (bad)
        outcomes = _batch(10, 0.5, vetoed=True) + _batch(30, 0.5)
        result = evaluate_strong_gate(
            outcomes,
            min_n_total=40,
            min_n_vetoed=10,
            min_precision=0.60,
            rollback_streak=10,
            rollback_streak_required=5,
            current_mode="full_enforce",
        )
        assert result.data_sufficient
        assert result.recommend == "rollback"
        assert result.proof_streak == 0  # Streak wiped out

    def test_nan_pnl_handled_safely(self) -> None:
        """NaN PnL shouldn't crash math (is_loss=True by default for fallback)."""
        outcomes = [
            _make_outcome(float("nan"), shadow_vetoed=True),
            _make_outcome(float("nan"), shadow_vetoed=False)
        ] * 20
        # 20 vetoed (loss since NaN), 20 passed (loss since NaN)
        result = evaluate_strong_gate(
            outcomes, min_n_total=20, min_n_vetoed=10, min_precision=0.5
        )
        assert result.data_sufficient
        # 20 vetoed "losers" (NaN is treated as loss) = 1.0 precision
        assert result.veto_precision == pytest.approx(1.0)

    def test_all_vetoed_dataset(self) -> None:
        """All trades in dataset were vetoed."""
        outcomes = _batch(50, -0.5, vetoed=True)
        result = evaluate_strong_gate(outcomes, min_n_total=30, min_n_vetoed=10)
        assert result.data_sufficient
        assert result.veto_precision == pytest.approx(1.0)

    def test_single_trade_dataset(self) -> None:
        """Single trade is below min_n_total, gracefully holds."""
        outcomes = _batch(1, -0.5, vetoed=True)
        result = evaluate_strong_gate(outcomes, min_n_total=30)
        assert not result.data_sufficient
        assert result.recommend == "hold"

    def test_zero_required_streak(self) -> None:
        """If required streak is 0, any passing batch immediately promotes."""
        outcomes = _batch(10, -0.5, vetoed=True) + _batch(30, 0.5)
        # Passed logic, 1.0 precision
        result = evaluate_strong_gate(
            outcomes, proof_streak=0, proof_streak_required=0,
        )
        assert result.recommend == "promote"
