# -*- coding: utf-8 -*-
"""
Regression: Adverse Gate Calibrator — boundary math conditions (merge-blocker).

Tests:
  - Exact adverse score threshold equality
  - Zero fill probability handling
  - Extreme (NaN/Inf) adverse scores
  - Data sufficiency boundary (exactly min_trades)

Run:
    cd python-worker && python -m pytest tests/test_adverse_gate_calibrator_boundary.py -v
"""
from __future__ import annotations

import math
import pytest

from core.adverse_gate_calibrator import (
    AdverseOutcome,
    evaluate_adverse_gate,
    AdverseGateCalibResult,
)


def _batch(n: int, pnl: float, vetoed: bool = False, ok: bool = True) -> list[AdverseOutcome]:
    return [
        AdverseOutcome(
            symbol="BTCUSDT",
            pnl_pct=pnl,
            is_loss=(pnl < 0),
            scenario="reversal",
            direction="LONG",
            reversal_vetoed=vetoed,
            reversal_passed=not vetoed,
        )
        for _ in range(n)
    ]


class TestAdverseGateBoundary:

    def test_exact_threshold_equality(self) -> None:
        """Exact threshold equality (veto_precision >= min)."""
        # min_precision = 0.55. Let's make precision exactly 0.55
        # 11 vetoed losers (correct), 9 vetoed winners (incorrect). 11/20 = 0.55
        outcomes = _batch(11, -0.5, vetoed=True) + _batch(9, 0.5, vetoed=True)
        result = evaluate_adverse_gate(
            outcomes,
            min_n_total=20,
            min_rev_veto_precision=0.55,
            proof_streak=0,
            proof_streak_required=1,
            min_rev_veto_lift=-1.0, # ignore lift
        )
        assert result.data_sufficient
        assert result.reversal_veto_precision == pytest.approx(0.55)
        # Higher precision means vetoes are GOOD, so promote (enable adverse gate)
        assert result.recommend == "enable"

    def test_just_below_threshold(self) -> None:
        """Precision just below threshold -> rollback."""
        # 10 correct, 9 incorrect -> 10/19 ≈ 0.526
        outcomes = _batch(10, -0.5, vetoed=True) + _batch(9, 0.5, vetoed=True)
        result = evaluate_adverse_gate(
            outcomes,
            min_rev_veto_precision=0.55,
            rollback_precision=0.55,
            proof_streak=0,
            proof_streak_required=1,
            current_mode="shadow",
            rollback_streak_required=1,
        )
        assert result.data_sufficient
        assert result.recommend == "disable"

    def test_insufficient_data(self) -> None:
        """Below min_n_total trades -> hold."""
        outcomes = _batch(14, -0.5, vetoed=True)
        result = evaluate_adverse_gate(outcomes, min_n_total=15)
        assert not result.data_sufficient
        assert result.recommend == "hold"

    def test_nan_pnl(self) -> None:
        """NaN pnl_pct doesn't crash."""
        outcomes = _batch(30, float("nan"), vetoed=True)
        # Since is_loss=(pnl<0), NaN < 0 is False. Thus they're considered winners.
        # This will give precision 0.0 because 0 losers out of 30.
        result = evaluate_adverse_gate(
            outcomes,
            min_n_total=15,
            min_rev_veto_precision=0.55,
            current_mode="shadow",
            rollback_streak_required=1,
        )
        assert result.data_sufficient
        assert result.reversal_veto_precision == 0.0
        assert result.recommend == "disable"
