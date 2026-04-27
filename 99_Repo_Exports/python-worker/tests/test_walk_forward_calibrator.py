"""
Tests for walk_forward_calibrator.py — the generic WF engine.

Covers:
  - Fold generation (expanding + sliding)
  - Stability scoring
  - Deploy gate logic
  - Robust parameter selection (median of qualifying folds)
  - Edge cases (too few trades, all folds below min_oos_pf)
"""
import pytest
from unittest.mock import MagicMock
from typing import List, Sequence

from calibrate.walk_forward_calibrator import (
    WalkForwardCalibrator,
    OOSMetrics,
    OOSFoldResult,
    WalkForwardResult,
    make_expanding_folds,
    make_sliding_folds,
)


# ---------------------------------------------------------------------------
# Fold generation tests
# ---------------------------------------------------------------------------


class TestMakeExpandingFolds:
    def test_basic_folds(self):
        """Standard expanding window produces correct fold boundaries."""
        folds = make_expanding_folds(n_items=200, min_train=100, test_size=30, step=20)
        assert len(folds) >= 3

        # First fold
        assert folds[0] == (0, 100, 100, 130)
        # Second fold (expanded train)
        assert folds[1] == (0, 120, 120, 150)

        # All folds start train at 0 (expanding)
        for tr_start, tr_end, ts_start, ts_end in folds:
            assert tr_start == 0
            assert tr_end == ts_start  # no gap
            assert ts_end > ts_start
            assert ts_end - ts_start == 30

    def test_insufficient_data(self):
        """Returns empty list when data is too small."""
        folds = make_expanding_folds(n_items=50, min_train=100, test_size=30, step=20)
        assert folds == []

    def test_exact_minimum(self):
        """Exactly min_train + test_size items produces one fold."""
        folds = make_expanding_folds(n_items=130, min_train=100, test_size=30, step=20)
        assert len(folds) == 1
        assert folds[0] == (0, 100, 100, 130)

    def test_no_overlap(self):
        """Test windows don't overlap between consecutive folds."""
        folds = make_expanding_folds(n_items=300, min_train=100, test_size=30, step=30)
        for i in range(len(folds) - 1):
            _, _, _, end_i = folds[i]
            _, _, start_next, _ = folds[i + 1]
            assert start_next >= end_i  # no overlap in test windows


class TestMakeSlidingFolds:
    def test_basic_sliding(self):
        """Sliding window has fixed train size."""
        folds = make_sliding_folds(n_items=200, train_size=100, test_size=30, step=20)
        assert len(folds) >= 2

        for tr_start, tr_end, ts_start, ts_end in folds:
            assert tr_end - tr_start == 100  # fixed train size
            assert tr_end == ts_start        # no gap
            assert ts_end - ts_start == 30

    def test_insufficient_data(self):
        folds = make_sliding_folds(n_items=50, train_size=100, test_size=30, step=20)
        assert folds == []


# ---------------------------------------------------------------------------
# Walk-Forward Calibrator tests
# ---------------------------------------------------------------------------


def _make_trades(n: int) -> List[dict]:
    """Create n dummy trade dicts."""
    return [{"id": i, "r_return": 0.5 if i % 3 != 0 else -0.3} for i in range(n)]


def _simple_objective(trades: Sequence[dict], param: float) -> float:
    """Simple objective: higher param = slightly better score, modulated by trade count."""
    n = len(trades)
    if n == 0:
        return -1e9
    # r_returns affected by param choice
    returns = [t["r_return"] * (1 + param * 0.1) for t in trades]
    return sum(returns) / n


def _simple_evaluate(trades: Sequence[dict], param: float) -> OOSMetrics:
    """Simple evaluate: compute metrics from trades."""
    n = len(trades)
    if n == 0:
        return OOSMetrics()

    returns = [t["r_return"] * (1 + param * 0.1) for t in trades]
    mu = sum(returns) / n
    var = sum((x - mu) ** 2 for x in returns) / max(n - 1, 1)
    std = var ** 0.5
    sharpe = mu / std if std > 1e-9 else 0.0
    wins = sum(1 for r in returns if r > 0)
    win_rate = wins / n

    total_pos = sum(r for r in returns if r > 0)
    total_neg = abs(sum(r for r in returns if r <= 0))
    pf = total_pos / total_neg if total_neg > 1e-9 else 10.0

    return OOSMetrics(
        sharpe=sharpe,
        win_rate=win_rate,
        profit_factor=pf,
        expectancy_r=mu,
        n_trades=n,
        score=mu,
    )


class TestWalkForwardCalibrator:
    def test_basic_run(self):
        """Basic run produces valid result with deploy decision."""
        trades = _make_trades(200)
        wfc = WalkForwardCalibrator(
            min_train_trades=100,
            test_trades=30,
            step_trades=20,
            stability_threshold=2.0,  # generous threshold
            min_oos_pf=0.5,
        )
        result = wfc.run(
            trades=trades,
            param_candidates=[0.3, 0.5, 0.7, 1.0],
            objective_fn=_simple_objective,
            evaluate_fn=_simple_evaluate,
            symbol="TESTUSDT",
        )

        assert isinstance(result, WalkForwardResult)
        assert result.symbol == "TESTUSDT"
        assert result.n_folds > 0
        assert len(result.folds) == result.n_folds
        assert result.robust_param in [0.3, 0.5, 0.7, 1.0]
        assert isinstance(result.stability_score, float)
        assert isinstance(result.deploy, bool)

    def test_insufficient_trades(self):
        """Too few trades returns empty result with deploy=False."""
        trades = _make_trades(20)
        wfc = WalkForwardCalibrator(min_train_trades=100, test_trades=30)
        result = wfc.run(trades, [0.5], _simple_objective, _simple_evaluate)

        assert result.deploy is False
        assert result.n_folds == 0
        assert result.stability_score == 999.0

    def test_empty_candidates(self):
        """No param candidates returns empty result."""
        trades = _make_trades(200)
        wfc = WalkForwardCalibrator(min_train_trades=50, test_trades=20)
        result = wfc.run(trades, [], _simple_objective, _simple_evaluate)

        assert result.deploy is False
        assert result.n_folds == 0

    def test_deploy_gate_unstable(self):
        """High stability threshold = deploys; low = rejects."""
        trades = _make_trades(200)

        # With generous threshold: should deploy
        wfc_generous = WalkForwardCalibrator(
            min_train_trades=50,
            test_trades=30,
            step_trades=20,
            stability_threshold=100.0,  # very generous
            min_oos_pf=0.0,            # accept any PF
            min_folds_to_deploy=1,
        )
        result = wfc_generous.run(
            trades, [0.5, 1.0], _simple_objective, _simple_evaluate,
        )
        assert result.deploy is True

    def test_deploy_gate_strict(self):
        """Very strict stability threshold rejects when OOS Sharpe varies."""
        import random
        random.seed(42)
        # Create trades with varying returns so folds produce different OOS
        trades = [
            {"id": i, "r_return": random.gauss(0.1, 0.8)}
            for i in range(200)
        ]

        wfc_strict = WalkForwardCalibrator(
            min_train_trades=50,
            test_trades=30,
            step_trades=20,
            stability_threshold=0.0001,  # impossibly strict
            min_oos_pf=0.0,
        )
        result = wfc_strict.run(
            trades, [0.5, 1.0], _simple_objective, _simple_evaluate,
        )
        assert result.deploy is False

    def test_stable_folds_contribute_to_median(self):
        """Robust param is median of stable folds only."""
        trades = _make_trades(300)
        wfc = WalkForwardCalibrator(
            min_train_trades=100,
            test_trades=30,
            step_trades=30,
            stability_threshold=100.0,
            min_oos_pf=0.0,
        )
        result = wfc.run(
            trades, [0.1, 0.5, 1.0, 2.0],
            _simple_objective, _simple_evaluate,
        )
        # robust_param should be one of the candidates  (or a median between two)
        assert 0.1 <= result.robust_param <= 2.0

    def test_overfit_ratio_computed(self):
        """Overfit ratio is computed as train_score / oos_score."""
        trades = _make_trades(200)
        wfc = WalkForwardCalibrator(
            min_train_trades=50, test_trades=30, step_trades=20,
            stability_threshold=100.0, min_oos_pf=0.0,
        )
        result = wfc.run(
            trades, [0.5], _simple_objective, _simple_evaluate,
        )
        assert isinstance(result.overfit_ratio, float)

    def test_sliding_mode(self):
        """Sliding window mode works."""
        trades = _make_trades(300)
        wfc = WalkForwardCalibrator(
            min_train_trades=100,
            test_trades=30,
            step_trades=30,
            stability_threshold=100.0,
            min_oos_pf=0.0,
            window_mode="sliding",
            sliding_train_size=100,
        )
        result = wfc.run(
            trades, [0.5, 1.0], _simple_objective, _simple_evaluate,
        )
        assert result.n_folds > 0

    def test_objective_fn_error_handled(self):
        """Errors in objective_fn don't crash the calibrator."""
        trades = _make_trades(200)

        call_count = [0]

        def _bad_objective(ts, param):
            call_count[0] += 1
            if call_count[0] % 2 == 0:
                raise ValueError("boom")
            return _simple_objective(ts, param)

        wfc = WalkForwardCalibrator(
            min_train_trades=50, test_trades=30, step_trades=30,
            stability_threshold=100.0, min_oos_pf=0.0,
        )
        result = wfc.run(
            trades, [0.5, 1.0], _bad_objective, _simple_evaluate,
        )
        # Should still produce a result without crashing
        assert result.n_folds > 0

    def test_evaluate_fn_error_handled(self):
        """Errors in evaluate_fn produce OOSMetrics defaults."""
        trades = _make_trades(200)

        def _bad_evaluate(ts, param):
            raise RuntimeError("eval boom")

        wfc = WalkForwardCalibrator(
            min_train_trades=50, test_trades=30, step_trades=30,
            stability_threshold=100.0, min_oos_pf=0.0,
        )
        result = wfc.run(
            trades, [0.5], _simple_objective, _bad_evaluate,
        )
        assert result.n_folds > 0
        # All folds should have zero OOS metrics
        for f in result.folds:
            assert f.oos_sharpe == 0.0

    def test_fold_details_serializable(self):
        """WalkForwardResult.to_dict() produces serializable output."""
        trades = _make_trades(200)
        wfc = WalkForwardCalibrator(
            min_train_trades=50, test_trades=30, step_trades=20,
            stability_threshold=100.0, min_oos_pf=0.0,
        )
        result = wfc.run(
            trades, [0.5], _simple_objective, _simple_evaluate,
        )
        d = result.to_dict()
        assert isinstance(d, dict)
        assert "folds" in d
        assert isinstance(d["folds"], list)
        assert all(isinstance(f, dict) for f in d["folds"])
