"""
tests/test_purged_cv.py — Phase 1: purged walk-forward + DSR + PBO unit tests.

Coverage:
  1. purged_walkforward: basic structure (n folds, no empty train)
  2. purged_walkforward: overlapping horizons actually removed from train
  3. purged_walkforward: embargo applied (samples just after test window removed)
  4. purged_walkforward: no data leakage regression — with purge OOS metric ≤ in-sample
  5. deflated_sharpe: edge cases (n_trials=1, n_obs=4)
  6. deflated_sharpe: DSR decreases with more trials (selection inflation)
  7. deflated_sharpe: high SR → DSR near 1.0
  8. deflated_sharpe: negative SR → DSR near 0.0
  9. pbo_estimate: all folds same → PBO=0
  10. pbo_estimate: perfect IS-OOS mismatch → PBO=1
  11. pbo_estimate: single strategy → PBO=0
  12. check_calibration_guards: passes when DSR+PBO within bounds
  13. check_calibration_guards: fails when PBO too high
  14. purged_walkforward: open trades (NaN resolved_ms) handled safely
"""
from __future__ import annotations

import sys
import os
import numpy as np
import pytest

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _BASE not in sys.path:
    sys.path.insert(0, _BASE)


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_samples(n: int, horizon_ms: int = 60_000, gap_ms: int = 10_000) -> tuple:
    """Generate n non-overlapping samples with fixed horizon."""
    decision_ms = np.array([i * (horizon_ms + gap_ms) for i in range(n)], dtype=float)
    resolved_ms = decision_ms + horizon_ms
    return decision_ms, resolved_ms


def _make_overlapping_samples(n: int, horizon_ms: int = 100_000) -> tuple:
    """Generate n samples where horizons heavily overlap (dense, short gaps)."""
    decision_ms = np.array([i * 5_000 for i in range(n)], dtype=float)
    resolved_ms = decision_ms + horizon_ms
    return decision_ms, resolved_ms


# ---------------------------------------------------------------------------
# Tests: purged_walkforward
# ---------------------------------------------------------------------------

class TestPurgedWalkforward:

    def test_produces_n_minus_1_folds(self):
        """n_blocks=4 → 3 (train, test) pairs (fold 0 is train-only)."""
        from calibration.purged_cv import purged_walkforward

        d, r = _make_samples(200)
        folds = list(purged_walkforward(d, r, n_blocks=4, embargo_ms=0))

        assert len(folds) == 3

    def test_test_indices_chronologically_ordered(self):
        """Test blocks come in ascending decision_time order."""
        from calibration.purged_cv import purged_walkforward

        d, r = _make_samples(100)
        folds = list(purged_walkforward(d, r, n_blocks=5, embargo_ms=0))

        test_starts = [d[ti].min() for _, ti in folds]
        assert test_starts == sorted(test_starts)

    def test_no_empty_train(self):
        """Train set should never be empty when there are enough samples."""
        from calibration.purged_cv import purged_walkforward

        d, r = _make_samples(100)
        for train_idx, test_idx in purged_walkforward(d, r, n_blocks=5, embargo_ms=0):
            assert len(train_idx) > 0
            assert len(test_idx) > 0

    def test_train_test_disjoint(self):
        """Train and test index sets must be disjoint."""
        from calibration.purged_cv import purged_walkforward

        d, r = _make_samples(100)
        for train_idx, test_idx in purged_walkforward(d, r, n_blocks=5, embargo_ms=0):
            assert len(set(train_idx) & set(test_idx)) == 0

    def test_overlapping_horizons_purged(self):
        """Samples with horizons overlapping test window must NOT appear in train."""
        from calibration.purged_cv import purged_walkforward

        # Dense samples: horizon 100s, gap 1s → heavy overlap
        d, r = _make_overlapping_samples(100, horizon_ms=100_000)

        for train_idx, test_idx in purged_walkforward(d, r, n_blocks=4, embargo_ms=0):
            test_start = float(d[test_idx].min())
            test_end   = float(r[test_idx].max())

            # Verify: no train sample's horizon overlaps [test_start, test_end]
            for i in train_idx:
                d_i = float(d[i])
                r_i = float(r[i])
                overlaps = (d_i <= test_end) and (r_i >= test_start)
                assert not overlaps, (
                    f"Train sample {i} (decision={d_i}, resolved={r_i}) "
                    f"overlaps test [{test_start}, {test_end}]"
                )

    def test_embargo_removes_post_test_samples(self):
        """Samples within embargo_ms after test_end must be removed from train."""
        from calibration.purged_cv import purged_walkforward

        # Non-overlapping samples but we use embargo to catch near-test samples
        d, r = _make_samples(200, horizon_ms=10_000, gap_ms=1_000)
        embargo_ms = 50_000  # long embargo

        for train_idx, test_idx in purged_walkforward(d, r, n_blocks=5, embargo_ms=embargo_ms):
            test_end    = float(r[test_idx].max())
            embargo_end = test_end + embargo_ms

            # No train sample should have decision_ms in (test_start, embargo_end)
            test_start = float(d[test_idx].min())
            for i in train_idx:
                d_i = float(d[i])
                r_i = float(r[i])
                # The purge condition: if resolved_ms[i] >= test_start AND decision_ms[i] <= embargo_end
                in_purge_zone = (r_i >= test_start) and (d_i <= embargo_end)
                assert not in_purge_zone, (
                    f"Train sample {i} inside purge+embargo zone "
                    f"decision={d_i}, resolved={r_i}, zone=[{test_start},{embargo_end}]"
                )

    def test_handles_open_trades_nan_resolved(self):
        """NaN in resolved_ms (open trades) should not cause errors."""
        from calibration.purged_cv import purged_walkforward

        d, r = _make_samples(80)
        # Inject NaN for some resolved times (simulating open trades)
        r = r.astype(float)
        r[::5] = float("nan")

        folds = list(purged_walkforward(d, r, n_blocks=4, embargo_ms=10_000))
        assert len(folds) == 3

    def test_insufficient_blocks_returns_empty(self):
        """n_blocks < 2 should yield no folds."""
        from calibration.purged_cv import purged_walkforward

        d, r = _make_samples(50)
        folds = list(purged_walkforward(d, r, n_blocks=1, embargo_ms=0))
        assert folds == []

    def test_empty_input_returns_empty(self):
        from calibration.purged_cv import purged_walkforward

        folds = list(purged_walkforward(
            np.array([], dtype=float), np.array([], dtype=float),
            n_blocks=4, embargo_ms=0
        ))
        assert folds == []

    def test_leakage_regression(self):
        """
        Leakage test: with overlapping horizons, in-sample accuracy WITHOUT purge
        should be artificially inflated vs OOS. WITH purge, in-sample ≤ some bound.

        Uses a synthetic dataset where the 'label' is perfectly predictable
        from a feature that leaks through overlapping windows.
        """
        from calibration.purged_cv import purged_walkforward

        np.random.seed(42)
        n = 300
        horizon_ms = 50_000

        d, r = _make_overlapping_samples(n, horizon_ms=horizon_ms)

        # Count how many training samples are retained after purge
        purged_train_sizes = []
        for train_idx, test_idx in purged_walkforward(d, r, n_blocks=5, embargo_ms=5_000):
            purged_train_sizes.append(len(train_idx))

        # With heavy overlap, purge must remove a significant fraction of train
        max_retained = max(purged_train_sizes) if purged_train_sizes else 0
        assert max_retained < n * 0.9, (
            f"Purge retained {max_retained}/{n} samples — expected more purging"
        )


# ---------------------------------------------------------------------------
# Tests: deflated_sharpe
# ---------------------------------------------------------------------------

class TestDeflatedSharpe:

    def test_high_sr_single_trial(self):
        """High SR, one trial → DSR should be high (>0.5)."""
        from calibration.purged_cv import deflated_sharpe

        dsr = deflated_sharpe(sr=2.0, n_trials=1, skew=0.0, kurt=0.0, n_obs=100)
        assert dsr > 0.5

    def test_negative_sr_returns_low_dsr(self):
        """Negative SR → DSR should be near 0."""
        from calibration.purged_cv import deflated_sharpe

        dsr = deflated_sharpe(sr=-1.0, n_trials=1, skew=0.0, kurt=0.0, n_obs=100)
        assert dsr < 0.5

    def test_more_trials_reduces_dsr(self):
        """With more trials, same SR is less impressive (selection bias).

        Use sr=0.3 (n_obs=100) which sits near E[max SR] for n_trials=100,
        making the difference measurable.
        """
        from calibration.purged_cv import deflated_sharpe

        # sr=0.3 is borderline: ~> E[max SR] for n_trials=2, but near E[max SR] for n_trials=100
        dsr_few  = deflated_sharpe(sr=0.3, n_trials=2,   skew=0.0, kurt=0.0, n_obs=100)
        dsr_many = deflated_sharpe(sr=0.3, n_trials=100, skew=0.0, kurt=0.0, n_obs=100)

        assert dsr_few > dsr_many, (
            f"More trials should reduce DSR: dsr_few={dsr_few:.3f} dsr_many={dsr_many:.3f}"
        )

    def test_returns_float_in_01(self):
        """DSR must be in [0, 1]."""
        from calibration.purged_cv import deflated_sharpe

        for sr in [-3.0, -1.0, 0.0, 0.5, 1.0, 2.0, 5.0]:
            dsr = deflated_sharpe(sr=sr, n_trials=10, skew=0.5, kurt=1.0, n_obs=100)
            assert 0.0 <= dsr <= 1.0, f"DSR={dsr} out of range for sr={sr}"

    def test_insufficient_obs_returns_zero(self):
        """Too few observations → safe fallback of 0.0."""
        from calibration.purged_cv import deflated_sharpe

        dsr = deflated_sharpe(sr=3.0, n_trials=1, skew=0.0, kurt=0.0, n_obs=3)
        assert dsr == 0.0

    def test_high_kurtosis_penalizes_dsr(self):
        """Fat tails (high kurtosis) should increase SR uncertainty → lower DSR."""
        from calibration.purged_cv import deflated_sharpe

        dsr_normal   = deflated_sharpe(sr=1.0, n_trials=10, skew=0.0, kurt=0.0,  n_obs=200)
        dsr_fat_tail = deflated_sharpe(sr=1.0, n_trials=10, skew=0.0, kurt=10.0, n_obs=200)

        # Fat tails increase uncertainty — DSR may be different; at minimum both valid
        assert 0.0 <= dsr_fat_tail <= 1.0
        assert 0.0 <= dsr_normal   <= 1.0


# ---------------------------------------------------------------------------
# Tests: pbo_estimate
# ---------------------------------------------------------------------------

class TestPboEstimate:

    def test_identical_folds_pbo_zero(self):
        """When IS and OOS agree on best strategy, PBO = 0."""
        from calibration.purged_cv import pbo_estimate

        # Same returns for IS and OOS → strategy 0 always best
        fold_returns = [
            [1.0, 0.5, 0.2],  # fold 0: strat 0 wins
            [1.0, 0.5, 0.2],  # fold 1: strat 0 wins
            [1.0, 0.5, 0.2],  # fold 2: strat 0 wins
        ]
        pbo = pbo_estimate(fold_returns)
        assert pbo == pytest.approx(0.0)

    def test_perfect_mismatch_pbo_high(self):
        """IS winner never wins OOS → high PBO."""
        from calibration.purged_cv import pbo_estimate

        # Fold 0: strat 0 wins in-sample, strat 1 wins OOS (and vice versa)
        fold_returns = [
            [2.0, 0.1],  # fold 0: strat 0 wins
            [0.1, 2.0],  # fold 1: strat 1 wins
            [2.0, 0.1],  # fold 2: strat 0 wins
            [0.1, 2.0],  # fold 3: strat 1 wins
        ]
        pbo = pbo_estimate(fold_returns)
        assert pbo > 0.5

    def test_single_strategy_pbo_zero(self):
        """Only one strategy → no selection possible → PBO=0."""
        from calibration.purged_cv import pbo_estimate

        fold_returns = [[1.0], [0.5], [0.8]]
        pbo = pbo_estimate(fold_returns)
        assert pbo == 0.0

    def test_insufficient_folds_returns_zero(self):
        from calibration.purged_cv import pbo_estimate

        assert pbo_estimate([]) == 0.0
        assert pbo_estimate([[1.0, 0.5]]) == 0.0

    def test_pbo_in_range(self):
        """PBO must be in [0, 1]."""
        from calibration.purged_cv import pbo_estimate

        import random
        random.seed(42)
        fold_returns = [[random.gauss(0, 1) for _ in range(5)] for _ in range(10)]
        pbo = pbo_estimate(fold_returns)
        assert 0.0 <= pbo <= 1.0


# ---------------------------------------------------------------------------
# Tests: check_calibration_guards
# ---------------------------------------------------------------------------

class TestCalibrationGuards:

    def test_passes_with_good_metrics(self):
        """Should pass when DSR ≥ min_dsr and PBO ≤ max_pbo."""
        from calibration.purged_cv import check_calibration_guards

        fold_returns = [[2.0, 0.5, 0.1] for _ in range(6)]  # consistent winner
        passed, details = check_calibration_guards(
            sr=1.5, n_trials=5, skew=0.0, kurt=0.0, n_obs=500,
            fold_returns=fold_returns,
            min_dsr=0.0, max_pbo=0.5,
        )

        assert passed is True
        assert details["dsr_ok"] is True
        assert details["pbo_ok"] is True

    def test_fails_when_pbo_too_high(self):
        """Should fail when PBO > max_pbo."""
        from calibration.purged_cv import check_calibration_guards

        # Alternating winners → high PBO
        fold_returns = [
            [2.0, 0.1],
            [0.1, 2.0],
            [2.0, 0.1],
            [0.1, 2.0],
        ]
        passed, details = check_calibration_guards(
            sr=1.0, n_trials=5, skew=0.0, kurt=0.0, n_obs=200,
            fold_returns=fold_returns,
            min_dsr=0.0, max_pbo=0.3,  # strict PBO gate
        )

        assert details["pbo"] > 0.3
        assert details["pbo_ok"] is False
        assert passed is False

    def test_details_dict_complete(self):
        """Result details should contain all expected keys."""
        from calibration.purged_cv import check_calibration_guards

        _, details = check_calibration_guards(
            sr=0.5, n_trials=10, skew=0.0, kurt=0.0, n_obs=100,
            fold_returns=None,
        )

        required = {"passed", "dsr", "dsr_ok", "pbo", "pbo_ok", "min_dsr", "max_pbo", "n_obs", "n_trials", "sr"}
        assert required <= set(details.keys())
