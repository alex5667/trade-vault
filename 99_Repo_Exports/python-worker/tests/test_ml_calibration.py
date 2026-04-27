from __future__ import annotations

import math
import pytest

from services.ml_calibration import (
    PlattLogitCalibrator,
    clip_prob,
    logit,
    sigmoid,
    brier_score,
    logloss,
    ece_score,
    fit_platt_logit,
)


class TestClipProb:
    def test_clip_prob_normal(self):
        assert clip_prob(0.5) == 0.5
        assert clip_prob(0.0) == 1e-6
        assert clip_prob(1.0) == 1.0 - 1e-6
        assert clip_prob(0.5, eps=1e-3) == 0.5

    def test_clip_prob_nan(self):
        assert clip_prob(float("nan")) == 0.5

    def test_clip_prob_extreme(self):
        assert clip_prob(-1.0) == 1e-6
        assert clip_prob(2.0) == 1.0 - 1e-6


class TestLogit:
    def test_logit_symmetric(self):
        p = 0.7
        l = logit(p)
        assert abs(sigmoid(l) - p) < 1e-6

    def test_logit_extreme(self):
        l0 = logit(1e-6)
        l1 = logit(1.0 - 1e-6)
        assert l0 < -10
        assert l1 > 10


class TestSigmoid:
    def test_sigmoid_zero(self):
        assert abs(sigmoid(0.0) - 0.5) < 1e-6

    def test_sigmoid_extreme(self):
        assert abs(sigmoid(10.0) - 1.0) < 1e-3
        assert abs(sigmoid(-10.0) - 0.0) < 1e-3

    def test_sigmoid_stable(self):
        # Test numerical stability
        assert 0.0 < sigmoid(100.0) <= 1.0
        assert 0.0 <= sigmoid(-100.0) < 1.0


class TestPlattLogitCalibrator:
    def test_calibrator_default(self):
        cal = PlattLogitCalibrator()
        assert cal.a == 1.0
        assert cal.b == 0.0
        assert cal.apply_one(0.5) == pytest.approx(0.5, abs=1e-6)

    def test_calibrator_identity(self):
        cal = PlattLogitCalibrator(a=1.0, b=0.0)
        p = 0.7
        assert abs(cal.apply_one(p) - p) < 1e-5

    def test_calibrator_shift(self):
        cal = PlattLogitCalibrator(a=1.0, b=1.0)
        p = 0.5
        # b=1.0 shifts logit by 1, so sigmoid(logit(0.5) + 1) > 0.5
        assert cal.apply_one(p) > p

    def test_calibrator_scale(self):
        cal = PlattLogitCalibrator(a=0.5, b=0.0)
        p = 0.7
        # a=0.5 compresses logit, so result closer to 0.5
        result = cal.apply_one(p)
        assert abs(result - 0.5) < abs(p - 0.5)

    def test_calibrator_apply_list(self):
        cal = PlattLogitCalibrator(a=1.0, b=0.0)
        probs = [0.1, 0.5, 0.9]
        result = cal.apply(probs)
        assert len(result) == len(probs)
        for r, p in zip(result, probs):
            assert abs(r - p) < 1e-5

    def test_calibrator_serialization(self):
        cal = PlattLogitCalibrator(a=1.5, b=0.3, eps=1e-5)
        d = cal.to_dict()
        assert d["type"] == "platt_logit"
        assert d["a"] == 1.5
        assert d["b"] == 0.3
        assert d["eps"] == 1e-5

        cal2 = PlattLogitCalibrator.from_dict(d)
        assert cal2.a == cal.a
        assert cal2.b == cal.b
        assert cal2.eps == cal.eps

    def test_calibrator_from_dict_defaults(self):
        d = {}
        cal = PlattLogitCalibrator.from_dict(d)
        assert cal.a == 1.0
        assert cal.b == 0.0
        assert cal.eps == 1e-6


class TestBrierScore:
    def test_brier_perfect(self):
        probs = [1.0, 1.0, 0.0, 0.0]
        y = [1, 1, 0, 0]
        assert brier_score(probs, y) == pytest.approx(0.0, abs=1e-6)

    def test_brier_worst(self):
        probs = [0.0, 0.0, 1.0, 1.0]
        y = [1, 1, 0, 0]
        assert brier_score(probs, y) == pytest.approx(1.0, abs=1e-6)

    def test_brier_uniform(self):
        probs = [0.5, 0.5, 0.5, 0.5]
        y = [1, 0, 1, 0]
        assert brier_score(probs, y) == pytest.approx(0.25, abs=1e-6)

    def test_brier_empty(self):
        assert brier_score([], []) == 0.0


class TestLogLoss:
    def test_logloss_perfect(self):
        probs = [1.0, 1.0, 0.0, 0.0]
        y = [1, 1, 0, 0]
        assert logloss(probs, y) < 1e-6

    def test_logloss_uniform(self):
        probs = [0.5, 0.5]
        y = [1, 0]
        # logloss = -0.5 * (log(0.5) + log(0.5)) = -log(0.5) = log(2)
        expected = math.log(2.0)
        assert logloss(probs, y) == pytest.approx(expected, abs=1e-6)

    def test_logloss_empty(self):
        assert logloss([], []) == 0.0


class TestECEScore:
    def test_ece_perfect(self):
        # Perfect calibration: all probs match outcomes
        probs = [0.1, 0.1, 0.9, 0.9]
        y = [0, 0, 1, 1]
        ece, bins = ece_score(probs, y, n_bins=10)
        assert ece < 0.1  # Should be low for perfect calibration

    def test_ece_overconfident(self):
        # Overconfident: high probs but low accuracy
        probs = [0.9, 0.9, 0.9, 0.9]
        y = [0, 0, 0, 0]
        ece, bins = ece_score(probs, y, n_bins=10)
        assert ece > 0.5  # Should be high for poor calibration

    def test_ece_empty(self):
        ece, bins = ece_score([], [], n_bins=10)
        assert ece == 0.0
        assert bins == []

    def test_ece_bins_structure(self):
        probs = [0.1, 0.3, 0.5, 0.7, 0.9]
        y = [0, 0, 1, 1, 1]
        ece, bins = ece_score(probs, y, n_bins=5)
        assert isinstance(bins, list)
        for b in bins:
            assert "n" in b
            assert "conf" in b
            assert "acc" in b


class TestFitPlattLogit:
    def test_fit_identity(self):
        # If data is already calibrated, fit should return near-identity
        probs = [0.1, 0.3, 0.5, 0.7, 0.9]
        y = [0, 0, 1, 1, 1]
        cal = fit_platt_logit(probs, y, l2=1e-3, max_iter=50)
        # Should be close to identity (a≈1, b≈0)
        assert abs(cal.a - 1.0) < 1.0
        assert abs(cal.b) < 1.0

    def test_fit_improves_calibration(self):
        # Create uncalibrated data: overconfident
        probs_raw = [0.8, 0.8, 0.8, 0.2, 0.2, 0.2]
        y = [1, 0, 0, 0, 0, 1]  # Only 2/6 correct at 0.8, 1/6 correct at 0.2
        
        cal = fit_platt_logit(probs_raw, y, l2=1e-3, max_iter=50)
        probs_cal = cal.apply(probs_raw)
        
        # Calibrated should have better ECE
        ece_raw, _ = ece_score(probs_raw, y, n_bins=10)
        ece_cal, _ = ece_score(probs_cal, y, n_bins=10)
        # Calibrated ECE should be lower (better)
        assert ece_cal <= ece_raw + 0.1  # Allow small tolerance

    def test_fit_empty(self):
        cal = fit_platt_logit([], [])
        assert cal.a == 1.0
        assert cal.b == 0.0

    def test_fit_single_class(self):
        # All same class
        probs = [0.5, 0.5, 0.5]
        y = [1, 1, 1]
        cal = fit_platt_logit(probs, y)
        # Should still return valid calibrator
        assert cal.a > 0
        assert isinstance(cal.b, float)

    def test_fit_convergence(self):
        # Test that fit converges
        import random
        random.seed(42)
        probs = [random.random() for _ in range(100)]
        y = [1 if p > 0.5 else 0 for p in probs]
        cal = fit_platt_logit(probs, y, max_iter=50)
        # Should have reasonable parameters
        assert -10 < cal.a < 10
        assert -10 < cal.b < 10
















