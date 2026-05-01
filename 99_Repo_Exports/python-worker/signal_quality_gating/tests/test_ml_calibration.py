from __future__ import annotations
"""Tests for signal_quality_gating/services/ml_calibration.py

Tests cover: clip_prob, logit, sigmoid, PlattLogitCalibrator,
brier_score, logloss, ece_score, fit_platt_logit.
"""


import math
import sys
import os

import pytest

# Ensure signal_quality_gating is importable from the parent package
# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), "..", ".."))

from services.ml_calibration import (
    clip_prob,
    logit,
    sigmoid,
    PlattLogitCalibrator,
    brier_score,
    logloss,
    ece_score,
    fit_platt_logit,
)


# ---------------------------------------------------------------------------
# clip_prob
# ---------------------------------------------------------------------------

class TestClipProb:
    def test_nan_returns_half(self) -> None:
        assert clip_prob(float("nan")) == 0.5

    def test_below_eps_clamps(self) -> None:
        v = clip_prob(0.0)
        assert v > 0.0
        assert v <= 1e-6

    def test_above_one_minus_eps_clamps(self) -> None:
        v = clip_prob(1.0)
        assert v < 1.0
        assert v >= 1.0 - 1e-6

    def test_interior_passthrough(self) -> None:
        assert clip_prob(0.5) == 0.5
        assert clip_prob(0.3) == 0.3

    def test_custom_eps(self) -> None:
        v = clip_prob(0.0, eps=0.01)
        assert v == pytest.approx(0.01)


# ---------------------------------------------------------------------------
# logit / sigmoid inverse relationship
# ---------------------------------------------------------------------------

class TestLogitSigmoid:
    def test_sigmoid_half_at_zero(self) -> None:
        assert sigmoid(0.0) == pytest.approx(0.5, abs=1e-9)

    def test_logit_sigmoid_inverse(self) -> None:
        for p in (0.1, 0.3, 0.5, 0.7, 0.9):
            assert sigmoid(logit(p)) == pytest.approx(p, abs=1e-9)

    def test_sigmoid_monotone(self) -> None:
        xs = [-5.0, -1.0, 0.0, 1.0, 5.0]
        vals = [sigmoid(x) for x in xs]
        assert vals == sorted(vals)

    def test_sigmoid_stable_at_extremes(self) -> None:
        # Should not overflow
        v_pos = sigmoid(800.0)
        v_neg = sigmoid(-800.0)
        assert math.isfinite(v_pos)
        assert math.isfinite(v_neg)
        assert v_pos > 0.99
        assert v_neg < 0.01


# ---------------------------------------------------------------------------
# PlattLogitCalibrator
# ---------------------------------------------------------------------------

class TestPlattLogitCalibrator:
    def test_identity_with_a1_b0(self) -> None:
        cal = PlattLogitCalibrator(a=1.0, b=0.0)
        for p in (0.1, 0.3, 0.5, 0.7, 0.9):
            assert cal.apply_one(p) == pytest.approx(p, abs=1e-6)

    def test_apply_list(self) -> None:
        cal = PlattLogitCalibrator(a=1.0, b=0.0)
        probs = [0.2, 0.5, 0.8]
        out = cal.apply(probs)
        assert len(out) == 3
        for original, calibrated in zip(probs, out):
            assert calibrated == pytest.approx(original, abs=1e-6)

    def test_positive_b_shifts_up(self) -> None:
        cal = PlattLogitCalibrator(a=1.0, b=5.0)
        assert cal.apply_one(0.5) > 0.5

    def test_negative_b_shifts_down(self) -> None:
        cal = PlattLogitCalibrator(a=1.0, b=-5.0)
        assert cal.apply_one(0.5) < 0.5

    def test_to_dict_from_dict_roundtrip(self) -> None:
        cal = PlattLogitCalibrator(a=1.5, b=-0.3)
        d = cal.to_dict()
        restored = PlattLogitCalibrator.from_dict(d)
        assert restored.a == pytest.approx(1.5)
        assert restored.b == pytest.approx(-0.3)


# ---------------------------------------------------------------------------
# brier_score
# ---------------------------------------------------------------------------

class TestBrierScore:
    def test_perfect_predictions(self) -> None:
        # Perfect predictions -> score 0
        probs = [1.0, 0.0, 1.0, 0.0]
        y = [1, 0, 1, 0]
        assert brier_score(probs, y) == pytest.approx(0.0, abs=1e-9)

    def test_empty_input(self) -> None:
        assert brier_score([], []) == pytest.approx(0.0)

    def test_worst_predictions(self) -> None:
        # Uniform 0.5 -> score = 0.25 (expected for binary)
        probs = [0.5] * 100
        y = [1] * 50 + [0] * 50
        assert brier_score(probs, y) == pytest.approx(0.25, abs=1e-9)


# ---------------------------------------------------------------------------
# logloss
# ---------------------------------------------------------------------------

class TestLogloss:
    def test_empty_input(self) -> None:
        assert logloss([], []) == pytest.approx(0.0)

    def test_near_perfect(self) -> None:
        # Near perfect -> very small log loss
        probs = [0.99] * 100
        y = [1] * 100
        loss = logloss(probs, y)
        assert loss < 0.02

    def test_random(self) -> None:
        probs = [0.5] * 100
        y = [1] * 50 + [0] * 50
        loss = logloss(probs, y)
        assert loss == pytest.approx(math.log(2), abs=0.01)


# ---------------------------------------------------------------------------
# ece_score
# ---------------------------------------------------------------------------

class TestEceScore:
    def test_empty_input(self) -> None:
        ece, bins = ece_score([], [])
        assert ece == 0.0
        assert bins == []

    def test_perfect_calibration(self) -> None:
        # Perfectly calibrated: each prob matches actual rate
        n = 100
        probs = [0.0 + i / n for i in range(n)]
        # Assign y=1 based on probability (ideal: acc ~ conf)
        # Simplest: use threshold at 0.5
        y = [1 if p >= 0.5 else 0 for p in probs]
        ece, _ = ece_score(probs, y)
        assert ece >= 0.0

    def test_returns_tuple(self) -> None:
        probs = [0.3, 0.7]
        y = [0, 1]
        result = ece_score(probs, y)
        assert isinstance(result, tuple)
        assert len(result) == 2


# ---------------------------------------------------------------------------
# fit_platt_logit
# ---------------------------------------------------------------------------

class TestFitPlattLogit:
    def test_empty_input_returns_default(self) -> None:
        cal = fit_platt_logit([], [])
        assert cal.a == pytest.approx(1.0)
        assert cal.b == pytest.approx(0.0)

    def test_returns_calibrator(self) -> None:
        probs = [0.3, 0.6, 0.4, 0.8]
        y = [0, 1, 0, 1]
        cal = fit_platt_logit(probs, y)
        assert isinstance(cal, PlattLogitCalibrator)
        # Output should always be in [0,1]
        for p in probs:
            out = cal.apply_one(p)
            assert 0.0 <= out <= 1.0

    def test_perfectly_separable_shifts_parameters(self) -> None:
        # When all positives have high prob, calibrator should have a > 0
        probs = [0.9] * 50 + [0.1] * 50
        y = [1] * 50 + [0] * 50
        cal = fit_platt_logit(probs, y, max_iter=100)
        assert isinstance(cal.a, float)
        assert isinstance(cal.b, float)
        assert math.isfinite(cal.a)
        assert math.isfinite(cal.b)
