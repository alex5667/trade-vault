from __future__ import annotations

import numpy as np

from ml_analysis.calibration_extended import calibration_regression, report


def _gen_probs(seed: int = 7, n: int = 4000):
    rng = np.random.default_rng(seed)
    p = rng.beta(2.5, 2.5, size=n)
    y = rng.binomial(1, p, size=n)
    return y, p


def test_perfect_calibration_has_low_errors_and_reasonable_regression():
    y, p = _gen_probs()
    rep = report(y, p, bins=20)
    assert rep["ece"] < 0.05
    assert rep["mce"] < 0.12
    assert 0.7 < rep["calibration_slope"] < 1.3
    assert abs(rep["calibration_intercept"]) < 0.2


def test_overconfident_model_has_worse_mce_than_base():
    y, p = _gen_probs()
    p_over = np.clip(np.where(p >= 0.5, p + 0.18, p - 0.18), 0.001, 0.999)
    rep_base = report(y, p, bins=20)
    rep_over = report(y, p_over, bins=20)
    assert rep_over["mce"] > rep_base["mce"]
    assert rep_over["ece"] > rep_base["ece"]


def test_underconfident_model_has_low_slope_and_more_mass_near_half():
    y, p = _gen_probs()
    p_under = 0.5 + 0.35 * (p - 0.5)
    rep = report(y, p_under, bins=20)
    assert rep["calibration_slope"] > 1.0
    assert rep["prob_mass_near_half"] > 0.2


def test_flat_probability_model_has_low_sharpness_and_high_entropy():
    y, _ = _gen_probs()
    p_flat = np.full_like(y, 0.5, dtype=float)
    rep = report(y, p_flat, bins=20)
    assert rep["sharpness_mean"] < 1e-6
    assert rep["sharpness_entropy"] > 0.95
    assert rep["prob_mass_near_half"] > 0.95


def test_calibration_regression_handles_degenerate_labels():
    y = np.zeros(20)
    p = np.full(20, 0.2)
    reg = calibration_regression(y, p)
    assert reg["calibration_slope"] == 1.0
    assert reg["calibration_intercept"] == 0.0
