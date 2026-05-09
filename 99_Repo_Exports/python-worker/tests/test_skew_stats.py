import math

import pytest

from core.skew_stats import calculate_proportion_skew, normal_cdf


def test_normal_cdf():
    # Known values for standard normal distribution
    assert math.isclose(normal_cdf(0), 0.5, rel_tol=1e-7)
    assert math.isclose(normal_cdf(1.96), 0.9750021, rel_tol=1e-6)
    assert math.isclose(normal_cdf(-1.96), 0.0249979, rel_tol=1e-6)


def test_calculate_proportion_skew_no_drift():
    # Same proportions
    res = calculate_proportion_skew(1000, 0.5, 1000, 0.5)
    assert res.z_score == 0.0
    assert res.p_value == 1.0
    assert res.drift_score == 0.0
    assert not res.is_significant


def test_calculate_proportion_skew_significant():
    # 5% drift in large sample (should be significant)
    # Train: 500/1000 (0.5), Serve: 550/1000 (0.55)
    res = calculate_proportion_skew(1000, 0.5, 1000, 0.55, alpha=0.05)
    assert res.drift_score == pytest.approx(0.05)
    assert res.is_significant
    assert res.p_value < 0.05


def test_calculate_proportion_skew_insignificant():
    # Small sample, small drift (should not be significant)
    res = calculate_proportion_skew(50, 0.5, 50, 0.55, alpha=0.01)
    assert not res.is_significant
    assert res.p_value > 0.01


def test_calculate_proportion_skew_zero_variance():
    # Both 0 or both 1
    res = calculate_proportion_skew(100, 0.0, 100, 0.0)
    assert res.z_score == 0.0
    assert res.p_value == 1.0

    res = calculate_proportion_skew(100, 1.0, 100, 1.0)
    assert res.z_score == 0.0
    assert res.p_value == 1.0


def test_calculate_proportion_skew_one_zero_one_non_zero():
    # Extreme drift: Train 0, Serve 1%
    res = calculate_proportion_skew(1000, 0.0, 1000, 0.01)
    assert res.z_score > 0
    assert res.is_significant
