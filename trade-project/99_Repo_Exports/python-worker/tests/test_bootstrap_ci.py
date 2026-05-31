"""Tests for core/bootstrap_ci.py.

Verifies that bootstrap CI is correctly computed for both mean diff and rate diff.
Uses deterministic seeds for reproducibility.
"""
from core.bootstrap_ci import bootstrap_mean_diff, bootstrap_rate_diff


def test_bootstrap_mean_diff_positive_ci():
    """CI for mean(a)-mean(b) where a≡1, b≡0 must be entirely positive (lo>0, hi>0)."""
    a = [1.0] * 200
    b = [0.0] * 200
    ci = bootstrap_mean_diff(a, b, n_boot=200, alpha=0.05, seed=1)
    assert ci.mean > 0, f"Expected positive mean diff, got {ci.mean}"
    assert ci.lo > 0, f"Expected positive CI lower bound, got {ci.lo}"
    assert ci.hi > 0, f"Expected positive CI upper bound, got {ci.hi}"


def test_bootstrap_rate_diff_positive_ci():
    """CI for rate(a)-rate(b) where a has 50% wins, b has 0% must be positive."""
    a = [1] * 250 + [0] * 250
    b = [0] * 500
    ci = bootstrap_rate_diff(a, b, n_boot=200, alpha=0.05, seed=2)
    assert ci.mean > 0, f"Expected positive mean rate diff, got {ci.mean}"
    assert ci.lo > 0, f"Expected positive CI lower bound for rate diff, got {ci.lo}"


def test_bootstrap_mean_diff_empty_returns_nan():
    """Empty input must produce NaN CI (not crash)."""
    import math
    ci = bootstrap_mean_diff([], [1.0, 2.0], n_boot=100, seed=42)
    assert math.isnan(ci.mean)
    assert math.isnan(ci.lo)
    assert math.isnan(ci.hi)


def test_bootstrap_mean_diff_zero_delta_wide_ci():
    """When a and b have same distribution the point estimate is near 0 (both sides of CI)."""
    import random
    rng = random.Random(99)
    vals = [rng.gauss(0, 1) for _ in range(300)]
    ci = bootstrap_mean_diff(vals, vals, n_boot=400, alpha=0.05, seed=5)
    # Observed diff should be exactly 0 (same list)
    assert abs(ci.mean) < 1e-12, f"Self-diff should be 0, got {ci.mean}"
    # CI should straddle 0 (lo < 0 < hi for symmetric distribution)
    assert ci.lo < 0 < ci.hi, f"Expected CI straddling 0, got [{ci.lo}, {ci.hi}]"


def test_bootstrap_ci_reproducibility():
    """Same seed + data must produce identical CI every time."""
    a = [float(i) for i in range(50)]
    b = [float(i) * 0.8 for i in range(50)]
    ci1 = bootstrap_mean_diff(a, b, n_boot=300, alpha=0.05, seed=77)
    ci2 = bootstrap_mean_diff(a, b, n_boot=300, alpha=0.05, seed=77)
    assert ci1.mean == ci2.mean
    assert ci1.lo == ci2.lo
    assert ci1.hi == ci2.hi
