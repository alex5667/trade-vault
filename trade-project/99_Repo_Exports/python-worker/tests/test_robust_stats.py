import pytest

from core.robust_stats import RollingRobustZ


def test_rolling_robust_z_basic():
    stats = RollingRobustZ(window=10)
    # Fill with 10.0
    for _ in range(10):
        stats.update(10.0)

    # Median should be 10, MAD should be 0 (or very small)
    # If MAD is 0, Z should be 0 (or very large if x != median, but median_mad handles n < 8)
    # Actually n=10 >= 8, so it will calculate.
    # z = (10 - 10) / (0 + eps) = 0
    assert stats.z(10.0) == 0.0
    # z = (15 - 10) / (0 + 1e-12) = 5 / 1e-12 = 5e12
    assert stats.z(15.0) == pytest.approx(5e12)

def test_rolling_robust_z_outlier():
    # Use larger window to get stable median
    stats = RollingRobustZ(window=20)
    # provide more varied data
    data = [10, 11, 12, 13, 14, 15, 16, 17, 18, 19]
    for x in data:
        stats.update(x)

    # Median [10...19] is 14.5
    # ADs = [4.5, 3.5, 2.5, 1.5, 0.5, 0.5, 1.5, 2.5, 3.5, 4.5]
    # Sorted ADs = [0.5, 0.5, 1.5, 1.5, 2.5, 2.5, 3.5, 3.5, 4.5, 4.5]
    # MAD = 2.5

    med, mad, n = stats.median_mad()

    assert med == pytest.approx(14.5)
    assert mad == pytest.approx(2.5)
    assert n == 10

    # Z(19) = (19 - 14.5) / (1.4826 * 2.5) = 4.5 / 3.7065 = 1.214
    z19 = stats.z(19.0)
    assert z19 == pytest.approx(1.21408, abs=1e-4)

def test_rolling_robust_z_empty():
    stats = RollingRobustZ(window=10)
    assert stats.z(10.0) == 0.0
    med, mad, n = stats.median_mad()
    assert med == 0.0
    assert mad == 0.0
    assert n == 0
