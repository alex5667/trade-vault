from __future__ import annotations

from common.isotonic_calibration import fit_isotonic_pav, IsotonicCalibrator, sanitize_breakpoints


def test_fit_isotonic_pav_enforces_monotonicity():
    # deliberately non-monotone empirical rates:
    # x: 0.1 -> p~0.9, x:0.2 -> p~0.1, x:0.3 -> p~0.8
    samples = [
        (0.1, 1, 1.0),
        (0.2, 0, 1.0),
        (0.3, 1, 1.0),
    ]
    cal = fit_isotonic_pav(samples)
    assert cal.x and cal.p
    # monotone p
    for i in range(len(cal.p) - 1):
        assert cal.p[i] <= cal.p[i + 1] + 1e-12


def test_predict_linear_bounds_and_interpolation():
    cal = IsotonicCalibrator(x=[0.0, 1.0], p=[0.2, 0.8], mode="linear")
    assert abs(cal.predict(-1.0) - 0.2) < 1e-12
    assert abs(cal.predict(2.0) - 0.8) < 1e-12
    mid = cal.predict(0.5)
    assert 0.2 < mid < 0.8


def test_sanitize_breakpoints_repairs_non_monotone():
    cal = sanitize_breakpoints([0.0, 1.0, 2.0], [0.6, 0.2, 0.7], mode="linear")
    assert cal is not None
    for i in range(len(cal.p) - 1):
        assert cal.p[i] <= cal.p[i + 1] + 1e-12