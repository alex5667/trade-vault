import math
import pytest

from common.math_safe import safe_float, clamp01, safe_div, safe_bps_dist

def test_safe_float_filters_nan_inf():
    assert safe_float(float("nan")) is None
    assert safe_float(float("inf")) is None
    assert safe_float("-inf") is None
    assert safe_float("1.25") == 1.25

def test_clamp01():
    assert clamp01(-1.0) == 0.0
    assert clamp01(2.0) == 1.0
    assert clamp01(0.4) == 0.4

def test_safe_div():
    assert safe_div(1.0, 0.0, default=7.0) == 7.0
    assert safe_div("x", 2.0, default=7.0) == 7.0
    assert safe_div(10.0, 2.0) == 5.0

def test_safe_bps_dist():
    assert safe_bps_dist(101, 100, base=100) == pytest.approx(100.0)
    assert safe_bps_dist(101, 100, base=0) is None
    assert safe_bps_dist(float("nan"), 100, base=100) is None
