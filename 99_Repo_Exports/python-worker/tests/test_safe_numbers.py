import math
from common.safe_numbers import safe_float, safe_isfinite


def test_safe_float_converts_valid():
    assert safe_float(1.5) == 1.5
    assert safe_float("2.5") == 2.5
    assert safe_float(0) == 0.0


def test_safe_float_returns_default_for_invalid():
    assert math.isnan(safe_float(None))
    assert math.isnan(safe_float("invalid"))
    assert math.isnan(safe_float(float("inf")))
    assert math.isnan(safe_float(float("-inf")))


def test_safe_float_custom_default():
    assert safe_float(None, 42.0) == 42.0
    assert safe_float("bad", -1.0) == -1.0


def test_safe_isfinite_true_for_valid():
    assert safe_isfinite(1.5) is True
    assert safe_isfinite(0.0) is True
    assert safe_isfinite(-1.5) is True


def test_safe_isfinite_false_for_invalid():
    assert safe_isfinite(None) is False
    assert safe_isfinite("invalid") is False
    assert safe_isfinite(float("inf")) is False
    assert safe_isfinite(float("-inf")) is False
    assert safe_isfinite(float("nan")) is False
