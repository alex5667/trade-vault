import math


def test_finite_or_handles_none():
    from handlers.crypto_orderflow_handler import finite_or
    assert finite_or(None, -1.0) == -1.0


def test_finite_or_handles_float():
    from handlers.crypto_orderflow_handler import finite_or
    assert finite_or(1.25, -1.0) == 1.25


def test_finite_or_handles_nan():
    from handlers.crypto_orderflow_handler import finite_or
    assert finite_or(float("nan"), -1.0) == -1.0
