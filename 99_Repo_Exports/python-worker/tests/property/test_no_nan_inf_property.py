import math
import pytest

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import given, strategies as st

from common.math_safe import safe_float, clamp01

@given(st.floats(allow_nan=True, allow_infinity=True, width=64))
def test_clamp01_never_nan_inf(x):
    # emulate pipeline normalization step
    v = safe_float(x, 0.0) or 0.0
    v = clamp01(v)
    assert math.isfinite(v)
    assert 0.0 <= v <= 1.0
