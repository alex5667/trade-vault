import pytest

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import given
from hypothesis import strategies as st

from common.decision_trace import Span


@given(n=st.integers(min_value=1, max_value=50))
def test_span_ms_is_non_negative_and_dual_mode(n):
    sp = Span()
    # hot-loop simulation
    x = 0
    for i in range(n):
        x += i
    # callable mode
    v1 = sp.ms()
    assert isinstance(v1, float)
    assert v1 >= 0.0

    # float-castable attr-like mode
    v2 = float(sp.ms)
    assert isinstance(v2, float)
    assert v2 >= 0.0


def test_span_context_manager_style():
    with Span() as sp:
        _ = 1 + 1
    # both styles after exit
    assert float(sp.ms) >= 0.0
    assert sp.ms() >= 0.0
