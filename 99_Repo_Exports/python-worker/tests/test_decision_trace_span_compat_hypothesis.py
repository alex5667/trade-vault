from __future__ import annotations

import time

from hypothesis import given, settings, strategies as st

from common.decision_trace import Span


@settings(max_examples=200, deadline=None)
@given(sleep_ms=st.integers(min_value=0, max_value=5))
def test_span_ms_is_floatish_and_callable(sleep_ms: int):
    sp = Span()
    _ = float(sp.ms)
    _ = sp.ms()
    if sleep_ms:
        time.sleep(sleep_ms / 1000.0)
    a = float(sp.ms)
    b = sp.ms()
    assert a >= 0.0
    assert b >= 0.0


def test_span_context_manager_sets_final_ms():
    with Span() as sp:
        time.sleep(0.001)
        mid = float(sp.ms)
        assert mid >= 0.0
    end = float(sp.ms)
    assert end >= 0.0
