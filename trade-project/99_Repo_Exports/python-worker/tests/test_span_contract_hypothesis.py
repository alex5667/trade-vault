from __future__ import annotations

from hypothesis import given
from hypothesis import strategies as st

from common.decision_trace import Span


@given(st.integers(min_value=0, max_value=10))
def test_span_ms_proxy_is_floatable_and_callable(_):
    sp = Span()
    # must support both styles:
    assert float(sp.ms) >= 0.0
    assert sp.ms() >= 0.0


def test_span_context_manager_contract():
    with Span() as sp:
        # inside with: should be usable as numeric without calling
        v = float(sp.ms)
        assert v >= 0.0
