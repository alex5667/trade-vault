from __future__ import annotations

import pytest

try:
    from types import SimpleNamespace
except ImportError:
    # Python < 3.3
    class SimpleNamespace:
        def __init__(self, **kwargs):
            self.__dict__.update(kwargs)

try:
    hypothesis = pytest.importorskip("hypothesis")
    from hypothesis import HealthCheck, given, settings
    from hypothesis import strategies as st
    HAS_HYPOTHESIS = True
except pytest.skip.Exception:
    # hypothesis not available, skip these tests
    hypothesis = None
    given = lambda *args, **kwargs: lambda f: f  # noop decorator
    settings = lambda *args, **kwargs: lambda f: f  # noop decorator
    class MockSt:
        def floats(self, *args, **kwargs): return None
        def booleans(self, *args, **kwargs): return None
    st = MockSt()
    HAS_HYPOTHESIS = False

from handlers.confirmations.engine import ConfirmationsEngine


class _PassBreakout:
    def confirm(self, *, ctx, l2, level_price):
        return SimpleNamespace(veto=False, score01=0.5, flags=[], parts={})


class _PassAbsorption:
    def confirm(self, *, ctx, l2, level_price):
        return SimpleNamespace(veto=False, score01=0.5, flags=[], parts={})


@given(
    spread=st.floats(allow_nan=True, allow_infinity=True, width=32),
    l2_stale=st.booleans(),
    is_trending=st.booleans(),
)
@settings(max_examples=500, deadline=None, suppress_health_check=[HealthCheck.function_scoped_fixture])
def test_validate_never_throws_and_clamps_conf(spread, l2_stale, is_trending, monkeypatch):
    # Non-strict: must never throw even on NaN/Inf; should veto safely.
    monkeypatch.setenv("STRICT_REASON_CODES", "0")
    eng = ConfirmationsEngine(breakout=_PassBreakout(), absorption=_PassAbsorption())
    ctx = SimpleNamespace(spread_bps=spread, l2_is_stale=l2_stale, is_trending=is_trending)
    res = eng.validate(kind="breakout", ctx=ctx, l2=object(), l3=None, level_price=100.0)
    assert res.conf_factor01 >= 0.0
    assert res.conf_factor01 <= 1.0
    assert isinstance(res.reason_code, str)
    assert isinstance(res.reason_u16, int)
