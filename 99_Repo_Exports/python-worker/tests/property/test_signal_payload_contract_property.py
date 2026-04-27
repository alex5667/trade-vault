from __future__ import annotations

from types import SimpleNamespace

import pytest

try:
    hypothesis = pytest.importorskip("hypothesis")
    from hypothesis import given, settings
    from hypothesis import strategies as st
    HAS_HYPOTHESIS = True
except pytest.skip.Exception:
    # hypothesis not available, skip these tests
    hypothesis = None
    given = lambda *args, **kwargs: lambda f: f  # noop decorator
    settings = lambda *args, **kwargs: lambda f: f  # noop decorator
    class MockSt:
        def text(self, *args, **kwargs): return None
    st = MockSt()
    HAS_HYPOTHESIS = False

from common.reason_codes import code_to_u16
from signal_scoring.reason_registry import normalize_reason


@given(
    reason=st.text(min_size=0, max_size=40),
    reason_code=st.text(min_size=0, max_size=40),
)
@settings(max_examples=500, deadline=None)
def test_reason_normalization_always_produces_u16(reason, reason_code, monkeypatch):
    monkeypatch.setenv("STRICT_REASON_CODES", "0")
    r, rc, u16 = normalize_reason(reason=reason, reason_code=reason_code)
    assert isinstance(r, str)
    assert isinstance(rc, str)
    assert isinstance(u16, int)
    assert 0 <= u16 <= 65535
    # u16 must match registry function
    assert u16 == code_to_u16(rc)
