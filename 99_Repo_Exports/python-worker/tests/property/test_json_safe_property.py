from __future__ import annotations

import json
import math
import pytest

try:
    hyp = pytest.importorskip("hypothesis")
    from hypothesis import given, strategies as st
    HAS_HYPOTHESIS = True
except pytest.skip.Exception:
    # hypothesis not available, skip these tests
    hyp = None
    given = lambda *args, **kwargs: lambda f: f  # noop decorator
    class MockSt:
        def one_of(self, *args, **kwargs): return None
        def recursive(self, *args, **kwargs): return None
        def floats(self, *args, **kwargs): return None
        def integers(self, *args, **kwargs): return None
        def text(self, *args, **kwargs): return None
        def binary(self, *args, **kwargs): return None
        def booleans(self, *args, **kwargs): return None
        def lists(self, *args, **kwargs): return None
        def dictionaries(self, *args, **kwargs): return None
        def tuples(self, *args, **kwargs): return None
        def sets(self, *args, **kwargs): return None
        def none(self): return None
    st = MockSt()
    HAS_HYPOTHESIS = False

from common.json_safe import to_json_safe


def _nan():
    return float("nan")


def _inf():
    return float("inf")


weird_scalars = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(),
    st.floats(allow_nan=True, allow_infinity=True, width=64),
    st.binary(),  # bytes
    st.text(max_size=50),
)


jsonish = st.recursive(
    weird_scalars,
    lambda children: st.one_of(
        st.lists(children, max_size=50),
        st.dictionaries(keys=st.one_of(st.text(max_size=20), st.integers(), st.floats(allow_nan=True, allow_infinity=True)), values=children, max_size=50),
        st.tuples(children, children),
        st.sets(children, max_size=20),
    ),
    max_leaves=300,
)


@given(jsonish)
def test_to_json_safe_always_serializable(x):
    """
    Инвариант 6.3 (усиленный):
      - любой "мусор" приводится к JSON-safe
      - стандартный json.dumps не падает
      - NaN/Inf не просачиваются (становятся None)
    """
    y = to_json_safe(x)
    s = json.dumps(y, ensure_ascii=False)
    assert isinstance(s, str)

    # Проверка отсутствия NaN/Inf в результирующем дереве
    def walk(v):
        if v is None or isinstance(v, (str, bool, int)):
            return
        if isinstance(v, float):
            assert math.isfinite(v)
            return
        if isinstance(v, list):
            for it in v:
                walk(it)
            return
        if isinstance(v, dict):
            for kk, vv in v.items():
                assert isinstance(kk, str)
                walk(vv)
            return
    walk(y)
