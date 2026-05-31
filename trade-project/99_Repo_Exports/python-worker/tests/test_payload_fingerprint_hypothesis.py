from __future__ import annotations

import pytest

try:
    from hypothesis import given
    from hypothesis import strategies as st
    HYPOTHESIS_AVAILABLE = True
except Exception:  # pragma: no cover
    HYPOTHESIS_AVAILABLE = False
    # Dummy st object that swallows calls
    class DummySt:
        def __getattr__(self, name):
            return lambda *args, **kwargs: None

    st = DummySt()

    def given(*args, **kwargs):
        def decorator(f):
            return f
        return decorator


from common.payload_fingerprint import fingerprint_tradeable_payload


def _jsonable():
    if not HYPOTHESIS_AVAILABLE:
        return None
    # JSON-safe subset: None/bool/int/float/str/list/dict (no NaN/Inf generation here).
    scalar = st.one_of(
        st.none(),
        st.booleans(),
        st.integers(min_value=-10_000, max_value=10_000),
        st.floats(allow_nan=False, allow_infinity=False, width=32),
        st.text(min_size=0, max_size=40),
    )
    return st.recursive(
        scalar,
        lambda children: st.lists(children, max_size=8) | st.dictionaries(st.text(min_size=1, max_size=12), children, max_size=8),
        max_leaves=40,
    )


@pytest.mark.skipif(not HYPOTHESIS_AVAILABLE, reason="hypothesis not installed")
@given(_jsonable())
def test_fingerprint_deterministic(obj):
    a1, n1 = fingerprint_tradeable_payload(obj)
    a2, n2 = fingerprint_tradeable_payload(obj)
    assert a1 == a2
    assert n1 == n2


@pytest.mark.skipif(not HYPOTHESIS_AVAILABLE, reason="hypothesis not installed")
@given(st.dictionaries(st.text(min_size=1, max_size=12), _jsonable(), max_size=10))
def test_fingerprint_independent_of_dict_order(d):
    # Reorder dict keys
    items = list(d.items())
    items.reverse()
    d2 = dict(items)
    a1, _ = fingerprint_tradeable_payload(d)
    a2, _ = fingerprint_tradeable_payload(d2)
    assert a1 == a2
