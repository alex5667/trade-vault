from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from common.payload_fingerprint import fingerprint_tradeable_payload

json_leaf = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(min_value=-10_000, max_value=10_000),
    st.floats(allow_nan=True, allow_infinity=True, width=32),
    st.text(min_size=0, max_size=32),
)

json_value = st.recursive(
    json_leaf,
    lambda ch: st.one_of(
        st.lists(ch, min_size=0, max_size=8),
        st.dictionaries(st.text(min_size=0, max_size=16), ch, min_size=0, max_size=8),
    ),
    max_leaves=30,
)


@settings(max_examples=250, deadline=None)
@given(obj=json_value)
def test_fingerprint_is_stable(obj):
    h1, n1 = fingerprint_tradeable_payload(obj)
    h2, n2 = fingerprint_tradeable_payload(obj)
    assert (h1, n1) == (h2, n2)
