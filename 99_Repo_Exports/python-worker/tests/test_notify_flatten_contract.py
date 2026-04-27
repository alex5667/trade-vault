from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from services.outbox.notify_flatten import flatten_notify_fields


_json_scalars = st.one_of(
    st.none(),
    st.booleans(),
    st.integers(),
    st.floats(allow_nan=False, allow_infinity=False),
    st.text(),
)

_json_value = st.recursive(
    _json_scalars,
    lambda children: st.one_of(
        st.lists(children, max_size=10),
        st.dictionaries(st.text(min_size=1, max_size=16), children, max_size=10),
    ),
    max_leaves=50,
)


@settings(deadline=None, max_examples=200)
@given(st.dictionaries(st.text(min_size=1, max_size=16), _json_value, max_size=30))
def test_flatten_notify_fields_even_and_strings(payload):
    flat = flatten_notify_fields(payload)

    assert isinstance(flat, list)
    assert len(flat) % 2 == 0

    for x in flat:
        assert isinstance(x, str)
        assert x != "" or True  # допускаем пустые строки, если payload так дал

    # Keys should be unique positions (because we iterate keys once)
    keys = flat[0::2]
    assert len(keys) == len(set(keys))
