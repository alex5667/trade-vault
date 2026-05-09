
from hypothesis import given, settings
from hypothesis import strategies as st


@given(
    d=st.dictionaries(
        keys=st.text(min_size=1, max_size=10),
        values=st.one_of(
            st.none(),
            st.booleans(),
            st.integers(min_value=-10_000, max_value=10_000),
            st.floats(allow_nan=False, allow_infinity=False, width=32),
            st.text(min_size=0, max_size=20),
        ),
        max_size=20,
    )
)
@settings(max_examples=200)
def test_fingerprint_deterministic(d):
    from common.payload_fingerprint import fingerprint_tradeable_payload

    sha1a, nb_a = fingerprint_tradeable_payload(d)
    sha1b, nb_b = fingerprint_tradeable_payload(dict(d))

    assert isinstance(sha1a, str)
    assert isinstance(nb_a, int)
    assert sha1a == sha1b
    assert nb_a == nb_b
    assert (sha1a == "" and nb_a == 0) or (len(sha1a) == 40 and nb_a > 0)


def test_fingerprint_stable_for_dict_order():
    from common.payload_fingerprint import fingerprint_tradeable_payload

    a = {"b": 2, "a": 1, "c": {"z": 9, "y": 8}}
    b = {"c": {"y": 8, "z": 9}, "a": 1, "b": 2}
    assert fingerprint_tradeable_payload(a) == fingerprint_tradeable_payload(b)
