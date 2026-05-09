from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from common.decision_trace import patch_trace_sidecar_obj


def _ev():
    # model both gate-like and target-like events
    return st.dictionaries(
        keys=st.text(min_size=1, max_size=12),
        values=st.one_of(
            st.none(),
            st.booleans(),
            st.integers(min_value=-10_000, max_value=10_000),
            st.floats(allow_nan=False, allow_infinity=False, width=32),
            st.text(min_size=0, max_size=400),
        ),
        min_size=1,
        max_size=20,
    )


@settings(max_examples=250, deadline=None)
@given(
    side=st.one_of(
        st.just({}),
        st.fixed_dictionaries(
            {
                "schema": st.just("decision_trace_sidecar:v1"),
                "trace": st.fixed_dictionaries({"events": st.lists(_ev(), min_size=0, max_size=30)}),
            }
        ),
        st.fixed_dictionaries(
            {
                "schema": st.just("decision_trace_sidecar:v1"),
                "decision_trace": st.fixed_dictionaries({"events": st.lists(_ev(), min_size=0, max_size=30)}),
            }
        ),
    ),
    patch=st.lists(_ev(), min_size=0, max_size=50),
)
def test_patch_sidecar_is_fail_open_and_keeps_both_keys(side, patch):
    out = patch_trace_sidecar_obj(side, patch)
    assert isinstance(out, dict)
    assert "trace_summary" in out
    assert "trace" in out
    assert "decision_trace" in out
    tr = out["trace"]
    dt = out["decision_trace"]
    assert isinstance(tr, dict)
    assert isinstance(dt, dict)
    # both representations must point to equivalent data shape
    assert (tr.get("events") or []) == (dt.get("events") or [])
    evs = tr.get("events") or []
    assert isinstance(evs, list)
