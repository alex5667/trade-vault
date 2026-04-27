from __future__ import annotations

from hypothesis import given, strategies as st

from common.metrics_stage import (
    candidates_total,
    veto_total,
    emit_ok_total,
    stage_ms_hist,
    dist,
)


class Weird:
    # object that may raise on attribute access (simulating broken handlers)
    def __getattr__(self, name):
        raise RuntimeError("boom")


@given(
    st.one_of(st.none(), st.integers(), st.text(), st.builds(object), st.builds(Weird)),
    st.text(min_size=0, max_size=30),
    st.text(min_size=0, max_size=30),
    st.floats(allow_nan=True, allow_infinity=True),
)
def test_metrics_stage_fail_open(host, kind, reason, x):
    # must never raise
    candidates_total(host, kind=kind)
    veto_total(host, kind=kind, reason_code=reason)
    emit_ok_total(host, kind=kind, symbol="BTCUSDT")
    stage_ms_hist(host, stage="detector", ms=float(x) if x == x else 0.0, kind=kind, symbol="BTCUSDT")
    dist(host, name="confidence_pct", value=0.0, kind=kind, symbol="BTCUSDT")
