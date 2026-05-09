from __future__ import annotations

from hypothesis import given, settings
from hypothesis import strategies as st

from common.metrics_stage import candidates_total, dist, emit_ok_total, stage_ms_hist, veto_total


class ExplodingMetrics:
    def inc(self, *a, **k):  # noqa
        raise RuntimeError("boom")
    def observe(self, *a, **k):  # noqa
        raise RuntimeError("boom")


class Host:
    def __init__(self, metrics):
        self.metrics = metrics


@settings(max_examples=300, deadline=None)
@given(
    kind=st.text(min_size=0, max_size=32),
    reason=st.text(min_size=0, max_size=48),
    stage=st.sampled_from(["detector", "gates", "scoring", "emit", "other"]),
    ms=st.floats(allow_nan=True, allow_infinity=True, width=32),
    name=st.sampled_from(["exp_bps", "cost_bps", "confidence_pct", "conf_factor"]),
    val=st.floats(allow_nan=True, allow_infinity=True, width=32),
)
def test_metrics_stage_fail_open(kind, reason, stage, ms, name, val):
    h = Host(ExplodingMetrics())
    # must never raise
    candidates_total(h, kind=kind)
    veto_total(h, kind=kind, reason_code=reason)
    emit_ok_total(h, kind=kind)
    stage_ms_hist(h, stage=stage, ms=float(ms) if isinstance(ms, (int, float)) else 0.0, kind=kind, symbol="BTCUSDT")
    dist(h, name=name, value=float(val) if isinstance(val, (int, float)) else 0.0, kind=kind)
