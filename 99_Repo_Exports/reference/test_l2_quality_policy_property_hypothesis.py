from __future__ import annotations

import pytest

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import given, strategies as st

from dataclasses import dataclass
from handlers.confirmations.l2_quality import L2QualityPolicy


@dataclass
class Ctx:
    ts: int


@given(st.floats(allow_nan=True, allow_infinity=True, width=64))
def test_hypothesis_l2_ts_ms_never_crashes(ts_ms: float):
    p = L2QualityPolicy(max_stale_ms=1000)
    ctx = Ctx(ts=10_000)

    class L2:
        pass

    l2 = L2()
    setattr(l2, "ts_ms", ts_ms)
    a = p.assess(kind="extreme", ctx=ctx, l2=l2)
    assert a.veto is False
    assert 0.0 <= float(a.l2_score01) <= 1.0
