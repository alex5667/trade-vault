from __future__ import annotations

import math
from dataclasses import dataclass

import pytest

from handlers.confirmations.l2_confirmations import L2ConfirmBreakout
from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Level

hyp = pytest.importorskip("hypothesis")
st = pytest.importorskip("hypothesis.strategies")
given = hyp.given


@dataclass
class L2Snap:
    ts_ms: int
    bids: list[L2Level]
    asks: list[L2Level]


@dataclass
class Ctx:
    price: float
    ts: int
    side: int = 1


@given(
    price=st.floats(min_value=0.0, max_value=500000.0, allow_nan=True, allow_infinity=True),
    p1=st.floats(min_value=-1e9, max_value=1e9, allow_nan=True, allow_infinity=True),
    p2=st.floats(min_value=-1e9, max_value=1e9, allow_nan=True, allow_infinity=True),
    notional=st.floats(min_value=-1e9, max_value=1e9, allow_nan=True, allow_infinity=True),
)
def test_l2_confirm_breakout_no_crash_on_nan_inf(price, p1, p2, notional):
    c = L2ConfirmBreakout()
    ctx = Ctx(price=price, ts=100_000, side=1)
    l2 = L2Snap(
        ts_ms=100_000,
        bids=[L2Level(price=p1, size=1.0, notional=notional)],
        asks=[L2Level(price=p2, size=1.0, notional=notional)],
    )
    r = c.confirm(ctx=ctx, l2=l2, level_price=None)
    assert isinstance(r.veto, bool)
    assert 0.0 <= float(r.score01) <= 1.0
    assert all(isinstance(x, str) for x in r.flags)
    # parts (если есть) должны быть числами/finite, либо -1.0
    for v in (r.parts or {}).values():
        assert isinstance(v, (int, float))
        assert math.isfinite(float(v)) or float(v) == -1.0
