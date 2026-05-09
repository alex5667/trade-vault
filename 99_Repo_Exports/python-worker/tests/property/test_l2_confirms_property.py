from __future__ import annotations

from types import SimpleNamespace

import pytest

try:
    hypothesis = pytest.importorskip("hypothesis")
    from hypothesis import given, settings
    from hypothesis import strategies as st
    HAS_HYPOTHESIS = True
except pytest.skip.Exception:
    # hypothesis not available, skip these tests
    hypothesis = None
    given = lambda *args, **kwargs: lambda f: f  # noop decorator
    settings = lambda *args, **kwargs: lambda f: f  # noop decorator
    st = None
    HAS_HYPOTHESIS = False

from handlers.confirmations.l2_confirm_absorption import L2ConfirmAbsorption
from handlers.confirmations.l2_confirm_breakout import L2ConfirmBreakout
from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Level, L2Snapshot


def _lvl():
    return st.builds(
        L2Level,
        price=st.floats(allow_nan=True, allow_infinity=True, width=32),
        size=st.floats(allow_nan=True, allow_infinity=True, width=32),
        notional=st.floats(allow_nan=True, allow_infinity=True, width=32),
    )


@given(
    price=st.floats(allow_nan=True, allow_infinity=True, width=32),
    level=st.floats(allow_nan=True, allow_infinity=True, width=32),
    bids=st.lists(_lvl(), min_size=0, max_size=20),
    asks=st.lists(_lvl(), min_size=0, max_size=20),
    side=st.sampled_from(["buy", "sell", "up", "down"]),
)
@settings(max_examples=500, deadline=None)
def test_l2_confirms_never_throw_and_scores_are_bounded(price, level, bids, asks, side):
    # make ctx
    ctx = SimpleNamespace(ts_ms=1000.0, l2_ts_ms=1000.0, price=price, microprice_shift=0.0)
    ctx.l2 = L2Snapshot(bids=bids, asks=asks)

    bo = L2ConfirmBreakout()
    ab = L2ConfirmAbsorption()

    r1 = bo.confirm(ctx=ctx, side=side, level_price=level)
    assert 0.0 <= float(r1.score01) <= 1.0
    assert isinstance(r1.reason_code, str)
    assert isinstance(r1.reason_u16, int)

    r2 = ab.confirm(ctx=ctx, side=side, level_price=level)
    assert 0.0 <= float(r2.score01) <= 1.0
    assert isinstance(r2.reason_code, str)
    assert isinstance(r2.reason_u16, int)
