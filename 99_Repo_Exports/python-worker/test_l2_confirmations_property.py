from __future__ import annotations

import pytest

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import given, strategies as st

from dataclasses import dataclass

from handlers.confirmations.l2_confirm_breakout import L2ConfirmBreakout
from handlers.confirmations.l2_confirm_absorption import L2ConfirmAbsorption
from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Level


@dataclass
class L2Snapshot:
    bids: list[L2Level]
    asks: list[L2Level]
    ts_ms: int = 0


@dataclass
class Ctx:
    ts: int
    taker_rate_ema: float | None = None


f64 = st.floats(allow_nan=True, allow_infinity=True, width=64)


@given(
    bid_prices=st.lists(f64, min_size=0, max_size=30),
    ask_prices=st.lists(f64, min_size=0, max_size=30),
    bid_sizes=st.lists(f64, min_size=0, max_size=30),
    ask_sizes=st.lists(f64, min_size=0, max_size=30),
    level_price=f64,
    side=st.integers(min_value=-1, max_value=1),
    taker=f64,
)
def test_breakout_confirm_never_crashes(
    bid_prices, ask_prices, bid_sizes, ask_sizes, level_price, side, taker
):
    bids = [
        L2Level(price=float(p), size=float(s), notional=0.0)
        for p, s in zip(bid_prices, bid_sizes)
    ]
    asks = [
        L2Level(price=float(p), size=float(s), notional=0.0)
        for p, s in zip(ask_prices, ask_sizes)
    ]
    l2 = L2Snapshot(bids=bids, asks=asks, ts_ms=10_000)
    ctx = Ctx(ts=10_000, taker_rate_ema=float(taker))

    c = L2ConfirmBreakout()
    res = c.confirm(ctx=ctx, l2=l2, side=int(side or 1), level_price=float(level_price))
    assert 0.0 <= float(res.score01) <= 1.0
    assert isinstance(res.reason, str)


@given(
    bid_prices=st.lists(f64, min_size=0, max_size=30),
    ask_prices=st.lists(f64, min_size=0, max_size=30),
    bid_sizes=st.lists(f64, min_size=0, max_size=30),
    ask_sizes=st.lists(f64, min_size=0, max_size=30),
    level_price=f64,
    side=st.integers(min_value=-1, max_value=1),
    taker=f64,
)
def test_absorption_confirm_never_crashes(
    bid_prices, ask_prices, bid_sizes, ask_sizes, level_price, side, taker
):
    bids = [
        L2Level(price=float(p), size=float(s), notional=0.0)
        for p, s in zip(bid_prices, bid_sizes)
    ]
    asks = [
        L2Level(price=float(p), size=float(s), notional=0.0)
        for p, s in zip(ask_prices, ask_sizes)
    ]
    l2 = L2Snapshot(bids=bids, asks=asks, ts_ms=10_000)
    ctx = Ctx(ts=10_000, taker_rate_ema=float(taker))

    c = L2ConfirmAbsorption()
    res = c.confirm(ctx=ctx, l2=l2, side=int(side or 1), level_price=float(level_price))
    assert 0.0 <= float(res.score01) <= 1.0
    assert isinstance(res.reason, str)


def test_wall_distance_is_non_negative_when_present():
    c = L2ConfirmBreakout(wall_within_bps=10.0)
    bids = [L2Level(price=99.0, size=1.0, notional=99.0)]
    asks = [
        L2Level(price=100.0, size=10.0, notional=1000.0),  # wall at level
        L2Level(price=101.0, size=1.0, notional=101.0),
    ]
    l2 = L2Snapshot(bids=bids, asks=asks, ts_ms=10_000)
    ctx = Ctx(ts=10_000)
    res = c.confirm(ctx=ctx, l2=l2, side=+1, level_price=100.0)
    d = res.details
    if d.get("wall_found"):
        assert float(d.get("wall_dist_bps", 0.0)) >= 0.0
