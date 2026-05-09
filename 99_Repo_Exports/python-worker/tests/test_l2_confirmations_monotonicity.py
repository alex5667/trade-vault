from __future__ import annotations

import pytest

hypothesis = pytest.importorskip("hypothesis")
from dataclasses import dataclass

from hypothesis import assume, given
from hypothesis import strategies as st

from handlers.confirmations.l2_confirm_absorption import L2ConfirmAbsorption
from handlers.confirmations.l2_confirm_breakout import L2ConfirmBreakout
from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Level


@dataclass
class L2Snapshot:
    bids: list[L2Level]
    asks: list[L2Level]
    ts_ms: int


@dataclass
class Ctx:
    ts: int
    taker_rate_ema: float | None = None


def _baseline_book(level: float = 100.0) -> tuple[list[L2Level], list[L2Level]]:
    # простая книга с понятным baseline notional ~ 100..120
    bids = [
        L2Level(price=level - 1.0, size=1.0, notional=(level - 1.0)),
        L2Level(price=level - 1.5, size=1.0, notional=(level - 1.5)),
        L2Level(price=level - 2.0, size=1.0, notional=(level - 2.0)),
    ]
    asks = [
        L2Level(price=level + 0.5, size=1.0, notional=(level + 0.5)),
        L2Level(price=level + 1.0, size=1.0, notional=(level + 1.0)),
        L2Level(price=level + 1.5, size=1.0, notional=(level + 1.5)),
    ]
    return bids, asks


@given(
    r1=st.floats(min_value=1.05, max_value=3.0, allow_nan=False, allow_infinity=False),
    r2=st.floats(min_value=1.05, max_value=6.0, allow_nan=False, allow_infinity=False),
    d1=st.floats(min_value=0.0, max_value=9.5, allow_nan=False, allow_infinity=False),
    d2=st.floats(min_value=0.0, max_value=9.5, allow_nan=False, allow_infinity=False),
)
def test_breakout_score_monotone_ratio_and_distance(r1, r2, d1, d2):
    # чем больше ratio => тем ниже score (при фиксированной близости)
    # чем ближе стена (меньше dist) => тем ниже score (при фиксированном ratio)
    assume(r2 >= r1)
    assume(d2 >= d1)

    level = 100.0
    bids, asks = _baseline_book(level)

    # сформируем ask-wall около level_price с заданным dist
    # dist задаём через цену: price = level * (1 + dist_bps/10000)
    def mk_wall(dist_bps: float, ratio: float) -> L2Level:
        wall_price = level * (1.0 + dist_bps / 10_000.0)
        # baseline median ~ около 101 => notional ~ 101
        base = (level + 1.0)
        return L2Level(price=wall_price, size=1.0, notional=base * ratio)

    # конфиг: within=10bps, veto=6, soft=2
    c = L2ConfirmBreakout(wall_within_bps=10.0, wall_ratio_veto=6.0, wall_ratio_soft=2.0)
    ctx = Ctx(ts=10_000)

    # case A: ratio monotonic at same distance
    l2a1 = L2Snapshot(bids=bids, asks=[mk_wall(d1, r1)] + asks, ts_ms=10_000)
    l2a2 = L2Snapshot(bids=bids, asks=[mk_wall(d1, r2)] + asks, ts_ms=10_000)
    s1 = c.confirm(ctx=ctx, l2=l2a1, side=+1, level_price=level).score01
    s2 = c.confirm(ctx=ctx, l2=l2a2, side=+1, level_price=level).score01
    assert float(s2) <= float(s1) + 1e-9

    # case B: distance monotonic at same ratio (при росте dist -> score не должен падать)
    l2b1 = L2Snapshot(bids=bids, asks=[mk_wall(d1, r1)] + asks, ts_ms=10_000)
    l2b2 = L2Snapshot(bids=bids, asks=[mk_wall(d2, r1)] + asks, ts_ms=10_000)
    t1 = c.confirm(ctx=ctx, l2=l2b1, side=+1, level_price=level).score01
    t2 = c.confirm(ctx=ctx, l2=l2b2, side=+1, level_price=level).score01
    assert float(t2) >= float(t1) - 1e-9


@given(
    r1=st.floats(min_value=2.0, max_value=4.0, allow_nan=False, allow_infinity=False),
    r2=st.floats(min_value=2.0, max_value=8.0, allow_nan=False, allow_infinity=False),
    d1=st.floats(min_value=0.0, max_value=9.5, allow_nan=False, allow_infinity=False),
    d2=st.floats(min_value=0.0, max_value=9.5, allow_nan=False, allow_infinity=False),
)
def test_absorption_score_monotone_ratio_and_distance(r1, r2, d1, d2):
    assume(r2 >= r1)
    assume(d2 >= d1)

    level = 100.0
    bids, asks = _baseline_book(level)

    def mk_wall_bid(dist_bps: float, ratio: float) -> L2Level:
        wall_price = level * (1.0 - dist_bps / 10_000.0)
        base = (level - 1.0)
        return L2Level(price=wall_price, size=1.0, notional=base * ratio)

    c = L2ConfirmAbsorption(wall_within_bps=10.0, wall_ratio_min=2.0, min_taker_rate_ema=0.0)
    ctx = Ctx(ts=10_000, taker_rate_ema=0.2)

    # ratio monotonic: r↑ => score↑
    l2a1 = L2Snapshot(bids=[mk_wall_bid(d1, r1)] + bids, asks=asks, ts_ms=10_000)
    l2a2 = L2Snapshot(bids=[mk_wall_bid(d1, r2)] + bids, asks=asks, ts_ms=10_000)
    s1 = c.confirm(ctx=ctx, l2=l2a1, side=+1, level_price=level).score01
    s2 = c.confirm(ctx=ctx, l2=l2a2, side=+1, level_price=level).score01
    assert float(s2) >= float(s1) - 1e-9

    # distance monotonic: dist↑ => score↓ (стена дальше — хуже для подтверждения)
    l2b1 = L2Snapshot(bids=[mk_wall_bid(d1, r1)] + bids, asks=asks, ts_ms=10_000)
    l2b2 = L2Snapshot(bids=[mk_wall_bid(d2, r1)] + bids, asks=asks, ts_ms=10_000)
    t1 = c.confirm(ctx=ctx, l2=l2b1, side=+1, level_price=level).score01
    t2 = c.confirm(ctx=ctx, l2=l2b2, side=+1, level_price=level).score01
    assert float(t2) <= float(t1) + 1e-9
