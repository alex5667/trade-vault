from __future__ import annotations

import pytest
import math
from dataclasses import dataclass

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import given, strategies as st

from handlers.confirmations.l2_confirmations import L2ConfirmBreakout, L2ConfirmAbsorption
from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Level


@dataclass
class L2Snap:
    ts_ms: int
    bids: list[L2Level]
    asks: list[L2Level]


@dataclass
class Ctx:
    ts: int
    price: float
    microprice_shift_bps_20: float | None = None
    taker_rate_ema: float | None = None
    cancel_to_trade_bid_5s: float | None = None
    cancel_to_trade_ask_5s: float | None = None
    cancel_to_trade_bid_20s: float | None = None
    cancel_to_trade_ask_20s: float | None = None
    refill_ratio: float | None = None
    micro_proxy_score01: float | None = None


def _mk_ok_book(ts: int, *, mid: float = 100.0, wall_px: float = 100.0, wall_notional: float = 100000.0) -> L2Snap:
    bb = mid - 0.05
    ba = mid + 0.05
    bids = [
        L2Level(price=bb, size=1.0, notional=1000.0),
        L2Level(price=bb - 0.1, size=1.0, notional=1000.0),
        L2Level(price=wall_px, size=1.0, notional=wall_notional),
    ]
    asks = [
        L2Level(price=ba, size=1.0, notional=1000.0),
        L2Level(price=ba + 0.1, size=1.0, notional=1000.0),
        L2Level(price=ba + 0.2, size=1.0, notional=1000.0),
    ]
    return L2Snap(ts_ms=ts, bids=bids, asks=asks)


def test_breakout_stale_l2_is_fail_closed():
    bo = L2ConfirmBreakout()
    ctx = Ctx(ts=100_000, price=100.0, microprice_shift_bps_20=0.0, taker_rate_ema=1.0)
    # stale: ts_ms сильно старый
    l2 = _mk_ok_book(ts=1, mid=100.0, wall_px=100.0, wall_notional=200000.0)
    r = bo.confirm(ctx=ctx, l2=l2, level_price=100.0)
    assert r.veto is True
    assert any("l2_stale" in f or "l2_no_ts" in f for f in r.flags)


def test_breakout_fake_bo_spoof_veto():
    bo = L2ConfirmBreakout()
    ctx = Ctx(
        ts=100_000,
        price=100.0,
        microprice_shift_bps_20=10.0,
        taker_rate_ema=0.10,
        cancel_to_trade_bid_5s=5.0,
    )
    l2 = _mk_ok_book(ts=ctx.ts, mid=100.0, wall_px=100.0, wall_notional=200000.0)
    r = bo.confirm(ctx=ctx, l2=l2, level_price=100.0)
    assert r.veto is True
    assert "fake_bo_spoof_veto" in r.flags


def test_absorption_requires_two_sources_and_taker_rate():
    ab = L2ConfirmAbsorption()
    ctx = Ctx(ts=100_000, price=100.0, taker_rate_ema=0.05, refill_ratio=1.0, micro_proxy_score01=1.0)
    l2 = _mk_ok_book(ts=ctx.ts, mid=100.0, wall_px=100.0, wall_notional=200000.0)
    r = ab.confirm(ctx=ctx, l2=l2, level_price=100.0)
    assert r.veto is True
    assert "abs_low_taker" in r.flags


@given(
    price=st.one_of(
        st.floats(allow_nan=True, allow_infinity=True),
        st.integers(),
    ),
    shift=st.floats(allow_nan=True, allow_infinity=True),
    tr=st.floats(allow_nan=True, allow_infinity=True),
)
def test_confirmations_do_not_crash_on_nan_inf(price, shift, tr):
    bo = L2ConfirmBreakout()
    ab = L2ConfirmAbsorption()
    ctx = Ctx(ts=100_000, price=float(price), microprice_shift_bps_20=float(shift), taker_rate_ema=float(tr))
    l2 = _mk_ok_book(ts=ctx.ts, mid=100.0, wall_px=100.0, wall_notional=200000.0)
    r1 = bo.confirm(ctx=ctx, l2=l2, level_price=100.0)
    r2 = ab.confirm(ctx=ctx, l2=l2, level_price=100.0)
    assert isinstance(r1.veto, bool)
    assert isinstance(r2.veto, bool)
    assert math.isfinite(float(r1.score01))
    assert math.isfinite(float(r2.score01))