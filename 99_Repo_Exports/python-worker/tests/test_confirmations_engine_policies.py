from __future__ import annotations

import pytest
from dataclasses import dataclass

from handlers.confirmations.engine import ConfirmationsEngine
from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Level


@dataclass
class L2Snap:
    ts_ms: int
    bids: list[L2Level]
    asks: list[L2Level]


@dataclass
class L3Snap:
    ts_ms: int


@dataclass
class Ctx:
    ts: int
    price: float
    taker_rate_ema: float | None = None
    microprice_shift_bps_20: float | None = None
    cancel_to_trade_bid_5s: float | None = None
    spread_bps: float | None = None
    geometry_score: float | None = 1.0


def _mk_book(ts: int, *, mid: float = 100.0, wall_notional: float = 200000.0) -> L2Snap:
    bb = mid - 0.05
    ba = mid + 0.05
    bids = [
        L2Level(price=bb, size=1.0, notional=1000.0),
        L2Level(price=bb - 0.1, size=1.0, notional=1000.0),
        L2Level(price=bb - 0.2, size=1.0, notional=wall_notional),
    ]
    asks = [
        L2Level(price=ba, size=1.0, notional=1000.0),
        L2Level(price=ba + 0.1, size=1.0, notional=1000.0),
        L2Level(price=ba + 0.2, size=1.0, notional=1000.0),
    ]
    return L2Snap(ts_ms=ts, bids=bids, asks=asks)


def test_breakout_l2_missing_is_fail_closed():
    eng = ConfirmationsEngine()
    ctx = Ctx(ts=100_000, price=100.0, taker_rate_ema=1.0, geometry_score=1.0)
    v = eng.validate(kind="breakout", ctx=ctx, l2=None, l3=None, level_price=100.0)
    assert v.veto is True
    assert "l2_missing" in v.flags


def test_extreme_l2_missing_is_fail_open():
    eng = ConfirmationsEngine()
    ctx = Ctx(ts=100_000, price=100.0, geometry_score=None)
    v = eng.validate(kind="extreme", ctx=ctx, l2=None, l3=None, level_price=None)
    assert v.veto is False
    assert "l2_missing" in v.flags
    # geometry missing => 0.1
    assert any(f == "geometry_missing" for f in v.flags)
    assert 0.0 < v.conf_factor01 <= 1.0


def test_l3_missing_neutral_does_not_veto():
    eng = ConfirmationsEngine()
    ctx = Ctx(ts=100_000, price=100.0, taker_rate_ema=1.0, geometry_score=1.0)
    l2 = _mk_book(ts=ctx.ts, mid=100.0, wall_notional=200000.0)
    v = eng.validate(kind="breakout", ctx=ctx, l2=l2, l3=None, level_price=100.0)
    assert v.veto is False
    assert "l3_missing" in v.flags
    assert v.parts["l3_score01"] == pytest.approx(0.50, abs=1e-9)


def test_geometry_missing_sets_neutral_low_score():
    eng = ConfirmationsEngine()
    ctx = Ctx(ts=100_000, price=100.0, taker_rate_ema=1.0, geometry_score=None)
    l2 = _mk_book(ts=ctx.ts, mid=100.0, wall_notional=200000.0)
    v = eng.validate(kind="breakout", ctx=ctx, l2=l2, l3=L3Snap(ts_ms=ctx.ts), level_price=100.0)
    assert v.veto is False
    assert "geometry_missing" in v.flags
    assert v.parts["geometry_score01"] == pytest.approx(0.10, abs=1e-9)