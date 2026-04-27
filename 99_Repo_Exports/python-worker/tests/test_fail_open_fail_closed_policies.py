from __future__ import annotations
from utils.time_utils import get_ny_time_millis

from dataclasses import dataclass
import time

from handlers.confirmations.engine import ConfirmationsEngine
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
    geometry_score: float | None = None
    data_quality_flags: list[str] | None = None


def _mk_l2(now_ms: int) -> L2Snap:
    return L2Snap(
        ts_ms=now_ms,
        bids=[L2Level(price=99.9, size=1.0, notional=500.0), L2Level(price=99.7, size=1.0, notional=30000.0)],
        asks=[L2Level(price=100.1, size=1.0, notional=500.0), L2Level(price=100.3, size=1.0, notional=1200.0)],
    )


def test_breakout_l2_missing_is_fail_closed_veto():
    eng = ConfirmationsEngine()
    ctx = Ctx(ts=get_ny_time_millis(), price=100.0, geometry_score=1.0)
    v = eng.validate(kind="breakout", ctx=ctx, l2=None, l3=None, level_price=100.0)
    assert v.veto is True
    assert "bo_l2_missing" in v.flags


def test_extreme_l2_missing_is_fail_open_penalty_not_veto():
    eng = ConfirmationsEngine()
    ctx = Ctx(ts=get_ny_time_millis(), price=100.0, geometry_score=1.0)
    v = eng.validate(kind="extreme", ctx=ctx, l2=None, l3=None, level_price=None)
    assert v.veto is False
    assert "ext_l2_missing_or_stale_penalty" in v.flags
    assert 0.0 < v.conf_factor01 <= 1.0
    assert v.parts["l3_score01"] == 0.5


def test_geometry_missing_neutral_0_1_no_veto():
    eng = ConfirmationsEngine()
    ctx = Ctx(ts=get_ny_time_millis(), price=100.0, geometry_score=None)
    v = eng.validate(kind="extreme", ctx=ctx, l2=None, l3=None, level_price=None)
    assert v.veto is False
    assert "geo_missing_neutral" in v.flags
    assert v.parts["geo_score01"] == 0.1


def test_l2_stale_breakout_veto():
    eng = ConfirmationsEngine()
    now_ms = get_ny_time_millis()
    stale = _mk_l2(now_ms - 10_000)  # stale beyond default 1500ms
    ctx = Ctx(ts=now_ms, price=100.0, geometry_score=1.0)
    v = eng.validate(kind="breakout", ctx=ctx, l2=stale, l3=None, level_price=100.0)
    assert v.veto is True
    assert "bo_l2_stale" in v.flags
