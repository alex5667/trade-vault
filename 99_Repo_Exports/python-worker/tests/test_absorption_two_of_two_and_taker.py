from __future__ import annotations
from utils.time_utils import get_ny_time_millis

from dataclasses import dataclass
import time

from handlers.confirmations.engine import ConfirmationsEngine
from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Level
from common.qf_codes import QF


@dataclass
class L2Snap:
    ts_ms: int
    bids: list[L2Level]
    asks: list[L2Level]


@dataclass
class Ctx:
    ts: int
    price: float
    geometry_score: float = 1.0
    taker_rate_ema: float = 0.30
    wall_here: bool = False
    refill: bool = False
    mp_contra: bool = False
    micro_proxy: bool = False


def _mk_l2(now_ms: int) -> L2Snap:
    return L2Snap(
        ts_ms=now_ms,
        bids=[L2Level(price=99.9, size=1.0, notional=25000.0)],
        asks=[L2Level(price=100.1, size=1.0, notional=25000.0)],
    )


def test_absorption_needs_two_sources_veto():
    eng = ConfirmationsEngine()
    now = get_ny_time_millis()
    ctx = Ctx(ts=now, price=100.0, wall_here=True, mp_contra=False, micro_proxy=False, taker_rate_ema=0.50)
    v = eng.validate(kind="absorption", ctx=ctx, l2=_mk_l2(now), l3=None, level_price=100.0)
    assert v.veto is True
    assert int(QF.AB_NEED_2OF2_VETO) in v.flags


def test_absorption_two_sources_and_taker_pass():
    eng = ConfirmationsEngine()
    now = get_ny_time_millis()
    ctx = Ctx(ts=now, price=100.0, wall_here=True, micro_proxy=True, taker_rate_ema=0.50)
    v = eng.validate(kind="absorption", ctx=ctx, l2=_mk_l2(now), l3=None, level_price=100.0)
    assert v.veto is False
    assert v.conf_factor01 > 0.0
