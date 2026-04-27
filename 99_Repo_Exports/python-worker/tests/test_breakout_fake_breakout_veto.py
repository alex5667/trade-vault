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
    side: int = 1
    taker_rate_ema: float = 0.10
    cancel_to_trade_bid_5s: float = 3.5
    microprice_shift_bps_20: float = 2.0


def _mk_l2(now_ms: int) -> L2Snap:
    return L2Snap(
        ts_ms=now_ms,
        bids=[L2Level(price=99.9, size=1.0, notional=40000.0), L2Level(price=99.8, size=1.0, notional=10000.0)],
        asks=[L2Level(price=100.1, size=1.0, notional=12000.0)],
    )


def test_breakout_fake_breakout_veto_cancel_high_taker_low():
    eng = ConfirmationsEngine()
    now = get_ny_time_millis()
    ctx = Ctx(ts=now, price=100.0)
    v = eng.validate(kind="breakout", ctx=ctx, l2=_mk_l2(now), l3=None, level_price=100.0)
    assert v.veto is True
    assert int(QF.BO_FAKE_BREAKOUT_VETO) in v.flags
