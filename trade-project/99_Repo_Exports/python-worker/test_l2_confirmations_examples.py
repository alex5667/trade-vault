from __future__ import annotations

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
    price: float = 100.0
    last_price: float = 100.0


def test_breakout_wall_veto():
    c = L2ConfirmBreakout(wall_within_bps=10.0, wall_ratio_veto=2.0, wall_ratio_soft=1.1, max_spread_bps=200.0)
    # Add more bids for proper baseline calculation
    bids = [
        L2Level(price=99.9, size=1.0, notional=99.9),
        L2Level(price=99.8, size=1.0, notional=99.8),
        L2Level(price=99.7, size=1.0, notional=99.7),
    ]
    # baseline ~ 99.8; wall notional huge => ratio>2 => veto
    # min_wall_notional in compat mode is 25000
    asks = [
        L2Level(price=100.1, size=500.0, notional=50005.0),
        L2Level(price=101.0, size=1.0, notional=101.0),
        L2Level(price=102.0, size=1.0, notional=102.0),
        L2Level(price=103.0, size=1.0, notional=103.0),
    ]
    l2 = L2Snapshot(bids=bids, asks=asks, ts_ms=10_000)
    ctx = Ctx(ts=10_000)
    # side=1 is "buy" in my new code
    res = c.confirm(ctx=ctx, l2=l2, side=1, level_price=100.0)
    assert res.veto is True
    assert res.reason == "VETO_WALL_NEAR"


def test_absorption_requires_wall_and_taker():
    c = L2ConfirmAbsorption(wall_within_bps=10.0, wall_ratio_min=2.0, min_taker_rate_ema=0.1, max_spread_bps=200.0)
    # Add more bids for proper baseline calculation
    bids = [
        L2Level(price=100.0, size=500.0, notional=50000.0),  # wall at level
        L2Level(price=99.5, size=1.0, notional=99.5),
        L2Level(price=99.4, size=1.0, notional=99.4),
        L2Level(price=99.3, size=1.0, notional=99.3),
    ]
    # Add more asks for proper baseline calculation
    asks = [
        L2Level(price=100.1, size=1.0, notional=100.1),
        L2Level(price=100.2, size=1.0, notional=100.2),
        L2Level(price=100.3, size=1.0, notional=100.3),
    ]
    l2 = L2Snapshot(bids=bids, asks=asks, ts_ms=10_000)
    ctx = Ctx(ts=10_000, taker_rate_ema=0.01)
    res = c.confirm(ctx=ctx, l2=l2, side=1, level_price=100.0)
    assert res.veto is True
    assert res.reason == "VETO_TAKER_RATE_LOW"
