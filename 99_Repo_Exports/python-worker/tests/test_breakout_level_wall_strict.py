from __future__ import annotations

from dataclasses import dataclass

from handlers.confirmations.l2_confirmations import L2ConfirmBreakout
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


def test_breakout_level_wall_far_is_veto():
    bo = L2ConfirmBreakout()
    ctx = Ctx(ts=100_000, price=100.0, microprice_shift_bps_20=0.0, taker_rate_ema=1.0)
    # стена далеко от level_price (например, на +0.50 при цене 100 => 50 bps)
    bids = [
        L2Level(price=99.95, size=1.0, notional=1000.0),
        L2Level(price=99.85, size=1.0, notional=1000.0),
        L2Level(price=100.50, size=1.0, notional=200000.0),  # far wall vs level=100.0
    ]
    asks = [
        L2Level(price=100.05, size=1.0, notional=1000.0),
        L2Level(price=100.15, size=1.0, notional=1000.0),
        L2Level(price=100.25, size=1.0, notional=1000.0),
    ]
    l2 = L2Snap(ts_ms=ctx.ts, bids=bids, asks=asks)
    r = bo.confirm(ctx=ctx, l2=l2, level_price=100.0)
    assert r.veto is True
    assert "bo_far_level_wall" in r.flags
