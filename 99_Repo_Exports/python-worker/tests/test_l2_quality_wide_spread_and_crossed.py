from __future__ import annotations

from dataclasses import dataclass

from handlers.confirmations.l2_quality import L2QualityPolicy
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


def test_breakout_crossed_is_veto():
    pol = L2QualityPolicy()
    ctx = Ctx(ts=100_000, price=100.0)
    l2 = L2Snap(
        ts_ms=ctx.ts,
        bids=[L2Level(price=100.10, size=1.0, notional=10000.0)],
        asks=[L2Level(price=100.05, size=1.0, notional=10000.0)],
    )
    a = pol.assess(kind="breakout", ctx=ctx, l2=l2)
    assert a.veto is True
    assert "l2_crossed" in a.flags


def test_breakout_wide_spread_is_veto():
    pol = L2QualityPolicy()
    ctx = Ctx(ts=100_000, price=100.0)
    l2 = L2Snap(
        ts_ms=ctx.ts,
        bids=[
            L2Level(price=99.00, size=1.0, notional=10000.0),
            L2Level(price=98.90, size=1.0, notional=10000.0),
            L2Level(price=98.80, size=1.0, notional=200000.0),
        ],
        asks=[
            L2Level(price=101.00, size=1.0, notional=10000.0),
            L2Level(price=101.10, size=1.0, notional=10000.0),
            L2Level(price=101.20, size=1.0, notional=1000.0),
        ],
    )
    a = pol.assess(kind="breakout", ctx=ctx, l2=l2)
    assert a.veto is True
    assert "l2_wide_spread" in a.flags