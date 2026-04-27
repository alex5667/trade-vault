from __future__ import annotations

from dataclasses import dataclass

from handlers.confirmations.l2_quality import L2QualityPolicy
from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Level


@dataclass
class L2Snapshot:
    bids: list[L2Level]
    asks: list[L2Level]
    ts_ms: int


@dataclass
class Ctx:
    ts: int
    data_quality_flags: list[str] | None = None


def test_breakout_fail_closed_on_missing():
    p = L2QualityPolicy()
    ctx = Ctx(ts=10_000)
    a = p.assess(kind="breakout", ctx=ctx, l2=None)
    assert a.veto is True
    assert "l2_missing" in a.flags


def test_extreme_fail_open_on_missing():
    p = L2QualityPolicy()
    ctx = Ctx(ts=10_000)
    a = p.assess(kind="extreme", ctx=ctx, l2=None)
    assert a.veto is False
    assert 0.0 < a.score01 <= 1.0


def test_breakout_fail_closed_on_stale():
    p = L2QualityPolicy(max_stale_ms=1000)
    ctx = Ctx(ts=10_000)
    l2 = L2Snapshot(
        bids=[L2Level(price=99.0, size=1.0, notional=99.0)],
        asks=[L2Level(price=101.0, size=1.0, notional=101.0)],
        ts_ms=0,  # stale
    )
    a = p.assess(kind="breakout", ctx=ctx, l2=l2)
    assert a.veto is True
    assert a.stale is True
    assert "l2_stale" in a.flags
