from __future__ import annotations

import pytest
from dataclasses import dataclass

from handlers.quality.quality_gate import QualityGate
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
    data_quality_flags: list[str] | None = None


def _mk_l2(ctx_ts: int, bb: float, ba: float, wall_price: float, wall_notional: float) -> L2Snap:
    bids = [
        L2Level(price=bb, size=1.0, notional=1000.0),
        L2Level(price=bb - 0.1, size=1.0, notional=1000.0),
        L2Level(price=wall_price, size=1.0, notional=wall_notional),
    ]
    asks = [
        L2Level(price=ba, size=1.0, notional=1000.0),
        L2Level(price=ba + 0.1, size=1.0, notional=1000.0),
        L2Level(price=ba + 0.2, size=1.0, notional=1000.0),
    ]
    return L2Snap(ts_ms=ctx_ts, bids=bids, asks=asks)


def test_breakout_veto_on_crossed_book():
    qg = QualityGate()
    ctx = Ctx(ts=10_000, price=100.0)
    l2 = _mk_l2(ctx_ts=ctx.ts, bb=100.2, ba=100.1, wall_price=99.9, wall_notional=99999.0)  # crossed
    qa = qg.assess_kind(kind="breakout", ctx=ctx, l2=l2)
    assert qa.veto is True
    assert "l2_crossed_book" in qa.flags


def test_breakout_veto_when_no_wall():
    qg = QualityGate()
    ctx = Ctx(ts=10_000, price=100.0)
    # wall_notional ниже порога => считаем "нет стены"
    l2 = _mk_l2(ctx_ts=ctx.ts, bb=99.9, ba=100.1, wall_price=99.8, wall_notional=1.0)
    qa = qg.assess_kind(kind="breakout", ctx=ctx, l2=l2)
    assert qa.veto is True
    assert ("l2_no_wall" in qa.flags) or ("l2_far_wall" in qa.flags)


def test_breakout_veto_on_bad_ctx_price():
    qg = QualityGate()
    ctx = Ctx(ts=10_000, price=0.0)  # invalid
    qa = qg.assess_kind(kind="breakout", ctx=ctx, l2=None)
    assert qa.veto is True
    assert "bad_ctx_price" in qa.flags
