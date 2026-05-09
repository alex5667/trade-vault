from __future__ import annotations

from dataclasses import dataclass

import pytest

hypothesis = pytest.importorskip("hypothesis")
from hypothesis import given
from hypothesis import strategies as st

from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Level
from handlers.quality.quality_gate import QualityGate


@dataclass
class L2Snap:
    ts_ms: int
    bids: list[L2Level]
    asks: list[L2Level]


@dataclass
class Ctx:
    ts: int
    data_quality_flags: list[str] | None = None
    l2_snapshot: object | None = None
    spread_bps: float | None = None
    geometry: object | None = None
    geometry_score: float | None = None
    cancel_to_trade_bid_5s: float | None = None
    cancel_to_trade_ask_5s: float | None = None
    microprice_shift_bps_20: float | None = None


def _mk_ok_l2(ts_ctx: int, age_ms: int = 0) -> L2Snap:
    ts = ts_ctx - age_ms
    bids = [L2Level(price=100.0, size=1.0, notional=1000.0)]
    asks = [L2Level(price=100.1, size=1.0, notional=1000.0)]
    return L2Snap(ts_ms=ts, bids=bids, asks=asks)


def test_breakout_missing_l2_is_veto():
    qg = QualityGate()
    ctx = Ctx(ts=10_000)
    qa = qg.assess_kind(kind="breakout", ctx=ctx, l2=None)
    assert qa.veto is True
    assert "l2_missing" in qa.flags


def test_extreme_missing_l2_is_fail_open_penalty():
    qg = QualityGate()
    ctx = Ctx(ts=10_000)
    qa = qg.assess_kind(kind="extreme", ctx=ctx, l2=None)
    assert qa.veto is False
    assert "l2_missing" in qa.flags
    assert 0.0 < qa.quality_score01 <= 1.0


def test_breakout_stale_l2_is_veto():
    qg = QualityGate()
    ctx = Ctx(ts=10_000)
    l2 = _mk_ok_l2(ts_ctx=ctx.ts, age_ms=10_000)  # очень stale
    qa = qg.assess_kind(kind="breakout", ctx=ctx, l2=l2)
    assert qa.veto is True
    assert "l2_stale" in qa.flags or "l2_no_ts" in qa.flags


def test_l3_missing_is_neutral_no_veto_in_global():
    qg = QualityGate()
    ctx = Ctx(ts=10_000)
    qa = qg.assess_global(ctx=ctx)
    assert qa.veto is False
    assert "l3_missing" in qa.flags
    assert 0.0 < qa.quality_score01 <= 1.0


def test_hlc_and_atr_flags_apply_penalty_only():
    qg = QualityGate()
    ctx = Ctx(ts=10_000, data_quality_flags=["hlc_fallback", "atr_fallback"])
    qa = qg.assess_global(ctx=ctx)
    assert qa.veto is False
    assert qa.quality_score01 < 1.0


@given(age_ms=st.integers(min_value=0, max_value=10_000))
def test_quality_is_bounded(age_ms: int):
    qg = QualityGate()
    ctx = Ctx(ts=10_000)
    l2 = _mk_ok_l2(ts_ctx=ctx.ts, age_ms=age_ms)
    qa = qg.assess_kind(kind="extreme", ctx=ctx, l2=l2)
    assert 0.0 <= float(qa.quality_score01) <= 1.0
