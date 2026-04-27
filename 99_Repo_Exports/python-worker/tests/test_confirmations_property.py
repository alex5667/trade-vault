from __future__ import annotations

from dataclasses import dataclass

import pytest

from confirmations import L2ConfirmBreakout, L2ConfirmAbsorption
from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Level, L2Snapshot


@dataclass
class Ctx:
    ts_ms: float | None = None
    l2_ts_ms: float | None = None
    price: float | None = 100.0
    l2: L2Snapshot | None = None
    microprice_shift_bps: float | None = None
    adverse_ratio_ema: float | None = None
    refill: bool | None = None


def _mk_l2(bids, asks):
    return L2Snapshot(bids=bids, asks=asks)


def test_stale_l2_is_veto_for_both_confirmations():
    l2 = _mk_l2([L2Level(price=100.0, size=10.0, notional=1000.0)], [L2Level(price=101.0, size=10.0, notional=1000.0)])
    ctx = Ctx(ts_ms=10_000.0, l2_ts_ms=1.0, l2=l2, price=100.0)

    br = L2ConfirmBreakout().confirm(ctx=ctx, side="up", level_price=100.0)
    assert br.veto is True
    ab = L2ConfirmAbsorption().confirm(ctx=ctx, side="down", level_price=100.0)
    assert ab.veto is True


def test_breakout_near_big_wall_soft_fail_not_veto():
    # ask wall very close above level with big notional => soft fail
    l2 = _mk_l2(
        bids=[L2Level(price=99.9, size=1.0, notional=100.0)],
        asks=[L2Level(price=100.02, size=300.0, notional=35_000.0)],
    )
    ctx = Ctx(ts_ms=1000.0, l2_ts_ms=900.0, l2=l2, price=100.0)
    br = L2ConfirmBreakout().confirm(ctx=ctx, side="up", level_price=100.0)
    assert br.veto is False
    assert br.passed is False
    assert br.flags.get("near_big_wall") is True


def test_absorption_flags_include_wall_here_and_micro_proxy_when_derivable():
    l2 = _mk_l2(
        bids=[L2Level(price=100.0, size=500.0, notional=60_000.0)],
        asks=[L2Level(price=100.1, size=1.0, notional=100.0)],
    )
    ctx = Ctx(ts_ms=1000.0, l2_ts_ms=900.0, l2=l2, price=100.0, adverse_ratio_ema=0.7)
    ab = L2ConfirmAbsorption().confirm(ctx=ctx, side="down", level_price=100.0)
    assert ab.veto is False
    assert ab.flags.get("wall_here") is True
    assert ab.flags.get("micro_proxy") is True


def test_optional_hypothesis_fuzz_confirmations_no_crash():
    hyp = pytest.importorskip("hypothesis")
    st = pytest.importorskip("hypothesis.strategies")

    float_any = st.floats(allow_nan=True, allow_infinity=True, width=64)
    price = st.floats(min_value=1.0, max_value=1e6, allow_nan=False, allow_infinity=False)

    @hyp.given(
        p=price,
        ts=float_any,
        l2ts=float_any,
        wall_price=price,
        wall_notional=st.floats(min_value=0.0, max_value=1e9, allow_nan=False, allow_infinity=False),
    )
    def _prop(p, ts, l2ts, wall_price, wall_notional):
        l2 = _mk_l2(
            bids=[L2Level(price=float(wall_price), size=1.0, notional=float(wall_notional))],
            asks=[L2Level(price=float(wall_price) * 1.001, size=1.0, notional=float(wall_notional))],
        )
        ctx = Ctx(ts_ms=ts, l2_ts_ms=l2ts, l2=l2, price=p)
        _ = L2ConfirmBreakout().confirm(ctx=ctx, side="up", level_price=p)
        _ = L2ConfirmAbsorption().confirm(ctx=ctx, side="down", level_price=p)

    _prop()
