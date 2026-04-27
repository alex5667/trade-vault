from __future__ import annotations

from types import SimpleNamespace

from handlers.confirmations.l2_confirm_breakout import L2ConfirmBreakout, BreakoutConfirmCfg
from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Snapshot, L2Level
from common.reason_codes import ReasonCode


def test_breakout_stale_is_veto_with_reason_code():
    v = L2ConfirmBreakout(BreakoutConfirmCfg(l2_stale_ms=10))
    ctx = SimpleNamespace(ts_ms=1000.0, l2_ts_ms=0.0, price=100.0)
    ctx.l2 = L2Snapshot(bids=[], asks=[])
    res = v.confirm(ctx=ctx, side="buy", level_price=100.0)
    assert res.veto is True
    assert res.reason_code == ReasonCode.VETO_L2_STALE.value
    assert res.reason_u16 > 0


def test_breakout_near_big_wall_is_soft_fail_not_veto():
    v = L2ConfirmBreakout(BreakoutConfirmCfg(min_wall_notional=10_000.0, max_near_wall_bps=5.0))
    lvl = 100.0
    # wall at 100.02 (2 bps) with big notional
    asks = [L2Level(price=100.02, size=200.0, notional=25_000.0)]
    ctx = SimpleNamespace(ts_ms=1000.0, l2_ts_ms=1000.0, price=100.05)
    ctx.l2 = L2Snapshot(bids=[], asks=asks)
    res = v.confirm(ctx=ctx, side="buy", level_price=lvl)
    assert res.veto is False
    assert res.passed is False  # soft fail
    assert res.flags.get("near_big_wall") is True
    assert 0.0 <= float(res.score01) <= 1.0
