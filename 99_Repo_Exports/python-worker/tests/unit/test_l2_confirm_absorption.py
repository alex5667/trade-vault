from __future__ import annotations

from types import SimpleNamespace

from common.reason_codes import ReasonCode
from handlers.confirmations.l2_confirm_absorption import AbsorptionConfirmCfg, L2ConfirmAbsorption
from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Level, L2Snapshot


def test_absorption_stale_is_veto_with_reason_code():
    v = L2ConfirmAbsorption(AbsorptionConfirmCfg(l2_stale_ms=10))
    ctx = SimpleNamespace(ts_ms=1000.0, l2_ts_ms=0.0, price=100.0)
    ctx.l2 = L2Snapshot(bids=[], asks=[])
    res = v.confirm(ctx=ctx, side="buy", level_price=100.0)
    assert res.veto is True
    assert res.reason_code == ReasonCode.VETO_L2_STALE.value


def test_absorption_sets_wall_here_flag_when_band_sum_big():
    v = L2ConfirmAbsorption(AbsorptionConfirmCfg(min_wall_notional=10_000.0, level_band_bps=2.0))
    lvl = 100.0
    bids = [
        L2Level(price=100.0, size=200.0, notional=15_000.0),
        L2Level(price=99.99, size=10.0, notional=1_000.0),
    ]
    ctx = SimpleNamespace(ts_ms=1000.0, l2_ts_ms=1000.0, price=100.0, adverse_ratio_ema=0.70)
    ctx.l2 = L2Snapshot(bids=bids, asks=[])
    res = v.confirm(ctx=ctx, side="buy", level_price=lvl)
    assert res.veto is False
    assert res.flags.get("wall_here") is True
    assert "wall_notional_here" in res.parts
