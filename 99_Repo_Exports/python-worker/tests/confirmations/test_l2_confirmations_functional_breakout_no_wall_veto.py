from __future__ import annotations

from types import SimpleNamespace

from handlers.confirmations.l2_confirmations import l2_confirm_breakout
from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Snapshot, L2Level


def test_functional_breakout_near_wall_delegates_to_class_validator():
    lvl = 100.0
    wall_price = 100.02  # 2 bps => near wall
    wall_size = 1000.0  # notional ~100k > min_wall_notional=50k => veto
    l2 = L2Snapshot(
        bids=[L2Level(price=99.99, size=10.0, notional=999.9)],
        asks=[L2Level(price=wall_price, size=wall_size, notional=wall_price * wall_size)],
    )
    ctx = SimpleNamespace(
        spread_bps=1.0,
        microprice_shift_bps_20=0.0,
        level_price=lvl,
        price=100.01,  # current price
        ts_ms=10_000,
        l2_ts_ms=10_000,  # not stale
        l2_snapshot=l2,
    )
    res = l2_confirm_breakout(ctx=ctx, l2=l2, side="buy", wall_near_bps=6.0, min_wall_notional=50_000.0)
    # Functional version delegates to class validator, which should veto for big near wall
    assert res.veto is True
    assert res.reason_code == "VETO_WALL_NEAR"
