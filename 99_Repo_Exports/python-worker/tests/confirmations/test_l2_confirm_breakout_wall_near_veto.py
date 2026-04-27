from __future__ import annotations

from types import SimpleNamespace

from handlers.confirmations.l2_confirm_breakout import L2ConfirmBreakout, BreakoutConfirmCfg
from handlers.crypto_orderflow.types.crypto_orderflow_handler_types import L2Snapshot, L2Level
from signal_scoring import reason_registry as rr


def test_breakout_near_big_wall_is_veto_with_u16():
    # Level at 100.0. Place a big ask wall at 100.02 => 2 bps (<= max_near_wall_bps=4).
    lvl = 100.0
    wall_price = 100.02
    wall_size = 500.0  # notional ~ 50k
    l2 = L2Snapshot(
        bids=[L2Level(price=99.99, size=10.0, notional=999.9)],
        asks=[L2Level(price=wall_price, size=wall_size, notional=wall_price * wall_size)],
    )

    ctx = SimpleNamespace(
        price=100.01,
        ts_ms=10_000,
        l2_ts_ms=10_000,
        l2_snapshot=l2,
        side="buy",
    )

    v = L2ConfirmBreakout(cfg=BreakoutConfirmCfg(min_wall_notional=25_000.0, max_near_wall_bps=4.0))
    r = v.confirm(ctx=ctx, side="buy", level_price=lvl)

    assert r.veto is True
    assert r.reason_code == rr.normalize_reason(reason="VETO_WALL_NEAR", reason_code="")[1]
    assert r.reason_u16 == rr.reason_code_to_u16(r.reason_code)
    assert r.score01 == 0.0
    assert r.flags.get("near_big_wall") is True
