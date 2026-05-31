from __future__ import annotations

import os

from tests._fakes import FakeRedis
from infra.redis_repo import RedisTradeRepository

from domain.models import PositionState, TradeClosed


def test_save_open_writes_trailing_profile_alias():
    r = FakeRedis()
    repo = RedisTradeRepository(r)

    pos = PositionState(
        id="OID1",
        sid="SID1",
        strategy="crypto_orderflow",
        source="CryptoOrderFlow",
        symbol="BTCUSDT",
        tf="tick",
        direction="LONG",
        entry_price=100.0,
        entry_ts_ms=1000,
        lot=1.0,
        remaining_qty=1.0,
        sl=90.0,
        tp_levels=[110.0, 120.0, 130.0],
    )
    pos.trail_profile = "rocket_v1"

    repo.save_open(pos)
    h = r.hgetall("order:OID1")
    assert h.get("trail_profile") == "rocket_v1"
    assert h.get("trailing_profile") == "rocket_v1"  # alias


def test_save_closed_writes_trail_profile_alias_and_zset_index():
    os.environ["ENABLE_CLOSED_ZSET_INDEX"] = "1"
    try:
        r = FakeRedis()
        repo = RedisTradeRepository(r)

        closed = TradeClosed(
            order_id="OID2",
            sid="SID2",
            strategy="crypto_orderflow",
            source="CryptoOrderFlow",
            symbol="BTCUSDT",
            tf="tick",
            direction="LONG",
            entry_ts_ms=1000,
            exit_ts_ms=2000,
            entry_price=100.0,
            exit_price=110.0,
            lot=1.0,
            pnl_net=10.0,
        )
        closed.trailing_profile = "rocket_v1"

        repo.save_closed(closed, health_snapshot={"health_l2_stale_ratio_tick": "0.1"})

        h = r.hgetall("order:OID2")
        assert h.get("trailing_profile") == "rocket_v1"
        assert h.get("trail_profile") == "rocket_v1"
        assert h.get("health_l2_stale_ratio_tick") == "0.1"

        # ZSET keys должны содержать OID2 со score=exit_ts_ms
        z1 = r.zsets.get("closed_z:crypto_orderflow:BTCUSDT:tick", {})
        z2 = r.zsets.get("closed_z:crypto_orderflow:BTCUSDT:tick:CryptoOrderFlow", {})
        assert float(z1.get("OID2", 0)) == 2000.0
        assert float(z2.get("OID2", 0)) == 2000.0
    finally:
        os.environ.pop("ENABLE_CLOSED_ZSET_INDEX", None)
