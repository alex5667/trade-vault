from __future__ import annotations

import os
from dataclasses import dataclass

from domain.normalizers import canon_tf, tf_variants
from infra.redis_repo import RedisTradeRepository
from services.trade_closed_hydrator import hydrate_trade_closed
from tests.fake_redis import FakeRedis


def test_canon_tf_and_variants():
    assert canon_tf("M1") == "1m"
    assert canon_tf("m1") == "1m"
    assert "1m" in tf_variants("M1")
    assert "m1" in tf_variants("M1")


@dataclass
class Closed:
    schema_version: int = 1
    order_id: str = "OID1"
    sid: str = "SID1"
    strategy: str = "aggregated"
    source: str = "AggregatedHub-V2"
    symbol: str = "XAUUSD"
    tf: str = "M1"  # легаси вход
    direction: str = "LONG"
    entry_ts_ms: int = 1
    exit_ts_ms: int = 1700000000000
    entry_price: float = 1.0
    exit_price: float = 2.0
    lot: float = 0.01
    notional_usd: float = 10.0
    pnl_net: float = 1.0
    pnl_gross: float = 1.0
    fees: float = 0.0
    pnl_pct: float = 0.1
    tp1_hit: bool = True
    tp2_hit: bool = False
    tp3_hit: bool = False
    tp_hits: int = 1
    tp_before_sl: int = 0
    trailing_started: bool = True
    trailing_active: bool = True
    trailing_moves: int = 2
    mfe_pnl: float = 1.0
    mae_pnl: float = -0.1
    giveback: float = 0.2
    missed_profit: float = 0.0
    one_r_money: float = 1.0
    r_multiple: float = 1.0
    duration_ms: int = 1000
    pnl_if_fixed_exit: float = 0.8
    close_reason: str = "TP1"
    close_reason_raw: str = "TP1"
    close_reason_detail: str = "TRAILING_PROFIT"
    baseline_exit_reason: str = "TP1"
    baseline_exit_ts_ms: int = 0
    baseline_exit_price: float = 0.0
    entry_tag: str = ""
    max_favorable_price: float = 0.0
    max_favorable_ts: int = 0
    is_final_close: bool = True
    remaining_qty: float = 0.0
    status: str = "CLOSED"
    trailing_profile: str = "rocket_v1"
    trailing_min_lock_r: float = 0.25
    min_lock_price: float = 0.0


def test_save_closed_writes_profile_aliases_and_tf_variants_and_zset():
    r = FakeRedis()
    repo = RedisTradeRepository(r)
    os.environ["TRADES_CLOSED_ZSET_INDEX"] = "1"
    try:
        c = Closed()
        repo.save_closed(c)

        # order hash should have BOTH profile keys
        h = r.hgetall("order:OID1")
        assert h.get("trailing_profile") == "rocket_v1"
        assert h.get("trail_profile") == "rocket_v1"

        # list keys should exist for both TF variants (1m + m1)
        assert "OID1" in r._l.get("closed:aggregated:XAUUSD:1m:AggregatedHub-V2", [])
        assert "OID1" in r._l.get("closed:aggregated:XAUUSD:m1:AggregatedHub-V2", [])

        # zset keys should exist for both variants too
        assert "OID1" in r.zrevrange("closed_z:aggregated:XAUUSD:1m:AggregatedHub-V2", 0, 10)
        assert "OID1" in r.zrevrange("closed_z:aggregated:XAUUSD:m1:AggregatedHub-V2", 0, 10)

        # hydrator must return both profile keys even if stream has only one
        stream_fields = {"order_id": "OID1", "trailing_profile": "rocket_v1"}
        full = hydrate_trade_closed(r, stream_fields, require_closed=True, merge_precedence="hash")
        assert full.get("trailing_profile") == "rocket_v1"
        assert full.get("trail_profile") == "rocket_v1"
    finally:
        os.environ.pop("TRADES_CLOSED_ZSET_INDEX", None)
