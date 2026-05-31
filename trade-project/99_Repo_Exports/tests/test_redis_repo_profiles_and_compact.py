from __future__ import annotations

import os
from dataclasses import dataclass

from tests._fakes import FakeRedis
from infra.redis_repo import RedisTradeRepository


@dataclass
class _Pos:
    id: str
    sid: str
    strategy: str
    source: str
    symbol: str
    tf: str
    direction: str
    entry_price: float
    entry_ts_ms: int
    lot: float
    remaining_qty: float
    sl: float
    tp_levels: list
    tp_hits: int = 0
    trailing_distance: float = 0.0
    trailing_point: float = 0.0
    max_favorable_price: float = 0.0
    max_favorable_ts: int = 0
    mfe_pnl: float = 0.0
    mae_pnl: float = 0.0
    one_r_money: float = 0.0
    entry_tag: str = ""
    trail_profile: str = "rocket_v1"
    trailing_min_lock_r: float = 0.25
    min_lock_price: float = 0.0
    baseline_mode: str = "tp_sl"
    baseline_horizon_ms: int = 0
    baseline_sl: float = 0.0
    baseline_tp1: float = 0.0
    baseline_tp2: float = 0.0
    baseline_tp3: float = 0.0


@dataclass
class _Closed:
    schema_version: int = 1
    order_id: str = "OID1"
    sid: str = "SID1"
    strategy: str = "s"
    source: str = "CryptoOrderFlow"
    symbol: str = "BTCUSDT"
    tf: str = "M1"  # intentionally legacy, to test writing to "1m" and "m1"
    direction: str = "LONG"
    entry_ts_ms: int = 1
    exit_ts_ms: int = 1700000000000
    entry_price: float = 100.0
    exit_price: float = 110.0
    lot: float = 1.0
    notional_usd: float = 100.0
    pnl_net: float = 10.0
    pnl_gross: float = 10.5
    fees: float = 0.5
    pnl_pct: float = 10.0
    tp1_hit: bool = True
    tp2_hit: bool = False
    tp3_hit: bool = False
    tp_hits: int = 1
    tp_before_sl: int = 0
    trailing_started: bool = True
    trailing_active: bool = True
    trailing_moves: int = 2
    mfe_pnl: float = 11.0
    mae_pnl: float = -1.0
    giveback: float = 2.0
    missed_profit: float = 0.0
    one_r_money: float = 5.0
    r_multiple: float = 2.0
    duration_ms: int = 1000
    pnl_if_fixed_exit: float = 9.0
    close_reason: str = "TP1"
    close_reason_raw: str = "TP1"
    close_reason_detail: str = "TRAILING_PROFIT"
    baseline_exit_reason: str = ""
    baseline_exit_ts_ms: int = 0
    baseline_exit_price: float = 0.0
    entry_tag: str = ""
    max_favorable_price: float = 111.0
    max_favorable_ts: int = 0
    is_final_close: bool = True
    remaining_qty: float = 0.0
    status: str = "CLOSED"
    trailing_profile: str = "rocket_v1"
    trailing_min_lock_r: float = 0.25
    min_lock_price: float = 0.0


def test_save_open_writes_both_profile_aliases():
    r = FakeRedis()
    repo = RedisTradeRepository(r)
    pos = _Pos(
        id="OIDOPEN", sid="SID", strategy="s", source="CryptoOrderFlow",
        symbol="BTCUSDT", tf="tick", direction="LONG",
        entry_price=1.0, entry_ts_ms=1, lot=1.0, remaining_qty=1.0, sl=0.9,
        tp_levels=[1.1, 1.2, 1.3],
    )
    repo.save_open(pos)
    h = r.hgetall("order:OIDOPEN")
    assert h.get("trail_profile") == "rocket_v1"
    assert h.get("trailing_profile") == "rocket_v1"


def test_save_closed_writes_profile_aliases_and_bool_01_and_compact_optional():
    r = FakeRedis()
    repo = RedisTradeRepository(r)
    c = _Closed()

    os.environ["TRADES_CLOSED_STREAM_COMPACT"] = "1"
    try:
        repo.save_closed(c)
        h = r.hgetall("order:OID1")
        assert h.get("trail_profile") == "rocket_v1"
        assert h.get("trailing_profile") == "rocket_v1"
        assert h.get("tp1_hit") == "1"
        assert h.get("tp2_hit") == "0"
        assert h.get("trailing_active") == "1"
        assert h.get("trailing_started") == "1"

        # stream payload должен содержать и trailing_profile, и trail_profile
        msg = r.xrevrange("trades:closed", count=1)[0][1]
        assert msg.get("order_id") == "OID1"
        assert msg.get("trailing_profile") == "rocket_v1"
        assert msg.get("trail_profile") == "rocket_v1"
    finally:
        os.environ.pop("TRADES_CLOSED_STREAM_COMPACT", None)


def test_save_closed_zset_indexing():
    r = FakeRedis()
    repo = RedisTradeRepository(r)
    c = _Closed()
    os.environ["TRADES_CLOSED_ZSET_INDEX"] = "1"
    try:
        repo.save_closed(c)
        # should appear in both keys: canonical "1m" and legacy "m1"
        zkey_c = "closed_z:s:BTCUSDT:1m:CryptoOrderFlow"
        zkey_l = "closed_z:s:BTCUSDT:m1:CryptoOrderFlow"
        assert "OID1" in r.zrevrange(zkey_c, 0, 10)
        assert "OID1" in r.zrevrange(zkey_l, 0, 10)
    finally:
        os.environ.pop("TRADES_CLOSED_ZSET_INDEX", None)
