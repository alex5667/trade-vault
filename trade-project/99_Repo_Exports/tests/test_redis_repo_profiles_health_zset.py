from __future__ import annotations

import os
import sys
from pathlib import Path
from dataclasses import dataclass

# Add python-worker to path
sys.path.insert(0, str(Path(__file__).parent.parent / "python-worker"))

from infra.redis_repo import RedisTradeRepository, TRADES_CLOSED_STREAM_NAME
from _fakes import FakeRedis


@dataclass
class Pos:
    id: str
    sid: str = "S1"
    strategy: str = "strat"
    source: str = "Binance"
    symbol: str = "BTCUSDT"
    tf: str = "1m"
    direction: str = "LONG"
    entry_price: float = 100.0
    entry_ts_ms: int = 1000
    lot: float = 1.0
    remaining_qty: float = 1.0
    sl: float = 90.0
    tp_levels: list = None
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

    def __post_init__(self):
        if self.tp_levels is None:
            self.tp_levels = [110.0, 120.0, 130.0]


@dataclass
class Closed:
    schema_version: int = 1
    order_id: str = "OID1"
    sid: str = "S1"
    strategy: str = "strat"
    source: str = "Binance"
    symbol: str = "BTCUSDT"
    tf: str = "1m"
    direction: str = "LONG"
    entry_ts_ms: int = 1000
    exit_ts_ms: int = 2000
    entry_price: float = 100.0
    exit_price: float = 105.0
    lot: float = 1.0
    notional_usd: float = 100.0
    pnl_net: float = 5.0
    pnl_gross: float = 5.2
    fees: float = 0.2
    pnl_pct: float = 0.05
    tp1_hit: bool = True
    tp2_hit: bool = False
    tp3_hit: bool = False
    tp_hits: int = 1
    tp_before_sl: int = 1
    trailing_started: bool = True
    trailing_active: bool = True
    trailing_moves: int = 2
    mfe_pnl: float = 6.0
    mae_pnl: float = -1.0
    giveback: float = 1.0
    missed_profit: float = 0.5
    one_r_money: float = 2.0
    r_multiple: float = 2.5
    duration_ms: int = 1000
    pnl_if_fixed_exit: float = 4.0
    close_reason: str = "TP1"
    close_reason_raw: str = "TP1"
    close_reason_detail: str = "TRAILING_PROFIT"
    baseline_exit_reason: str = ""
    baseline_exit_ts_ms: int = 0
    baseline_exit_price: float = 0.0
    entry_tag: str = ""
    max_favorable_price: float = 108.0
    max_favorable_ts: int = 1500
    is_final_close: bool = True
    remaining_qty: float = 0.0
    status: str = "CLOSED"
    trailing_profile: str = "rocket_v1"
    trailing_min_lock_r: float = 0.25
    min_lock_price: float = 0.0


def test_save_open_writes_profile_aliases():
    r = FakeRedis()
    repo = RedisTradeRepository(r)
    repo.save_open(Pos(id="OID1"))
    h = r.hgetall("order:OID1")
    assert h["trail_profile"] == "rocket_v1"
    assert h["trailing_profile"] == "rocket_v1"


def test_save_closed_writes_profile_aliases_and_health_snapshot():
    r = FakeRedis()

    def provider(symbol: str):
        assert symbol == "BTCUSDT"
        return {"l2_stale_ratio_tick": "0.123", "avg_l2_age_ms": "45.0"}

    repo = RedisTradeRepository(r, health_provider=provider)
    c = Closed(order_id="OID1")
    repo.save_closed(c)

    # order hash updated with both keys
    h = r.hgetall("order:OID1")
    assert h["trailing_profile"] == "rocket_v1"
    assert h["trail_profile"] == "rocket_v1"

    # stream got health_* keys (full mode by default)
    ev = r.streams[TRADES_CLOSED_STREAM_NAME][-1]
    assert ev["health_l2_stale_ratio_tick"] == "0.123"
    assert ev["health_avg_l2_age_ms"] == "45.0"


def test_zset_index_and_queries():
    os.environ["ENABLE_CLOSED_ZSET_INDEX"] = "1"
    try:
        r = FakeRedis()
        repo = RedisTradeRepository(r)

        c1 = Closed(order_id="A", exit_ts_ms=1000)
        c2 = Closed(order_id="B", exit_ts_ms=2000)
        c3 = Closed(order_id="C", exit_ts_ms=3000)
        repo.save_closed(c1)
        repo.save_closed(c2)
        repo.save_closed(c3)

        rows = repo.get_closed_by_time(strategy="strat", symbol="BTCUSDT", tf="1m", from_ts_ms=1500, to_ts_ms=3500)
        ids = [x.get("id") or x.get("order_id") for x in rows]
        # save_closed пишет в order hash "status=closed", но id там нет всегда; для теста проверяем что hashes существуют:
        assert r.hgetall("order:B")["status"] == "closed"
        assert r.hgetall("order:C")["status"] == "closed"

        last2 = repo.get_closed_last_n(strategy="strat", symbol="BTCUSDT", tf="1m", n=2, with_hash=False)
        assert [x["order_id"] for x in last2] == ["C", "B"]
    finally:
        os.environ.pop("ENABLE_CLOSED_ZSET_INDEX", None)


def test_compact_stream_mode():
    os.environ["TRADES_CLOSED_STREAM_COMPACT"] = "1"
    try:
        r = FakeRedis()
        repo = RedisTradeRepository(r)
        c = Closed(order_id="OID1")
        repo.save_closed(c)
        ev = r.streams[TRADES_CLOSED_STREAM_NAME][-1]
        # minimal обязательные поля есть
        assert ev["order_id"] == "OID1"
        assert ev["symbol"] == "BTCUSDT"
        assert ev["exit_ts_ms"] == "2000"
        # а вот "тяжелых" полей в compact может не быть (например pnl_gross)
        assert "pnl_gross" not in ev
    finally:
        os.environ.pop("TRADES_CLOSED_STREAM_COMPACT", None)
