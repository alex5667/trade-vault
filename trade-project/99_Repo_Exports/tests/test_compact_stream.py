from __future__ import annotations

import os
from dataclasses import dataclass

from infra.redis_repo import RedisTradeRepository
from services.trade_closed_hydrator import hydrate_trade_closed
from tests.fake_redis import FakeRedis


@dataclass
class Closed:
    schema_version: int = 1
    order_id: str = "OID-C"
    sid: str = "SID-C"
    strategy: str = "aggregated"
    source: str = "AggregatedHub-V2"
    symbol: str = "XAUUSD"
    tf: str = "tick"
    direction: str = "LONG"
    entry_ts_ms: int = 1700000000000
    exit_ts_ms: int = 1700000005000
    entry_price: float = 4522.0
    exit_price: float = 4525.0
    lot: float = 0.01
    notional_usd: float = 45.0
    pnl_net: float = 3.43
    pnl_gross: float = 3.43
    fees: float = 0.0
    pnl_pct: float = 0.01
    tp1_hit: bool = True
    tp2_hit: bool = False
    tp3_hit: bool = False
    tp_hits: int = 1
    tp_before_sl: int = 0
    trailing_started: bool = True
    trailing_active: bool = True
    trailing_moves: int = 2
    mfe_pnl: float = 3.8
    mae_pnl: float = -0.2
    giveback: float = 0.5
    missed_profit: float = 0.0
    one_r_money: float = 1.0
    r_multiple: float = 3.43
    duration_ms: int = 5000
    pnl_if_fixed_exit: float = 2.9
    close_reason: str = "TP1"
    close_reason_raw: str = "TP1"
    close_reason_detail: str = "TRAILING_PROFIT"
    baseline_exit_reason: str = "TP1"
    baseline_exit_ts_ms: int = 0
    baseline_exit_price: float = 0.0
    entry_tag: str = ""
    max_favorable_price: float = 4526.0
    max_favorable_ts: int = 1700000004500
    is_final_close: bool = True
    remaining_qty: float = 0.0
    status: str = "CLOSED"
    trailing_profile: str = "rocket_v1"
    trailing_min_lock_r: float = 0.25
    min_lock_price: float = 0.0


def test_compact_stream_writes_minimal_payload_and_hydrates_full_from_order_hash():
    r = FakeRedis()
    repo = RedisTradeRepository(r)

    os.environ["TRADES_CLOSED_STREAM_COMPACT"] = "1"
    try:
        c = Closed()
        # имитируем наличие health snapshot, который должен попасть в order hash
        c._health_snapshot = {"l2_stale_ratio_tick": "0.100000", "avg_l2_age_ms": "12.34"}  # type: ignore[attr-defined]

        repo.save_closed(c)

        # 1) order hash должен содержать ДЕТАЛИ
        h = r.hgetall("order:OID-C")
        assert h.get("status") == "closed"
        assert h.get("pnl_net") == "3.43"
        assert h.get("mfe_pnl") == "3.8"
        # trail/trailing aliases
        assert h.get("trailing_profile") == "rocket_v1"
        assert h.get("trail_profile") == "rocket_v1"
        # health prefixed
        assert h.get("health_l2_stale_ratio_tick") == "0.100000"
        assert h.get("health_avg_l2_age_ms") == "12.34"

        # 2) stream payload должен быть МИНИМАЛЬНЫМ (без тяжёлых полей)
        stream = r._streams.get("trades:closed") or []
        assert len(stream) == 1
        f = stream[0]
        assert f.get("order_id") == "OID-C"
        assert f.get("symbol") == "XAUUSD"
        assert f.get("source") == "AggregatedHub-V2"
        # тяжёлые поля не должны попадать в stream в compact режиме
        assert "mfe_pnl" not in f
        assert "mae_pnl" not in f
        assert "giveback" not in f
        assert "health_l2_stale_ratio_tick" not in f

        # 3) hydrate по stream должен вернуть ПОЛНЫЙ dict из order hash
        full = hydrate_trade_closed(r, f, require_closed=True, merge_precedence="hash")
        assert full.get("order_id") == "OID-C"
        assert full.get("pnl_net") == "3.43"
        assert full.get("mfe_pnl") == "3.8"
        # health должен быть доступен из order hash
        assert full.get("health_l2_stale_ratio_tick") == "0.100000"
    finally:
        os.environ.pop("TRADES_CLOSED_STREAM_COMPACT", None)
