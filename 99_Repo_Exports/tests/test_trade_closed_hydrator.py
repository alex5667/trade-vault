from __future__ import annotations

import os
import sys
from pathlib import Path

# Add paths for imports
sys.path.insert(0, str(Path(__file__).parent.parent / "python-worker"))

from _fakes import FakeRedis
from services.trade_closed_hydrator import hydrate_trade_closed, hydrate_trade_closed_batch


def test_hydrate_no_order_id_returns_input():
    r = FakeRedis()
    out = hydrate_trade_closed(r, {"symbol": "BTCUSDT"})
    assert out.get("symbol") == "BTCUSDT"


def test_hydrate_compact_always_reads_hash_and_hash_wins():
    os.environ["TRADES_CLOSED_STREAM_COMPACT"] = "1"
    try:
        r = FakeRedis()
        # stream частичный
        stream = {"order_id": "OID1", "symbol": "BTCUSDT", "exit_ts_ms": "2000", "trail_profile": "rocket_stream"}
        # hash полный/каноничный
        r.hset("order:OID1", mapping={
            "status": "closed",
            "symbol": "BTCUSDT",
            "closed_time": "2000",
            "trailing_profile": "rocket_hash",
        })
        out = hydrate_trade_closed(r, stream, require_closed=True, merge_precedence="hash")
        # hash wins
        assert out["trailing_profile"] == "rocket_hash"
        # алиас должен восстановиться
        assert out["trail_profile"] == "rocket_hash"
        # exit_ts_ms должен существовать даже если был только closed_time
        assert out["exit_ts_ms"] == "2000"
        assert out["closed_time"] == "2000"
    finally:
        os.environ.pop("TRADES_CLOSED_STREAM_COMPACT", None)


def test_hydrate_require_closed_true_does_not_override_when_hash_not_closed():
    os.environ["TRADES_CLOSED_STREAM_COMPACT"] = "1"
    try:
        r = FakeRedis()
        stream = {"order_id": "OID2", "exit_ts_ms": "2000", "trailing_profile": "rocket_stream"}
        r.hset("order:OID2", mapping={
            "status": "open",  # не closed
            "trailing_profile": "rocket_hash",
        })
        out = hydrate_trade_closed(r, stream, require_closed=True, merge_precedence="hash")
        # должен вернуться stream best-effort (не подменяем)
        assert out["trailing_profile"] == "rocket_stream"
        assert out["trail_profile"] == "rocket_stream"
    finally:
        os.environ.pop("TRADES_CLOSED_STREAM_COMPACT", None)


def test_hydrate_non_compact_reads_hash_only_if_missing_critical_fields():
    os.environ["TRADES_CLOSED_STREAM_COMPACT"] = "0"
    try:
        r = FakeRedis()
        # stream уже "полный" по критичным полям
        stream = {"order_id": "OID3", "exit_ts_ms": "2000", "pnl_net": "1.0", "close_reason": "TP"}
        out = hydrate_trade_closed(r, stream)
        assert out["order_id"] == "OID3"
        assert out["exit_ts_ms"] == "2000"

        # stream критичных полей не содержит -> должен подтянуть hash
        stream2 = {"order_id": "OID4", "symbol": "BTCUSDT"}
        r.hset("order:OID4", mapping={"status": "closed", "closed_time": "3000", "pnl_net": "2.0", "close_reason": "SL"})
        out2 = hydrate_trade_closed(r, stream2, require_closed=True)
        assert out2["pnl_net"] == "2.0"
        assert out2["exit_ts_ms"] == "3000"
        assert out2["closed_time"] == "3000"
    finally:
        os.environ.pop("TRADES_CLOSED_STREAM_COMPACT", None)


def test_hydrate_batch_pipelines_hashes_and_normalizes_aliases():
    os.environ["TRADES_CLOSED_STREAM_COMPACT"] = "1"
    try:
        r = FakeRedis()
        r.hset("order:OID10", mapping={"status": "closed", "closed_time": "111", "trailing_profile": "rocket"})
        r.hset("order:OID11", mapping={"status": "closed", "exit_ts_ms": "222", "trail_profile": "v2"})
        items = [
            {"order_id": "OID10", "symbol": "BTCUSDT"},
            {"order_id": "OID11", "symbol": "BTCUSDT"},
        ]
        out = hydrate_trade_closed_batch(r, items, require_closed=True, merge_precedence="hash")
        assert out[0]["exit_ts_ms"] == "111"
        assert out[0]["trail_profile"] == "rocket"
        assert out[0]["trailing_profile"] == "rocket"
        assert out[1]["exit_ts_ms"] == "222"
        assert out[1]["trail_profile"] == "v2"
        assert out[1]["trailing_profile"] == "v2"
    finally:
        os.environ.pop("TRADES_CLOSED_STREAM_COMPACT", None)
