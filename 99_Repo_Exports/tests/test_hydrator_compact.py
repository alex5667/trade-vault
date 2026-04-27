from __future__ import annotations

import os
import sys
from pathlib import Path
# Add python-worker to path
sys.path.insert(0, str(Path(__file__).parent.parent / "python-worker"))

from _fakes import FakeRedis
from services.trade_closed_hydrator import hydrate_trade_closed


def test_hydrate_compact_stream_loads_order_hash():
    os.environ["TRADES_CLOSED_STREAM_COMPACT"] = "1"
    try:
        r = FakeRedis()
        # stream поля минимальные
        fields = {"order_id": "OID1", "symbol": "BTCUSDT", "exit_ts_ms": "2000"}
        # order hash полный
        r.hset("order:OID1", mapping={
            "status": "closed",
            "closed_time": "2000",
            "pnl_net": "5.0",
            "close_reason_norm": "TP1",
            "trailing_profile": "rocket_v1",
        })
        t = hydrate_trade_closed(r, fields, require_closed=True, merge_precedence="hash")
        assert t["order_id"] == "OID1"
        assert t["pnl_net"] == "5.0"
        assert t["close_reason_norm"] == "TP1"
        assert t["trailing_profile"] == "rocket_v1"
    finally:
        os.environ.pop("TRADES_CLOSED_STREAM_COMPACT", None)


def test_hydrate_non_compact_missing_fields_still_loads_hash():
    os.environ.pop("TRADES_CLOSED_STREAM_COMPACT", None)
    r = FakeRedis()
    fields = {"order_id": "OID2"}  # совсем пусто
    r.hset("order:OID2", mapping={"status": "closed", "pnl_net": "1.0"})
    t = hydrate_trade_closed(r, fields, require_closed=True, merge_precedence="hash")
    assert t["pnl_net"] == "1.0"
