from __future__ import annotations

import os

from tests._fakes import FakeRedis
from services.trailing_edge_analyzer import TrailingEdgeAnalyzer


def test_trailing_edge_analyzer_compact_stream_hydrates_from_order_hash():
    # Включаем compact-stream: stream может быть частичным
    os.environ["TRADES_CLOSED_STREAM_COMPACT"] = "1"
    try:
        r = FakeRedis()
        # stream сообщение только с order_id (как в compact)
        r.xadd("trades:closed", {"order_id": "OID1", "source": "CryptoOrderFlow", "symbol": "BTCUSDT"})
        # детали лежат в order hash
        r.hset("order:OID1", mapping={
            "status": "closed",
            "exit_ts_ms": "1700000000000",
            "pnl_net": "12.5",
            "source": "CryptoOrderFlow",
            "symbol": "BTCUSDT",
            "trailing_profile": "rocket_v1",
            "trailing_started": "1",
            "trailing_active": "1",
        })

        a = TrailingEdgeAnalyzer(r)
        res = a.analyze_last_trades(source="CryptoOrderFlow", symbol="BTCUSDT", limit=10)
        assert res is not None
        assert len(res.trades) == 1
        t = res.trades[0]
        assert t.pnl_net == 12.5
        assert t.trailing_profile == "rocket_v1"
        assert t.trail_profile == "rocket_v1"
        assert t.trailing_started is True
        assert t.trailing_active is True
    finally:
        os.environ.pop("TRADES_CLOSED_STREAM_COMPACT", None)
