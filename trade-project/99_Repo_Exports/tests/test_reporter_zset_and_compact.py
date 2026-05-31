from __future__ import annotations

import os

from tests._fakes import FakeRedis
from services.periodic_reporter import PeriodicReporter


class DummyTM:
    def new_metrics(self):
        # минимальный контракт для _gather_window_metrics_stream
        return {"total_trades": 0}

    def finalize(self, m):
        return m


class TestReporter(PeriodicReporter):
    def __init__(self, redis):
        self.redis = redis
        self.tm = DummyTM()

    def _add_health_metrics(self, m, source, symbol):
        return

    def _accumulate_trade_metrics(self, m, t):
        m["total_trades"] = int(m.get("total_trades") or 0) + 1


def test_reporter_reads_from_zset_first():
    os.environ["PERIODIC_REPORT_USE_ZSET"] = "1"
    os.environ["ENABLE_CLOSED_ZSET_INDEX"] = "1"
    os.environ["PERIODIC_REPORT_TRADE_WINDOW_COUNT"] = "2"  # count-mode => cutoff_ms=0
    try:
        r = FakeRedis()
        rep = TestReporter(r)

        # order hash (то, что reporter должен брать через pipeline)
        r.hset("order:OID1", mapping={
            "status": "closed",
            "closed_time": "1000",
            "source": "CryptoOrderFlow",
            "strategy": "crypto_orderflow",
            "symbol": "BTCUSDT",
        })
        r.hset("order:OID2", mapping={
            "status": "closed",
            "closed_time": "2000",
            "source": "CryptoOrderFlow",
            "strategy": "crypto_orderflow",
            "symbol": "BTCUSDT",
        })
        # zset ключ как в periodic_reporter._closed_zkey()
        r.zadd("closed_z:crypto_orderflow:BTCUSDT:tick:CryptoOrderFlow", {"OID1": 1000, "OID2": 2000})

        m = rep._gather_window_metrics_stream("CryptoOrderFlow", "BTCUSDT")
        assert m["total_trades"] == 2
    finally:
        for k in ("PERIODIC_REPORT_USE_ZSET", "ENABLE_CLOSED_ZSET_INDEX", "PERIODIC_REPORT_TRADE_WINDOW_COUNT"):
            os.environ.pop(k, None)


def test_reporter_handles_compact_stream_via_hydrate():
    os.environ["TRADES_CLOSED_STREAM_COMPACT"] = "1"
    os.environ["PERIODIC_REPORT_USE_ZSET"] = "0"
    os.environ["PERIODIC_REPORT_TRADE_WINDOW_COUNT"] = "2"
    try:
        r = FakeRedis()
        rep = TestReporter(r)

        # stream запись минимальная
        r.xadd("trades:closed", {"order_id": "OIDX", "source": "CryptoOrderFlow", "symbol": "BTCUSDT", "exit_ts_ms": "2000"})
        # детали лежат в order hash
        r.hset("order:OIDX", mapping={
            "status": "closed",
            "closed_time": "2000",
            "source": "CryptoOrderFlow",
            "strategy": "crypto_orderflow",
            "symbol": "BTCUSDT",
        })

        m = rep._gather_window_metrics_stream("CryptoOrderFlow", "BTCUSDT")
        assert m["total_trades"] == 1
    finally:
        for k in ("TRADES_CLOSED_STREAM_COMPACT", "PERIODIC_REPORT_USE_ZSET", "PERIODIC_REPORT_TRADE_WINDOW_COUNT"):
            os.environ.pop(k, None)
