import asyncio
import json
import sqlite3
import sys
import unittest
from pathlib import Path

# Ensure python-worker takes priority so we import the A4 writer (DLQ/PEL/thread-safety).
# tests/ lives at scanner_infra/tests/ — go up one level to scanner_infra, then down into python-worker.
_TESTS_DIR = Path(__file__).resolve().parent          # scanner_infra/tests/
_REPO_ROOT = _TESTS_DIR.parent                        # scanner_infra/
_PYWORKER = _REPO_ROOT / "python-worker"              # scanner_infra/python-worker/

for _p in (str(_PYWORKER), str(_REPO_ROOT)):
    if _p not in sys.path:
        sys.path.insert(0, _p)

from services.posttrade.decision_snapshot_db import SQLiteDecisionSnapshotDB
from services.posttrade.decision_snapshot_writer import (
    DecisionSnapshotWriterConfig,
    DecisionSnapshotStreamWorker,
)

class FakeRedis:
    """Minimal Redis Streams stub for integration smoke test.

    Implements only the subset used by DecisionSnapshotStreamWorker:
    - xgroup_create
    - xreadgroup
    - xack
    """

    def __init__(self, stream: str, entries):
        self.stream = stream
        self.entries = entries[:]  # list of (id, fields)
        self.acked = []
        self.created = False

    async def xgroup_create(self, name, groupname, id, mkstream):
        self.created = True
        return True

    async def xreadgroup(self, groupname, consumername, streams, count, block):
        # streams is dict {stream: ">"}
        if not self.entries:
            return []
        batch = self.entries[:count]
        self.entries = self.entries[count:]
        return [(self.stream, batch)]

    async def xack(self, stream, group, *ids):
        self.acked.extend(list(ids))
        return len(ids)

class DecisionSnapshotWriterIntegrationTest(unittest.TestCase):
    def test_fake_redis_to_sqlite_idempotent(self):
        # check_same_thread=False is safe for this test: single-test isolation, no concurrent writers.
        # Without it, asyncio.to_thread() raises sqlite3.ProgrammingError on the SQLite connection.
        conn = sqlite3.connect(":memory:", check_same_thread=False)
        db = SQLiteDecisionSnapshotDB(conn=conn)
        db.ensure_schema()

        evt = {
            "schema_version": 1,
            "producer": "python-worker",
            "sid": "S1",
            "symbol": "BTCUSDT",
            "venue": "binance",
            "session": "utc",
            "tf": "1m",
            "kind": "breakout",
            "side": "LONG",
            "direction": "LONG",
            "decision_ts_ms": 1710000000000,
            "decision_bid": 100.0,
            "decision_ask": 100.1,
            "decision_mid": 100.05,
            "decision_spread_bps": 10.0,
            "tca_ready": True,
            "book_sanity_flags": [],
        }
        fields = {b"payload": json.dumps(evt).encode("utf-8")}
        fake = FakeRedis("events:decision_snapshot", [("1-0", fields), ("1-0", fields)])  # duplicate

        cfg = DecisionSnapshotWriterConfig()
        cfg.stream = "events:decision_snapshot"
        cfg.group = "decision_snapshot_writer"
        cfg.consumer = "test"
        cfg.batch_size = 10
        cfg.block_ms = 1
        cfg.upsert_chunk = 100
        cfg.log_every_n = 0

        worker = DecisionSnapshotStreamWorker(cfg=cfg, redis=fake, db=db)

        async def run():
            await worker.ensure_group()
            await worker.run_once()
            await worker.run_once()

        asyncio.run(run())

        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM decision_snapshot")
        n = cur.fetchone()[0]
        self.assertEqual(n, 1, "duplicate events must upsert into a single row")
        self.assertGreaterEqual(len(fake.acked), 2, "entries must be acked")


class DecisionSnapshotWriterDSNPriorityTest(unittest.TestCase):
    """A4 v3 — verify DSN env-var priority chain: TRADES_DB_DSN takes precedence."""

    def _make_cfg(self, env: dict) -> "DecisionSnapshotWriterConfig":
        """Build a fresh DecisionSnapshotWriterConfig with the given ENV overrides."""
        import importlib
        import os

        old = {k: os.environ.get(k) for k in env}
        try:
            for k, v in env.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v
            # Re-import to force dataclass defaults to re-evaluate _env() calls.
            import services.posttrade.decision_snapshot_writer as mod
            importlib.reload(mod)
            return mod.DecisionSnapshotWriterConfig()
        finally:
            for k, v in old.items():
                if v is None:
                    os.environ.pop(k, None)
                else:
                    os.environ[k] = v

    def test_trades_db_dsn_takes_priority(self):
        """TRADES_DB_DSN must win over TIMESCALE_DSN when both are set (A4 v3)."""
        cfg = self._make_cfg({
            "TRADES_DB_DSN": "postgresql://trading:pw@pg:5432/scanner_analytics",
            "TIMESCALE_DSN": "postgresql://old:old@pg:5432/old",
            "DATABASE_URL": "",
            "ANALYTICS_DB_DSN": "",
            "ANALYTICS_DSN": "",
            "PG_DSN": "",
        })
        self.assertEqual(cfg.timescale_dsn, "postgresql://trading:pw@pg:5432/scanner_analytics",
                         "TRADES_DB_DSN must take priority over TIMESCALE_DSN")

    def test_timescale_dsn_fallback(self):
        """When TRADES_DB_DSN is absent, TIMESCALE_DSN is used."""
        cfg = self._make_cfg({
            "TRADES_DB_DSN": "",
            "TIMESCALE_DSN": "postgresql://ts:ts@pg:5432/ts_db",
            "DATABASE_URL": "",
            "ANALYTICS_DB_DSN": "",
            "ANALYTICS_DSN": "",
            "PG_DSN": "",
        })
        self.assertEqual(cfg.timescale_dsn, "postgresql://ts:ts@pg:5432/ts_db",
                         "TIMESCALE_DSN must be used when TRADES_DB_DSN is unset")

    def test_database_url_last_fallback(self):
        """DATABASE_URL is the last resort in the fallback chain."""
        cfg = self._make_cfg({
            "TRADES_DB_DSN": "",
            "TIMESCALE_DSN": "",
            "ANALYTICS_DB_DSN": "",
            "ANALYTICS_DSN": "",
            "PG_DSN": "",
            "DATABASE_URL": "postgresql://url:url@pg:5432/url_db",
        })
        self.assertEqual(cfg.timescale_dsn, "postgresql://url:url@pg:5432/url_db",
                         "DATABASE_URL must be used as last fallback")

    def test_empty_dsn_gives_empty_string(self):
        """When no DSN env var is set, timescale_dsn must be empty string (not None)."""
        cfg = self._make_cfg({
            "TRADES_DB_DSN": "",
            "TIMESCALE_DSN": "",
            "ANALYTICS_DB_DSN": "",
            "ANALYTICS_DSN": "",
            "PG_DSN": "",
            "DATABASE_URL": "",
        })
        self.assertIsInstance(cfg.timescale_dsn, str)
        self.assertEqual(cfg.timescale_dsn, "")
