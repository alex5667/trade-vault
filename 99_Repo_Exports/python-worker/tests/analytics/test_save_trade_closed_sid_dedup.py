from __future__ import annotations

"""
P0 regression (2026-05-28): two parallel trade-monitor instances may try to
insert two rows for the same sid (different order_id) when their TP/SL paths
race. The partial UNIQUE INDEX idx_trades_closed_sid_final_uniq enforces one
final row per sid at the DB level; analytics_db.save_trade_closed must:

  - swallow psycopg2.errors.UniqueViolation tagged with that constraint name,
  - bump trades_closed_dedup_skip_total{reason="sid_final"},
  - still call conn.commit() so the connection is returned cleanly to the pool.

Other UniqueViolations (e.g. order_id PK race) must continue to propagate.
"""

import unittest
from contextlib import contextmanager
from unittest.mock import MagicMock, patch

import psycopg2


def _make_closed_trade(sid: str = "SID-DUP-001", order_id: str = "ORD-A"):
    t = MagicMock()
    t.order_id = order_id
    t.sid = sid
    t.strategy = "crypto_of"
    t.source = "CryptoOrderFlow"
    t.symbol = "BTCUSDT"
    t.tf = "1m"
    t.direction = "SHORT"
    t.entry_ts_ms = 1_700_000_000_000
    t.exit_ts_ms = 1_700_000_060_000
    t.entry_price = 70000.0
    t.exit_price = 70700.0
    t.lot = 0.001
    t.notional_usd = 70.0
    t.pnl_net = -5.5
    t.pnl_gross = -5.0
    t.fees = 0.5
    t.pnl_pct = -0.01
    t.pnl_if_fixed_exit = -5.0
    t.tp1_hit = False
    t.tp2_hit = False
    t.tp3_hit = False
    t.tp_hits = 0
    t.tp_before_sl = False
    t.trailing_started = False
    t.trailing_active = False
    t.trailing_moves = 0
    t.mfe_pnl = 0.5
    t.mae_pnl = -5.0
    t.giveback = 0.0
    t.missed_profit = 0.0
    t.one_r_money = 5.0
    t.r_multiple = -1.1
    t.duration_ms = 60_000
    t.close_reason = "SL"
    t.signal_payload = {}
    t.is_final_close = True
    return t


class _FakeUniqueViolation(psycopg2.errors.UniqueViolation):
    """Subclass so we can stub out the read-only `diag` attribute."""

    def __init__(self, constraint_name: str):
        super().__init__("duplicate key value violates unique constraint")
        self._constraint_name = constraint_name

    @property
    def diag(self):  # type: ignore[override]
        d = MagicMock()
        d.constraint_name = self._constraint_name
        return d


def _make_unique_violation(constraint_name: str) -> psycopg2.errors.UniqueViolation:
    return _FakeUniqueViolation(constraint_name)


class _FakeCursor:
    """Records SQL strings, optionally raises a UniqueViolation on main INSERT."""

    def __init__(self, raise_on_main: Exception | None = None):
        self._raise_on_main = raise_on_main
        self.calls: list[str] = []

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass

    def execute(self, sql, params=None):
        s = sql.strip()
        self.calls.append(s[:120])
        upper = s.upper()
        # SAVEPOINT / ROLLBACK / RELEASE are bookkeeping — never raise.
        if any(kw in upper for kw in ("SAVEPOINT", "ROLLBACK TO", "RELEASE")):
            return
        # The first real INSERT we see is the main trades_closed one.
        if (
            self._raise_on_main is not None
            and "INSERT INTO TRADES_CLOSED" in upper
            and "TRADES_CLOSED_P0" not in upper
        ):
            exc = self._raise_on_main
            self._raise_on_main = None  # raise once, then behave normally
            raise exc


class _FakeConn:
    def __init__(self, cursor: _FakeCursor):
        self._cursor = cursor
        self.commits = 0
        self.rollbacks = 0

    def cursor(self):
        return self._cursor

    def __enter__(self):
        return self

    def __exit__(self, *a):
        pass

    def commit(self):
        self.commits += 1

    def rollback(self):
        self.rollbacks += 1


def _patch_get_conn(adb, conn: _FakeConn):
    @contextmanager
    def _ctx():
        yield conn

    return patch.object(adb, "get_conn", lambda: _ctx())


class TestSaveTradeClosedSidDedup(unittest.TestCase):
    def setUp(self):
        import services.analytics_db as adb

        self.adb = adb
        self._orig_p0 = adb.ANALYTICS_P0_ENABLED
        adb.ANALYTICS_P0_ENABLED = False  # focus on main insert path

    def tearDown(self):
        self.adb.ANALYTICS_P0_ENABLED = self._orig_p0

    def test_skips_on_sid_unique_violation(self):
        """UniqueViolation on idx_trades_closed_sid_final_uniq must NOT raise."""
        uv = _make_unique_violation("idx_trades_closed_sid_final_uniq")
        cursor = _FakeCursor(raise_on_main=uv)
        conn = _FakeConn(cursor)

        with _patch_get_conn(self.adb, conn):
            # Must return cleanly, not raise.
            self.adb.save_trade_closed(_make_closed_trade())

        # The SAVEPOINT around the main insert was created and rolled back.
        joined = " | ".join(cursor.calls).upper()
        assert "SAVEPOINT TRADES_CLOSED_MAIN_INSERT" in joined
        assert "ROLLBACK TO SAVEPOINT TRADES_CLOSED_MAIN_INSERT" in joined
        # Commit still called so the connection is released cleanly.
        assert conn.commits == 1, f"expected 1 commit, got {conn.commits}"

    def test_propagates_unknown_unique_violation(self):
        """UniqueViolation on an unrelated constraint must still propagate."""
        uv = _make_unique_violation("trades_closed_order_id_key")
        cursor = _FakeCursor(raise_on_main=uv)
        conn = _FakeConn(cursor)

        with _patch_get_conn(self.adb, conn):
            with self.assertRaises(psycopg2.errors.UniqueViolation):
                self.adb.save_trade_closed(_make_closed_trade(order_id="ORD-DUP"))

    def test_writer_disabled_short_circuits(self):
        """ANALYTICS_DB_WRITE_ENABLED=0 must skip the entire DB call."""
        cursor = _FakeCursor(raise_on_main=None)
        conn = _FakeConn(cursor)

        orig = self.adb.ANALYTICS_DB_WRITE_ENABLED
        self.adb.ANALYTICS_DB_WRITE_ENABLED = False
        try:
            with _patch_get_conn(self.adb, conn):
                self.adb.save_trade_closed(
                    _make_closed_trade(sid="SID-TM2", order_id="ORD-TM2")
                )
        finally:
            self.adb.ANALYTICS_DB_WRITE_ENABLED = orig

        # Nothing executed, nothing committed — pure short-circuit.
        assert cursor.calls == [], f"expected no SQL, got {cursor.calls}"
        assert conn.commits == 0

    def test_happy_path_releases_savepoint(self):
        """No conflict: SAVEPOINT is released and commit happens."""
        cursor = _FakeCursor(raise_on_main=None)
        conn = _FakeConn(cursor)

        with _patch_get_conn(self.adb, conn):
            self.adb.save_trade_closed(_make_closed_trade(sid="SID-CLEAN", order_id="ORD-CLEAN"))

        joined = " | ".join(cursor.calls).upper()
        assert "SAVEPOINT TRADES_CLOSED_MAIN_INSERT" in joined
        assert "RELEASE SAVEPOINT TRADES_CLOSED_MAIN_INSERT" in joined
        assert "ROLLBACK TO SAVEPOINT TRADES_CLOSED_MAIN_INSERT" not in joined
        assert conn.commits == 1


if __name__ == "__main__":
    unittest.main()
