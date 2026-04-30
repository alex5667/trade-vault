"""
P0-3 regression: trades_closed_p0 failure must NOT abort the main trades_closed insert.

Uses a fake psycopg2 cursor that raises on the second execute() (P0 insert)
to confirm that the SAVEPOINT mechanism prevents the main transaction from
being rolled back.
"""
from __future__ import annotations

import sys
import types
import unittest
from unittest.mock import MagicMock, patch, call


def _make_closed_trade():
    """Minimal TradeClosed-like object with required fields."""
    t = MagicMock()
    t.order_id = "ORD-001"
    t.sid = "SID-001"
    t.strategy = "crypto_of"
    t.source = "CryptoOrderFlow"
    t.symbol = "BTCUSDT"
    t.tf = "1m"
    t.direction = "LONG"
    t.entry_ts_ms = 1_700_000_000_000
    t.exit_ts_ms = 1_700_000_060_000
    t.entry_price = 50000.0
    t.exit_price = 50500.0
    t.lot = 0.01
    t.notional_usd = 500.0
    t.pnl_net = 5.0
    t.pnl_gross = 6.0
    t.fees = 1.0
    t.pnl_pct = 0.01
    t.pnl_if_fixed_exit = 5.0
    t.tp1_hit = True
    t.tp2_hit = False
    t.tp3_hit = False
    t.tp_hits = 1
    t.tp_before_sl = True
    t.trailing_started = False
    t.trailing_active = False
    t.trailing_moves = 0
    t.mfe_pnl = 7.0
    t.mae_pnl = -1.0
    t.giveback = 2.0
    t.missed_profit = 0.0
    t.one_r_money = 5.0
    t.r_multiple = 1.0
    t.duration_ms = 60_000
    t.close_reason = "TP1"
    t.signal_payload = {}
    t.is_final_close = True
    return t


class TestSaveTradeClosedSavepoint(unittest.TestCase):
    """P0-3: SAVEPOINT protects main insert from P0 upsert failure."""

    def _patch_module(self, p0_fails: bool = False):
        """Return a patcher context that installs a fake DB connection."""
        execute_calls = []
        commit_calls = []
        rollback_calls = []

        _p0_insert_count = [0]

        class FakeCursor:
            def __enter__(self):
                return self

            def __exit__(self, *a):
                pass

            def execute(self, sql, params=None):
                stripped = sql.strip()[:80]
                execute_calls.append(stripped)
                # Only raise on the actual INSERT into trades_closed_p0 —
                # not on SAVEPOINT / ROLLBACK TO SAVEPOINT / RELEASE SAVEPOINT.
                is_savepoint_ctrl = any(kw in stripped.upper() for kw in (
                    "SAVEPOINT", "ROLLBACK TO", "RELEASE"
                ))
                if p0_fails and "trades_closed_p0" in sql and not is_savepoint_ctrl:
                    _p0_insert_count[0] += 1
                    raise Exception("P0 upsert simulated failure")

        class FakeConn:
            def cursor(self):
                return FakeCursor()

            def __enter__(self):
                return self

            def __exit__(self, *a):
                pass

            def commit(self):
                commit_calls.append("commit")

            def rollback(self):
                rollback_calls.append("rollback")

        return FakeConn(), execute_calls, commit_calls

    def test_main_insert_committed_when_p0_fails(self):
        import services.analytics_db as adb

        # Ensure P0 is enabled and hard-fail is off
        original_p0 = adb.ANALYTICS_P0_ENABLED
        original_hard = adb.ANALYTICS_P0_HARD_FAIL
        adb.ANALYTICS_P0_ENABLED = True
        adb.ANALYTICS_P0_HARD_FAIL = False

        fake_conn, execute_calls, commit_calls = self._patch_module(p0_fails=True)

        closed = _make_closed_trade()

        def _fake_get_conn():
            from contextlib import contextmanager
            @contextmanager
            def _ctx():
                yield fake_conn
            return _ctx()

        with patch.object(adb, "get_conn", _fake_get_conn):
            # Should NOT raise — SAVEPOINT swallows P0 error
            adb.save_trade_closed(closed)

        # SAVEPOINT SQL must appear
        sp_sqls = [s for s in execute_calls if "SAVEPOINT" in s.upper()]
        rollback_sp = [s for s in execute_calls if "ROLLBACK TO SAVEPOINT" in s.upper()]
        assert sp_sqls, f"SAVEPOINT not found in execute calls: {execute_calls}"
        assert rollback_sp, f"ROLLBACK TO SAVEPOINT not found: {execute_calls}"

        # Commit must still be called
        assert commit_calls, "conn.commit() was not called — main insert lost!"

        adb.ANALYTICS_P0_ENABLED = original_p0
        adb.ANALYTICS_P0_HARD_FAIL = original_hard

    def test_main_insert_committed_when_p0_succeeds(self):
        import services.analytics_db as adb

        original_p0 = adb.ANALYTICS_P0_ENABLED
        adb.ANALYTICS_P0_ENABLED = True

        fake_conn, execute_calls, commit_calls = self._patch_module(p0_fails=False)
        closed = _make_closed_trade()

        def _fake_get_conn():
            from contextlib import contextmanager
            @contextmanager
            def _ctx():
                yield fake_conn
            return _ctx()

        with patch.object(adb, "get_conn", _fake_get_conn):
            adb.save_trade_closed(closed)

        # SAVEPOINT appears, RELEASE appears (no rollback)
        sp_sqls = [s for s in execute_calls if "SAVEPOINT" in s.upper()]
        release_sqls = [s for s in execute_calls if "RELEASE SAVEPOINT" in s.upper()]
        rollback_sp = [s for s in execute_calls if "ROLLBACK TO SAVEPOINT" in s.upper()]
        assert sp_sqls
        assert release_sqls
        assert not rollback_sp, f"Unexpected rollback on successful P0: {rollback_sp}"
        assert commit_calls

        adb.ANALYTICS_P0_ENABLED = original_p0


if __name__ == "__main__":
    unittest.main()
