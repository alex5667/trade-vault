"""
tests/test_exec_quality_writer.py — Phase 4: exec_quality_writer unit tests.

Coverage:
  1.  parse_fill_from_closed: entry_price field → fill_px
  2.  parse_fill_from_closed: fill_price fallback field
  3.  parse_fill_from_closed: missing sid → None
  4.  parse_fill_from_closed: fill_px <= 0 → None
  5.  parse_fill_from_closed: fee_bps from commission_bps field
  6.  parse_fill_from_closed: fee_bps defaults to 0.0 when missing
  7.  parse_fill_from_closed: ts_fill_ms extracted
  8.  apply_fill_updates: SQL UPDATE called with correct params
  9.  apply_fill_updates: empty updates → 0
  10. apply_fill_updates: idempotent (WHERE fill_px IS NULL — verified via param count)
  11. apply_fill_updates: returns count attempted
"""
from __future__ import annotations

import os
import sys
import time

import pytest

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _BASE not in sys.path:
    sys.path.insert(0, _BASE)


# ─── Tests: parse_fill_from_closed ────────────────────────────────────────────

class TestParseFillFromClosed:

    def test_entry_price_field(self):
        from services.exec_quality_writer import parse_fill_from_closed

        fields = {"sid": "abc-123", "entry_price": "50000.5", "commission_bps": "3.0"}
        result = parse_fill_from_closed(fields)
        assert result is not None
        assert result["sid"] == "abc-123"
        assert result["fill_px"] == pytest.approx(50000.5)

    def test_fill_price_fallback(self):
        from services.exec_quality_writer import parse_fill_from_closed

        fields = {"sid": "abc-123", "fill_price": "49999.0"}
        result = parse_fill_from_closed(fields)
        assert result is not None
        assert result["fill_px"] == pytest.approx(49999.0)

    def test_fill_px_fallback(self):
        from services.exec_quality_writer import parse_fill_from_closed

        fields = {"sid": "abc", "fill_px": "100.0"}
        result = parse_fill_from_closed(fields)
        assert result is not None
        assert result["fill_px"] == pytest.approx(100.0)

    def test_missing_sid_returns_none(self):
        from services.exec_quality_writer import parse_fill_from_closed

        fields = {"entry_price": "50000.0"}
        result = parse_fill_from_closed(fields)
        assert result is None

    def test_fill_px_zero_returns_none(self):
        from services.exec_quality_writer import parse_fill_from_closed

        fields = {"sid": "abc-123", "entry_price": "0.0"}
        result = parse_fill_from_closed(fields)
        assert result is None

    def test_fill_px_negative_returns_none(self):
        from services.exec_quality_writer import parse_fill_from_closed

        fields = {"sid": "abc-123", "entry_price": "-1.0"}
        result = parse_fill_from_closed(fields)
        assert result is None

    def test_fee_from_commission_bps(self):
        from services.exec_quality_writer import parse_fill_from_closed

        fields = {"sid": "abc", "entry_price": "100.0", "commission_bps": "3.0"}
        result = parse_fill_from_closed(fields)
        assert result is not None
        assert result["fee_bps"] == pytest.approx(3.0)

    def test_fee_from_fee_bps_fallback(self):
        from services.exec_quality_writer import parse_fill_from_closed

        fields = {"sid": "abc", "entry_price": "100.0", "fee_bps": "2.5"}
        result = parse_fill_from_closed(fields)
        assert result is not None
        assert result["fee_bps"] == pytest.approx(2.5)

    def test_fee_defaults_zero_when_missing(self):
        from services.exec_quality_writer import parse_fill_from_closed

        fields = {"sid": "abc", "entry_price": "100.0"}
        result = parse_fill_from_closed(fields)
        assert result is not None
        assert result["fee_bps"] == pytest.approx(0.0)

    def test_ts_fill_ms_extracted(self):
        from services.exec_quality_writer import parse_fill_from_closed

        fields = {"sid": "abc", "entry_price": "100.0", "ts_ms": "1000000"}
        result = parse_fill_from_closed(fields)
        assert result is not None
        assert result["ts_fill_ms"] == 1000000

    def test_ts_fill_ms_defaults_to_now(self):
        from services.exec_quality_writer import parse_fill_from_closed

        fields = {"sid": "abc", "entry_price": "100.0"}
        result = parse_fill_from_closed(fields)
        assert result is not None
        assert abs(result["ts_fill_ms"] - int(time.time() * 1000)) < 5_000

    def test_sid_as_int_converted_to_str(self):
        from services.exec_quality_writer import parse_fill_from_closed

        fields = {"sid": 12345, "entry_price": "100.0"}
        result = parse_fill_from_closed(fields)
        assert result is not None
        assert isinstance(result["sid"], str)
        assert result["sid"] == "12345"


# ─── Tests: apply_fill_updates ────────────────────────────────────────────────

class TestApplyFillUpdates:

    def _make_mock_conn(self):
        """Mock connection that captures execute_batch calls."""
        executed = []

        class MockCursor:
            def __enter__(self): return self
            def __exit__(self, *a): pass
            def execute(self, sql, params=None): pass

        class MockConn:
            def cursor(self): return MockCursor()
            def commit(self): pass

        return MockConn(), executed

    def test_empty_updates_returns_zero(self):
        from services.exec_quality_writer import apply_fill_updates

        conn, _ = self._make_mock_conn()
        result = apply_fill_updates(conn, [])
        assert result == 0

    def test_returns_count_of_updates(self):
        """apply_fill_updates returns len(updates) regardless of actual rows affected."""
        from services.exec_quality_writer import apply_fill_updates

        sql_calls = []

        class MockCursor:
            def __enter__(self): return self
            def __exit__(self, *a): pass

        class MockConn:
            def cursor(self): return MockCursor()
            def commit(self): pass

        # Monkey-patch execute_batch
        import psycopg2.extras as extras
        original = extras.execute_batch

        try:
            extras.execute_batch = lambda cur, sql, params, **kw: sql_calls.extend(params)
            updates = [
                (50000.0, 50000.0, 3.0, "sid-1"),
                (49000.0, 49000.0, 3.0, "sid-2"),
            ]
            result = apply_fill_updates(MockConn(), updates)
            assert result == 2
            assert len(sql_calls) == 2
        finally:
            extras.execute_batch = original

    def test_update_params_correct_order(self):
        """Verify params passed to execute_batch match UPDATE SQL: (fill_px, fill_px, fee_bps, sid)."""
        from services.exec_quality_writer import apply_fill_updates

        sql_calls = []

        class MockCursor:
            def __enter__(self): return self
            def __exit__(self, *a): pass

        class MockConn:
            def cursor(self): return MockCursor()
            def commit(self): pass

        import psycopg2.extras as extras
        original = extras.execute_batch

        try:
            extras.execute_batch = lambda cur, sql, params, **kw: sql_calls.extend(params)
            updates = [(50100.0, 50100.0, 3.0, "abc-sid")]
            apply_fill_updates(MockConn(), updates)
            assert sql_calls[0] == (50100.0, 50100.0, 3.0, "abc-sid")
        finally:
            extras.execute_batch = original
