from __future__ import annotations

"""Unit tests for services.db_batch_writer.AsyncBatchWriter.

All tests run without a real PostgreSQL database: psycopg2 is mocked
so that we can verify batching logic, retry behaviour, Prometheus counters,
and shutdown/drain semantics without any external dependencies.
"""

import time
from unittest.mock import MagicMock, patch
import contextlib

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_writer(**kwargs):
    """Create an AsyncBatchWriter with a fake pool (no real DB needed)."""
    from services.db_batch_writer import AsyncBatchWriter

    defaults = dict(
        table="test_table",
        columns=["col_a", "col_b"],
        dsn="postgresql://fake/db",
        batch_size=5,
        flush_interval_s=60.0,  # long interval — flush only via size or flush_now()
        on_conflict_sql="ON CONFLICT DO NOTHING",
        max_retries=1,
    )
    defaults.update(kwargs)
    writer = AsyncBatchWriter(**defaults)
    return writer


def _mock_pool(execute_side_effect=None):
    """Return a mock pool and mock connection/cursor."""
    mock_cursor = MagicMock()
    if execute_side_effect is not None:
        mock_cursor.executemany.side_effect = execute_side_effect

    mock_conn = MagicMock()
    mock_conn.cursor.return_value.__enter__ = MagicMock(return_value=mock_cursor)
    mock_conn.cursor.return_value.__exit__ = MagicMock(return_value=False)

    mock_pool = MagicMock()
    mock_pool.getconn.return_value = mock_conn

    return mock_pool, mock_conn, mock_cursor


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

class TestAsyncBatchWriterEnqueueFlush:
    """Basic enqueue / flush mechanics."""

    def test_enqueue_and_flush_now(self):
        """flush_now() drains the queue and calls executemany once with all rows."""
        writer = _make_writer()
        pool, conn, cursor = _mock_pool()
        writer._pool = pool
        writer.start()  # start background thread so enqueue goes to queue (not direct flush)

        try:
            for i in range(3):
                writer.enqueue({"col_a": i, "col_b": f"v{i}"})

            flushed = writer.flush_now()
            assert flushed == 3
            cursor.executemany.assert_called_once()
            # Check correct number of params rows
            call_args = cursor.executemany.call_args[0]  # positional args: (sql, rows)
            assert len(call_args[1]) == 3
        finally:
            writer.shutdown()


    def test_flush_now_empty_queue_returns_zero(self):
        writer = _make_writer()
        pool, conn, cursor = _mock_pool()
        writer._pool = pool
        assert writer.flush_now() == 0
        cursor.executemany.assert_not_called()

    def test_auto_flush_by_size(self):
        """Background thread should flush when queue reaches batch_size."""
        writer = _make_writer(batch_size=3, flush_interval_s=60.0)
        pool, conn, cursor = _mock_pool()
        writer._pool = pool
        writer.start()

        try:
            for i in range(3):
                writer.enqueue({"col_a": i, "col_b": "x"})
            # Give the background thread time to flush
            deadline = time.monotonic() + 2.0
            while cursor.executemany.call_count == 0 and time.monotonic() < deadline:
                time.sleep(0.05)
            assert cursor.executemany.call_count >= 1
        finally:
            writer.shutdown()

    def test_shutdown_flushes_pending(self):
        """shutdown() must drain the queue even if batch_size not reached."""
        writer = _make_writer(batch_size=100, flush_interval_s=60.0)
        pool, conn, cursor = _mock_pool()
        writer._pool = pool
        writer.start()

        writer.enqueue({"col_a": 1, "col_b": "pending"})
        writer.shutdown()

        # After shutdown, executemany should have been called at least once
        assert cursor.executemany.call_count >= 1


class TestAsyncBatchWriterSQLGeneration:
    """Verify the generated INSERT SQL is correct."""

    def test_sql_contains_table_and_columns(self):
        writer = _make_writer(
            table="execution_order_events",
            columns=["sid", "event_type", "event_ts_ms"],
            on_conflict_sql="",
        )
        assert "INSERT INTO execution_order_events" in writer._sql
        assert "(sid, event_type, event_ts_ms)" in writer._sql
        assert "%(sid)s" in writer._sql
        assert "%(event_type)s" in writer._sql

    def test_on_conflict_appended(self):
        writer = _make_writer(on_conflict_sql="ON CONFLICT DO NOTHING")
        assert "ON CONFLICT DO NOTHING" in writer._sql


class TestAsyncBatchWriterRetry:
    """Retry and Prometheus counter behaviour on DB errors."""

    def test_flush_failure_retries_and_increments_counter(self):
        """On DB error, retry up to max_retries and increment Prometheus counter."""
        writer = _make_writer(max_retries=2, flush_interval_s=60.0)
        pool, conn, cursor = _mock_pool(execute_side_effect=Exception("DB down"))
        writer._pool = pool

        from services import db_batch_writer as bw_mod

        # Patch the Prometheus counter
        mock_counter_labels = MagicMock()
        mock_counter = MagicMock()
        mock_counter.labels.return_value = mock_counter_labels

        with patch.object(bw_mod, "_FLUSH_FAIL", mock_counter):
            # _flush_direct should NOT raise — it logs and increments counter
            writer._flush_direct([{"col_a": 1, "col_b": "x"}])

        assert cursor.executemany.call_count == 2  # tried max_retries times
        mock_counter.labels.assert_called_with(table="test_table")
        mock_counter_labels.inc.assert_called_once()

    def test_no_crash_when_dsn_empty(self):
        """Writer with empty DSN should not crash on flush attempt."""
        writer = _make_writer(dsn="")
        # _get_pool returns None, _flush_direct falls through gracefully
        writer._flush_direct([{"col_a": 1, "col_b": "x"}])  # must not raise


class TestAsyncBatchWriterRegistry:
    """Test get_or_create_writer module-level registry."""

    def test_get_or_create_returns_same_instance(self):
        from services.db_batch_writer import _writers, _writers_lock, get_or_create_writer
        # Clean slate for this test
        with _writers_lock:
            _writers.pop("registry_test_table", None)

        pool, conn, cursor = _mock_pool()

        w1 = get_or_create_writer(
            "registry_test_table", ["a", "b"], dsn="postgresql://fake/db",
            pool_minconn=1, pool_maxconn=2,
        )
        w1._pool = pool  # inject mock pool so no real connection
        w2 = get_or_create_writer(
            "registry_test_table", ["a", "b"], dsn="postgresql://fake/db",
        )
        assert w1 is w2

        # Cleanup
        with contextlib.suppress(Exception):
            w1.shutdown()
        with _writers_lock:
            _writers.pop("registry_test_table", None)


class TestAsyncBatchWriterPreStartFallback:
    """If enqueue() is called before start(), it should do a direct flush."""

    def test_enqueue_before_start_calls_direct_flush(self):
        writer = _make_writer()
        pool, conn, cursor = _mock_pool()
        writer._pool = pool

        # Do NOT call writer.start()
        writer.enqueue({"col_a": 99, "col_b": "direct"})
        # _flush_direct should have been called synchronously
        cursor.executemany.assert_called_once()
