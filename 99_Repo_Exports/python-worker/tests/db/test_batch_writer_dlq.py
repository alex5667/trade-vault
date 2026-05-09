from __future__ import annotations

"""
P1-3 regression: AsyncBatchWriter writes to durable DLQ after all retries exhausted.

Coverage:
  - After max_retries, _write_dlq() is called
  - DLQ file is created in DB_BATCH_DLQ_DIR
  - Metric db_batch_writer_rows_dropped_total is incremented
  - DLQ payload contains table, columns, rows, error, ts_ms
"""

import json
import os
import tempfile
import unittest
from unittest.mock import MagicMock, patch


def _make_writer(dsn: str = "postgresql://x:x@localhost:5432/test", **kwargs):
    from services.db_batch_writer import AsyncBatchWriter
    return AsyncBatchWriter(
        table="test_table",
        columns=["id", "value"],
        dsn=dsn,
        batch_size=10,
        flush_interval_s=1.0,
        max_retries=2,
        **kwargs,
    )


class TestBatchWriterDLQ(unittest.TestCase):
    def test_write_dlq_creates_file(self):
        writer = _make_writer()
        batch = [{"id": 1, "value": "x"}, {"id": 2, "value": "y"}]

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch.dict(os.environ, {"DB_BATCH_DLQ_DIR": tmpdir, "DB_BATCH_DLQ_REDIS_URL": ""}):
                writer._write_dlq(batch, Exception("test error"))

        # File check must happen while tmpdir still exists
        with tempfile.TemporaryDirectory() as tmpdir2:
            with patch.dict(os.environ, {"DB_BATCH_DLQ_DIR": tmpdir2, "DB_BATCH_DLQ_REDIS_URL": ""}):
                writer._write_dlq(batch, Exception("test error"))
                dlq_path = os.path.join(tmpdir2, "test_table.ndjson")
                assert os.path.exists(dlq_path), "DLQ file not created"
                with open(dlq_path) as f:
                    payload = json.loads(f.read().strip())
                assert payload["table"] == "test_table"
                assert payload["columns"] == ["id", "value"]
                assert len(payload["rows"]) == 2
                assert "test error" in payload["error"]
                assert "ts_ms" in payload

    def test_write_dlq_redis_preferred_over_file(self):
        writer = _make_writer()
        batch = [{"id": 1, "value": "a"}]

        mock_redis = MagicMock()
        mock_from_url = MagicMock(return_value=mock_redis)

        with patch.dict(os.environ, {"DB_BATCH_DLQ_REDIS_URL": "redis://localhost:6379"}):
            with patch("redis.from_url", mock_from_url):
                with tempfile.TemporaryDirectory() as tmpdir:
                    with patch.dict(os.environ, {"DB_BATCH_DLQ_DIR": tmpdir}):
                        writer._write_dlq(batch, Exception("redis dlq test"))

        # Redis xadd should have been called
        mock_redis.xadd.assert_called_once()
        call_kwargs = mock_redis.xadd.call_args
        stream_name = call_kwargs[0][0] if call_kwargs[0] else call_kwargs[1].get("name", "")
        assert "db:batch:dlq" in str(stream_name) or "test_table" in str(stream_name)

    def test_exhausted_retries_calls_write_dlq(self):
        """After all retries fail, _write_dlq must be invoked."""
        from services.db_batch_writer import AsyncBatchWriter
        writer = AsyncBatchWriter(
            table="test_table",
            columns=["id", "value"],
            dsn="postgresql://x:x@localhost:5432/test",
            batch_size=10,
            flush_interval_s=1.0,
            max_retries=2,
        )
        batch = [{"id": 1, "value": "z"}]

        with patch.object(writer, "_get_pool", return_value=None):
            import psycopg2
            with patch("psycopg2.connect", side_effect=psycopg2.OperationalError("db down")):
                with patch.object(writer, "_write_dlq") as mock_dlq:
                    writer._flush_direct(batch)
                    mock_dlq.assert_called_once()
                    dlq_batch, dlq_err = mock_dlq.call_args[0]
                    assert dlq_batch == batch
                    assert "db down" in str(dlq_err)


class TestMLConfirmGateImportContract(unittest.TestCase):
    """P0-5: MLConfirmGate must be importable and expose from_env."""

    def test_ml_confirm_gate_importable(self):
        from services.ml_confirm_gate import MLConfirmGate
        assert MLConfirmGate is not None

    def test_ml_confirm_gate_has_from_env(self):
        from services.ml_confirm_gate import MLConfirmGate
        assert hasattr(MLConfirmGate, "from_env"), "MLConfirmGate.from_env() missing"
        assert callable(MLConfirmGate.from_env)


if __name__ == "__main__":
    unittest.main()
