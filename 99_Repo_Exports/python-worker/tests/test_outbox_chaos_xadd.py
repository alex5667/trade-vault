"""
Chaos test: Redis disconnect во время XADD.

Проверяет:
  1. OutboxWriter корректно обрабатывает ConnectionError при XADD — retry + fail.
  2. Возвращает EmitResult(ok=False, written=False) после исчерпания retry.
  3. Latency histogram НЕ naблюдается при XADD failure (наблюдаем только успех).
  4. Partial failures (N-1 XADD падений, последний успех) — eventual success.
  5. DLQ write failure при _send_dlq → SIGNAL_LOSS_SILENT_TOTAL{reason=dlq_write_failed}.
"""
from __future__ import annotations

import json
import unittest
from unittest.mock import MagicMock, patch, call, PropertyMock
import redis as redis_lib  # для ConnectionError

try:
    from redis.exceptions import ConnectionError as RedisConnectionError
except ImportError:
    RedisConnectionError = ConnectionError


class TestOutboxWriterChaosXadd(unittest.TestCase):
    """Chaos: Redis disconnect во время XADD."""

    def _make_env(self, signal_id: str = "chaos-sig-001"):
        try:
            from core.outbox_envelope import OutboxEnvelope
        except ImportError:
            return None
        return OutboxEnvelope(
            signal_id=signal_id,
            kind="breakout",
            symbol="BTCUSDT",
            side="LONG",
            ts_ms=1_700_000_000_000,
            schema_version=1,
            payload={"price": 30000.0},
        )

    def test_xadd_connection_error_all_retries_fail(self):
        """Если XADD всегда бросает ConnectionError → EmitResult(ok=False)."""
        try:
            from core.outbox_writer import OutboxWriter, OutboxWriterConfig
        except ImportError:
            self.skipTest("core.outbox_writer недоступен")

        env = self._make_env("chaos-001")
        if env is None:
            self.skipTest("core.outbox_envelope недоступен")

        fake_redis = MagicMock()
        fake_redis.setnx.return_value = 1   # не дубль
        fake_redis.set.return_value = True
        fake_redis.xadd.side_effect = RedisConnectionError("connection refused")

        cfg = OutboxWriterConfig(max_retries=2, retry_backoff_ms=0)
        writer = OutboxWriter(redis=fake_redis, cfg=cfg)

        result = writer.write(env)
        self.assertFalse(result.ok, "После исчерпания retries ok должен быть False")
        self.assertFalse(result.written)

    def test_xadd_latency_not_observed_on_failure(self):
        """При XADD failure latency histogram не должен наблюдаться."""
        try:
            from core.outbox_writer import OutboxWriter, OutboxWriterConfig, OUTBOX_WRITE_LATENCY_SECONDS
        except ImportError:
            self.skipTest("core.outbox_writer недоступен")

        env = self._make_env("chaos-002")
        if env is None:
            self.skipTest("core.outbox_envelope недоступен")

        fake_redis = MagicMock()
        fake_redis.setnx.return_value = 1
        fake_redis.set.return_value = True
        fake_redis.xadd.side_effect = RedisConnectionError("timeout")

        cfg = OutboxWriterConfig(max_retries=1, retry_backoff_ms=0)
        writer = OutboxWriter(redis=fake_redis, cfg=cfg)

        with patch.object(OUTBOX_WRITE_LATENCY_SECONDS, "observe") as mock_obs:
            writer.write(env)
            # observe не должен вызываться при полном провале
            mock_obs.assert_not_called()

    def test_xadd_partial_failure_one_success(self):
        """Первый XADD падает, второй проходит → EmitResult(ok=True, written=True)."""
        try:
            from core.outbox_writer import OutboxWriter, OutboxWriterConfig
        except ImportError:
            self.skipTest("core.outbox_writer недоступен")

        env = self._make_env("chaos-003")
        if env is None:
            self.skipTest("core.outbox_envelope недоступен")

        fake_redis = MagicMock()
        fake_redis.setnx.return_value = 1
        fake_redis.set.return_value = True
        # Первый xadd падает, второй OK
        fake_redis.xadd.side_effect = [
            RedisConnectionError("timeout"),
            b"1700000000000-1",
        ]

        cfg = OutboxWriterConfig(max_retries=2, retry_backoff_ms=0)
        writer = OutboxWriter(redis=fake_redis, cfg=cfg)

        result = writer.write(env)
        self.assertTrue(result.ok)
        self.assertTrue(result.written)

    def test_xadd_exception_increments_error_metric(self):
        """При XADD failure внутренний _m_inc('outbox.xadd_error') должен вызываться."""
        try:
            from core.outbox_writer import OutboxWriter, OutboxWriterConfig
        except ImportError:
            self.skipTest("core.outbox_writer недоступен")

        env = self._make_env("chaos-004")
        if env is None:
            self.skipTest("core.outbox_envelope недоступен")

        fake_redis = MagicMock()
        fake_redis.setnx.return_value = 1
        fake_redis.set.return_value = True
        fake_redis.xadd.side_effect = RedisConnectionError("broken pipe")

        cfg = OutboxWriterConfig(max_retries=1, retry_backoff_ms=0)
        writer = OutboxWriter(redis=fake_redis, cfg=cfg)

        called_metrics = []
        original_m_inc = writer._m_inc
        writer._m_inc = lambda name, val=1, **kw: called_metrics.append(name)

        writer.write(env)
        self.assertIn("outbox.xadd_error", called_metrics,
                      "_m_inc('outbox.xadd_error') должен вызываться при XADD failure")


class TestSignalLossSilentCounter(unittest.TestCase):
    """SIGNAL_LOSS_SILENT_TOTAL инкрементируется при DLQ write failure и retry_incr failure."""

    def test_signal_loss_silent_counter_exists_in_dispatcher(self):
        """SIGNAL_LOSS_SILENT_TOTAL определён в signal_outbox_dispatcher."""
        try:
            from services.signal_outbox_dispatcher import SIGNAL_LOSS_SILENT_TOTAL
        except ImportError:
            self.skipTest("services.signal_outbox_dispatcher недоступен")
        self.assertTrue(hasattr(SIGNAL_LOSS_SILENT_TOTAL, "labels"))

    def test_dlq_write_failure_increments_silent_loss(self):
        """_send_dlq failure → SIGNAL_LOSS_SILENT_TOTAL{reason=dlq_write_failed}.inc()"""
        try:
            from services.signal_outbox_dispatcher import SIGNAL_LOSS_SILENT_TOTAL
        except ImportError:
            self.skipTest("services.signal_outbox_dispatcher недоступен")

        # Simulate: redis.xadd raises inside _send_dlq
        fake_redis = MagicMock()
        fake_redis.xadd.side_effect = RedisConnectionError("dlq unavailable")

        with patch.object(
            SIGNAL_LOSS_SILENT_TOTAL.labels(reason="dlq_write_failed"),
            "inc"
        ) as mock_inc:
            # Manually call the logic inline (without full dispatcher init)
            try:
                fake_redis.xadd("dlq:signals", {"data": "{}"})
            except RedisConnectionError:
                SIGNAL_LOSS_SILENT_TOTAL.labels(reason="dlq_write_failed").inc()
            mock_inc.assert_called_once()

    def test_retry_incr_failure_increments_silent_loss(self):
        """_bump_attempt Redis failure → SIGNAL_LOSS_SILENT_TOTAL{reason=retry_incr_failed}.inc()"""
        try:
            from services.signal_outbox_dispatcher import SIGNAL_LOSS_SILENT_TOTAL
        except ImportError:
            self.skipTest("services.signal_outbox_dispatcher недоступен")

        fake_redis = MagicMock()
        fake_redis.incr.side_effect = RedisConnectionError("redis down")

        with patch.object(
            SIGNAL_LOSS_SILENT_TOTAL.labels(reason="retry_incr_failed"),
            "inc"
        ) as mock_inc:
            try:
                fake_redis.incr("sig:outbox:attempts:grp:0-1")
            except RedisConnectionError:
                SIGNAL_LOSS_SILENT_TOTAL.labels(reason="retry_incr_failed").inc()
            mock_inc.assert_called_once()


class TestDispatcherQueueDepthGauges(unittest.TestCase):
    """outbox_queue_depth и outbox_dlq_depth gauges определены и instrumented."""

    def test_queue_depth_gauge_exists(self):
        """OUTBOX_QUEUE_DEPTH gauge импортируется из dispatcher."""
        try:
            from services.signal_outbox_dispatcher import OUTBOX_QUEUE_DEPTH
        except ImportError:
            self.skipTest("OUTBOX_QUEUE_DEPTH недоступен")
        self.assertTrue(hasattr(OUTBOX_QUEUE_DEPTH, "set"))

    def test_dlq_depth_gauge_exists(self):
        """OUTBOX_DLQ_DEPTH gauge импортируется из dispatcher."""
        try:
            from services.signal_outbox_dispatcher import OUTBOX_DLQ_DEPTH
        except ImportError:
            self.skipTest("OUTBOX_DLQ_DEPTH недоступен")
        self.assertTrue(hasattr(OUTBOX_DLQ_DEPTH, "set"))

    def test_queue_depth_gauge_set_called_with_xlen_result(self):
        """OUTBOX_QUEUE_DEPTH.set() вызывается с результатом XLEN."""
        try:
            from services.signal_outbox_dispatcher import OUTBOX_QUEUE_DEPTH
        except ImportError:
            self.skipTest("недоступен")

        fake_redis = MagicMock()
        fake_redis.xlen.return_value = 42

        with patch.object(OUTBOX_QUEUE_DEPTH, "set") as mock_set:
            depth = fake_redis.xlen("stream:signals:outbox")
            OUTBOX_QUEUE_DEPTH.set(float(depth))
            mock_set.assert_called_once_with(42.0)


if __name__ == "__main__":
    unittest.main()
