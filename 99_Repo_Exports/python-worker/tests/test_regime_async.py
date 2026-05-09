import os
import time
import unittest
from unittest.mock import MagicMock, patch

from regime.guard import RegimeGuardService

# Ensure env vars are set before import if possible, or patch them
# But we import the class, instantiation happens in test.
from services.trade_monitor import TradeMonitorService


class TestRegimeAsyncPersist(unittest.TestCase):
    def setUp(self):
        self.mock_guard = MagicMock(spec=RegimeGuardService)
        self.mock_redis = MagicMock()
        self.mock_repo = MagicMock()
        self.mock_health = MagicMock()

    @patch.dict(os.environ, {
        "TM_RG_ASYNC_PERSIST": "1",
        "TM_RG_MAX_PENDING": "1",  # Small limit for testing
        "TM_RG_DB_MAX_WORKERS": "1"
    })
    def test_async_submit_success(self):
        service = TradeMonitorService(
            redis_client=self.mock_redis,
            repo=self.mock_repo,
            regime_guard=self.mock_guard,
            health_metrics=self.mock_health
        )

        # Verify executor is created
        self.assertIsNotNone(service._rg_db_executor)
        self.assertIsNotNone(service._rg_persist_sem)

        # Mock on_signal_closed to return a task
        mock_task = MagicMock()
        self.mock_guard.on_signal_closed.return_value = mock_task

        # Call _submit_regime_guard_persist_task directly or via internal logic
        # Since calling on_signal_closed happens deep in check_sl_tp,
        # we can test the submitter method directly.

        service._submit_regime_guard_persist_task(mock_task)

        # Wait a bit for executor
        time.sleep(0.1)

        mock_task.assert_called_once()

    @patch.dict(os.environ, {
        "TM_RG_ASYNC_PERSIST": "1",
        "TM_RG_MAX_PENDING": "1",
        "TM_RG_DB_MAX_WORKERS": "1"
    })
    def test_backpressure_drop(self):
        service = TradeMonitorService(
            redis_client=self.mock_redis,
            repo=self.mock_repo,
            regime_guard=self.mock_guard,
            health_metrics=self.mock_health
        )

        # Fill the semaphore
        service._rg_persist_sem.acquire()

        mock_task = MagicMock()

        # Capture logs to verify warning
        with self.assertLogs("TradeMonitorService", level='WARNING') as cm:
            service._submit_regime_guard_persist_task(mock_task)

        # Verify drop
        mock_task.assert_not_called()
        self.assertTrue(any("DROPPED" in log for log in cm.output), f"Logs were: {cm.output}")

        # Release to clean up
        service._rg_persist_sem.release()

    @patch.dict(os.environ, {"TM_RG_ASYNC_PERSIST": "0"})
    def test_sync_fallback(self):
        service = TradeMonitorService(
            redis_client=self.mock_redis,
            repo=self.mock_repo,
            regime_guard=self.mock_guard,
            health_metrics=self.mock_health
        )

        self.assertIsNone(service._rg_db_executor)

        mock_task = MagicMock()
        service._submit_regime_guard_persist_task(mock_task)

        mock_task.assert_called_once()

if __name__ == '__main__':
    unittest.main()

