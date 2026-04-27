
import asyncio
import json
import zlib
from unittest.mock import MagicMock, patch
import pytest
from orderflow_services.confidence_cal_decision_logger_v1 import (
    schedule_conf_cal_decision_log,
    deterministic_sample,
)

class TestConfidenceCalDecisionLogger:
    def test_deterministic_sample(self):
        # Always true
        assert deterministic_sample("abc", 1.0) is True
        assert deterministic_sample("xyz", 1.0) is True
        # Always false
        assert deterministic_sample("abc", 0.0) is False
        assert deterministic_sample("xyz", 0.0) is False
        
        # Specific cases
        # crc32("test") = 0xd87f7e0c = 3632233996
        # 3632233996 / 2**32 ~= 0.8456
        assert deterministic_sample("test", 0.85) is True
        assert deterministic_sample("test", 0.84) is False

    @patch("orderflow_services.confidence_cal_decision_logger_v1.inc_decision_log")
    @patch("orderflow_services.confidence_cal_decision_logger_v1.inc_decision_log_sampled_out")
    def test_schedule_skipped_sampling(self, mock_inc_sampled, mock_inc_log):
        redis_mock = MagicMock()
        payload = {"sid": "test", "symbol": "BTCUSDT"}
        
        # rate 0.0 -> should skip
        res = schedule_conf_cal_decision_log(
            redis_mock, payload, sample_rate=0.0, symbol="BTCUSDT", stage="test"
        )
        assert res is False
        mock_inc_sampled.assert_called_once()
        mock_inc_log.assert_not_called()
        redis_mock.xadd.assert_not_called()

    @patch("orderflow_services.confidence_cal_decision_logger_v1.inc_decision_log")
    @patch("orderflow_services.confidence_cal_decision_logger_v1.inc_decision_log_error")
    def test_schedule_success(self, mock_inc_err, mock_inc_log):
        redis_mock = MagicMock()
        # redis.xadd returns a future in async mock, or just value. 
        # The code checks asyncio.iscoroutine. 
        # We'll just mock it to return None (not coroutine) for simplicity in sync test 
        # but the code wraps it in create_task.
        # Actually, schedule_conf_cal_decision_log needs a running loop to schedule.
        
        async def run_test():
            payload = {"sid": "test_ok", "symbol": "ETHUSDT"}
            # We need a loop for create_task
            res = schedule_conf_cal_decision_log(
                redis_mock, payload, sample_rate=1.0, symbol="ETHUSDT", stage="test_stage"
            )
            assert res is True
            # Allow the background task to run
            await asyncio.sleep(0.01)
            
            # Verify xadd called
            redis_mock.xadd.assert_called()
            args, kwargs = redis_mock.xadd.call_args
            assert args[0] == "logs:conf_cal:decision"
            assert "payload" in args[1]
            
            # Verify metrics
            mock_inc_log.assert_called_with("ETHUSDT", "test_stage", "", "")
            mock_inc_err.assert_not_called()

        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(run_test())
        loop.close()

    @patch("orderflow_services.confidence_cal_decision_logger_v1.inc_decision_log_error")
    def test_schedule_no_redis(self, mock_inc_err):
        assert schedule_conf_cal_decision_log(None, {}) is False

