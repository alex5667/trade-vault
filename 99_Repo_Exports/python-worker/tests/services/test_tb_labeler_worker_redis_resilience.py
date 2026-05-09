import json
import os
from unittest.mock import MagicMock, patch

import pytest
import redis.exceptions

# Disable metrics for tests to avoid port conflicts
os.environ["TB_METRICS_ENABLE"] = "0"

from services.tb_labeler_worker_v10_2 import TB_JOBS_ZSET, TBLabelerWorkerV10_2


@patch('redis.Redis.from_url')
def test_process_due_resilience_on_busy_loading(mock_from_url):
    # Setup mocks
    mock_redis = MagicMock()
    mock_from_url.return_value = mock_redis

    worker = TBLabelerWorkerV10_2()

    # Simulate a job in the queue
    job_id = "test_sid:180000"
    mock_redis.zrangebyscore.return_value = [job_id.encode()]

    # Simulate Redis loading when trying to get job data
    mock_redis.get.side_effect = redis.exceptions.BusyLoadingError("Redis is loading")

    # Run process_due
    with pytest.raises(redis.exceptions.BusyLoadingError):
        worker.process_due(limit=10)

    # Verify that zrem was NEVER called for the job_id
    mock_redis.zrem.assert_not_called()

    # Now simulate success after recovery
    mock_redis.get.side_effect = None
    mock_redis.get.return_value = json.dumps({
        "sid": "test_sid",
        "symbol": "BTCUSDT",
        "ts_ms": 1000,
        "h_ms": 180000,
        "direction": "LONG",
        "msg_id": "1-0"
    })

    # Mock _load_of_input to return something
    with patch.object(worker, '_load_of_input', return_value={"indicators": {"spread_bps": 1.0}}):
        # Mock _fetch_ticks to return something
        with patch.object(worker, '_fetch_ticks', return_value=[(1000, 50000.0), (2000, 51000.0)]):
            # Mock infer_tp_sl_bps and eval_barrier to avoid import issues or real logic
            with patch('services.tb_labeler_worker_v10_2.infer_tp_sl_bps') as mock_infer, \
                 patch('services.tb_labeler_worker_v10_2.eval_barrier') as mock_eval:

                mock_infer.return_value = MagicMock(tp_bps=30, sl_bps=30)
                mock_eval.return_value = MagicMock(y_edge=1, hit_ms=2000, ret_bps=20, r_mult=1, util_r=0.5, exec_cost_r=0.5, adv_r=0.1, ticks_used=10)

                # Mock xadd
                mock_redis.xadd.return_value = "msg_id"

                # Run process_due again
                worker.process_due(limit=10)

                # Verify that zrem WAS called this time
                mock_redis.zrem.assert_called_with(TB_JOBS_ZSET, job_id.encode())

if __name__ == "__main__":
    pytest.main([__file__])


if __name__ == "__main__":
    pytest.main([__file__])
