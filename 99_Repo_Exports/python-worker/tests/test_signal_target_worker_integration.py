import pytest
from unittest.mock import patch, MagicMock
from services.signal_target_worker import SignalTargetWorker
from redis.exceptions import TimeoutError
import json

def test_signal_target_worker_http_post():
    worker = SignalTargetWorker("trade_back")
    worker._ensure_scripts = MagicMock()
    worker.redis = MagicMock()
    
    # Test op == "http_post" successful delivery
    worker.redis.exists.return_value = 0
    worker.redis.set.return_value = True
    
    with patch("requests.post") as mock_post:
        mock_response = MagicMock()
        mock_response.raise_for_status.return_value = None
        mock_post.return_value = mock_response
        
        ok = worker._deliver_http_post(
            sid="123",
            url="http://test",
            headers={"Content-Type": "application/json"},
            payload="{}",
            timeout_sec=1.0
        )
        assert ok is True
        mock_post.assert_called_once()
        worker.redis.set.assert_any_call("deliver:done:trade_back:123", "1", nx=True, ex=worker.task_ttl_sec)

def test_signal_target_worker_http_post_transient_failure():
    worker = SignalTargetWorker("trade_back")
    worker._ensure_scripts = MagicMock()
    worker.redis = MagicMock()
    
    worker.redis.exists.return_value = 0
    worker.redis.set.return_value = True
    
    import requests
    with patch("requests.post") as mock_post:
        mock_post.side_effect = requests.RequestException("Timeout")
        
        with pytest.raises(TimeoutError, match="http_post_failed_transient"):
            worker._deliver_http_post(
                sid="123",
                url="http://test",
                headers={},
                payload="{}",
                timeout_sec=1.0
            )

def test_notify_gate_pcall_wrapper():
    worker = SignalTargetWorker("notify")
    worker.redis = MagicMock()
    worker.redis.evalsha.return_value = ["-4", "0", "0"] # Represents pcall INCR failure
    
    with pytest.raises(TimeoutError, match="notify_incr_failed_retry"):
        worker._deliver_notify("123", {})

