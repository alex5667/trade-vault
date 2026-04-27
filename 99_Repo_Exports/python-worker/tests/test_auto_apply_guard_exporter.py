import pytest
from unittest.mock import MagicMock, patch
import time
from tools.auto_apply_guard_prom_exporter_v1 import AutoApplyGuardExporter

@pytest.fixture
def mock_redis():
    with patch('redis.Redis.from_url') as mock:
        yield mock

def test_get_time_bucket():
    exporter = AutoApplyGuardExporter("redis://localhost")
    # Test a specific timestamp: 2023-10-27 10:00:00 UTC
    ts = 1698400800 
    assert exporter.get_time_bucket(ts) == "202310271000"

def test_collect_metrics(mock_redis):
    # Setup mock redis
    mock_client = MagicMock()
    mock_redis.return_value = mock_client
    
    exporter = AutoApplyGuardExporter("redis://localhost")
    exporter.redis = mock_client
    
    # Mock pipeline
    mock_pipe = MagicMock()
    mock_client.pipeline.return_value = mock_pipe
    
    # Mock return values for hgetall
    # Let's verify logic for 5m window.
    # Suppose we have 2 buckets populated in the last 5 minutes.
    # Bucket 1: blocked_total=10, run_ok_total=5, run_err_total=1, blocked:TooMany=10
    # Bucket 2: blocked_total=0, run_ok_total=2, run_err_total=0
    # Other buckets empty.
    
    # The exporter fetches max_window (60) + 1 buckets.
    # We need to simulate the pipeline execute return list.
    
    num_buckets = 60 + 1
    mock_results = [{}] * num_buckets
    
    # Let's say index 0 is "now", index 1 is "1 min ago".
    mock_results[0] = {
        'blocked_total': '10',
        'run_ok_total': '5',
        'run_err_total': '1',
        'blocked:TooMany': '10'
    }
    mock_results[1] = {
        'blocked_total': '0',
        'run_ok_total': '2',
        'run_err_total': '0'
    }
    
    mock_pipe.execute.return_value = mock_results
    
    # Mock prometheus gauges to check set calls
    with patch('tools.auto_apply_guard_prom_exporter_v1.GAUGE_BLOCKED') as g_blocked, \
         patch('tools.auto_apply_guard_prom_exporter_v1.GAUGE_RUN_OK') as g_run_ok, \
         patch('tools.auto_apply_guard_prom_exporter_v1.GAUGE_RUN_ERR') as g_run_err, \
         patch('tools.auto_apply_guard_prom_exporter_v1.GAUGE_BLOCKED_RATIO') as g_ratio:
        
        # We need the mocks to return a child object when labels() is called
        g_blocked.labels.return_value = MagicMock()
        g_run_ok.labels.return_value = MagicMock()
        g_run_err.labels.return_value = MagicMock()
        g_ratio.labels.return_value = MagicMock()

        exporter.collect_metrics()
        
        # Verify 5m window
        # Data sum: 
        # blocked = 10 + 0 = 10
        # ok = 5 + 2 = 7
        # err = 1 + 0 = 1
        # total_runs = 10 + 7 + 1 = 18
        
        g_blocked.labels.assert_any_call(window='5m')
        g_blocked.labels(window='5m').set.assert_called_with(10)
        
        g_run_ok.labels.assert_any_call(window='5m')
        g_run_ok.labels(window='5m').set.assert_called_with(7)
        
        g_run_err.labels.assert_any_call(window='5m')
        g_run_err.labels(window='5m').set.assert_called_with(1)
        
        # Ratio
        # blocked / total = 10 / 18 ~= 0.555
        expected_ratio = 10 / 18
        g_ratio.labels(window='5m').set.assert_called_with(expected_ratio)

