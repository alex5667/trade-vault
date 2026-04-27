from utils.time_utils import get_ny_time_millis
import pytest
import json
import time
from unittest.mock import MagicMock, patch
from tools.cfg_suggestions_sre_monitor_v2 import SugSREMonitor

@pytest.fixture
def mock_redis():
    with patch('tools.cfg_suggestions_sre_monitor_v2.Redis') as mock:
        client = MagicMock()
        mock.from_url.return_value = client
        yield client

def test_monitor_pending_stuck(mock_redis):
    # Setup mock data for one pending suggestion, older than 1h
    now_ms = get_ny_time_millis()
    created_at = now_ms - 4000000  # ~1.1h ago
    
    mock_redis.get.side_effect = lambda k: {
        "latest:meta_freeze:ALL": "sid_1",
        "cfg:suggestions:meta_freeze:ALL:sid_1": json.dumps({
            "sid": "sid_1",
            "state": "pending",
            "created_at": created_at
        }),
        "flap:cnt:meta_freeze:ALL": "0",
        "sre:alert:lock:meta_freeze:ALL:sid_1:WARN": None
    }.get(k)
    
    mock_redis.set.return_value = True

    monitor = SugSREMonitor("redis://localhost", dry_run=False)
    monitor.notify = MagicMock()
    
    rc = monitor.run(kinds=["meta_freeze"], scopes=["ALL"], emit_metrics=True, do_notify=True)
    
    assert rc == 2
    # Should notify WARN
    monitor.notify.assert_called()
    call_args = monitor.notify.call_args[0]
    assert "PENDING" in call_args[0]
    assert monitor.notify.call_args[1]["severity"] == "WARN"

def test_monitor_approved_stuck(mock_redis):
    # Setup mock data for one approved suggestion, older than 10m
    now_ms = get_ny_time_millis()
    approved_at = now_ms - 700000  # ~11.6m ago
    
    mock_redis.get.side_effect = lambda k: {
        "latest:meta_freeze:ALL": "sid_2",
        "cfg:suggestions:meta_freeze:ALL:sid_2": json.dumps({
            "sid": "sid_2",
            "state": "approved",
            "approved_at": approved_at
        }),
        "flap:cnt:meta_freeze:ALL": "0",
        "sre:alert:lock:meta_freeze:ALL:sid_2:CRIT": None
    }.get(k)
    
    mock_redis.set.return_value = True

    monitor = SugSREMonitor("redis://localhost", dry_run=False)
    monitor.notify = MagicMock()
    
    rc = monitor.run(kinds=["meta_freeze"], scopes=["ALL"], emit_metrics=True, do_notify=True)
    
    assert rc == 2
    # Should notify CRIT
    monitor.notify.assert_called()
    assert monitor.notify.call_args[1]["severity"] == "CRIT"

def test_monitor_flapping(mock_redis):
    # Suggestion is applied, but flapping is high
    mock_redis.get.side_effect = lambda k: {
        "latest:meta_freeze:ALL": "sid_3",
        "cfg:suggestions:meta_freeze:ALL:sid_3": json.dumps({
            "sid": "sid_3",
            "state": "applied"
        }),
        "flap:cnt:meta_freeze:ALL": "5", # Threshold is 4
        "sre:alert:lock:meta_freeze:ALL:sid_3:CRIT": None
    }.get(k)
    
    mock_redis.set.return_value = True

    monitor = SugSREMonitor("redis://localhost", dry_run=False)
    monitor.notify = MagicMock()
    
    rc = monitor.run(kinds=["meta_freeze"], scopes=["ALL"], emit_metrics=True, do_notify=True)
    
    assert rc == 2
    monitor.notify.assert_called()
    assert "FLAPPING" in monitor.notify.call_args[0][0]
    assert monitor.notify.call_args[1]["severity"] == "CRIT"
