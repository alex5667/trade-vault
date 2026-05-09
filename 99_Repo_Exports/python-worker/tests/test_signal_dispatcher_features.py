from __future__ import annotations

import time
from unittest.mock import Mock

from services.signal_dispatcher import SignalDispatcher


def test_lease_methods():
    """Test lease acquire/release functionality."""
    # Create minimal dispatcher instance
    sd = object.__new__(SignalDispatcher)
    sd.redis = Mock()
    sd.msg_lease_ttl_ms = 90000
    sd.msg_lease_prefix = "outbox:lease:v1"
    from collections import defaultdict
    sd._ctr = defaultdict(int)

    # Test lease key generation
    assert sd._lease_key("msg123") == "outbox:lease:v1:msg123"

    # Test successful lease acquire
    sd.redis.set.return_value = True
    assert sd._try_acquire_lease("msg123") == True
    sd.redis.set.assert_called_with("outbox:lease:v1:msg123", "1", nx=True, px=90000)

    # Test lease contention (already acquired)
    sd.redis.set.return_value = None
    assert sd._try_acquire_lease("msg123") == False
    assert sd._ctr["lease_contention"] == 1

    # Test lease release
    sd._release_lease("msg123")
    sd.redis.delete.assert_called_with("outbox:lease:v1:msg123")


def test_circuit_breaker():
    """Test circuit breaker functionality."""
    sd = object.__new__(SignalDispatcher)
    sd.cb_fail_threshold = 3
    sd.cb_open_sec = 10.0
    sd._cb_state = {}
    from collections import defaultdict
    sd._ctr = defaultdict(int)

    # Test initial allow
    assert sd._cb_allow("test_target") == True

    # Test failures accumulate
    sd._cb_on_fail("test_target")
    assert sd._cb_state["test_target"] == (1, 0.0)
    assert sd._cb_allow("test_target") == True

    # Test circuit opens after threshold
    sd._cb_on_fail("test_target")
    sd._cb_on_fail("test_target")
    assert sd._cb_state["test_target"][0] == 3
    assert sd._cb_allow("test_target") == False
    assert sd._ctr["cb_open:test_target"] == 1

    # Test success resets
    sd._cb_on_success("test_target")
    assert sd._cb_state["test_target"] == (0, 0.0)
    assert sd._cb_allow("test_target") == True


def test_sleep_retry():
    """Test retry sleep backoff."""
    sd = object.__new__(SignalDispatcher)
    sd.retry_sleep_sec = 0.25
    sd.retry_sleep_max_sec = 2.0

    # Mock time.sleep to verify calls
    original_sleep = time.sleep
    sleep_calls = []
    def mock_sleep(secs):
        sleep_calls.append(secs)
    time.sleep = mock_sleep

    try:
        # Test increasing backoff
        sd._sleep_retry(1)  # 0.25
        sd._sleep_retry(2)  # 0.5
        sd._sleep_retry(10)  # 2.0 (capped)

        assert sleep_calls[0] == 0.25
        assert sleep_calls[1] == 0.5
        assert sleep_calls[2] == 2.0
    finally:
        time.sleep = original_sleep


def test_cleanup_dead_consumers():
    """Test dead consumer cleanup functionality."""
    sd = object.__new__(SignalDispatcher)
    sd.cleanup_dead_consumers = True
    sd.dead_consumer_idle_ms = 600000  # 10 minutes
    sd.outbox_stream = "test_stream"
    sd.group = "test_group"
    sd.redis = Mock()
    sd._last_consumer_cleanup = 0.0
    from collections import defaultdict
    sd._ctr = defaultdict(int)

    # Mock helper
    mock_helper = Mock()
    mock_helper.consumers_info.return_value = [
        {"name": "active_consumer", "pending": 5, "idle": 10000},
        {"name": "dead_consumer", "pending": 10, "idle": 700000},  # > 10 minutes
        {"name": "idle_no_pending", "pending": 0, "idle": 700000},  # No pending, skip
    ]

    # Test cleanup
    sd._cleanup_dead_consumers(mock_helper)

    # Should call xgroup_delconsumer for dead_consumer only
    sd.redis.xgroup_delconsumer.assert_called_once_with("test_stream", "test_group", "dead_consumer")
    assert sd._ctr["delconsumer"] == 1

    # Test disabled cleanup
    sd.cleanup_dead_consumers = False
    sd.redis.reset_mock()
    sd._ctr = defaultdict(int)
    sd._cleanup_dead_consumers(mock_helper)
    sd.redis.xgroup_delconsumer.assert_not_called()
