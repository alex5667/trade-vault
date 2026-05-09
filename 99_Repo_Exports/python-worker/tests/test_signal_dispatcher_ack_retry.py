from __future__ import annotations

from unittest.mock import Mock

from services.signal_dispatcher import SignalDispatcher


def test_ack_retry_functionality():
    """Test ACK retry cache functionality."""
    sd = object.__new__(SignalDispatcher)
    sd._ack_retry = {}
    sd._ack_retry_ttl_s = 600.0
    sd._ack_retry_max = 10
    from collections import defaultdict
    sd._ctr = defaultdict(int)
    sd._last_ack_cleanup_mono = 0.0
    sd.metrics_every_sec = 60  # Don't trigger metrics
    sd._last_metrics_mono = float('inf')  # Don't trigger metrics
    # Mock redis for _emit_metrics
    mock_redis = Mock()
    mock_redis.xlen.return_value = 100
    sd.redis = mock_redis
    sd.outbox_stream = "test_stream"
    sd.read_count = 200
    sd.read_block_ms = 1000
    sd.claim_min_idle_ms = 60000

    # Test remembering ACK retry
    sd._remember_ack_retry("stream1", "msg123")
    assert ("stream1", "msg123") in sd._ack_retry
    assert len(sd._ack_retry) == 1

    # Test cache cleanup (simulate old entries)
    import time
    old_time = time.monotonic() - 700  # Older than TTL
    sd._ack_retry[("stream1", "old_msg")] = old_time
    sd._last_ack_cleanup_mono = time.monotonic() - 70  # Trigger cleanup

    # Create mock helper for _tick_housekeeping
    mock_helper = Mock()
    sd._tick_housekeeping(mock_helper)

    # Old entry should be cleaned up
    assert ("stream1", "old_msg") not in sd._ack_retry
    assert ("stream1", "msg123") in sd._ack_retry


def test_pending_by_consumer():
    """Test pending by consumer diagnostic functionality."""
    sd = object.__new__(SignalDispatcher)
    sd.outbox_stream = "test_stream"
    sd.group = "test_group"

    # Mock redis to return XPENDING data
    mock_redis = Mock()
    mock_redis.execute_command.return_value = [
        ["id1", "consumer1", 1000, 1],
        ["id2", "consumer1", 2000, 2],
        ["id3", "consumer2", 3000, 1],
    ]
    sd.redis = mock_redis

    result = sd._pending_by_consumer(limit=10)
    assert result == {"consumer1": 2, "consumer2": 1}

    # Verify the XPENDING command was called correctly
    mock_redis.execute_command.assert_called_once_with(
        "XPENDING", "test_stream", "test_group", "-", "+", 10
    )


def test_maybe_claim_pending():
    """Test periodic pending claim functionality."""
    sd = object.__new__(SignalDispatcher)
    sd.claim_min_idle_ms = 60000
    sd.claim_count = 5
    sd.claim_every_ms = 1000  # 1 second
    sd._pending_start_id = "0-0"
    sd._last_claim_mono = 0.0
    from collections import defaultdict
    sd._ctr = defaultdict(int)
    sd.outbox_stream = "test_stream"
    # Add required attributes for _try_ack_retry_only
    sd._ack_retry = {}

    # Mock helper
    mock_helper = Mock()
    mock_helper.claim_pending.return_value = ("0-100", ["msg1", "msg2"])

    # First call should trigger claim (last_claim_mono = 0)
    import time
    current_time = time.monotonic()
    sd._maybe_claim_pending(mock_helper)

    # Should have called claim_pending
    mock_helper.claim_pending.assert_called_once()
    assert sd._pending_start_id == "0-100"
    assert sd._ctr["claimed"] == 2
