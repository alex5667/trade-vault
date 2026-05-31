from __future__ import annotations

from unittest.mock import Mock

from core.redis_stream_consumer import SyncRedisStreamHelper


def test_consumers_info_empty_stream():
    """Test consumers_info with empty/nonexistent stream."""
    mock_client = Mock()
    mock_client.xinfo_consumers.side_effect = Exception("NOGROUP")
    helper = SyncRedisStreamHelper(mock_client, "test_group", "test_consumer")

    result = helper.consumers_info("nonexistent_stream")
    assert result == []


def test_consumers_info_with_data():
    """Test consumers_info with valid consumer data."""
    mock_client = Mock()
    # Simulate redis-py response format
    mock_client.xinfo_consumers.return_value = [
        {"name": b"consumer1", "pending": 5, "idle": 1000},
        {"name": "consumer2", "pending": 3, "idle": 2000},
    ]
    helper = SyncRedisStreamHelper(mock_client, "test_group", "test_consumer")

    result = helper.consumers_info("test_stream")
    assert len(result) == 2
    assert result[0]["name"] == "consumer1"
    assert result[0]["pending"] == 5
    assert result[0]["idle"] == 1000
    assert result[1]["name"] == "consumer2"


def test_pending_oldest_idle_ms_empty():
    """Test pending_oldest_idle_ms with empty pending list."""
    mock_client = Mock()
    mock_client.xpending_range.return_value = []
    helper = SyncRedisStreamHelper(mock_client, "test_group", "test_consumer")

    result = helper.pending_oldest_idle_ms("test_stream")
    assert result == 0


def test_pending_oldest_idle_ms_with_data():
    """Test pending_oldest_idle_ms with pending messages."""
    mock_client = Mock()
    mock_client.xpending_range.return_value = [
        {"time_since_delivered": 5000},
        {"time_since_delivered": 3000},
    ]
    helper = SyncRedisStreamHelper(mock_client, "test_group", "test_consumer")

    result = helper.pending_oldest_idle_ms("test_stream", sample=2)
    assert result == 5000  # Should return the first (oldest) item

    # Verify the call was made correctly
    mock_client.xpending_range.assert_called_once_with("test_stream", "test_group", "-", "+", 2)


def test_pending_oldest_idle_ms_exception():
    """Test pending_oldest_idle_ms handles exceptions gracefully."""
    mock_client = Mock()
    mock_client.xpending_range.side_effect = Exception("Redis error")
    helper = SyncRedisStreamHelper(mock_client, "test_group", "test_consumer")

    result = helper.pending_oldest_idle_ms("test_stream")
    assert result == 0
