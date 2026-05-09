
from unittest.mock import AsyncMock, MagicMock, patch

import pytest
from redis.exceptions import ConnectionError, TimeoutError

from core.redis_stream_consumer import AsyncRedisStreamHelper


@pytest.mark.asyncio
async def test_ensure_group_retries_on_connection_error():
    # Mock Redis client
    mock_redis = MagicMock()
    mock_redis.xgroup_create = AsyncMock()

    # Setup the mock to raise ConnectionError twice, then succeed
    mock_redis.xgroup_create.side_effect = [
        ConnectionError("Connection refused"),
        TimeoutError("Timeout"),
        None  # Success
    ]

    helper = AsyncRedisStreamHelper(client=mock_redis, group="test_group", consumer="test_consumer")

    # Patch asyncio.sleep to avoid waiting
    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        # This should succeed after retries
        await helper.ensure_group("test_stream")

    assert mock_redis.xgroup_create.call_count == 3
    assert mock_sleep.call_count == 2

@pytest.mark.asyncio
async def test_ensure_group_fails_after_max_retries():
    # Mock Redis client
    mock_redis = MagicMock()
    mock_redis.xgroup_create = AsyncMock()

    # Always raise ConnectionError
    mock_redis.xgroup_create.side_effect = ConnectionError("Connection refused")

    helper = AsyncRedisStreamHelper(client=mock_redis, group="test_group", consumer="test_consumer")

    # Patch asyncio.sleep to avoid waiting
    with patch("asyncio.sleep", new_callable=AsyncMock) as mock_sleep:
        with pytest.raises(RuntimeError) as excinfo:
            await helper.ensure_group("test_stream")

        assert "Redis unavailable or still loading after 30 attempts" in str(excinfo.value)

    # 30 retries mean 30 calls
    assert mock_redis.xgroup_create.call_count >= 30
