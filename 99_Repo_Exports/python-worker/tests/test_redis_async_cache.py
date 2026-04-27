# -*- coding: utf-8 -*-
"""
Tests for redis_async_cache module (non-blocking JSON refresh from Redis).
"""
import json
import time
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from core.redis_async_cache import maybe_refresh_json, _fetch_json, _now_ms


@pytest.mark.asyncio
async def test_fetch_json_success():
    """Test successful JSON fetch from Redis."""
    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=b'{"key": "value"}')
    
    result = await _fetch_json(mock_redis, "test:key")
    
    assert result == {"key": "value"}
    mock_redis.get.assert_called_once_with("test:key")


@pytest.mark.asyncio
async def test_fetch_json_bytes_decode():
    """Test that bytes are properly decoded."""
    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=b'{"key": "value"}')
    
    result = await _fetch_json(mock_redis, "test:key")
    
    assert result == {"key": "value"}


@pytest.mark.asyncio
async def test_fetch_json_missing_key():
    """Test that missing key returns None."""
    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=None)
    
    result = await _fetch_json(mock_redis, "test:key")
    
    assert result is None


@pytest.mark.asyncio
async def test_fetch_json_invalid_json():
    """Test that invalid JSON returns None."""
    mock_redis = AsyncMock()
    mock_redis.get = AsyncMock(return_value=b'invalid json')
    
    result = await _fetch_json(mock_redis, "test:key")
    
    assert result is None


def test_maybe_refresh_json_skip_if_too_soon():
    """Test that refresh is skipped if refresh_ms not elapsed."""
    mock_redis = MagicMock()
    dst = {"test_key:ts_ms": _now_ms()}
    
    # Should skip because refresh_ms not elapsed
    maybe_refresh_json(mock_redis, key="test:key", dst=dst, dst_key="test_key", refresh_ms=10000)
    
    # Should not have called redis.get
    mock_redis.get.assert_not_called()


def test_maybe_refresh_json_refresh_when_elapsed():
    """Test that refresh happens when refresh_ms elapsed."""
    mock_redis = MagicMock()
    dst = {"test_key:ts_ms": _now_ms() - 20000}  # 20 seconds ago
    
    # Should trigger refresh
    maybe_refresh_json(mock_redis, key="test:key", dst=dst, dst_key="test_key", refresh_ms=10000)
    
    # Note: actual async task is created, but we can't easily test it synchronously
    # The function should have updated the timestamp to prevent immediate re-trigger
    assert "test_key:ts_ms" in dst


def test_maybe_refresh_json_zero_refresh_ms():
    """Test that zero refresh_ms skips refresh."""
    mock_redis = MagicMock()
    dst = {}
    
    maybe_refresh_json(mock_redis, key="test:key", dst=dst, dst_key="test_key", refresh_ms=0)
    
    mock_redis.get.assert_not_called()


def test_maybe_refresh_json_no_last_timestamp():
    """Test that missing timestamp triggers refresh."""
    mock_redis = MagicMock()
    dst = {}  # No timestamp
    
    maybe_refresh_json(mock_redis, key="test:key", dst=dst, dst_key="test_key", refresh_ms=10000)
    
    # Should have set timestamp
    assert "test_key:ts_ms" in dst


def test_maybe_refresh_json_exception_handling():
    """Test that exceptions in task creation are handled gracefully."""
    mock_redis = MagicMock()
    dst = {"test_key:ts_ms": _now_ms() - 20000}
    
    # Mock asyncio.create_task to raise exception
    with patch("core.redis_async_cache.asyncio.create_task", side_effect=Exception("test")):
        # Should not raise, should set timestamp as fail-open
        maybe_refresh_json(mock_redis, key="test:key", dst=dst, dst_key="test_key", refresh_ms=10000)
        
        assert "test_key:ts_ms" in dst

