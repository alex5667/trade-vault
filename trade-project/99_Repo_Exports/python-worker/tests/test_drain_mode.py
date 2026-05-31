import asyncio
import os
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.crypto_orderflow_service import CryptoOrderflowService


@pytest.mark.asyncio
async def test_shutdown_sets_flag():
    """Test that shutdown sets the _shutdown flag."""
    with patch('redis.asyncio.from_url') as mock_redis:
        mock_client = AsyncMock()
        mock_redis.return_value = mock_client
        service = CryptoOrderflowService("redis://localhost")
        service.main = mock_client
        service.ticks = mock_client

        assert service._shutdown is False
        await service.shutdown()
        assert service._shutdown is True

@pytest.mark.asyncio
async def test_shutdown_waits_for_tasks_drain():
    """Test that shutdown waits for tasks to finish (drain)."""
    with patch('redis.asyncio.from_url') as mock_redis:
        mock_client = AsyncMock()
        mock_redis.return_value = mock_client
        service = CryptoOrderflowService("redis://localhost")
        service.main = mock_client
        service.ticks = mock_client

        # Create a task that finished after 0.5s
        async def mock_task():
            await asyncio.sleep(0.5)
            return "ok"

        task = asyncio.create_task(mock_task())
        service.symbol_tasks["BTCUSDT"] = (task, None)
        service.symbol_contexts["BTCUSDT"] = MagicMock()

        # Shutdown with 1s timeout
        with patch.dict(os.environ, {"CRYPTO_OF_DRAIN_TIMEOUT_SEC": "1.0"}):
            await service.shutdown()

        assert task.done()
        assert not task.cancelled()

@pytest.mark.asyncio
async def test_shutdown_force_cancels_after_timeout():
    """Test that shutdown cancels tasks after timeout."""
    with patch('redis.asyncio.from_url') as mock_redis, \
         patch('asyncio.wait', wraps=asyncio.wait) as mock_wait:
        mock_client = AsyncMock()
        mock_redis.return_value = mock_client
        service = CryptoOrderflowService("redis://localhost")
        service.main = mock_client
        service.ticks = mock_client

        # Task that sleeps longer than timeout
        async def slow_task():
            try:
                await asyncio.sleep(5)
            except asyncio.CancelledError:
                raise

        task = asyncio.create_task(slow_task())
        service.symbol_tasks["BTCUSDT"] = (task, None)
        service.symbol_contexts["BTCUSDT"] = MagicMock()

        # Shutdown with 0.1s timeout
        with patch.dict(os.environ, {"CRYPTO_OF_DRAIN_TIMEOUT_SEC": "0.1"}):
            await service.shutdown()

        assert task.done()
        assert task.cancelled()

@pytest.mark.asyncio
async def test_consume_ticks_exits_on_shutdown():
    """Test that consume_ticks exits when _shutdown is True."""
    with patch('redis.asyncio.from_url') as mock_redis:
        mock_client = AsyncMock()
        mock_redis.return_value = mock_client
        service = CryptoOrderflowService("redis://localhost")
        service.main = mock_client
        service.ticks = mock_client

        # Start consume_ticks
        service._shutdown = False
        task = asyncio.create_task(service.consume_ticks("BTCUSDT"))

        # Wait a bit
        await asyncio.sleep(0.1)

        # Signal shutdown
        service._shutdown = True

        # Task should exit soon
        try:
            await asyncio.wait_for(task, timeout=1.0)
        except TimeoutError:
            pytest.fail("consume_ticks did not exit on shutdown")

        assert task.done()
