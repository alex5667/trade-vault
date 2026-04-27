import asyncio
import pytest
from unittest.mock import AsyncMock, MagicMock, patch
from collections import deque
from services.crypto_orderflow_service import CryptoOrderflowService

@pytest.mark.asyncio
async def test_stop_symbol_clears_caches():
    """
    Verifies that _stop_symbol clears technical caches:
    _task_restart_hist, tick_helpers, book_helpers.
    """
    with patch('redis.asyncio.from_url') as mock_redis:
        mock_client = AsyncMock()
        mock_redis.return_value = mock_client
        
        service = CryptoOrderflowService(
            redis_dsn="redis://test:6379/0",
            ticks_dsn="redis://test:6379/1"
        )
        
        symbol = "BTCUSDT"
        
        # 1. Populate caches
        service.symbol_contexts[symbol] = AsyncMock()
        service._task_restart_hist[(symbol, "ticks")] = deque([1, 2, 3])
        service._task_restart_hist[(symbol, "books")] = deque([4, 5, 6])
        service.tick_helpers[symbol] = AsyncMock()
        service.book_helpers[symbol] = AsyncMock()
        service.poison_pill_counts[symbol] = 5  # ✅ P0: добавляем для проверки cleanup
        
        # Mock tasks
        tick_task = asyncio.Future()
        book_task = asyncio.Future()
        # Mark as cancelable and done
        tick_task.set_result(None)
        book_task.set_result(None)
        
        # We need to wrap them in a way that cancel() can be checked
        tick_task.cancel = MagicMock()
        book_task.cancel = MagicMock()
        
        service.symbol_tasks[symbol] = (tick_task, book_task)
        
        # 2. Call _stop_symbol
        await service._stop_symbol(symbol)
        
        # 3. Verify caches are cleared
        assert symbol not in service.symbol_contexts
        assert (symbol, "ticks") not in service._task_restart_hist
        assert (symbol, "books") not in service._task_restart_hist
        assert symbol not in service.tick_helpers
        assert symbol not in service.book_helpers
        assert symbol not in service.poison_pill_counts  # ✅ P0: проверяем cleanup
        assert symbol not in service.symbol_tasks
        
        # Check task cancellation
        tick_task.cancel.assert_called_once()
        book_task.cancel.assert_called_once()

@pytest.mark.asyncio
async def test_stop_symbol_silent_if_not_exists():
    """
    Verifies that _stop_symbol doesn't crash if symbol is not in caches.
    """
    with patch('redis.asyncio.from_url') as mock_redis:
        mock_client = AsyncMock()
        mock_redis.return_value = mock_client
        
        service = CryptoOrderflowService(
            redis_dsn="redis://test:6379/0",
            ticks_dsn="redis://test:6379/1"
        )
        
        # Should NOT raise any KeyError
        await service._stop_symbol("NON_EXISTENT")
