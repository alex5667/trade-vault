from __future__ import annotations
import asyncio
import os
import time
import pytest
from unittest.mock import MagicMock, AsyncMock, patch
from services.crypto_orderflow_service import CryptoOrderflowService, SymbolRuntime
from core.burst_gate import BurstCandidate

@pytest.mark.asyncio
async def test_burst_flush_loop_integration():
    # Patch aioredis in the service module
    with patch("services.crypto_orderflow_service.aioredis.from_url") as mock_from_url:
        mock_redis = AsyncMock()
        mock_from_url.return_value = mock_redis
        
        # Instantiate service
        service = CryptoOrderflowService(redis_dsn="redis://mock")
        service.strategy = AsyncMock()
        service.strategy.publish_signal = AsyncMock()
        service._pre_publish_allows_signal = AsyncMock(return_value=True)
        
        # Mock SymbolRuntime
        rt = SymbolRuntime(symbol="BTCUSDT", config={"burst_window_ms": 1000, "burst_max_age_ms": 5000})
        # Add a candidate that should be flushed
        rt.burst.consider(ts_ms=1000, cand=BurstCandidate(ts_ms=1000, score=0.9, payload={"signal": 1}))
        
        service.symbol_contexts = {"BTCUSDT": rt}
        
        # Mock env vars
        with patch.dict(os.environ, {"BURST_FLUSH_MODE": "wall", "BURST_FLUSH_INTERVAL_MS": "100"}):
            # Start the loop task
            service._shutdown = False
            flush_task = asyncio.create_task(service._burst_flush_loop())
            
            # Mock time.time to be 3000 (ms) -> 3.0 (sec)
            with patch("time.time", return_value=3.0):
                # Wait for loop to run
                await asyncio.sleep(0.3)
                # Should have been flushed (deadline was 2000)
                service.strategy.publish_signal.assert_called()
                
            # Cleanup
            service._shutdown = True
            await flush_task
        
@pytest.mark.asyncio
async def test_burst_flush_loop_tick_mode():
    with patch("services.crypto_orderflow_service.aioredis.from_url") as mock_from_url:
        mock_redis = AsyncMock()
        mock_from_url.return_value = mock_redis
        
        service = CryptoOrderflowService(redis_dsn="redis://mock")
        service.strategy = AsyncMock()
        service.strategy.publish_signal = AsyncMock()
        service._pre_publish_allows_signal = AsyncMock(return_value=True)
        
        rt = SymbolRuntime(symbol="BTCUSDT", config={"burst_window_ms": 1000, "burst_max_age_ms": 5000})
        rt.burst.consider(ts_ms=1000, cand=BurstCandidate(ts_ms=1000, score=0.9, payload={"signal": 1}))
        rt.last_ts_ms = 1999 # just before deadline (1000 + 1000)
        
        service.symbol_contexts = {"BTCUSDT": rt}
        
        with patch.dict(os.environ, {"BURST_FLUSH_MODE": "tick", "BURST_FLUSH_INTERVAL_MS": "50"}):
            service._shutdown = False
            flush_task = asyncio.create_task(service._burst_flush_loop())
            
            await asyncio.sleep(0.2)
            service.strategy.publish_signal.assert_not_called()
            
            # Advance tick time
            rt.last_ts_ms = 2000
            await asyncio.sleep(0.2)
            service.strategy.publish_signal.assert_called()
            
            service._shutdown = True
            await flush_task
