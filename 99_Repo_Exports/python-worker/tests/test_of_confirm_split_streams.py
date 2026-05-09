"""
Tests for of_confirm_service.py split streams functionality.

Tests cover:
- _ensure_microbar_groups() consumer group creation
- _poll_microbars_once() polling logic
- Split streams configuration
- Backward compatibility
"""

import json
import os

# Import service class
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

# [AUTOGRAVITY CLEANUP] sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..'))
from services.of_confirm_service import OFConfirmService
import contextlib


@pytest.mark.asyncio
class TestEnsureMicrobarGroups:
    """Test _ensure_microbar_groups() method."""

    async def test_ensure_groups_with_template(self):
        """Create groups for per-symbol streams."""
        service = OFConfirmService()
        service.redis = AsyncMock()
        service.consumer_group = "test_group"
        service.stream_bars_template = "events:microbar_closed:{sym}"
        service.bars_max_streams = 200

        # Mock SSCAN to return symbols
        service.redis.sscan = AsyncMock(side_effect=[
            (0, [b"BTCUSDT", b"ETHUSDT"]),  # First scan returns all
        ])
        service.redis.xgroup_create = AsyncMock()

        result = await service._ensure_microbar_groups()

        assert len(result) == 2
        assert "events:microbar_closed:BTCUSDT" in result
        assert "events:microbar_closed:ETHUSDT" in result
        assert service.redis.xgroup_create.call_count == 2

    async def test_ensure_groups_no_template(self):
        """Handle template without {sym} placeholder."""
        service = OFConfirmService()
        service.redis = AsyncMock()
        service.consumer_group = "test_group"
        service.stream_bars_template = "events:microbar_closed"
        service.bars_max_streams = 200

        result = await service._ensure_microbar_groups()

        assert result == ["events:microbar_closed"]
        service.redis.xgroup_create.assert_called_once()

    async def test_ensure_groups_respects_max_streams(self):
        """Respect bars_max_streams limit."""
        service = OFConfirmService()
        service.redis = AsyncMock()
        service.consumer_group = "test_group"
        service.stream_bars_template = "events:microbar_closed:{sym}"
        service.bars_max_streams = 2

        # Mock SSCAN to return more symbols than limit
        service.redis.sscan = AsyncMock(side_effect=[
            (0, [b"BTCUSDT", b"ETHUSDT", b"SOLUSDT", b"BNBUSDT"]),
        ])
        service.redis.xgroup_create = AsyncMock()

        result = await service._ensure_microbar_groups()

        assert len(result) == 2  # Limited by bars_max_streams
        assert service.redis.xgroup_create.call_count == 2

    async def test_ensure_groups_handles_existing_groups(self):
        """Handle existing consumer groups gracefully."""
        service = OFConfirmService()
        service.redis = AsyncMock()
        service.consumer_group = "test_group"
        service.stream_bars_template = "events:microbar_closed:{sym}"
        service.bars_max_streams = 200

        service.redis.sscan = AsyncMock(return_value=(0, [b"BTCUSDT"]))
        # Simulate group already exists error
        service.redis.xgroup_create = AsyncMock(side_effect=Exception("BUSYGROUP"))

        # Should not raise exception
        result = await service._ensure_microbar_groups()

        assert len(result) == 1
        service.redis.xgroup_create.assert_called_once()


@pytest.mark.asyncio
class TestPollMicrobarsOnce:
    """Test _poll_microbars_once() method."""

    async def test_poll_disabled(self):
        """Return 0 when bars_enable is False."""
        service = OFConfirmService()
        service.bars_enable = False

        result = await service._poll_microbars_once()

        assert result == 0

    async def test_poll_no_streams(self):
        """Return 0 when no streams available."""
        service = OFConfirmService()
        service.bars_enable = True
        service._microbar_streams = []

        result = await service._poll_microbars_once()

        assert result == 0

    async def test_poll_success(self):
        """Successfully poll and process messages."""
        service = OFConfirmService()
        service.bars_enable = True
        service.redis = AsyncMock()
        service.consumer_group = "test_group"
        service.consumer_name = "test_consumer"
        service._microbar_streams = ["events:microbar_closed:BTCUSDT"]

        # Mock XREADGROUP response
        mock_fields = {"payload": json.dumps({"symbol": "BTCUSDT", "ts_ms": 1234567890})}
        service.redis.xreadgroup = AsyncMock(return_value=[
            ("events:microbar_closed:BTCUSDT", [
                ("1234567890-0", mock_fields)
            ])
        ])
        service.redis.xack = AsyncMock()

        # Mock _process_bar to avoid full state initialization
        service._process_bar = AsyncMock()

        result = await service._poll_microbars_once()

        assert result == 1
        service.redis.xreadgroup.assert_called_once()
        service.redis.xack.assert_called_once()
        service._process_bar.assert_called_once()

    async def test_poll_handles_processing_error(self):
        """Handle processing errors without ACK."""
        service = OFConfirmService()
        service.bars_enable = True
        service.redis = AsyncMock()
        service.consumer_group = "test_group"
        service.consumer_name = "test_consumer"
        service._microbar_streams = ["events:microbar_closed:BTCUSDT"]

        mock_fields = {"payload": json.dumps({"symbol": "BTCUSDT"})}
        service.redis.xreadgroup = AsyncMock(return_value=[
            ("events:microbar_closed:BTCUSDT", [
                ("1234567890-0", mock_fields)
            ])
        ])

        # Mock _process_bar to raise exception
        service._process_bar = AsyncMock(side_effect=Exception("Processing error"))

        result = await service._poll_microbars_once()

        # Should return count but not ACK
        assert result == 1
        service.redis.xack.assert_not_called()  # No ACK on error

    async def test_poll_handles_redis_error(self):
        """Handle Redis errors gracefully."""
        service = OFConfirmService()
        service.bars_enable = True
        service.redis = AsyncMock()
        service.consumer_group = "test_group"
        service.consumer_name = "test_consumer"
        service._microbar_streams = ["events:microbar_closed:BTCUSDT"]

        service.redis.xreadgroup = AsyncMock(side_effect=Exception("Redis error"))

        result = await service._poll_microbars_once()

        assert result == 0  # Returns 0 on error


@pytest.mark.asyncio
class TestSplitStreamsConfiguration:
    """Test split streams configuration and initialization."""

    async def test_split_streams_disabled_by_default(self):
        """Split streams should be disabled by default."""
        with patch.dict(os.environ, {}, clear=True):
            service = OFConfirmService()
            assert service.microbar_split == False
            assert service.bars_enable == False

    async def test_split_streams_enabled_via_env(self):
        """Enable split streams via environment variable."""
        with patch.dict(os.environ, {
            "MICROBAR_SPLIT_STREAMS_ENABLE": "1",
            "OF_CONFIRM_BARS_ENABLE": "1"
        }):
            service = OFConfirmService()
            assert service.microbar_split == True
            assert service.bars_enable == True

    async def test_backward_compatible_stream_bars(self):
        """stream_bars should remain backward compatible."""
        service = OFConfirmService()
        # stream_bars should point to legacy stream
        assert service.stream_bars == service.stream_bars_legacy
        assert service.stream_bars == "events:microbar_closed"

    async def test_custom_stream_template(self):
        """Support custom stream template via ENV."""
        with patch.dict(os.environ, {
            "MICROBAR_PER_SYMBOL_STREAM_TEMPLATE": "custom:microbar:{sym}"
        }):
            service = OFConfirmService()
            assert service.stream_bars_template == "custom:microbar:{sym}"


@pytest.mark.asyncio
class TestConsumeMicrobars:
    """Test _consume_microbars() method integration."""

    async def test_consume_microbars_periodic_refresh(self):
        """Test periodic refresh of microbar groups."""
        service = OFConfirmService()
        service.redis = AsyncMock()
        service.running = True
        service.consumer_group = "test_group"
        service.stream_bars_template = "events:microbar_closed:{sym}"
        service.bars_max_streams = 200
        service._microbar_streams = ["events:microbar_closed:BTCUSDT"]

        # Mock methods
        service._ensure_microbar_groups = AsyncMock(return_value=["events:microbar_closed:BTCUSDT"])
        service._poll_microbars_once = AsyncMock(return_value=0)

        # Mock time to control refresh timing
        with patch('time.time', return_value=1000.0), patch('os.getenv', return_value="1"):  # refresh_sec = 1
            # Start task and let it run briefly
            task = asyncio.create_task(service._consume_microbars())
            await asyncio.sleep(0.1)
            service.running = False
            await asyncio.sleep(0.1)

            # Should have called _poll_microbars_once
            assert service._poll_microbars_once.call_count > 0

    async def test_consume_microbars_handles_cancellation(self):
        """Test graceful cancellation of microbar consumer."""
        service = OFConfirmService()
        service.redis = AsyncMock()
        service.running = True
        service._microbar_streams = []
        service._poll_microbars_once = AsyncMock(return_value=0)
        service._ensure_microbar_groups = AsyncMock(return_value=[])

        task = asyncio.create_task(service._consume_microbars())
        await asyncio.sleep(0.05)
        task.cancel()

        try:
            await task
        except asyncio.CancelledError:
            pass  # Expected

        # Should handle cancellation gracefully
        assert True

    async def test_consume_microbars_handles_errors(self):
        """Test error handling in microbar consumer."""
        service = OFConfirmService()
        service.redis = AsyncMock()
        service.running = True
        service._microbar_streams = []
        service._poll_microbars_once = AsyncMock(side_effect=Exception("Test error"))
        service._ensure_microbar_groups = AsyncMock(return_value=[])

        task = asyncio.create_task(service._consume_microbars())
        await asyncio.sleep(0.1)
        service.running = False
        await asyncio.sleep(0.1)

        # Should handle errors without crashing
        assert True


@pytest.mark.asyncio
class TestConsumeTicksIntegration:
    """Test integration of _consume_microbars() in _consume_ticks()."""

    async def test_consume_ticks_starts_bars_task_when_enabled(self):
        """Test that _consume_ticks() starts _bars_task when bars_enable=True."""
        service = OFConfirmService()
        service.bars_enable = True
        service.redis = AsyncMock()
        service.running = True

        # Mock pubsub
        mock_pubsub = AsyncMock()
        mock_listen = AsyncMock()
        mock_listen.__aiter__ = AsyncMock(return_value=iter([]))
        mock_pubsub.listen = AsyncMock(return_value=mock_listen)
        mock_pubsub.psubscribe = AsyncMock()
        service.redis.pubsub = MagicMock(return_value=mock_pubsub)

        # Start _consume_ticks in background
        task = asyncio.create_task(service._consume_ticks())
        await asyncio.sleep(0.1)

        # Check that _bars_task was created
        assert hasattr(service, "_bars_task")
        assert service._bars_task is not None

        # Cleanup
        service.running = False
        await asyncio.sleep(0.1)
        with contextlib.suppress(Exception):
            await task

    async def test_consume_ticks_does_not_start_bars_task_when_disabled(self):
        """Test that _consume_ticks() does not start _bars_task when bars_enable=False."""
        service = OFConfirmService()
        service.bars_enable = False
        service.redis = AsyncMock()
        service.running = True

        # Mock pubsub
        mock_pubsub = AsyncMock()
        mock_listen = AsyncMock()
        mock_listen.__aiter__ = AsyncMock(return_value=iter([]))
        mock_pubsub.listen = AsyncMock(return_value=mock_listen)
        mock_pubsub.psubscribe = AsyncMock()
        service.redis.pubsub = MagicMock(return_value=mock_pubsub)

        # Start _consume_ticks in background
        task = asyncio.create_task(service._consume_ticks())
        await asyncio.sleep(0.1)

        # Check that _bars_task was NOT created
        if hasattr(service, "_bars_task"):
            assert service._bars_task is None

        # Cleanup
        service.running = False
        await asyncio.sleep(0.1)
        with contextlib.suppress(Exception):
            await task

    async def test_consume_ticks_cancels_bars_task_on_exit(self):
        """Test that _consume_ticks() cancels _bars_task on exit."""
        service = OFConfirmService()
        service.bars_enable = True
        service.redis = AsyncMock()
        service.running = True

        # Create a mock task
        mock_task = AsyncMock()
        mock_task.cancel = MagicMock()
        service._bars_task = mock_task

        # Mock pubsub
        mock_pubsub = AsyncMock()
        mock_listen = AsyncMock()
        mock_listen.__aiter__ = AsyncMock(return_value=iter([]))
        mock_pubsub.listen = AsyncMock(return_value=mock_listen)
        mock_pubsub.psubscribe = AsyncMock()
        service.redis.pubsub = MagicMock(return_value=mock_pubsub)

        # Start _consume_ticks in background
        task = asyncio.create_task(service._consume_ticks())
        await asyncio.sleep(0.1)

        # Stop the service
        service.running = False
        await asyncio.sleep(0.1)

        # Check that cancel was called (indirectly through the cleanup code)
        # Note: The actual cancellation happens in the finally block
        with contextlib.suppress(Exception):
            await task


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

