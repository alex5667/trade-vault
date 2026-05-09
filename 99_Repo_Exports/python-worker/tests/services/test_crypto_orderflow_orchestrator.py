import asyncio
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.crypto_orderflow_service import CryptoOrderflowService
import contextlib


@pytest.fixture
def mock_service():
    with patch("redis.asyncio.from_url") as mock_redis_from_url:
        mock_redis = MagicMock()
        mock_redis_from_url.return_value = mock_redis

        # Mock other dependencies that might be initialized
        with patch("services.async_signal_publisher.AsyncSignalPublisher"), \
             patch("core.of_confirm_engine.OFConfirmEngine"):
            service = CryptoOrderflowService(redis_dsn="redis://localhost")
            # Clear automatically created tasks or mocks if needed
            service.main = mock_redis
            service.ticks = mock_redis
            return service

@pytest.mark.asyncio
async def test_load_dynamic_symbols_applies_config(mock_service):
    """Verify that apply_config is called for both new and existing runtimes."""
    service = mock_service
    service.main.smembers = AsyncMock(return_value=["BTCUSDT"])

    # Mock config loader
    service.config_loader = MagicMock()
    mock_config = {"some_key": "some_val"}
    service.config_loader.build_symbol_config = AsyncMock(return_value=mock_config)

    # Mock _resolve_streams
    service._resolve_streams = AsyncMock(return_value=("stream:tick_BTCUSDT", "stream:book_BTCUSDT"))

    # 1. First load (new symbol)
    with patch("services.crypto_orderflow_service.SymbolRuntime") as MockRuntime:
        mock_runtime_inst = MockRuntime.return_value
        await service.load_dynamic_symbols()

        # Should create new runtime
        MockRuntime.assert_called()
        # Should apply config
        mock_runtime_inst.apply_config.assert_called_with(mock_config)

    # 2. Second load (existing symbol)
    new_config = {"new_key": "new_val"}
    service.config_loader.build_symbol_config.return_value = new_config

    # Manually add the symbol to bypass re-creation in the second call
    service.symbol_contexts["BTCUSDT"] = mock_runtime_inst

    await service.load_dynamic_symbols()

    # Should stay in contexts
    assert "BTCUSDT" in service.symbol_contexts
    # Should apply NEW config
    mock_runtime_inst.apply_config.assert_called_with(new_config)

@pytest.mark.asyncio
async def test_consume_ticks_acks_on_success(mock_service):
    """Verify that messages are ACKED even if no signal is generated."""
    service = mock_service

    symbol = "BTCUSDT"
    runtime = MagicMock()
    runtime.tick_stream = "stream:tick_BTCUSDT"
    runtime.tick_group = "group"
    runtime.config = {"read_count": 10, "read_block_ms": 100}
    service.symbol_contexts[symbol] = runtime

    # Mock AsyncRedisStreamHelper
    with patch("services.crypto_orderflow_service.AsyncRedisStreamHelper") as MockHelper:
        helper_inst = MockHelper.return_value
        helper_inst.ensure_group = AsyncMock()
        helper_inst.read = AsyncMock()
        helper_inst.ack = AsyncMock()

        helper_inst.read.side_effect = [
            [("stream:tick_BTCUSDT", [("msg_id_1", {"data": '{"price":100, "side":"BUY"}'})])],
            asyncio.CancelledError() # Stop the loop
        ]

        # Mock strategy to return NO signal
        service.strategy = AsyncMock()
        service.strategy.process_tick = AsyncMock(return_value=None)

        with contextlib.suppress(asyncio.CancelledError):
            await service.consume_ticks(symbol)

        # Verify ACK was called
        helper_inst.ack.assert_called_with("stream:tick_BTCUSDT", "msg_id_1")

@pytest.mark.asyncio
async def test_consume_books_parses_correctly(mock_service):
    """Verify that consume_books uses _fields_to_dict before parsing."""
    service = mock_service

    symbol = "BTCUSDT"
    runtime = MagicMock()
    runtime.book_stream = "stream:book_BTCUSDT"
    runtime.book_group = "group"
    runtime.config = {"read_count": 10, "read_block_ms": 100}
    service.symbol_contexts[symbol] = runtime

    with patch("services.crypto_orderflow_service.AsyncRedisStreamHelper") as MockHelper:
        helper_inst = MockHelper.return_value
        helper_inst.ensure_group = AsyncMock()
        helper_inst.read = AsyncMock()
        helper_inst.ack = AsyncMock()

        # Mock book payload as a list of tuples (Redis style)
        book_fields = [("bids", "[[100,1]]"), ("asks", "[[101,1]]")]
        helper_inst.read.side_effect = [
            [("stream:book_BTCUSDT", [("msg_id_b1", book_fields)])],
            asyncio.CancelledError()
        ]

        with patch("services.crypto_orderflow_service._fields_to_dict") as mock_f2d, \
             patch("services.crypto_orderflow_service._parse_book_payload") as mock_parse:
            mock_f2d.return_value = {"bids": "[[100,1]]", "asks": "[[101,1]]"}

            with contextlib.suppress(asyncio.CancelledError):
                 await service.consume_books(symbol)

            # Verify _fields_to_dict was called with raw payload
            mock_f2d.assert_called_with(book_fields)
            # Verify ACK was called
            helper_inst.ack.assert_called_with("stream:book_BTCUSDT", "msg_id_b1")

