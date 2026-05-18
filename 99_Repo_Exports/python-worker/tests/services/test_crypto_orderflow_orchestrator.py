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

        with patch("services.async_signal_publisher.AsyncSignalPublisher"), \
             patch("core.of_confirm_engine.OFConfirmEngine"):
            service = CryptoOrderflowService(redis_dsn="redis://localhost")
            service.main = mock_redis
            service.ticks = mock_redis
            return service


@pytest.mark.asyncio
async def test_load_dynamic_symbols_applies_config(mock_service):
    """Verify that apply_config is called for both new and existing runtimes."""
    service = mock_service
    service.main.smembers = AsyncMock(return_value=["BTCUSDT"])

    service.config_loader = MagicMock()
    mock_config = {"some_key": "some_val"}
    service.config_loader.build_symbol_config = AsyncMock(return_value=mock_config)

    service._resolve_streams = AsyncMock(return_value=("stream:tick_BTCUSDT", "stream:book_BTCUSDT"))

    with patch("services.crypto_orderflow_service.SymbolRuntime") as MockRuntime:
        mock_runtime_inst = MockRuntime.return_value
        await service.load_dynamic_symbols()

        MockRuntime.assert_called()
        mock_runtime_inst.apply_config.assert_called_with(mock_config)

    new_config = {"new_key": "new_val"}
    service.config_loader.build_symbol_config.return_value = new_config

    service.symbol_contexts["BTCUSDT"] = mock_runtime_inst

    await service.load_dynamic_symbols()

    assert "BTCUSDT" in service.symbol_contexts
    mock_runtime_inst.apply_config.assert_called_with(new_config)


@pytest.mark.asyncio
async def test_consume_ticks_acks_on_success(mock_service):
    """Verify that messages are ACKed via _xack_pipeline after successful tick processing."""
    service = mock_service

    symbol = "BTCUSDT"
    runtime = MagicMock()
    runtime.tick_stream = "stream:tick_BTCUSDT"
    runtime.tick_group = "group"
    runtime.config = {"read_count": 10, "read_block_ms": 100}
    runtime.loop_log_sampler = MagicMock()
    runtime.loop_log_sampler.should_log.return_value = False
    runtime.throttle_log_sampler = MagicMock()
    runtime.throttle_log_sampler.should_log.return_value = False
    runtime.ensure_specs_fresh = AsyncMock()
    service.symbol_contexts[symbol] = runtime

    # Stub calib_svc so the coroutine doesn't block
    service.calib_svc = MagicMock()
    service.calib_svc.ensure_loaded = AsyncMock()

    with patch("services.crypto_orderflow_service.AsyncRedisStreamHelper") as MockHelper:
        helper_inst = MockHelper.return_value
        helper_inst.ensure_group = AsyncMock()
        helper_inst.read = AsyncMock(side_effect=[
            [("stream:tick_BTCUSDT", [("msg_id_1", {"data": '{"price":100, "side":"BUY"}'})])],
            asyncio.CancelledError(),
        ])

        # _tick_proc.process_tick returns True → msg_id goes into ack_ids
        service._tick_proc = MagicMock()
        service._tick_proc.process_tick = AsyncMock(return_value=True)

        # Capture _xack_pipeline calls (replaces helper.ack in the refactored code)
        service._xack_pipeline = AsyncMock()

        with contextlib.suppress(asyncio.CancelledError):
            await service.consume_ticks(symbol)

        # XACK pipeline must have been called for the processed message
        service._xack_pipeline.assert_called_once()
        call_kw = service._xack_pipeline.call_args.kwargs
        assert call_kw["stream"] == "stream:tick_BTCUSDT"
        assert "msg_id_1" in call_kw["ids"]


@pytest.mark.asyncio
async def test_consume_books_parses_correctly(mock_service):
    """Verify that consume_books delegates to strategy.process_book and ACKs via _xack_pipeline."""
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

        book_fields = [("bids", "[[100,1]]"), ("asks", "[[101,1]]")]
        helper_inst.read = AsyncMock(side_effect=[
            [("stream:book_BTCUSDT", [("msg_id_b1", book_fields)])],
            asyncio.CancelledError(),
        ])

        # Stub book processing
        service.strategy = MagicMock()
        service.strategy.process_book = AsyncMock()

        # Capture _xack_pipeline calls
        service._xack_pipeline = AsyncMock()

        with contextlib.suppress(asyncio.CancelledError):
            await service.consume_books(symbol)

        # process_book must have been called once
        service.strategy.process_book.assert_called_once()
        call_args = service.strategy.process_book.call_args
        assert call_args.args[0] is runtime  # first positional: runtime

        # ACK must have happened via _xack_pipeline (not helper.ack)
        service._xack_pipeline.assert_called_once()
        call_kw = service._xack_pipeline.call_args.kwargs
        assert call_kw["stream"] == "stream:book_BTCUSDT"
        assert "msg_id_b1" in call_kw["ids"]
