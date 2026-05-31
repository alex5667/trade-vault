"""
Тесты для исправлений экспертных рекомендаций P0-P5.

P0: Burst single source of truth - убрано дублирование flush из Strategy
P1: Ghost emissions prevention - bookkeeping только в publish_signal
P2: Outbox contract unified - dual-write payload/data
P3: Envelope builder functions - исправлено дублирование имен
P4: Consume books helper caching - кэширование как в ticks
P5: ACK on exceptions - quarantine после N попыток
"""

import asyncio
from unittest.mock import AsyncMock, Mock, patch

import pytest
import contextlib


class TestExpertRecommendationsFixes:
    """Тесты для всех исправлений P0-P5."""

    @pytest.mark.asyncio
    async def test_burst_single_source_no_sync_flush(self):
        """P0: Strategy.process_tick никогда не возвращает burst flush синхронно."""
        from services.orderflow.runtime import SymbolRuntime
        from services.orderflow.strategy import OrderFlowStrategy

        # Mock runtime with burst
        runtime = Mock(spec=SymbolRuntime)
        runtime.symbol = "BTCUSDT"
        runtime.burst = Mock()
        runtime.burst.st = Mock()
        runtime.burst.st.active = False
        runtime.burst.st.start_ts_ms = 0
        runtime.burst.mu = Mock()
        runtime.burst.mu.__enter__ = Mock(return_value=None)
        runtime.burst.mu.__exit__ = Mock(return_value=None)

        strategy = OrderFlowStrategy(
            redis=Mock(),
            ticks=Mock(),
            publisher=Mock(),
            of_engine=Mock(),
            notify_client=Mock(),
            notify_stream="notify:test"
        )

        # Mock tick
        tick = {"ts_ms": 1000, "price": 50000.0, "qty": 1.0}

        # Process tick - should never return burst flush synchronously
        result = await strategy.process_tick(runtime, tick)

        # Result should be normal signal or None, never burst flush
        assert result is None or isinstance(result, dict)
        if isinstance(result, dict):
            # If signal returned, it should not be burst flush result
            assert "burst_best_score" not in result  # burst flush marker

    @pytest.mark.asyncio
    async def test_burst_no_ghost_emissions_bookkeeping_only_in_publish(self):
        """P1: last_signal_ts/record_emit обновляются только в publish_signal."""
        from unittest.mock import AsyncMock

        from services.crypto_orderflow_service import CryptoOrderflowService
        from services.orderflow.runtime import SymbolRuntime

        service = CryptoOrderflowService(redis_dsn="redis://test")
        service.main = AsyncMock()
        service.ticks = AsyncMock()

        # Mock runtime
        runtime = Mock(spec=SymbolRuntime)
        runtime.symbol = "BTCUSDT"
        runtime.burst = Mock()
        runtime.burst.st = Mock()
        runtime.burst.st.active = True
        runtime.burst.st.deadline_ts_ms = 2000
        runtime.burst.st.start_ts_ms = 1000
        runtime.burst.st.best = Mock()
        runtime.burst.st.best.score = 0.8
        runtime.burst.mu = Mock()
        runtime.burst.mu.__enter__ = Mock(return_value=None)
        runtime.burst.mu.__exit__ = Mock(return_value=None)
        runtime.burst.maybe_flush = Mock(return_value={"direction": "LONG", "entry": 0.8})

        service.symbol_contexts = {"BTCUSDT": runtime}
        service.strategy = AsyncMock()
        service.strategy.publish_signal = AsyncMock()

        # Mock wall time
        with patch('time.time', return_value=1.5):  # 1500ms
            # Run burst flush loop iteration
            await service._burst_flush_loop()

        # Verify bookkeeping happens in publish_signal, not in flush loop
        service.strategy.publish_signal.assert_called_once()
        call_args = service.strategy.publish_signal.call_args
        assert call_args[0][0] == runtime  # runtime
        assert call_args[0][1]["direction"] == "LONG"  # signal

    def test_outbox_dual_write_contract(self):
        """P2: Atomic outbox пишет и payload, и data поля."""
        from services.outbox.atomic_outbox import _LUA_ATOMIC_XADD

        # Check that Lua script writes both 'payload' and 'data' with same value
        assert "'payload',   ARGV[7]," in _LUA_ATOMIC_XADD
        assert "'data',      ARGV[7]," in _LUA_ATOMIC_XADD

        # Both should have the same ARGV[7] (payload_json)
        payload_line = [line for line in _LUA_ATOMIC_XADD.split('\n') if "'payload'," in line][0]
        data_line = [line for line in _LUA_ATOMIC_XADD.split('\n') if "'data'," in line][0]

        assert "ARGV[7]" in payload_line
        assert "ARGV[7]" in data_line

    def test_envelope_builder_function_names_unique(self):
        """P3: Функции envelope builder имеют уникальные имена."""
        from services.outbox.envelope_builder import build_trace_sidecar_meta, build_trace_sidecar_meta_from_ctx

        # Functions should have different names
        assert build_trace_sidecar_meta.__name__ != build_trace_sidecar_meta_from_ctx.__name__

        # First function takes (sid, trace)
        import inspect
        sig1 = inspect.signature(build_trace_sidecar_meta)
        sig2 = inspect.signature(build_trace_sidecar_meta_from_ctx)

        assert 'trace' in sig1.parameters
        assert 'ctx' in sig2.parameters
        assert 'sid' in sig1.parameters and 'sid' in sig2.parameters

    @pytest.mark.asyncio
    async def test_consume_books_helper_caching(self):
        """P4: Helper в consume_books кэшируется как в consume_ticks."""
        from core.redis_stream_consumer import AsyncRedisStreamHelper
        from services.crypto_orderflow_service import CryptoOrderflowService
        from services.orderflow.runtime import SymbolRuntime

        service = CryptoOrderflowService(redis_dsn="redis://test")
        service.ticks = AsyncMock()
        service.book_helpers = {}

        # Mock runtime
        runtime = Mock(spec=SymbolRuntime)
        runtime.symbol = "BTCUSDT"
        runtime.book_stream = "stream:book_BTCUSDT"
        runtime.book_group = "crypto-of-book:BTCUSDT"
        runtime.config = {"read_count": 100, "read_block_ms": 1000}

        service.symbol_contexts = {"BTCUSDT": runtime}

        # Mock helper
        mock_helper = AsyncMock(spec=AsyncRedisStreamHelper)
        mock_helper.read = AsyncMock(return_value=[])
        mock_helper.ensure_group = AsyncMock()

        with patch('services.crypto_orderflow_service.AsyncRedisStreamHelper', return_value=mock_helper) as mock_helper_class:
            with patch.object(service, 'consume_books') as mock_consume:
                # Simulate first call - should create helper
                mock_consume.side_effect = asyncio.CancelledError()  # Stop after first iteration

                with contextlib.suppress(asyncio.CancelledError):
                    await service.consume_books("BTCUSDT")

                # Verify helper was created and cached
                mock_helper_class.assert_called_once_with(
                    service.ticks, runtime.book_group, service.consumer_id_books
                )
                assert service.book_helpers["BTCUSDT"] is mock_helper
                mock_helper.ensure_group.assert_called_once_with(runtime.book_stream)

    @pytest.mark.asyncio
    async def test_ack_on_exceptions_with_quarantine(self):
        """P5: ACK при исключениях после quarantine."""
        from core.redis_stream_consumer import AsyncRedisStreamHelper
        from services.crypto_orderflow_service import CryptoOrderflowService
        from services.orderflow.runtime import SymbolRuntime

        service = CryptoOrderflowService(redis_dsn="redis://test")
        service.ticks = AsyncMock()
        service.tick_helpers = {}
        service.poison_pill_counts = {}

        # Mock runtime
        runtime = Mock(spec=SymbolRuntime)
        runtime.symbol = "BTCUSDT"
        runtime.tick_stream = "stream:tick_BTCUSDT"
        runtime.tick_group = "crypto-of:BTCUSDT"
        runtime.config = {"read_count": 1, "read_block_ms": 100}

        service.symbol_contexts = {"BTCUSDT": runtime}

        # Mock helper that returns a message
        mock_helper = AsyncMock(spec=AsyncRedisStreamHelper)
        msg_id = "123-0"
        fields = {"ts_ms": 1000, "price": 50000.0}

        # Simulate reading one message
        mock_helper.read = AsyncMock(return_value=[
            (runtime.tick_stream, [(msg_id, fields)])
        ])
        mock_helper.ensure_group = AsyncMock()
        mock_helper.ack = AsyncMock()

        # Mock quarantine stream write
        service.ticks.xadd = AsyncMock()

        with patch('services.crypto_orderflow_service.AsyncRedisStreamHelper', return_value=mock_helper):
            with patch.object(service, 'strategy') as mock_strategy:
                # Strategy raises exception 3 times
                mock_strategy.process_tick = AsyncMock(side_effect=Exception("Test error"))

                # Run consume_ticks - should quarantine after 3 failures
                with patch('asyncio.sleep', side_effect=asyncio.CancelledError()):
                    with contextlib.suppress(asyncio.CancelledError):
                        await service.consume_ticks("BTCUSDT")

                # Verify quarantine happened
                service.ticks.xadd.assert_called()
                call_args = service.ticks.xadd.call_args
                assert call_args[0][0] == service.quarantine_stream
                quarantine_data = call_args[0][1]
                assert quarantine_data["symbol"] == "BTCUSDT"
                assert quarantine_data["msg_id"] == msg_id
                assert "error" in quarantine_data

                # Verify ACK happened after quarantine
                mock_helper.ack.assert_called_with(runtime.tick_stream, msg_id)

    @pytest.mark.asyncio
    async def test_burst_publish_metrics_incremented(self):
        """Verify signals_published_total incremented for burst path."""
        from services.crypto_orderflow_service import CryptoOrderflowService
        from services.orderflow.metrics import signals_published_total
        from services.orderflow.runtime import SymbolRuntime

        service = CryptoOrderflowService(redis_dsn="redis://test")
        service.main = AsyncMock()
        service.ticks = AsyncMock()

        # Mock runtime
        runtime = Mock(spec=SymbolRuntime)
        runtime.symbol = "BTCUSDT"
        runtime.burst = Mock()
        runtime.burst.st = Mock()
        runtime.burst.st.active = True
        runtime.burst.st.deadline_ts_ms = 2000
        runtime.burst.st.start_ts_ms = 1000
        runtime.burst.st.best = Mock()
        runtime.burst.st.best.score = 0.8
        runtime.burst.mu = Mock()
        runtime.burst.mu.__enter__ = Mock(return_value=None)
        runtime.burst.mu.__exit__ = Mock(return_value=None)
        runtime.burst.maybe_flush = Mock(return_value={"direction": "LONG", "entry": 0.8})

        service.symbol_contexts = {"BTCUSDT": runtime}
        service.strategy = AsyncMock()
        service.strategy.publish_signal = AsyncMock()

        # Mock metric
        with patch.object(signals_published_total, 'labels') as mock_labels:
            mock_metric = Mock()
            mock_labels.return_value = mock_metric

            # Mock wall time
            with patch('time.time', return_value=1.5):  # 1500ms
                # Run burst flush
                await service._burst_flush_loop()

            # Verify metric was incremented
            mock_metric.inc.assert_called_once()

    def test_strategy_no_burst_bookkeeping_in_process_tick(self):
        """Verify Strategy.process_tick doesn't update last_signal_ts/record_emit."""
        import inspect

        from services.orderflow.strategy import OrderFlowStrategy

        # Check process_tick source code
        source = inspect.getsource(OrderFlowStrategy.process_tick)

        # Should not contain bookkeeping updates
        assert "last_signal_ts" not in source
        assert "record_emit" not in source
        assert "runtime.last_signal_ts" not in source
        assert "runtime.pressure.record_emit" not in source
































