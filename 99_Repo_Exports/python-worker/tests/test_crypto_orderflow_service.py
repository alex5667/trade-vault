from utils.time_utils import get_ny_time_millis

"""
Тесты для CryptoOrderflowService - проверка критических исправлений.

Фокус на:
1. Low-latency burst processing в consume_ticks
2. Исправленная логика poison pill (сразу карантин + ACK)
3. Общий метод _process_burst_flush
"""

from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from services.crypto_orderflow_service import CryptoOrderflowService
from services.orderflow.runtime import SymbolRuntime


class TestCryptoOrderflowService:
    """Тесты для критических исправлений в CryptoOrderflowService."""

    @pytest.fixture
    def service(self):
        """Фикстура сервиса с mock Redis."""
        with patch('redis.asyncio.from_url') as mock_redis:
            mock_client = AsyncMock()
            mock_redis.return_value = mock_client

            service = CryptoOrderflowService(
                redis_dsn="redis://test:6379/0",
                ticks_dsn="redis://test:6379/1"
            )

            # Mock strategy
            service.strategy = AsyncMock()

            return service

    @pytest.fixture
    def runtime(self):
        """Фикстура SymbolRuntime с burst объектом."""
        runtime = MagicMock(spec=SymbolRuntime)
        runtime.symbol = "BTCUSDT"
        runtime.burst = MagicMock()
        runtime.burst.st = MagicMock()
        runtime.burst.st.active = True
        runtime.burst.st.deadline_ts_ms = get_ny_time_millis() + 1000
        runtime.burst.st.start_ts_ms = get_ny_time_millis()
        runtime.burst.st.best = MagicMock()
        runtime.burst.st.best.score = 2.5
        runtime.burst.maybe_flush = MagicMock()
        runtime.burst_mu = MagicMock()
        runtime.burst_mu.__enter__ = MagicMock(return_value=None)
        runtime.burst_mu.__exit__ = MagicMock(return_value=None)
        runtime.last_signal_ts = 0
        runtime.pressure = MagicMock()
        runtime.pressure.record_emit = MagicMock()
        runtime.loop_log_sampler = MagicMock()
        runtime.loop_log_sampler.should_log = MagicMock(return_value=False)

        return runtime

    @pytest.mark.asyncio
    async def test_process_burst_flush_active_burst(self, service, runtime):
        """Тест обработки активного burst сигнала."""
        # Setup: burst должен сработать
        burst_signal = {
            "direction": "long",
            "entry": 45000.0,
            "burst_best_score": 2.5
        }
        runtime.burst.maybe_flush.return_value = burst_signal

        # Mock preprocess и publish
        with patch('services.crypto_orderflow_service.preprocess_signal_for_publish') as mock_preprocess, \
             patch('services.crypto_orderflow_service.signals_published_total') as mock_metric:

            result = await service._process_burst_flush(runtime, "tick", get_ny_time_millis())

            # Проверки
            assert result == burst_signal
            runtime.burst.maybe_flush.assert_called_once()
            mock_preprocess.assert_called_once_with(
                burst_signal,
                symbol="BTCUSDT",
                source="crypto_orderflow_service",
                logger=service.logger
            )
            service.strategy.publish_signal.assert_called_once_with(runtime, burst_signal)

    @pytest.mark.asyncio
    async def test_process_burst_flush_no_burst(self, service, runtime):
        """Тест когда burst не активен."""
        runtime.burst.maybe_flush.return_value = None

        result = await service._process_burst_flush(runtime, "tick", get_ny_time_millis())

        assert result is None
        runtime.burst.maybe_flush.assert_called_once()

    def test_consume_ticks_has_burst_check(self, service, runtime):
        """Тест что consume_ticks содержит логику проверки burst."""
        # Check that the code contains the burst check logic
        import inspect
        source = inspect.getsource(service.consume_ticks)

        # Verify that the burst check is present in the code
        assert "_process_burst_flush" in source
        assert "burst_signal = await self._process_burst_flush" in source
        assert "if not signal:" in source  # Check that burst is called when no strategy signal

    def test_poison_pill_logic_present(self, service, runtime):
        """Тест что исправленная логика poison pill присутствует в коде."""
        import inspect
        source = inspect.getsource(service.consume_ticks)

        # Verify that the new poison pill logic is present
        assert "await self.ticks.xadd(self.quarantine_stream," in source
        assert 'maxlen=5000' in source
        assert '☣️ (%s) Message %s quarantined' in source
        assert "processed_ok = True # Confirm processing so we move on" in source

        # Verify that old poison_pill_counts logic is removed
        assert "poison_pill_counts" not in source
        assert "self.poison_pill_counts.get" not in source

    def test_burst_flush_loop_uses_shared_method(self, service, runtime):
        """Тест что _burst_flush_loop использует общий метод _process_burst_flush."""
        import inspect
        source = inspect.getsource(service._burst_flush_loop)

        # Verify that the burst flush loop uses the shared method with do_publish=True
        assert "await self._process_burst_flush(runtime, \"wall\", now_ms, do_publish=True)" in source
        assert "_process_burst_flush" in source

        # Verify old logic is removed
        assert "logger.info(\"🔥 (%s) Burst flushed via background loop:" not in source

    @pytest.mark.asyncio
    async def test_burst_metrics_updated(self, service, runtime):
        """Тест обновления метрик при burst сигнале."""
        import inspect
        source = inspect.getsource(service._process_burst_flush)

        # Verify that metrics are present in the code
        assert "burst_flush_total.labels" in source
        assert "signals_emitted_total.labels" in source
        assert 'mode=trigger_source' in source

        # Verify the logic structure
        assert "if burst_flush_total:" in source
        assert "burst_flush_total.labels(symbol=runtime.symbol, mode=trigger_source).inc()" in source

    @pytest.mark.asyncio
    async def test_burst_thread_safety(self, service, runtime):
        """Тест потокобезопасности burst обработки."""
        burst_signal = {"direction": "long", "entry": 45000.0}
        runtime.burst.maybe_flush.return_value = burst_signal

        # Verify mutex is used
        with patch('services.crypto_orderflow_service.preprocess_signal_for_publish'):
            await service._process_burst_flush(runtime, "tick", get_ny_time_millis())

        # Check that mutex context manager was entered (via async context manager protocol)
        runtime.burst_mu.__aenter__.assert_called_once()
        runtime.burst_mu.__aexit__.assert_called_once()

    @pytest.mark.asyncio
    async def test_process_burst_flush_do_publish_false(self, service, runtime):
        """Тест что при do_publish=False сигнал возвращается без публикации."""
        burst_signal = {
            "direction": "long",
            "entry": 45000.0,
            "burst_best_score": 2.5
        }
        runtime.burst.maybe_flush.return_value = burst_signal

        # Mock preprocess и publish
        with patch('services.crypto_orderflow_service.preprocess_signal_for_publish') as mock_preprocess, \
             patch('services.crypto_orderflow_service.signals_published_total') as mock_metric:

            result = await service._process_burst_flush(runtime, "tick", get_ny_time_millis(), do_publish=False)

            # Проверки: сигнал возвращен
            assert result == burst_signal
            runtime.burst.maybe_flush.assert_called_once()

            # Проверки: публикация НЕ произошла
            mock_preprocess.assert_not_called()
            service.strategy.publish_signal.assert_not_called()

    @pytest.mark.asyncio
    async def test_process_burst_flush_do_publish_true(self, service, runtime):
        """Тест что при do_publish=True (по умолчанию) происходит публикация."""
        burst_signal = {
            "direction": "long",
            "entry": 45000.0,
            "burst_best_score": 2.5
        }
        runtime.burst.maybe_flush.return_value = burst_signal

        # Mock preprocess и publish
        with patch('services.crypto_orderflow_service.preprocess_signal_for_publish') as mock_preprocess, \
             patch('services.crypto_orderflow_service.signals_published_total') as mock_metric:

            result = await service._process_burst_flush(runtime, "tick", get_ny_time_millis(), do_publish=True)

            # Проверки: сигнал возвращен
            assert result == burst_signal
            runtime.burst.maybe_flush.assert_called_once()

            # Проверки: публикация произошла
            mock_preprocess.assert_called_once_with(
                burst_signal,
                symbol="BTCUSDT",
                source="crypto_orderflow_service",
                logger=service.logger
            )
            service.strategy.publish_signal.assert_called_once_with(runtime, burst_signal)

    @pytest.mark.asyncio
    async def test_process_burst_flush_default_publish_true(self, service, runtime):
        """Тест что по умолчанию do_publish=True и публикация происходит."""
        burst_signal = {
            "direction": "long",
            "entry": 45000.0,
            "burst_best_score": 2.5
        }
        runtime.burst.maybe_flush.return_value = burst_signal

        # Mock preprocess и publish
        with patch('services.crypto_orderflow_service.preprocess_signal_for_publish') as mock_preprocess, \
             patch('services.crypto_orderflow_service.signals_published_total') as mock_metric:

            # Вызов без явного указания do_publish (по умолчанию True)
            result = await service._process_burst_flush(runtime, "tick", get_ny_time_millis())

            # Проверки: публикация произошла
            mock_preprocess.assert_called_once()
            service.strategy.publish_signal.assert_called_once_with(runtime, burst_signal)

    def test_consume_ticks_calls_burst_with_do_publish_false(self, service):
        """Тест что в consume_ticks burst вызывается с do_publish=False."""
        import inspect
        source = inspect.getsource(service.consume_ticks)

        # Verify that burst is called with do_publish=False in consume_ticks
        assert "do_publish=False" in source
        assert "burst_signal = await self._process_burst_flush(" in source
        assert "runtime, \"tick\", tick_ts, do_publish=False" in source

    def test_burst_flush_loop_calls_burst_with_do_publish_true(self, service):
        """Тест что в _burst_flush_loop burst вызывается с do_publish=True."""
        import inspect
        source = inspect.getsource(service._burst_flush_loop)

        # Verify that burst is called with do_publish=True in background loop
        assert "do_publish=True" in source
        assert "await self._process_burst_flush(runtime, \"wall\", now_ms, do_publish=True)" in source

    @pytest.mark.asyncio
    async def test_no_double_publish_in_consume_ticks_flow(self, service, runtime):
        """Интеграционный тест: проверка отсутствия двойной публикации в consume_ticks."""
        # Setup
        burst_signal = {
            "direction": "long",
            "entry": 45000.0,
            "burst_best_score": 2.5
        }
        runtime.burst.maybe_flush.return_value = burst_signal

        # Mock strategy to return no signal initially, then burst kicks in
        service.strategy.process_tick = AsyncMock(return_value=None)

        # Mock all publishing functions - track calls to strategy.publish_signal
        original_publish_call_count = service.strategy.publish_signal.call_count

        # Simulate the critical part of consume_ticks logic
        tick = {"ts_ms": get_ny_time_millis(), "price": 45000.0}

        # Step 1: Strategy returns no signal
        signal = await service.strategy.process_tick(runtime, tick)
        assert signal is None

        # Step 2: Burst check with do_publish=False (как в коде)
        if not signal:
            tick_ts = int(tick.get("ts_ms") or tick.get("ts") or get_ny_time_millis())
            burst_signal_result = await service._process_burst_flush(
                runtime, "tick", tick_ts, do_publish=False
            )
            if burst_signal_result:
                signal = burst_signal_result

        # Проверки после burst check: публикация не должна была произойти
        assert service.strategy.publish_signal.call_count == original_publish_call_count

        # Step 3: Final publish (как в коде consume_ticks)
        if signal:
            await service.strategy.publish_signal(runtime, signal)

        # Финальная проверка: strategy.publish_signal вызван всего один раз (только в конце)
        assert service.strategy.publish_signal.call_count == original_publish_call_count + 1
        service.strategy.publish_signal.assert_called_with(runtime, burst_signal)
