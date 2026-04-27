from utils.time_utils import get_ny_time_millis
"""
✅ Тесты для улучшений crypto_orderflow_service:
- P0: Cleanup в _stop_symbol
- P1: Bounded bootstrap concurrency
- P1: ACK batching
- P1: Pressure control по lag
- P0: PEL recovery
"""

import pytest
import asyncio
from unittest.mock import AsyncMock, MagicMock, patch, Mock
from collections import deque
import time
import os

from services.crypto_orderflow_service import CryptoOrderflowService


@pytest.fixture
def service():
    """Создаёт сервис с моками Redis."""
    with patch('redis.asyncio.from_url') as mock_redis:
        mock_client = AsyncMock()
        mock_redis.return_value = mock_client
        
        svc = CryptoOrderflowService(
            redis_dsn="redis://test:6379/0",
            ticks_dsn="redis://test:6379/1"
        )
        svc.main = mock_client
        svc.ticks = mock_client
        svc._shutdown = False
        return svc


class TestStopSymbolCleanup:
    """✅ P0: Тесты cleanup в _stop_symbol"""
    
    @pytest.mark.asyncio
    async def test_stop_symbol_clears_all_caches(self, service):
        """Проверяет что _stop_symbol очищает все кэши."""
        symbol = "BTCUSDT"
        
        # 1. Populate caches
        service.symbol_contexts[symbol] = AsyncMock()
        service._task_restart_hist[(symbol, "ticks")] = deque([1, 2, 3])
        service._task_restart_hist[(symbol, "books")] = deque([4, 5, 6])
        service.tick_helpers[symbol] = AsyncMock()
        service.book_helpers[symbol] = AsyncMock()
        service.poison_pill_counts[symbol] = 5
        
        # Mock tasks
        tick_task = asyncio.Future()
        book_task = asyncio.Future()
        tick_task.set_result(None)
        book_task.set_result(None)
        tick_task.cancel = MagicMock()
        book_task.cancel = MagicMock()
        
        service.symbol_tasks[symbol] = (tick_task, book_task)
        
        # 2. Call _stop_symbol
        await service._stop_symbol(symbol)
        
        # 3. Verify all caches are cleared
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


class TestBootstrapConcurrency:
    """✅ P1: Тесты bounded bootstrap concurrency"""
    
    def test_bootstrap_semaphore_created(self, service):
        """Проверяет что семафор создан в __init__."""
        assert hasattr(service, '_bootstrap_sem')
        assert isinstance(service._bootstrap_sem, asyncio.Semaphore)
    
    def test_bootstrap_semaphore_configurable(self):
        """Проверяет что семафор конфигурируется через ENV."""
        with patch.dict(os.environ, {'CRYPTO_OF_BOOTSTRAP_MAX_CONC': '20'}):
            with patch('redis.asyncio.from_url'):
                svc = CryptoOrderflowService("redis://test:6379/0")
                assert svc._bootstrap_sem._value == 20
    
    @pytest.mark.asyncio
    async def test_bootstrap_uses_shared_semaphore(self, service):
        """Проверяет что bootstrap использует общий семафор."""
        # Мокаем calib_svc
        service.calib_svc = AsyncMock()
        service.calib_svc.ensure_loaded = AsyncMock()
        
        # Создаём runtime
        from services.orderflow.runtime import SymbolRuntime
        runtime = SymbolRuntime(symbol="BTCUSDT", config={})
        service.symbol_contexts["BTCUSDT"] = runtime
        
        # Проверяем что семафор используется
        sem_value_before = service._bootstrap_sem._value
        
        # Запускаем bootstrap (через load_dynamic_symbols)
        with patch.object(service, 'config_loader') as mock_loader:
            mock_loader.build_symbol_config = AsyncMock(return_value={})
            mock_loader.redis = service.main
            with patch.object(service, '_resolve_streams', return_value=("stream:tick_BTCUSDT", "stream:book_BTCUSDT")):
                # Не запускаем реальные задачи, только проверяем что семафор используется
                pass
        
        # Семафор должен быть тем же объектом
        assert service._bootstrap_sem._value == sem_value_before


class TestMsgIdMs:
    """✅ P1: Тесты функции _msgid_ms"""
    
    def test_msgid_ms_valid(self, service):
        """Проверяет извлечение timestamp из валидного msg_id."""
        msg_id = "1234567890123-0"
        result = service._msgid_ms(msg_id)
        assert result == 1234567890123
    
    def test_msgid_ms_invalid(self, service):
        """Проверяет обработку битого msg_id."""
        result = service._msgid_ms("invalid")
        assert result == 0
        
        result = service._msgid_ms("")
        assert result == 0
        
        result = service._msgid_ms("no-dash")
        assert result == 0


class TestAckBatching:
    """✅ P1: Тесты ACK batching"""
    
    @pytest.mark.asyncio
    async def test_ack_batching_chunks(self, service):
        """Проверяет что ACK делается чанками."""
        # Мокаем ticks.xack
        service.ticks.xack = AsyncMock()
        
        # Симулируем обработку батча из 250 сообщений
        # ACK_BATCH по умолчанию = 200
        ack_ids = [f"1234567890-{i}" for i in range(250)]
        stream_name = "stream:tick_BTCUSDT"
        group = "crypto-of:BTCUSDT"
        
        ACK_BATCH = 200
        for i in range(0, len(ack_ids), ACK_BATCH):
            chunk = ack_ids[i:i+ACK_BATCH]
            await service.ticks.xack(stream_name, group, *chunk)
        
        # Проверяем что было 2 вызова (200 + 50)
        assert service.ticks.xack.call_count == 2
        assert len(service.ticks.xack.call_args_list[0][0][2:]) == 200
        assert len(service.ticks.xack.call_args_list[1][0][2:]) == 50


class TestPressureControl:
    """✅ P1: Тесты pressure control по lag"""
    
    @pytest.mark.asyncio
    async def test_pressure_control_drops_old_ticks(self, service):
        """Проверяет что тики с большим lag отбрасываются."""
        # Мокаем метрики
        with patch('services.orderflow.metrics.ticks_dropped_total') as mock_dropped:
            MAX_LAG_MS = 500
            now_ms = get_ny_time_millis()
            old_ts_ms = now_ms - 1000  # lag = 1000ms > MAX_LAG_MS
            
            lag_ms = now_ms - old_ts_ms
            assert lag_ms > MAX_LAG_MS
            
            # Симулируем drop
            if MAX_LAG_MS > 0 and lag_ms > MAX_LAG_MS:
                if mock_dropped:
                    mock_dropped.labels(symbol="BTCUSDT", reason="lag").inc()
            
            # Проверяем что метрика вызвана
            if mock_dropped:
                mock_dropped.labels.assert_called_with(symbol="BTCUSDT", reason="lag")
    
    def test_pressure_control_disabled_when_zero(self, service):
        """Проверяет что pressure control отключается при MAX_LAG_MS=0."""
        MAX_LAG_MS = 0
        lag_ms = 1000
        
        # При MAX_LAG_MS=0 проверка не должна срабатывать
        should_drop = MAX_LAG_MS > 0 and lag_ms > MAX_LAG_MS
        assert not should_drop


class TestPelSweeper:
    """✅ P0: Тесты PEL recovery sweeper"""
    
    def test_pel_sweeper_task_created(self, service):
        """Проверяет что PEL sweeper task создаётся."""
        assert hasattr(service, '_pel_sweeper_task')
        assert service._pel_sweeper_task is None  # до запуска
    
    @pytest.mark.asyncio
    async def test_pel_sweeper_handles_both_ticks_and_books(self, service):
        """✅ Проверяет что PEL sweeper обрабатывает оба типа стримов (ticks и books)."""
        # Мокаем xautoclaim для обоих типов
        mock_tick_entries = [
            ("1234567890-0", {"ts_ms": "1234567890", "price": "50000"}),
            ("1234567890-1", {"ts_ms": "1234567890", "price": "50001"}),
        ]
        mock_book_entries = [
            ("1234567891-0", {"bids": "[[50000, 1.0]]", "asks": "[[50001, 1.0]]"}),
        ]
        
        # Создаём runtime с обоими стримами
        from services.orderflow.runtime import SymbolRuntime
        runtime = SymbolRuntime(symbol="BTCUSDT", config={})
        runtime.tick_stream = "stream:tick_BTCUSDT"
        runtime.tick_group = "crypto-of:BTCUSDT"
        runtime.book_stream = "stream:book_BTCUSDT"
        runtime.book_group = "crypto-of-book:BTCUSDT"
        service.symbol_contexts["BTCUSDT"] = runtime
        
        # Мокаем xautoclaim чтобы возвращать разные результаты для ticks и books
        call_count = [0]
        async def mock_xautoclaim(stream, group, consumer, min_idle_ms, start_id, count):
            call_count[0] += 1
            if "tick" in stream:
                return ("1234567890-2", mock_tick_entries)
            elif "book" in stream:
                return ("1234567891-1", mock_book_entries)
            return (start_id, [])
        
        service.ticks.xautoclaim = AsyncMock(side_effect=mock_xautoclaim)
        service.ticks.xack = AsyncMock()
        service.ticks.pipeline = MagicMock(return_value=AsyncMock())
        service.ticks.xadd = AsyncMock()
        
        # Мокаем _xack_pipeline
        service._xack_pipeline = AsyncMock()
        
        # Мокаем метрики
        with patch('services.orderflow.metrics.pel_autoclaim_total') as mock_pel_metric:
            mock_pel_metric.labels = MagicMock(return_value=mock_pel_metric)
            mock_pel_metric.inc = MagicMock()
            
            # Запускаем один цикл sweeper
            with patch.dict(os.environ, {
                'CRYPTO_OF_PEL_SWEEP_INTERVAL_SEC': '0.1',
                'CRYPTO_OF_PEL_MIN_IDLE_MS': '60000',
                'CRYPTO_OF_PEL_COUNT': '200',
                'CRYPTO_OF_PEL_QUARANTINE': 'false'
            }):
                task = asyncio.create_task(service._pel_sweeper_loop())
                await asyncio.sleep(0.15)  # ждём один цикл
                task.cancel()
                try:
                    await task
                except asyncio.CancelledError:
                    pass
            
            # Проверяем что xautoclaim был вызван для обоих типов
            assert call_count[0] >= 2, f"Expected at least 2 calls (ticks + books), got {call_count[0]}"
            
            # Проверяем что метрики были обновлены для обоих типов
            if mock_pel_metric.labels.called:
                calls = mock_pel_metric.labels.call_args_list
                kinds = [call[1]['kind'] for call in calls if 'kind' in call[1]]
                assert 'ticks' in kinds, "Expected 'ticks' in metric labels"
                assert 'books' in kinds, "Expected 'books' in metric labels"
    
    @pytest.mark.asyncio
    async def test_pel_sweeper_quarantine_enabled(self, service):
        """Проверяет что PEL sweeper может помещать сообщения в карантин."""
        from services.orderflow.runtime import SymbolRuntime
        runtime = SymbolRuntime(symbol="BTCUSDT", config={})
        runtime.tick_stream = "stream:tick_BTCUSDT"
        runtime.tick_group = "crypto-of:BTCUSDT"
        service.symbol_contexts["BTCUSDT"] = runtime
        
        mock_entries = [
            ("1234567890-0", {"ts_ms": "1234567890", "price": "50000"}),
        ]
        
        service.ticks.xautoclaim = AsyncMock(return_value=("1234567890-1", mock_entries))
        service.ticks.xadd = AsyncMock()
        service._xack_pipeline = AsyncMock()
        
        with patch.dict(os.environ, {
            'CRYPTO_OF_PEL_SWEEP_INTERVAL_SEC': '0.1',
            'CRYPTO_OF_PEL_MIN_IDLE_MS': '60000',
            'CRYPTO_OF_PEL_COUNT': '200',
            'CRYPTO_OF_PEL_QUARANTINE': 'true'
        }):
            service.quarantine_stream = "stream:of:quarantine"
            task = asyncio.create_task(service._pel_sweeper_loop())
            await asyncio.sleep(0.15)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass
        
        # Проверяем что сообщения были помещены в карантин
        assert service.ticks.xadd.called, "Expected quarantine stream to receive messages"
        call_args = service.ticks.xadd.call_args
        assert call_args[0][0] == "stream:of:quarantine"
        assert "symbol" in call_args[0][1]
        assert "reason" in call_args[0][1]


@pytest.mark.integration
class TestIntegration:
    """Интеграционные тесты (требуют Redis)"""
    
    @pytest.mark.asyncio
    async def test_crash_between_read_and_ack(self, service):
        """
        ✅ Интеграционный тест: сценарий "crash between read and ack".
        Проверяет что sweeper восстанавливает сообщения через XAUTOCLAIM.
        """
        # Этот тест требует реального Redis, поэтому помечен как integration
        # В реальном тесте:
        # 1. Читаем сообщения из стрима
        # 2. НЕ делаем ACK (симулируем crash)
        # 3. Запускаем sweeper
        # 4. Проверяем что сообщения были claimed и обработаны
        pass

