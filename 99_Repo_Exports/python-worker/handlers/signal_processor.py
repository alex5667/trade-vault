import sys
import time
import threading
from typing import Callable, List, Union

from .binance_data_handler import BinanceDataHandler
from .kline_data_handler import KlineDataHandler
from .xauusd_orderflow_handler_v2 import XAUUSDOrderFlowHandlerV2 as XAUOrderFlowHandler
from of.candle_of_worker import CandleOrderFlowWorker
from signals.orchestrator import run_metrics_screener
from core.config import XAU_HANDLER_ENABLED, METRICS_SCHEDULER_INTERVAL_SEC


class SignalProcessor:
    """
    Менеджер для координации всех обработчиков сигналов
    """
    
    def __init__(self, ws_callback: Callable[[List[str]], None]):
        """
        Инициализация менеджера обработчиков
        
        Args:
            ws_callback: Функция обратного вызова для обновления WebSocket подключений
        """
        self.ws_callback = ws_callback
        self.binance_handler = BinanceDataHandler(ws_callback)
        self.kline_handler = KlineDataHandler()
        self.of_handler = CandleOrderFlowWorker()
        self.xau_handler = XAUOrderFlowHandler() if XAU_HANDLER_ENABLED else None
        self.is_running = False
        self._metrics_thread: Union[threading.Thread, None] = None
        self._metrics_stop = threading.Event()
        
    def _metrics_loop(self) -> None:
        """Фоновый цикл: раз в N секунд публикует агрегированные метрики (top/volume/funding)."""
        # Ждём небольшую паузу после старта, чтобы данные подтянулись
        time.sleep(5)
        
        import asyncio
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        try:
            while not self._metrics_stop.is_set():
                try:
                    # Запускаем скринер метрик (он публикует top, volume, funding)
                    loop.run_until_complete(run_metrics_screener())
                except Exception as e:
                    print(f"❌ Ошибка фоновой публикации метрик: {e}")
                    sys.stdout.flush()
                # Ожидание следующего периода или остановки
                self._metrics_stop.wait(METRICS_SCHEDULER_INTERVAL_SEC)
        finally:
            try:
                # Cancel all pending tasks before closing the loop
                pending = asyncio.all_tasks(loop)
                for task in pending:
                    task.cancel()
                if pending:
                    loop.run_until_complete(asyncio.gather(*pending, return_exceptions=True))
                loop.close()
            except Exception as e:
                print(f"⚠️ Ошибка при закрытии asyncio loop: {e}")
                sys.stdout.flush()
    
    def start_all(self) -> None:
        """Запускает все обработчики"""
        if self.is_running:
            print("⚠️ SignalProcessor уже запущен")
            return
            
        self.is_running = True
        
        print("🚀 Запуск обработчиков сигналов...")
        sys.stdout.flush()
        
        # Запускаем обработчик рыночных данных
        self.binance_handler.start()
        
        # Запускаем обработчик данных свечей
        self.kline_handler.start()
        
        # Запускаем обработчик Order Flow
        self.of_handler.start()
        
        # Запускаем обработчик XAUUSD Order Flow (если включен)
        if self.xau_handler:
            self.xau_handler.start()
            print("✅ XAUUSD OrderFlow Handler включен")
        else:
            print("ℹ️ XAUUSD OrderFlow Handler отключен (установите XAU_HANDLER_ENABLED=true)")
        
        # Запускаем фоновую публикацию агрегированных метрик раз в час
        self._metrics_stop.clear()
        self._metrics_thread = threading.Thread(target=self._metrics_loop, name="metrics-scheduler", daemon=True)
        self._metrics_thread.start()
        
        print("✅ Все обработчики сигналов запущены")
        sys.stdout.flush()
        
    def stop_all(self) -> None:
        """Останавливает все обработчики"""
        if not self.is_running:
            print("⚠️ SignalProcessor уже остановлен")
            return
            
        self.is_running = False
        
        print("⛔ Остановка обработчиков сигналов...")
        sys.stdout.flush()
        
        # Останавливаем обработчики
        self.binance_handler.stop()
        self.kline_handler.stop()
        self.of_handler.stop()
        
        # Останавливаем XAUUSD обработчик (если включен)
        if self.xau_handler:
            self.xau_handler.stop()
        
        # Останавливаем фоновую публикацию
        self._metrics_stop.set()
        if self._metrics_thread and self._metrics_thread.is_alive():
            self._metrics_thread.join(timeout=5)
        
        print("✅ Все обработчики сигналов остановлены")
        sys.stdout.flush()
        
    def get_kline_histories(self):
        """
        Возвращает истории торговых пар из KlineDataHandler
        
        Returns:
            Словарь с историями торговых пар
        """
        return self.kline_handler.get_histories()
        
    def clear_kline_history(self, symbol: Union[str, None] = None) -> None:
        """
        Очищает историю торговых пар в KlineDataHandler
        
        Args:
            symbol: Символ пары для очистки. Если None - очищает все
        """
        self.kline_handler.clear_history(symbol)
        
    def wait_forever(self) -> None:
        """Ждет завершения работы обработчиков"""
        try:
            while self.is_running:
                time.sleep(1)
        except KeyboardInterrupt:
            print("⛔ Получен сигнал завершения...")
            sys.stdout.flush()
            self.stop_all() 