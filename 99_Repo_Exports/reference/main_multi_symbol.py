#!/usr/bin/env python3
"""
Multi-Symbol OrderFlow Handler - Главный сервис для обработки множественных инструментов.

Запускает обработчики Order Flow для нескольких инструментов одновременно
(XAUUSD, BTCUSD, ETHUSD и т.д.)

Usage:
    # Из environment variable
    SYMBOLS="XAUUSD,BTCUSD,ETHUSD" python main_multi_symbol.py
    
    # Из командной строки
    python main_multi_symbol.py XAUUSD BTCUSD ETHUSD
    
    # Только один символ
    python main_multi_symbol.py XAUUSD
"""

import os
import sys
import time
import signal
from typing import List, Dict
from handlers.handler_factory import OrderFlowHandlerFactory, create_handler
from handlers.base_orderflow_handler import BaseOrderFlowHandler
from health_metrics import HealthMetrics
import warnings

# Suppress annoying NVIDIA driver warnings when running on CPU nodes
warnings.filterwarnings("ignore", message=".*NVIDIA Driver not detected.*")
warnings.filterwarnings("ignore", message=".*CUDA initialization.*")


class MultiSymbolOrderFlowService:
    """
    Сервис для управления множественными обработчиками Order Flow.
    
    Запускает и мониторит обработчики для нескольких инструментов,
    автоматически перезапускает при сбоях.
    """
    
    def __init__(self, symbols: List[str]):
        """
        Инициализация мультисимвольного сервиса.
        
        Args:
            symbols: Список символов для обработки (XAUUSD, BTCUSD и т.д.)
        """
        self.symbols = symbols
        self.handlers: Dict[str, BaseOrderFlowHandler] = {}
        self.is_running = False

        # Health metrics for monitoring
        redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.health_metrics = HealthMetrics(redis_url=redis_url, window_sec=5)
        self.health_metrics.start_background_loop()

        # Статистика
        self.start_time = None
        self.restart_counts: Dict[str, int] = {symbol: 0 for symbol in symbols}
        
        print(f"🚀 MultiSymbolOrderFlowService инициализирован")
        print(f"   Symbols: {', '.join(symbols)}")
        sys.stdout.flush()
    
    def start(self) -> None:
        """Запускает обработчики для всех символов"""
        print("═" * 70)
        print("Starting OrderFlow handlers...")
        print("═" * 70)
        sys.stdout.flush()
        
        self.is_running = True
        self.start_time = time.time()
        
        # Создаем и запускаем обработчики
        for symbol in self.symbols:
            self._start_handler(symbol)
        
        print()
        print("✅ All handlers started successfully")
        print("═" * 70)
        sys.stdout.flush()
    
    def _start_handler(self, symbol: str) -> bool:
        """
        Запускает обработчик для одного символа.
        
        Args:
            symbol: Символ инструмента
            
        Returns:
            True если успешно запущен, False иначе
        """
        try:
            print(f"🔄 Starting handler for {symbol}...")
            
            # Создаем обработчик через фабрику
            handler = create_handler(symbol, health_metrics=self.health_metrics)
            
            # Запускаем
            handler.start()
            
            # Сохраняем в реестре
            self.handlers[symbol] = handler
            
            print(f"✅ Handler for {symbol} started")
            sys.stdout.flush()
            return True
            
        except Exception as e:
            print(f"❌ Failed to start handler for {symbol}: {e}")
            sys.stdout.flush()
            return False
    
    def stop(self) -> None:
        """Останавливает все обработчики"""
        print()
        print("═" * 70)
        print("Stopping all handlers...")
        print("═" * 70)
        
        self.is_running = False
        
        for symbol, handler in self.handlers.items():
            try:
                print(f"⏹️  Stopping handler for {symbol}...")
                handler.stop()
                print(f"✅ Handler for {symbol} stopped")
            except Exception as e:
                print(f"❌ Error stopping handler for {symbol}: {e}")
        
        self.handlers.clear()

        # Stop health metrics
        try:
            self.health_metrics.stop()
            print("✅ Health metrics stopped")
        except Exception as e:
            print(f"❌ Error stopping health metrics: {e}")

        print("═" * 70)
        print("✅ All handlers stopped")
        print("═" * 70)
        sys.stdout.flush()
    
    def health_check(self) -> Dict[str, bool]:
        """
        Проверяет состояние всех обработчиков.
        
        Returns:
            Словарь {symbol: is_running}
        """
        status = {}
        for symbol, handler in self.handlers.items():
            status[symbol] = handler.is_running
        return status
    
    def restart_handler(self, symbol: str) -> bool:
        """
        Перезапускает обработчик для указанного символа.
        
        Args:
            symbol: Символ инструмента
            
        Returns:
            True если успешно перезапущен, False иначе
        """
        print(f"🔄 Restarting handler for {symbol}...")
        
        # Останавливаем старый обработчик (если есть)
        if symbol in self.handlers:
            try:
                self.handlers[symbol].stop()
            except Exception:
                pass
            del self.handlers[symbol]
        
        # Запускаем новый
        success = self._start_handler(symbol)
        
        if success:
            self.restart_counts[symbol] += 1
            print(f"✅ Handler for {symbol} restarted (restarts: {self.restart_counts[symbol]})")
        else:
            print(f"❌ Failed to restart handler for {symbol}")
        
        sys.stdout.flush()
        return success
    
    def run(self, health_check_interval: int = 60) -> None:
        """
        Главный цикл сервиса с мониторингом здоровья обработчиков.
        
        Args:
            health_check_interval: Интервал проверки состояния (секунды)
        """
        print()
        print("🔄 Starting main monitoring loop...")
        print(f"   Health check interval: {health_check_interval}s")
        sys.stdout.flush()
        
        try:
            while self.is_running:
                time.sleep(health_check_interval)
                
                # Проверяем состояние всех обработчиков
                status = self.health_check()
                
                # Выводим статистику
                self._print_statistics(status)
                
                # Перезапускаем упавшие обработчики
                for symbol, is_running in status.items():
                    if not is_running and self.is_running:
                        print(f"⚠️  Handler for {symbol} is not running, attempting restart...")
                        self.restart_handler(symbol)
                
        except KeyboardInterrupt:
            print()
            print("🛑 Received interrupt signal, stopping...")
        finally:
            self.stop()
    
    def _print_statistics(self, status: Dict[str, bool]) -> None:
        """Выводит статистику работы сервиса"""
        uptime = time.time() - self.start_time if self.start_time else 0
        uptime_hours = uptime / 3600
        
        print()
        print("═" * 70)
        print(f"📊 Service Statistics (uptime: {uptime_hours:.2f}h)")
        print("═" * 70)
        
        for symbol in self.symbols:
            is_running = status.get(symbol, False)
            restarts = self.restart_counts.get(symbol, 0)
            
            status_icon = "✅" if is_running else "❌"
            status_text = "RUNNING" if is_running else "STOPPED"
            
            print(f"   {status_icon} {symbol:12s} | {status_text:8s} | Restarts: {restarts}")
        
        print("═" * 70)
        sys.stdout.flush()


def parse_symbols() -> List[str]:
    """
    Парсит символы из environment variable или командной строки.
    
    Приоритет:
    1. Аргументы командной строки
    2. Environment variable SYMBOLS
    3. Default: XAUUSD
    
    Returns:
        Список символов для обработки
    """
    # Из командной строки
    if len(sys.argv) > 1:
        symbols = sys.argv[1:]
        print(f"📋 Symbols from command line: {symbols}")
        return symbols
    
    # Из environment variable
    symbols_str = os.getenv("SYMBOLS", "")
    if symbols_str:
        symbols = [s.strip() for s in symbols_str.split(",") if s.strip()]
        print(f"📋 Symbols from SYMBOLS env: {symbols}")
        return symbols
    
    # Default
    default_symbols = ["XAUUSD"]
    print(f"📋 Using default symbols: {default_symbols}")
    return default_symbols


def main():
    """Главная функция"""
    print("═" * 70)
    print("🚀 Multi-Symbol OrderFlow Handler Service")
    print("═" * 70)
    print()
    
    # Парсим символы
    symbols = parse_symbols()
    
    # Проверяем поддержку символов
    print()
    print("Checking symbol support...")
    unsupported = []
    for symbol in symbols:
        if not OrderFlowHandlerFactory.is_supported(symbol):
            print(f"⚠️  Symbol {symbol} is not supported")
            unsupported.append(symbol)
        else:
            print(f"✅ Symbol {symbol} is supported")
    
    if unsupported:
        print()
        print(f"❌ Some symbols are not supported: {unsupported}")
        print(f"Supported symbols: {OrderFlowHandlerFactory.list_supported_symbols()}")
        sys.exit(1)
    
    print()
    
    # Создаем сервис
    service = MultiSymbolOrderFlowService(symbols)
    
    # Обработка сигналов для graceful shutdown
    def signal_handler(sig, frame):
        print()
        print(f"🛑 Received signal {sig}, initiating shutdown...")
        service.is_running = False
    
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)
    
    # Запускаем сервис
    service.start()
    
    # Главный цикл с мониторингом
    health_check_interval = int(os.getenv("HEALTH_CHECK_INTERVAL", "60"))
    service.run(health_check_interval=health_check_interval)
    
    print()
    print("═" * 70)
    print("✅ Service shutdown complete")
    print("═" * 70)


if __name__ == "__main__":
    main()

