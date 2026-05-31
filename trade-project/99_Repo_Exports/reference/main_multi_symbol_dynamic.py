#!/usr/bin/env python3
"""
Multi-Symbol OrderFlow Handler with Dynamic Symbol Management

Поддерживает динамическое управление символами через Redis stream:
- Добавление новых символов на лету
- Удаление символов с graceful shutdown
- Hot-reload без перезапуска контейнера

Redis Stream: config:symbols
Commands:
  - ADD symbols    : Добавить символы
  - REMOVE symbols : Удалить символы
  - SET symbols    : Установить список (заменить текущий)

Usage:
    # Статический список из ENV
    SYMBOLS="XAUUSD,BTCUSD" python main_multi_symbol_dynamic.py
    
    # Динамическое управление через Redis
    DYNAMIC_SYMBOLS=true python main_multi_symbol_dynamic.py
"""

import os
import sys
import time
import signal as sig
from typing import List
from datetime import datetime, timezone
from core.symbol_manager import SymbolManager
from core.utc_utils import utc_strftime
import warnings

# Suppress annoying NVIDIA driver warnings when running on CPU nodes
warnings.filterwarnings("ignore", message=".*NVIDIA Driver not detected.*")
warnings.filterwarnings("ignore", message=".*CUDA initialization.*")


def parse_symbols_from_env() -> List[str]:
    """Парсит начальный список символов из ENV"""
    symbols_str = os.getenv("SYMBOLS", "")
    if symbols_str:
        return [s.strip() for s in symbols_str.split(",") if s.strip()]
    return []


def main():
    """Главная функция с динамическим управлением символами"""
    print("═" * 70)
    print("🚀 Multi-Symbol OrderFlow Handler (Dynamic)")
    print(f"⏰ Start time: {utc_strftime()}")
    print("═" * 70)
    print()
    
    # Проверяем режим работы
    dynamic_mode = os.getenv("DYNAMIC_SYMBOLS", "false").lower() == "true"
    
    # Начальные символы
    initial_symbols = parse_symbols_from_env()
    
    if dynamic_mode:
        print("📡 Режим: DYNAMIC (управление через Redis stream)")
        print(f"   Stream: config:symbols")
        print(f"   Initial symbols: {initial_symbols or 'none'}")
    else:
        print("📋 Режим: STATIC (символы из ENV)")
        print(f"   Symbols: {initial_symbols}")
    
    print()
    
    # Создаем Symbol Manager
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    config_stream = os.getenv("SYMBOL_CONFIG_STREAM", "config:symbols")
    
    manager = SymbolManager(
        redis_url=redis_url,
        config_stream=config_stream,
        initial_symbols=initial_symbols
    )
    
    # Обработка сигналов для graceful shutdown
    def signal_handler(signum, frame):
        print()
        print(f"🛑 Received signal {signum}, initiating shutdown...")
        manager.stop()
        sys.exit(0)
    
    sig.signal(sig.SIGINT, signal_handler)
    sig.signal(sig.SIGTERM, signal_handler)
    
    # Запускаем manager
    manager.start()
    
    print()
    print("═" * 70)
    print("✅ Service started")
    
    if dynamic_mode:
        print()
        print("💡 Управление символами:")
        print("   # Добавить символ")
        print("   python core/symbol_manager.py add BTCUSD")
        print()
        print("   # Удалить символ")
        print("   python core/symbol_manager.py remove BTCUSD")
        print()
        print("   # Установить список")
        print("   python core/symbol_manager.py set XAUUSD BTCUSD ETHUSD")
    
    print("═" * 70)
    print()
    
    # Главный цикл с мониторингом
    health_check_interval = int(os.getenv("HEALTH_CHECK_INTERVAL", "60"))
    last_status_time = time.time()
    
    try:
        while True:
            time.sleep(health_check_interval)
            
            # Выводим статус периодически
            status_interval = int(os.getenv("STATUS_REPORT_INTERVAL", "3600"))
            if time.time() - last_status_time >= status_interval:
                print()
                print("═" * 70)
                print(f"📊 Status Report")
                print("═" * 70)
                
                status = manager.get_status()
                for symbol, info in status.items():
                    status_icon = "✅" if info["is_running"] else "❌"
                    print(f"   {status_icon} {symbol:12s} | "
                          f"Ticks: {info['processed_ticks']:6d} | "
                          f"Signals: L={info['signal_count_long']} S={info['signal_count_short']}")
                
                print("═" * 70)
                last_status_time = time.time()
                
    except KeyboardInterrupt:
        print()
        print("🛑 Keyboard interrupt...")
        manager.stop()


if __name__ == "__main__":
    main()

