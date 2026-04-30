#!/usr/bin/env python3
"""
Crypto Futures OrderFlow Handler Service.

Запускает OrderFlow-обработчики для нескольких криптосимволов, читая тики и книги
из Redis Ticks, используя существующую инфраструктуру (BaseOrderFlowHandler
CryptoOrderFlowHandler, MultiSymbolOrderFlowService).

Функциональность:
- Поддержка нескольких символов одновременно (конфиг через CRYPTO_ORDERFLOW_SYMBOLS).
- Автоматическое создание consumer group `ticks-orderflow-<SYMBOL>`.
- Публикация сигналов в `signals:orderflow:<symbol>`, `notify:telegram` и
  дополнительный дубликат в `stream:manual-signals`.
"""

import os
import signal
import sys
from typing import List

from handlers.handler_factory import OrderFlowHandlerFactory
from main_multi_symbol import MultiSymbolOrderFlowService

DEFAULT_SYMBOLS = ["BTCUSDT", "ETHUSDT"]


def parse_symbols() -> List[str]:
    """
    Возвращает список символов для запуска обработчиков.

    Приоритет источников:
    1. Переменная окружения CRYPTO_ORDERFLOW_SYMBOLS
    2. Переменная окружения SYMBOLS (для совместимости)
    3. Значение по умолчанию DEFAULT_SYMBOLS
    """
    env_symbols = os.getenv("CRYPTO_ORDERFLOW_SYMBOLS")
    if env_symbols:
        symbols = [sym.strip().upper() for sym in env_symbols.split(",") if sym.strip()]
        if symbols:
            print(f"📋 Symbols from CRYPTO_ORDERFLOW_SYMBOLS: {symbols}")
            return symbols

    fallback_symbols = os.getenv("SYMBOLS")
    if fallback_symbols:
        symbols = [sym.strip().upper() for sym in fallback_symbols.split(",") if sym.strip()]
        if symbols:
            print(f"📋 Symbols from SYMBOLS: {symbols}")
            return symbols

    print(f"📋 Using default crypto symbols: {DEFAULT_SYMBOLS}")
    return DEFAULT_SYMBOLS


def configure_environment(symbols: List[str]) -> None:
    """
    Готовит переменные окружения для обработчиков.

    - Группы consumer'ов вида ticks-orderflow-<SYMBOL>
    - Потоки тиков и книг
    - Настройки дублирования сигналов в manual stream
    """
    os.environ.setdefault("MANUAL_SIGNAL_STREAM", "stream:manual-signals")
    os.environ.setdefault("ENABLE_MANUAL_SIGNAL_STREAM", "true")

    for symbol in symbols:
        key = symbol.upper()
        os.environ.setdefault(f"{key}_GROUP", f"ticks-orderflow-{key}")
        os.environ.setdefault(f"{key}_TICK_STREAM", f"stream:tick_{key}")
        os.environ.setdefault(f"{key}_BOOK_STREAM", f"stream:book_{key}")


def ensure_support(symbols: List[str]) -> None:
    """
    Проверяет, что все символы поддерживаются фабрикой обработчиков.
    """
    unsupported = [sym for sym in symbols if not OrderFlowHandlerFactory.is_supported(sym)]
    if unsupported:
        print("❌ Unsupported symbols detected:", unsupported)
        print("   Supported symbols:", OrderFlowHandlerFactory.list_supported_symbols())
        sys.exit(1)


def main() -> None:
    """Точка входа сервиса."""
    print("═" * 70)
    print("🚀 Crypto Futures OrderFlow Handler Service")
    print("═" * 70)
    print()

    symbols = parse_symbols()
    configure_environment(symbols)
    ensure_support(symbols)

    service = MultiSymbolOrderFlowService(symbols)

    def handle_signal(signum, frame):
        print()
        print(f"🛑 Received signal {signum}, initiating shutdown...")
        service.is_running = False

    signal.signal(signal.SIGINT, handle_signal)
    signal.signal(signal.SIGTERM, handle_signal)

    service.start()

    health_interval = int(os.getenv("CRYPTO_HEALTH_CHECK_INTERVAL", os.getenv("HEALTH_CHECK_INTERVAL", "60")))
    service.run(health_check_interval=health_interval)

    print()
    print("═" * 70)
    print("✅ Crypto OrderFlow service stopped")
    print("═" * 70)


if __name__ == "__main__":
    main()


