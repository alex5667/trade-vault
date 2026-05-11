#!/usr/bin/env python3
"""
Пример использования handlers модуля
"""

import time

from handlers import SignalProcessor  # type: ignore


def sample_ws_callback(symbols):
    """
    Пример функции обратного вызова для WebSocket подключений
    
    Args:
        symbols: Список символов для подключения
    """
    print(f"📡 Пример callback: получено {len(symbols)} символов для WS: {symbols}")


def main():
    """Пример запуска обработчиков сигналов"""
    print("🚀 Запуск примера использования handlers модуля")

    # Создаем обработчик сигналов
    signal_processor = SignalProcessor(sample_ws_callback)

    try:
        # Запускаем все обработчики
        signal_processor.start_all()

        # Демонстрируем работу в течение 30 секунд
        print("⏰ Демонстрация работы в течение 30 секунд...")
        time.sleep(30)

        # Показываем статистику
        histories = signal_processor.get_kline_histories()
        print(f"📊 Количество торговых пар в истории: {len(histories)}")

    except KeyboardInterrupt:
        print("⛔ Получен сигнал завершения")
    finally:
        # Останавливаем обработчики
        signal_processor.stop_all()
        print("✅ Пример завершен")


if __name__ == "__main__":
    main()
