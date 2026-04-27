from utils.time_utils import get_ny_time_millis
"""
Примеры использования Signal Performance Tracker.

Этот файл демонстрирует различные способы использования системы
отслеживания эффективности торговых сигналов.
"""

import time
import json
from signal_performance_tracker import SignalPerformanceTracker
from trade_monitor import TradeMonitor
from reporting_service import ReportingService


def example_1_standalone_tracker():
    """
    Пример 1: Запуск полной системы как standalone сервис.
    
    Использует конфигурацию из файла или переменных окружения.
    Работает в бесконечном режиме, обрабатывая сигналы и тики.
    """
    print("=" * 60)
    print("Пример 1: Standalone Signal Performance Tracker")
    print("=" * 60)
    
    # Конфигурация
    config = {
        "symbols": ["XAUUSD"],
        "strategies": ["orderflow"],
        "monitor": {
            "default_lot": 1.0,
            "stop_atr_mult": 1.0,
            "rr_levels": [1.0, 2.0, 3.0],
            "tp_ratio": [0.50, 0.30, 0.20]
        },
        "telegram": {
            "bot_token": None,  # Заполнить из ENV
            "chat_id": None
        },
        "daily_summary_enabled": True,
        "daily_summary_hour": 0
    }
    
    # Создание и запуск
    tracker = SignalPerformanceTracker(config)
    
    try:
        print("🚀 Запуск трекера...")
        tracker.start()
        
        # Работа в течение некоторого времени
        print("⏳ Система работает. Нажмите Ctrl+C для остановки...")
        
        while True:
            # Периодический вывод статуса
            status = tracker.get_status()
            print(f"\n📊 Статус:")
            print(f"   Сигналов обработано: {status['signals_read']}")
            print(f"   Тиков обработано: {status['ticks_processed']}")
            print(f"   Открытых позиций: {status['monitor']['open_positions']}")
            print(f"   Закрытых позиций: {status['monitor']['positions_closed']}")
            
            time.sleep(60)
            
    except KeyboardInterrupt:
        print("\n⚠️ Получен сигнал остановки")
    finally:
        tracker.stop()
        print("✅ Трекер остановлен")


def example_2_manual_components():
    """
    Пример 2: Ручное управление компонентами.
    
    Демонстрирует использование отдельных компонентов
    для специфических задач.
    """
    print("=" * 60)
    print("Пример 2: Ручное управление компонентами")
    print("=" * 60)
    
    # Создание компонентов
    monitor = TradeMonitor(config={
        "default_lot": 1.0,
        "stop_atr_mult": 1.0,
        "rr_levels": [1.0, 2.0, 3.0],
        "tp_ratio": [0.50, 0.30, 0.20]
    })
    
    reporting = ReportingService()
    
    # Обработка тестового сигнала
    test_signal = {
        "strategy": "orderflow",
        "symbol": "XAUUSD",
        "tf": "tick",
        "direction": "LONG",
        "price": 2650.50,
        "atr": 1.2,
        "timestamp": get_ny_time_millis()
    }
    
    print("\n📨 Обработка тестового сигнала...")
    pos_id = monitor.process_signal(test_signal)
    print(f"✅ Позиция создана: {pos_id}")
    
    # Симуляция движения цены
    print("\n📈 Симуляция движения цены...")
    
    # Тик 1: движение к TP1
    tick_1 = {
        "symbol": "XAUUSD",
        "last": 2651.70,  # TP1 = 2650.50 + 1.2 = 2651.70
        "bid": 2651.68,
        "ask": 2651.72
    }
    monitor.process_tick(tick_1)
    print(f"   Тик 1: Цена {tick_1['last']}")
    
    # Тик 2: движение к TP2
    tick_2 = {
        "symbol": "XAUUSD",
        "last": 2652.90,  # TP2 = 2650.50 + 2.4 = 2652.90
        "bid": 2652.88,
        "ask": 2652.92
    }
    monitor.process_tick(tick_2)
    print(f"   Тик 2: Цена {tick_2['last']}")
    
    # Получение статистики (используем статический метод)
    print("\n📊 Получение статистики...")
    from core.redis_client import get_redis
    redis_client = get_redis()
    
    from services.stats_aggregator import StatsAggregator
    stats = StatsAggregator.get_stats(redis_client, "orderflow", "XAUUSD", "tick")
    if stats:
        print(f"   Сделок: {stats.get('total_trades', 0)}")
        print(f"   WinRate: {stats.get('winrate', 0)}%")
        print(f"   Total P/L: {stats.get('total_pnl', 0)}")
    
    # Получение отчёта
    print("\n📋 Получение отчёта...")
    report = reporting.get_strategy_report("orderflow", "XAUUSD", "tick")
    print(json.dumps(report, indent=2))


def example_3_statistics_and_reports():
    """
    Пример 3: Работа со статистикой и отчётами.
    
    Демонстрирует различные способы получения
    и анализа статистики по сигналам.
    """
    print("=" * 60)
    print("Пример 3: Статистика и отчёты")
    print("=" * 60)
    
    from core.redis_client import get_redis
    from services.stats_aggregator import StatsAggregator
    
    redis_client = get_redis()
    reporting = ReportingService()
    
    # Получение списка всех стратегий
    print("\n📊 Список стратегий:")
    strategies = StatsAggregator.get_all_strategies(redis_client)
    for strategy in strategies:
        print(f"   - {strategy}")
    
    # Сводка по каждой стратегии
    print("\n📈 Сводка по стратегиям:")
    for strategy in strategies:
        summary = StatsAggregator.get_strategy_summary(redis_client, strategy)
        print(f"\n   {strategy}:")
        print(f"      Сделок: {summary.get('total_trades', 0)}")
        print(f"      WinRate: {summary.get('winrate', 0):.1f}%")
        print(f"      Total P/L: {summary.get('total_pnl', 0):+.2f}")
        print(f"      Avg P/L: {summary.get('avg_pnl', 0):+.2f}")
    
    # Детальная статистика по конкретной комбинации
    print("\n📊 Детальная статистика (orderflow/XAUUSD/tick):")
    stats = StatsAggregator.get_stats(redis_client, "orderflow", "XAUUSD", "tick")
    if stats:
        print(f"   Всего сделок: {stats['total_trades']}")
        print(f"   Выигрышей: {stats['wins']}")
        print(f"   Проигрышей: {stats['losses']}")
        print(f"   WinRate: {stats['winrate']}%")
        print(f"   Total P/L: {stats['total_pnl']}")
        print(f"   Avg P/L: {stats['avg_pnl']}")
        print(f"   Max Win: {stats['max_win']}")
        print(f"   Max Loss: {stats['max_loss']}")
        print(f"   TP1 Rate: {stats['tp1_rate']}%")
        print(f"   TP2 Rate: {stats['tp2_rate']}%")
        print(f"   TP3 Rate: {stats['tp3_rate']}%")
    else:
        print("   Нет данных")
    
    # Получение последних сделок
    print("\n📜 Последние 10 сделок:")
    trades = reporting.get_recent_trades("orderflow", "XAUUSD", "tick", limit=10)
    for i, trade in enumerate(trades, 1):
        print(f"\n   Сделка {i}:")
        print(f"      ID: {trade.get('id', 'N/A')}")
        print(f"      Направление: {trade.get('direction', 'N/A')}")
        print(f"      Вход: {trade.get('entry_price', 0)}")
        print(f"      Выход: {trade.get('exit_price', 0)}")
        print(f"      P/L: {trade.get('pnl', 0):+.2f} ({trade.get('pnl_pct', 0):+.2f}%)")
        print(f"      Результат: {trade.get('result', 'N/A').upper()}")
        print(f"      TP достигнуто: {trade.get('tp_count', 0)}/3")


def example_4_telegram_notifications():
    """
    Пример 4: Работа с Telegram уведомлениями.
    
    Демонстрирует отправку различных типов
    уведомлений в Telegram.
    """
    print("=" * 60)
    print("Пример 4: Telegram уведомления")
    print("=" * 60)
    
    # ВАЖНО: Заполнить реальные значения
    telegram_config = {
        "bot_token": "YOUR_BOT_TOKEN",
        "chat_id": "YOUR_CHAT_ID"
    }
    
    reporting = ReportingService(
        telegram_config=telegram_config
    )
    
    if not reporting.telegram_enabled:
        print("⚠️ Telegram не настроен. Пропуск примера.")
        return
    
    # Тестовое сообщение
    print("\n📤 Отправка тестового сообщения...")
    success = reporting.send_telegram_message("🧪 Тестовое сообщение от Signal Performance Tracker")
    if success:
        print("✅ Сообщение отправлено")
    else:
        print("❌ Ошибка отправки")
    
    # Уведомление о закрытии сделки
    print("\n📤 Отправка уведомления о сделке...")
    test_trade = {
        "strategy": "orderflow",
        "symbol": "XAUUSD",
        "tf": "tick",
        "direction": "LONG",
        "result": "win",
        "pnl": 45.50,
        "pnl_pct": 1.8,
        "close_reason": "TP2",
        "tp_count": 2
    }
    reporting.notify_trade_closed(test_trade)
    
    # Ежедневная сводка
    print("\n📤 Отправка ежедневной сводки...")
    reporting.send_daily_summary()
    
    # Отчёт по стратегии
    print("\n📤 Отправка отчёта по стратегии...")
    reporting.send_strategy_report("orderflow")


def example_5_export_data():
    """
    Пример 5: Экспорт данных.
    
    Демонстрирует экспорт сделок в различные форматы.
    """
    print("=" * 60)
    print("Пример 5: Экспорт данных")
    print("=" * 60)
    
    reporting = ReportingService()
    
    # Экспорт в JSON
    print("\n💾 Экспорт сделок в JSON...")
    output_file = "/tmp/trades_orderflow_xauusd.json"
    success = reporting.export_trades_to_json(
        "orderflow", 
        "XAUUSD", 
        "tick", 
        output_file
    )
    
    if success:
        print(f"✅ Данные экспортированы в {output_file}")
    else:
        print("❌ Ошибка экспорта")
    
    # Получение сводки производительности
    print("\n📊 Сводка производительности:")
    performance = reporting.get_performance_summary()
    print(json.dumps(performance, indent=2))


def example_6_real_time_monitoring():
    """
    Пример 6: Мониторинг в реальном времени.
    
    Демонстрирует мониторинг текущего состояния системы.
    """
    print("=" * 60)
    print("Пример 6: Мониторинг в реальном времени")
    print("=" * 60)
    
    config = {
        "symbols": ["XAUUSD"],
        "strategies": ["orderflow"]
    }
    
    tracker = SignalPerformanceTracker(config)
    tracker.start()
    
    try:
        print("\n📊 Мониторинг запущен. Обновление каждые 10 секунд...")
        print("   Нажмите Ctrl+C для остановки\n")
        
        iteration = 0
        while True:
            iteration += 1
            status = tracker.get_status()
            
            print(f"Итерация {iteration} ({time.strftime('%H:%M:%S')})")
            print(f"├─ Uptime: {status['uptime_sec']}s")
            print(f"├─ Сигналов: {status['signals_read']}")
            print(f"├─ Тиков: {status['ticks_processed']}")
            print(f"├─ Ошибок: {status['errors']}")
            print(f"├─ Открытых позиций: {status['monitor']['open_positions']}")
            print(f"└─ Закрытых позиций: {status['monitor']['positions_closed']}")
            print()
            
            time.sleep(10)
            
    except KeyboardInterrupt:
        print("\n⚠️ Остановка мониторинга...")
    finally:
        tracker.stop()


def main():
    """Главная функция для запуска примеров"""
    print("\n" + "=" * 60)
    print("Signal Performance Tracker - Примеры использования")
    print("=" * 60 + "\n")
    
    examples = {
        "1": ("Standalone Tracker", example_1_standalone_tracker),
        "2": ("Ручное управление компонентами", example_2_manual_components),
        "3": ("Статистика и отчёты", example_3_statistics_and_reports),
        "4": ("Telegram уведомления", example_4_telegram_notifications),
        "5": ("Экспорт данных", example_5_export_data),
        "6": ("Real-time мониторинг", example_6_real_time_monitoring),
    }
    
    print("Доступные примеры:")
    for num, (name, _) in examples.items():
        print(f"  {num}. {name}")
    
    print("\nДля запуска конкретного примера:")
    print("  python example_usage.py <номер>")
    print("\nПример: python example_usage.py 3\n")
    
    # Запуск примера по номеру из аргументов
    import sys
    if len(sys.argv) > 1:
        example_num = sys.argv[1]
        if example_num in examples:
            _, example_func = examples[example_num]
            example_func()
        else:
            print(f"❌ Неизвестный пример: {example_num}")
    else:
        # По умолчанию запускаем пример 2 (безопасный для демонстрации)
        print("Запуск примера 2 (Ручное управление компонентами)...\n")
        example_2_manual_components()


if __name__ == "__main__":
    main()

