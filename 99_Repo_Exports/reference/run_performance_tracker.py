#!/usr/bin/env python3
"""
Standalone запуск Signal Performance Tracker.

Простой скрипт для запуска системы отслеживания эффективности
торговых сигналов в виде отдельного сервиса.

Использование:
    python run_performance_tracker.py
    
Или с кастомной конфигурацией:
    TRACKER_CONFIG=config/my_tracker.json python run_performance_tracker.py
"""

import os
import sys
import json
import signal as sig
from pathlib import Path

# Добавляем python-worker в путь
sys.path.insert(0, str(Path(__file__).parent))

from services.signal_performance_tracker import SignalPerformanceTracker


def load_config():
    """
    Загрузка конфигурации из файла или переменных окружения.
    
    Приоритет:
    1. TRACKER_CONFIG env var (путь к JSON файлу)
    2. config/signal_tracker_config.json
    3. Конфигурация по умолчанию
    """
    # Проверяем переменную окружения
    config_path = os.getenv("TRACKER_CONFIG")
    
    if not config_path:
        # Используем стандартный путь
        config_path = Path(__file__).parent / "config" / "signal_tracker_config.json"
    
    # Загружаем конфиг из файла
    if os.path.exists(config_path):
        try:
            with open(config_path, 'r', encoding='utf-8') as f:
                config = json.load(f)
            print(f"✅ Конфигурация загружена из: {config_path}")
            return config
        except Exception as e:
            print(f"⚠️ Ошибка загрузки конфигурации: {e}")
            print("   Используем конфигурацию по умолчанию")
    
    # Конфигурация по умолчанию
    print("📋 Используется конфигурация по умолчанию")
    return {
        "streams": {
            "symbols": os.getenv("SYMBOLS", "XAUUSD").split(","),
            "strategies": os.getenv("STRATEGIES", "orderflow").split(",")
        },
        "consumer_group": os.getenv("CONSUMER_GROUP", "signal-tracker-group"),
        "consumer_name": os.getenv("CONSUMER_NAME", "tracker-main"),
        "monitor": {
            "default_lot": float(os.getenv("DEFAULT_LOT", "1.0")),
            "risk_pct": float(os.getenv("RISK_PCT", "1.0")),
            "stop_atr_mult": float(os.getenv("STOP_ATR_MULT", "1.0")),
            "rr_levels": [1.0, 2.0, 3.0],
            "tp_ratio": [0.50, 0.30, 0.20],
            "notify_on_trade_close": os.getenv("NOTIFY_ON_TRADE_CLOSE", "false").lower() == "true"
        },
        "telegram": {
            "bot_token": os.getenv("TELEGRAM_BOT_TOKEN"),
            "chat_id": os.getenv("TELEGRAM_CHAT_ID"),
            "notify_on_trade_close": os.getenv("NOTIFY_ON_TRADE_CLOSE", "false").lower() == "true"
        },
        "reporting": {
            "daily_summary_enabled": os.getenv("DAILY_SUMMARY", "true").lower() == "true",
            "daily_summary_hour": int(os.getenv("DAILY_SUMMARY_HOUR", "0")),
            "periodic_summary_enabled": os.getenv("PERIODIC_SUMMARY", "true").lower() == "true",
            "periodic_summary_interval_hours": int(os.getenv("PERIODIC_SUMMARY_HOURS", "3"))
        }
    }


def main():
    """Главная функция запуска"""
    print("\n" + "=" * 70)
    print("🚀 Signal Performance Tracker")
    print("=" * 70 + "\n")
    
    # Загрузка конфигурации
    config = load_config()
    
    # Вывод конфигурации
    print("📊 Конфигурация:")
    print(f"   Символы: {config['streams']['symbols']}")
    print(f"   Стратегии: {config['streams']['strategies']}")
    print(f"   Consumer Group: {config.get('consumer_group', 'N/A')}")
    print(f"   Redis: {os.getenv('REDIS_HOST', 'redis-worker-1')}:{os.getenv('REDIS_PORT', '6379')}")
    
    telegram_enabled = (
        config.get('telegram', {}).get('bot_token') and 
        config.get('telegram', {}).get('chat_id')
    )
    print(f"   Telegram: {'✅ Включен' if telegram_enabled else '❌ Выключен'}")
    
    notify_on_close = config.get('monitor', {}).get('notify_on_trade_close', False)
    print(f"   Уведомления при закрытии: {'✅ Да' if notify_on_close else '❌ Нет'}")
    
    daily_summary = config.get('reporting', {}).get('daily_summary_enabled', True)
    print(f"   Ежедневная сводка: {'✅ Да' if daily_summary else '❌ Нет'}")
    
    periodic_summary = config.get('reporting', {}).get('periodic_summary_enabled', False)
    periodic_hours = config.get('reporting', {}).get('periodic_summary_interval_hours', 3)
    print(f"   Периодическая сводка: {'✅ Каждые ' + str(periodic_hours) + 'ч' if periodic_summary else '❌ Нет'}")
    print()
    
    # Создание трекера
    try:
        tracker = SignalPerformanceTracker(config)
    except Exception as e:
        print(f"\n❌ Ошибка инициализации трекера: {e}")
        sys.exit(1)
    
    # Обработка сигналов для graceful shutdown
    def signal_handler(signum, frame):
        print(f"\n⚠️ Получен сигнал {signum}, завершение работы...")
        tracker.stop()
        sys.exit(0)
    
    sig.signal(sig.SIGINT, signal_handler)
    sig.signal(sig.SIGTERM, signal_handler)
    
    # Запуск трекера
    print("🎬 Запуск трекера...")
    print("   Нажмите Ctrl+C для остановки\n")
    
    try:
        tracker.run_forever()
    except KeyboardInterrupt:
        print("\n⚠️ Получен KeyboardInterrupt")
        tracker.stop()
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        tracker.stop()
        sys.exit(1)


if __name__ == "__main__":
    main()

