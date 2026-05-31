#!/usr/bin/env python3
"""
Тестовая отправка отчета в Telegram через ReportingService.

Использует ReportingService для формирования и отправки реального отчета
с текущей статистикой через Redis stream notify:telegram.
"""

import sys
import os

# Добавляем путь к python-worker
sys.path.insert(0, '/app')
sys.path.insert(0, '/app/python-worker')

from services.reporting_service import ReportingService
from common.log import setup_logger

def main():
    logger = setup_logger("TestReport")
    
    print("\n" + "="*70)
    print("🧪 Тестовая отправка отчета в Telegram")
    print("="*70 + "\n")
    
    try:
        # Инициализация ReportingService
        redis_url = os.getenv("REDIS_URL", "redis://scanner-redis:6379/0")
        print(f"📡 Подключение к Redis: {redis_url}")
        
        reporting = ReportingService(redis_url=redis_url)
        print("✅ ReportingService инициализирован\n")
        
        # 1. Тестовое сообщение
        print("📤 Отправка тестового сообщения...")
        success = reporting.send_telegram_message(
            "🧪 <b>Тестовое сообщение</b>\n\n"
            "Это тестовая отправка из Signal Performance Tracker.\n"
            "Если вы видите это сообщение - система работает корректно! ✅"
        )
        
        if success:
            print("   ✅ Тестовое сообщение отправлено в notify:telegram\n")
        else:
            print("   ❌ Ошибка отправки тестового сообщения\n")
        
        # 2. Проверка наличия данных
        print("📊 Проверка наличия статистики...")
        report_data = reporting.get_all_strategies_report()
        
        if report_data.get("strategies"):
            print(f"   ✅ Найдено стратегий: {len(report_data['strategies'])}")
            print(f"   📈 Всего сделок: {report_data.get('total_trades', 0)}")
            print(f"   💰 Общий P/L: {report_data.get('total_pnl', 0):+.2f}\n")
            
            # 3. Отправка полной ежедневной сводки
            print("📅 Отправка полной ежедневной сводки...")
            reporting.send_daily_summary(include_sources=True)
            print("   ✅ Ежедневная сводка отправлена\n")
            
            # 4. Отправка детальных отчетов по стратегиям
            symbols = os.getenv("SYMBOLS", "XAUUSD").split(",")
            strategies = ["orderflow", "ta"]
            
            print(f"📊 Отправка детальных отчетов...")
            for symbol in symbols:
                for strategy in strategies:
                    print(f"   → {strategy}:{symbol}:tick")
                    try:
                        reporting.send_strategy_report(
                            strategy=strategy,
                            symbol=symbol,
                            tf="tick"
                        )
                    except Exception as e:
                        print(f"      ⚠️ Нет данных или ошибка: {e}")
            
            print("\n✅ Все отчеты отправлены!")
            
        else:
            print("   ⚠️ Нет статистики для отправки")
            print("   💡 Возможные причины:")
            print("      - Сигналы еще не обрабатывались")
            print("      - Нет закрытых позиций")
            print("      - Проверьте TradeMonitor и StatsAggregator\n")
            
            # Отправляем хотя бы тестовое сообщение
            reporting.send_telegram_message(
                "📊 <b>Тестовый отчет</b>\n\n"
                "⚠️ В данный момент нет статистики для отчета.\n"
                "Система работает корректно, ожидаем закрытия позиций для формирования отчета."
            )
            print("✅ Тестовое уведомление отправлено")
        
        print("\n" + "="*70)
        print("✅ Тест завершен успешно!")
        print("="*70)
        
        print("\n💡 Проверьте Telegram бот - отчеты должны прийти через notify-worker")
        print("   Если не пришли - проверьте:")
        print("   1. docker logs scanner-notify-worker -f")
        print("   2. docker exec -it scanner-redis redis-cli XLEN notify:telegram")
        
    except Exception as e:
        logger.error(f"❌ Ошибка при отправке отчета: {e}", exc_info=True)
        print(f"\n❌ ОШИБКА: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()


