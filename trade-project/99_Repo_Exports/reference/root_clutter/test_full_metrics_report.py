#!/usr/bin/env python3
"""
Тестовый скрипт для проверки отправки полного отчета со ВСЕМИ метриками в Telegram.
"""
import sys
import os

sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'python-worker'))

from services.reporting_service import ReportingService

def main():
    print("=" * 70)
    print("🧪 ТЕСТ: Отправка полного отчета со ВСЕМИ метриками")
    print("=" * 70)
    print()
    
    # Инициализируем ReportingService
    redis_url = os.getenv("REDIS_URL", "redis://scanner-redis-worker-1:6379/0")
    reporting = ReportingService(redis_url=redis_url)
    
    print("✅ ReportingService инициализирован")
    print(f"   Redis: {redis_url}")
    print()
    
    # Отправляем отчет по стратегии orderflow
    print("📊 Отправка отчета по стратегии 'orderflow'...")
    reporting.send_strategy_report("orderflow", "XAUUSD", "tick")
    print()
    
    print("=" * 70)
    print("✅ ТЕСТ ЗАВЕРШЕН")
    print()
    print("Проверьте Telegram бот - должен прийти отчет с метриками:")
    print("  ✓ Основные (8): total_trades, wins, losses, winrate, total_pnl, avg_pnl, etc")
    print("  ✓ TP метрики (6): tp1/2/3_hits, tp1/2/3_rate")
    print("  ⭐ Упущенная прибыль (6): tp1/2/3_then_sl, tp1/2/3_then_sl_rate")
    print("  ✓ По источникам: OrderFlow, AggregatedHub-V2, TechnicalAnalysis")
    print("=" * 70)

if __name__ == "__main__":
    main()


