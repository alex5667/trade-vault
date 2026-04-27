from utils.time_utils import get_ny_time_millis
#!/usr/bin/env python3
"""
Тестовый скрипт для проверки Signal Performance Tracker.

Отправляет тестовые сигналы и тики для проверки работоспособности системы.

Использование:
    python test_performance_tracker.py
"""

import time
import json
import sys
from pathlib import Path

# Добавляем python-worker в путь
sys.path.insert(0, str(Path(__file__).parent))

from core.redis_client import get_redis
from services.stats_aggregator import StatsAggregator


def test_signal_processing():
    """Тест обработки сигналов"""
    print("=" * 70)
    print("🧪 Тест 1: Обработка сигналов")
    print("=" * 70 + "\n")
    
    redis = get_redis()
    
    # Отправляем тестовый сигнал
    test_signal = {
        "strategy": "orderflow",
        "symbol": "XAUUSD",
        "tf": "tick",
        "direction": "LONG",
        "price": 2650.50,
        "atr": 1.2,
        "source": "OrderFlow",
        "timestamp": get_ny_time_millis()
    }
    
    print("📨 Отправка тестового сигнала...")
    print(f"   Strategy: {test_signal['strategy']}")
    print(f"   Symbol: {test_signal['symbol']}")
    print(f"   Direction: {test_signal['direction']}")
    print(f"   Price: {test_signal['price']}")
    print(f"   Source: {test_signal['source']}")
    
    # Публикуем в stream
    stream_name = f"signals:{test_signal['strategy']}:{test_signal['symbol']}"
    
    try:
        msg_id = redis.xadd(
            stream_name,
            {"data": json.dumps(test_signal)},
            maxlen=1000
        )
        print(f"\n✅ Сигнал отправлен в {stream_name}")
        print(f"   Message ID: {msg_id}")
        
        # Ждём обработки
        print("\n⏳ Ждём обработки (3 секунды)...")
        time.sleep(3)
        
        # Проверяем создание consumer group
        try:
            groups = redis.execute_command('XINFO', 'GROUPS', stream_name)
            print(f"\n✅ Consumer groups: {len(groups)} найдено")
        except Exception as e:
            print(f"\n⚠️ Consumer groups: {e}")
        
        return True
        
    except Exception as e:
        print(f"\n❌ Ошибка отправки сигнала: {e}")
        return False


def test_tick_processing():
    """Тест обработки тиков"""
    print("\n" + "=" * 70)
    print("🧪 Тест 2: Обработка тиков")
    print("=" * 70 + "\n")
    
    redis = get_redis()
    
    symbol = "XAUUSD"
    
    # Серия тиков для достижения TP1, TP2, затем SL
    ticks = [
        {"symbol": symbol, "last": 2651.70, "bid": 2651.68, "ask": 2651.72, "volume": 100},  # TP1
        {"symbol": symbol, "last": 2652.90, "bid": 2652.88, "ask": 2652.92, "volume": 100},  # TP2
        {"symbol": symbol, "last": 2649.30, "bid": 2649.28, "ask": 2649.32, "volume": 100},  # SL (упущенная прибыль!)
    ]
    
    stream_name = f"stream:tick_{symbol}"
    
    print(f"📨 Отправка тестовых тиков в {stream_name}...")
    
    for i, tick in enumerate(ticks, 1):
        try:
            msg_id = redis.xadd(stream_name, tick, maxlen=10000)
            print(f"   Тик {i}: price={tick['last']} → {msg_id}")
            time.sleep(0.5)
        except Exception as e:
            print(f"   ❌ Ошибка тика {i}: {e}")
            return False
    
    print("\n✅ Все тики отправлены")
    
    # Ждём обработки
    print("\n⏳ Ждём обработки (5 секунд)...")
    time.sleep(5)
    
    return True


def test_statistics():
    """Тест получения статистики"""
    print("\n" + "=" * 70)
    print("🧪 Тест 3: Получение статистики")
    print("=" * 70 + "\n")
    
    redis = get_redis()
    
    # Проверяем общую статистику
    print("📊 Общая статистика:")
    stats = StatsAggregator.get_stats(redis, "orderflow", "XAUUSD", "tick")
    
    if stats:
        print(f"   ✅ Данные найдены")
        print(f"   Total Trades: {stats.get('total_trades', 0)}")
        print(f"   WinRate: {stats.get('winrate', 0)}%")
        print(f"   Total P/L: {stats.get('total_pnl', 0)}")
        print(f"   TP1→SL: {stats.get('tp1_then_sl', 0)} ({stats.get('tp1_then_sl_rate', 0)}%)")
        print(f"   TP2→SL: {stats.get('tp2_then_sl', 0)} ({stats.get('tp2_then_sl_rate', 0)}%)")
    else:
        print("   ℹ️ Статистика ещё не создана (нормально для первого запуска)")
    
    # Проверяем статистику по источникам
    print("\n📊 Статистика по источникам:")
    sources = StatsAggregator.get_strategy_sources(redis, "orderflow", "XAUUSD", "tick")
    
    if sources:
        print(f"   ✅ Найдено источников: {len(sources)}")
        for source in sources:
            source_stats = StatsAggregator.get_stats_by_source(
                redis, "orderflow", "XAUUSD", "tick", source
            )
            if source_stats:
                print(f"\n   {source}:")
                print(f"     Trades: {source_stats.get('total_trades', 0)}")
                print(f"     WinRate: {source_stats.get('winrate', 0)}%")
                print(f"     TP1→SL: {source_stats.get('tp1_then_sl_rate', 0)}%")
    else:
        print("   ℹ️ Источники ещё не зарегистрированы")
    
    return True


def test_streams_exist():
    """Проверка существования необходимых streams"""
    print("\n" + "=" * 70)
    print("🧪 Тест 4: Проверка Redis Streams")
    print("=" * 70 + "\n")
    
    redis = get_redis()
    
    streams_to_check = [
        "signals:orderflow:XAUUSD",
        "stream:tick_XAUUSD",
        "events:trades",
        "trades:closed"
    ]
    
    print("Проверка streams:")
    all_exist = True
    
    for stream in streams_to_check:
        try:
            length = redis.xlen(stream)
            print(f"   ✅ {stream}: {length} сообщений")
        except Exception as e:
            print(f"   ℹ️ {stream}: не существует (будет создан)")
            all_exist = False
    
    return all_exist


def test_redis_keys():
    """Проверка ключей Redis"""
    print("\n" + "=" * 70)
    print("🧪 Тест 5: Проверка Redis ключей")
    print("=" * 70 + "\n")
    
    redis = get_redis()
    
    # Проверяем индексы
    print("Индексы:")
    
    strategies = redis.smembers("stats:strategies")
    print(f"   Стратегии: {strategies or 'пусто'}")
    
    if strategies:
        for strategy in strategies:
            symbols = redis.smembers(f"stats:symbols:{strategy}")
            print(f"   Символы для {strategy}: {symbols}")
            
            for symbol in symbols:
                tfs = redis.smembers(f"stats:tfs:{strategy}:{symbol}")
                print(f"   TF для {strategy}/{symbol}: {tfs}")
                
                for tf in tfs:
                    sources = redis.smembers(f"stats:sources:{strategy}:{symbol}:{tf}")
                    print(f"   Sources для {strategy}/{symbol}/{tf}: {sources}")
    
    return True


def test_reporting():
    """Тест Reporting Service"""
    print("\n" + "=" * 70)
    print("🧪 Тест 6: Reporting Service")
    print("=" * 70 + "\n")
    
    try:
        from services.reporting_service import ReportingService
        
        reporting = ReportingService()
        
        print("📊 Получение отчёта...")
        report = reporting.get_strategy_report("orderflow", "XAUUSD", "tick", include_sources=True)
        
        if report:
            print("   ✅ Отчёт получен")
            print(f"   Total Trades: {report.get('total_trades', 0)}")
            print(f"   WinRate: {report.get('winrate', 0)}%")
            
            sources = report.get('sources', {})
            if sources:
                print(f"\n   Источники: {len(sources)}")
                for source in sources.keys():
                    print(f"     - {source}")
        else:
            print("   ℹ️ Отчёт пуст (нет данных)")
        
        # Проверка сводки по источникам
        print("\n📊 Сводка по источникам...")
        sources_summary = reporting.get_sources_summary()
        
        if sources_summary:
            print("   ✅ Сводка получена")
            for source, data in sources_summary.items():
                print(f"   {source}: {data.get('total_trades', 0)} сделок, WR {data.get('winrate', 0)}%")
        else:
            print("   ℹ️ Сводка пуста (нет данных)")
        
        return True
        
    except Exception as e:
        print(f"   ❌ Ошибка: {e}")
        return False


def run_all_tests():
    """Запуск всех тестов"""
    print("\n" + "=" * 70)
    print("🧪 Signal Performance Tracker - Тестирование")
    print("=" * 70 + "\n")
    
    tests = [
        ("Redis Streams", test_streams_exist),
        ("Обработка сигналов", test_signal_processing),
        ("Обработка тиков", test_tick_processing),
        ("Получение статистики", test_statistics),
        ("Redis ключи", test_redis_keys),
        ("Reporting Service", test_reporting),
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"\n❌ Критическая ошибка в тесте '{test_name}': {e}")
            results.append((test_name, False))
    
    # Итоговый отчёт
    print("\n" + "=" * 70)
    print("📊 Итоговый отчёт")
    print("=" * 70 + "\n")
    
    passed = sum(1 for _, result in results if result)
    total = len(results)
    
    for test_name, result in results:
        status = "✅ PASS" if result else "❌ FAIL"
        print(f"{status:10} {test_name}")
    
    print(f"\n{'=' * 70}")
    print(f"Результат: {passed}/{total} тестов пройдено ({passed/total*100:.0f}%)")
    print("=" * 70 + "\n")
    
    if passed == total:
        print("🎉 Все тесты пройдены! Система готова к работе.\n")
        return 0
    else:
        print("⚠️ Некоторые тесты не пройдены. Проверьте конфигурацию.\n")
        return 1


def main():
    """Главная функция"""
    try:
        exit_code = run_all_tests()
        
        if exit_code == 0:
            print("💡 Следующие шаги:")
            print("   1. Запустите: python run_performance_tracker.py")
            print("   2. Анализируйте: python services/analyze_missed_profit.py")
            print("   3. Сравните источники: python services/example_sources_analysis.py 1\n")
        
        sys.exit(exit_code)
        
    except KeyboardInterrupt:
        print("\n\n⚠️ Тестирование прервано пользователем")
        sys.exit(1)
    except Exception as e:
        print(f"\n❌ Критическая ошибка: {e}")
        sys.exit(1)


if __name__ == "__main__":
    main()

