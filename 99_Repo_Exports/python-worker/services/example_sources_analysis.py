"""
Пример анализа эффективности источников сигналов.

Демонстрирует работу с разбивкой статистики по источникам:
- OrderFlow
- AggregatedHub-V2
- TechnicalAnalysis
"""

import json

from core.redis_client import get_redis
from services.reporting_service import ReportingService
from services.stats_aggregator import StatsAggregator


def compare_sources():
    """Сравнение эффективности различных источников сигналов"""
    print("=" * 70)
    print("📊 Сравнение источников сигналов")
    print("=" * 70 + "\n")

    redis_client = get_redis()
    reporting = ReportingService()

    # Получаем список всех источников
    strategy = "orderflow"
    symbol = ""
    tf = "tick"

    sources = StatsAggregator.get_strategy_sources(redis_client, strategy, symbol, tf)

    if not sources:
        print("⚠️ Нет данных по источникам")
        return

    print(f"Найдено источников: {len(sources)}")
    print(f"Источники: {', '.join(sources)}\n")

    # Собираем статистику по каждому источнику
    comparison = []

    for source in sources:
        stats = StatsAggregator.get_stats_by_source(
            redis_client, strategy, symbol, tf, source
        )

        if stats:
            total_trades = int(stats.get("total_trades", 0))

            if total_trades > 0:
                comparison.append({
                    "source": source,
                    "trades": total_trades,
                    "wins": int(stats.get("wins", 0)),
                    "losses": int(stats.get("losses", 0)),
                    "winrate": float(stats.get("winrate", 0)),
                    "total_pnl": float(stats.get("total_pnl", 0)),
                    "avg_pnl": float(stats.get("avg_pnl", 0)),
                    "tp1_rate": float(stats.get("tp1_rate", 0)),
                    "tp2_rate": float(stats.get("tp2_rate", 0)),
                    "tp3_rate": float(stats.get("tp3_rate", 0))
                })

    if not comparison:
        print("⚠️ Недостаточно данных для сравнения")
        return

    # Выводим детальную информацию
    print("-" * 70)
    for item in comparison:
        print(f"\n{item['source']}:")
        print(f"  📊 Всего сделок: {item['trades']}")
        print(f"  ✅ Выигрышей: {item['wins']}")
        print(f"  ❌ Проигрышей: {item['losses']}")
        print(f"  📈 WinRate: {item['winrate']:.1f}%")
        print(f"  💰 Total P/L: {item['total_pnl']:+.2f}")
        print(f"  💵 Avg P/L: {item['avg_pnl']:+.2f}")
        print(f"  🎯 TP Rates: TP1={item['tp1_rate']:.1f}% | TP2={item['tp2_rate']:.1f}% | TP3={item['tp3_rate']:.1f}%")

    print("\n" + "-" * 70)

    # Рейтинг по различным критериям
    print("\n🏆 Рейтинги:\n")

    # По WinRate
    best_wr = max(comparison, key=lambda x: x["winrate"])
    print(f"🥇 Лучший WinRate: {best_wr['source']} ({best_wr['winrate']:.1f}%)")

    # По Average P/L
    best_pnl = max(comparison, key=lambda x: x["avg_pnl"])
    print(f"💰 Лучший Avg P/L: {best_pnl['source']} ({best_pnl['avg_pnl']:+.2f})")

    # По TP3 достижениям
    best_tp3 = max(comparison, key=lambda x: x["tp3_rate"])
    print(f"🎯 Лучший TP3 Rate: {best_tp3['source']} ({best_tp3['tp3_rate']:.1f}%)")

    # По объёму сделок
    most_active = max(comparison, key=lambda x: x["trades"])
    print(f"📊 Самый активный: {most_active['source']} ({most_active['trades']} сделок)")

    # Взвешенная оценка (учитывает и WinRate и объём)
    print("\n📊 Взвешенная оценка (WinRate × confidence):\n")

    for item in comparison:
        confidence = min(item['trades'] / 50.0, 1.0)  # макс при 50+ сделках
        weighted_score = item['winrate'] * confidence

        print(f"  {item['source']:20s} Score: {weighted_score:6.1f} "
              f"(WR={item['winrate']:5.1f}%, Trades={item['trades']:3d}, Conf={confidence:.2f})")

    # Определяем лучший с учётом надёжности
    best_weighted = max(comparison, key=lambda x: x['winrate'] * min(x['trades'] / 50.0, 1.0))
    print(f"\n🏅 Рекомендуемый источник: {best_weighted['source']}")


def sources_summary_report():
    """Получение агрегированной сводки по всем источникам"""
    print("\n" + "=" * 70)
    print("📊 Сводка по всем источникам")
    print("=" * 70 + "\n")

    reporting = ReportingService()

    sources_summary = reporting.get_sources_summary()

    if not sources_summary:
        print("⚠️ Нет данных")
        return

    # Выводим в виде таблицы
    print(f"{'Источник':<25} {'Сделок':>8} {'WinRate':>8} {'Total P/L':>12} {'Avg P/L':>10}")
    print("-" * 70)

    for source, data in sources_summary.items():
        print(f"{source:<25} {data['total_trades']:>8} {data['winrate']:>7.1f}% "
              f"{data['total_pnl']:>+11.2f} {data.get('avg_pnl', 0):>+9.2f}")

    print("-" * 70)

    # Общий итог
    total_trades = sum(d['total_trades'] for d in sources_summary.values())
    total_wins = sum(d['wins'] for d in sources_summary.values())
    total_pnl = sum(d['total_pnl'] for d in sources_summary.values())

    overall_wr = (total_wins / total_trades * 100.0) if total_trades > 0 else 0.0
    overall_avg = total_pnl / total_trades if total_trades > 0 else 0.0

    print(f"{'ИТОГО':<25} {total_trades:>8} {overall_wr:>7.1f}% "
          f"{total_pnl:>+11.2f} {overall_avg:>+9.2f}")
    print()


def export_sources_comparison():
    """Экспорт сравнительной статистики в JSON"""
    print("\n" + "=" * 70)
    print("💾 Экспорт сравнительной статистики")
    print("=" * 70 + "\n")

    reporting = ReportingService()
    sources_summary = reporting.get_sources_summary()

    if not sources_summary:
        print("⚠️ Нет данных для экспорта")
        return

    # Формируем детальный отчёт
    detailed_report = {
        "timestamp": int(__import__('time').time() * 1000),
        "strategy": "orderflow",
        "symbol": "",
        "tf": "tick",
        "sources": sources_summary,
        "summary": {
            "total_trades": sum(d['total_trades'] for d in sources_summary.values()),
            "total_wins": sum(d['wins'] for d in sources_summary.values()),
            "total_pnl": sum(d['total_pnl'] for d in sources_summary.values())
        }
    }

    # Сохранение
    output_file = "/tmp/sources_comparison.json"
    with open(output_file, 'w', encoding='utf-8') as f:
        json.dump(detailed_report, f, indent=2, ensure_ascii=False)

    print(f"✅ Отчёт сохранён: {output_file}")
    print(f"📊 Источников: {len(sources_summary)}")
    print(f"📊 Всего сделок: {detailed_report['summary']['total_trades']}")


def get_best_source():
    """Определение наиболее эффективного источника"""
    print("\n" + "=" * 70)
    print("🏆 Определение лучшего источника")
    print("=" * 70 + "\n")

    reporting = ReportingService()
    sources_summary = reporting.get_sources_summary()

    if not sources_summary:
        print("⚠️ Нет данных")
        return None

    # Фильтруем источники с минимальным количеством сделок
    MIN_TRADES = 10

    qualified = {
        source: data
        for source, data in sources_summary.items()
        if data.get("total_trades", 0) >= MIN_TRADES
    }

    if not qualified:
        print(f"⚠️ Нет источников с {MIN_TRADES}+ сделками")
        return None

    print(f"Квалифицированных источников: {len(qualified)}\n")

    # Различные критерии
    criteria = {}

    # 1. По WinRate
    best_wr = max(qualified.items(), key=lambda x: x[1]["winrate"])
    criteria["winrate"] = best_wr
    print(f"🥇 Лучший WinRate: {best_wr[0]} ({best_wr[1]['winrate']:.1f}%)")

    # 2. По Average P/L
    best_avg = max(qualified.items(), key=lambda x: x[1].get("avg_pnl", 0))
    criteria["avg_pnl"] = best_avg
    print(f"💰 Лучший Avg P/L: {best_avg[0]} ({best_avg[1].get('avg_pnl', 0):+.2f})")

    # 3. По Total P/L
    best_total = max(qualified.items(), key=lambda x: x[1]["total_pnl"])
    criteria["total_pnl"] = best_total
    print(f"💵 Лучший Total P/L: {best_total[0]} ({best_total[1]['total_pnl']:+.2f})")

    # 4. Взвешенный score
    scored = []
    for source, data in qualified.items():
        trades = data.get("total_trades", 0)
        winrate = data.get("winrate", 0)
        confidence = min(trades / 50.0, 1.0)
        score = winrate * confidence

        scored.append((source, score, trades, winrate))

    best_weighted = max(scored, key=lambda x: x[1])
    criteria["weighted"] = best_weighted

    print(f"\n🏅 Взвешенная оценка: {best_weighted[0]} "
          f"(Score={best_weighted[1]:.1f}, WR={best_weighted[3]:.1f}%, Trades={best_weighted[2]})")

    # Подсчёт голосов (какой источник чаще лучший)
    votes = {}
    for criterion, (source, *_) in criteria.items():
        votes[source] = votes.get(source, 0) + 1

    overall_best = max(votes.items(), key=lambda x: x[1])

    print(f"\n🎖️  ИТОГОВЫЙ ЛУЧШИЙ ИСТОЧНИК: {overall_best[0]} ({overall_best[1]}/4 критериев)")

    return overall_best[0]


def monitor_source_performance():
    """Мониторинг производительности источников в реальном времени"""
    import time

    print("\n" + "=" * 70)
    print("📡 Мониторинг производительности источников")
    print("=" * 70)
    print("Обновление каждые 30 секунд. Ctrl+C для остановки.\n")

    redis_client = get_redis()

    try:
        while True:
            sources = StatsAggregator.get_strategy_sources(
                redis_client, "orderflow", "tick"
            )

            print(f"\n[{time.strftime('%H:%M:%S')}]")
            print(f"{'Источник':<25} {'Сделок':>8} {'WinRate':>8} {'P/L':>10}")
            print("-" * 55)

            for source in sources:
                stats = StatsAggregator.get_stats_by_source(
                    redis_client, "orderflow", "tick", source
                )

                if stats:
                    trades = stats.get("total_trades", 0)
                    winrate = stats.get("winrate", 0)
                    pnl = stats.get("total_pnl", 0)

                    print(f"{source:<25} {trades:>8} {winrate:>7}% {pnl:>+9}")

            time.sleep(30)

    except KeyboardInterrupt:
        print("\n\n⚠️ Мониторинг остановлен")


def detailed_source_report(source_name: str):
    """Детальный отчёт по конкретному источнику"""
    print("=" * 70)
    print(f"📊 Детальный отчёт: {source_name}")
    print("=" * 70 + "\n")

    redis_client = get_redis()

    stats = StatsAggregator.get_stats_by_source(
        redis_client, "orderflow", "tick", source_name
    )

    if not stats:
        print(f"⚠️ Нет данных для источника {source_name}")
        return

    # Основные метрики
    print("Основные метрики:")
    print(f"  Всего сделок: {stats.get('total_trades', 0)}")
    print(f"  Выигрышей: {stats.get('wins', 0)}")
    print(f"  Проигрышей: {stats.get('losses', 0)}")
    print(f"  WinRate: {stats.get('winrate', 0)}%")

    # P/L метрики
    print("\nP/L метрики:")
    print(f"  Total P/L: {stats.get('total_pnl', 0):+.2f}")
    print(f"  Average P/L: {stats.get('avg_pnl', 0):+.2f}")
    print(f"  Total P/L %: {stats.get('total_pnl_pct', 0):+.4f}%")
    print(f"  Average P/L %: {stats.get('avg_pnl_pct', 0):+.4f}%")

    # TP метрики
    print("\nTP метрики:")
    print(f"  TP1 Hits: {stats.get('tp1_hits', 0)} ({stats.get('tp1_rate', 0)}%)")
    print(f"  TP2 Hits: {stats.get('tp2_hits', 0)} ({stats.get('tp2_rate', 0)}%)")
    print(f"  TP3 Hits: {stats.get('tp3_hits', 0)} ({stats.get('tp3_rate', 0)}%)")

    # Дополнительная информация
    print(f"\nПоследнее обновление: {stats.get('last_update', 'N/A')}")
    print()


def main():
    """Главная функция"""
    print("\n" + "=" * 70)
    print("📊 Анализ источников сигналов - Примеры")
    print("=" * 70 + "\n")

    examples = {
        "1": ("Сравнение источников", compare_sources),
        "2": ("Сводка по всем источникам", sources_summary_report),
        "3": ("Экспорт данных", export_sources_comparison),
        "4": ("Определение лучшего источника", get_best_source),
        "5": ("Мониторинг в реальном времени", monitor_source_performance),
        "6": ("Детальный отчёт OrderFlow", lambda: detailed_source_report("OrderFlow")),
        "7": ("Детальный отчёт AggregatedHub-V2", lambda: detailed_source_report("AggregatedHub-V2")),
    }

    print("Доступные примеры:")
    for num, (name, _) in examples.items():
        print(f"  {num}. {name}")

    print("\nДля запуска:")
    print("  python example_sources_analysis.py <номер>\n")

    import sys
    if len(sys.argv) > 1:
        example_num = sys.argv[1]
        if example_num in examples:
            _, example_func = examples[example_num]
            example_func()
        else:
            print(f"❌ Неизвестный пример: {example_num}")
    else:
        # По умолчанию - сравнение
        compare_sources()


if __name__ == "__main__":
    main()

