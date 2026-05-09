#!/usr/bin/env python3
"""
Анализ упущенной прибыли (TP→SL метрики).

Скрипт для детального анализа сделок, которые достигли тейк-профита,
но затем развернулись и закрылись по стоп-лоссу.

Использование:
    python analyze_missed_profit.py
    python analyze_missed_profit.py --strategy orderflow --symbol 
"""

import sys
from pathlib import Path

# Добавляем python-worker в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from collections import Counter

from core.redis_client import get_redis
from services.reporting_service import ReportingService
from services.stats_aggregator import StatsAggregator


def analyze_missed_profit_metrics(strategy: str, symbol: str, tf: str):
    """
    Детальный анализ упущенной прибыли.
    
    Args:
        strategy: Название стратегии
        symbol: Символ
        tf: Таймфрейм
    """
    print("=" * 80)
    print(f"📊 Анализ упущенной прибыли: {strategy}/{symbol}/{tf}")
    print("=" * 80 + "\n")

    redis_client = get_redis()

    # Получаем общую статистику
    stats = StatsAggregator.get_stats(redis_client, strategy, symbol, tf)

    if not stats:
        print("⚠️ Нет данных")
        return

    total = int(stats.get("total_trades", 0))

    print(f"Всего сделок: {total}\n")

    # Анализ по каждому уровню TP
    for lvl in (1, 2, 3):
        print(f"TP{lvl} Анализ:")
        print("-" * 80)

        # Основные метрики
        tp_hits = int(stats.get(f"tp{lvl}_hits", 0))
        tp_hit_rate = float(stats.get(f"tp{lvl}_rate", 0))

        # Метрики упущенной прибыли
        tp_then_sl = int(stats.get(f"tp{lvl}_then_sl", 0))
        tp_then_sl_rate = float(stats.get(f"tp{lvl}_then_sl_rate", 0))

        # Процент разворотов среди достигших этого TP
        reversal_rate = (tp_then_sl / tp_hits * 100.0) if tp_hits > 0 else 0.0

        print(f"  Достигли TP{lvl}: {tp_hits}/{total} ({tp_hit_rate}%)")
        print(f"  Развороты (TP{lvl}→SL): {tp_then_sl} ({tp_then_sl_rate}% от всех)")
        print(f"  Разворот среди достигших TP{lvl}: {reversal_rate:.1f}%")

        # Оценка
        if lvl == 1:
            thresholds = (10, 20)
        elif lvl == 2:
            thresholds = (5, 10)
        else:
            thresholds = (2, 5)

        if reversal_rate < thresholds[0]:
            status = "✅ ОТЛИЧНО"
            color = "green"
        elif reversal_rate < thresholds[1]:
            status = "⚠️ ПРИЕМЛЕМО"
            color = "yellow"
        else:
            status = "❌ ПЛОХО"
            color = "red"

        print(f"  Оценка: {status}\n")

    print("=" * 80 + "\n")

    # Рекомендации
    print("💡 Рекомендации:\n")

    tp1_rev = (int(stats.get("tp1_then_sl", 0)) / max(int(stats.get("tp1_hits", 1)), 1)) * 100
    tp2_rev = (int(stats.get("tp2_then_sl", 0)) / max(int(stats.get("tp2_hits", 1)), 1)) * 100

    recommendations = []

    if tp1_rev > 20:
        recommendations.append(
            "  • ⚠️ Высокий TP1→SL: увеличьте долю закрытия на TP1 с 50% до 60-70%"
        )

    if tp2_rev > 15:
        recommendations.append(
            "  • ⚠️ Высокий TP2→SL: рассмотрите trailing stop после TP2"
        )

    if tp1_rev < 10 and tp2_rev < 5:
        recommendations.append(
            "  • ✅ Отличные показатели! Стратегия работает стабильно"
        )

    if recommendations:
        for rec in recommendations:
            print(rec)
    else:
        print("  • ℹ️ Показатели в пределах нормы")

    print()


def compare_sources_missed_profit(strategy: str, symbol: str, tf: str):
    """Сравнение упущенной прибыли по источникам"""
    print("=" * 80)
    print(f"📊 Сравнение упущенной прибыли по источникам: {strategy}/{symbol}/{tf}")
    print("=" * 80 + "\n")

    redis_client = get_redis()

    # Получаем список источников
    sources = StatsAggregator.get_strategy_sources(redis_client, strategy, symbol, tf)

    if not sources:
        print("⚠️ Нет данных по источникам")
        return

    # Таблица сравнения
    print(f"{'Источник':<25} {'TP1→SL':>10} {'TP2→SL':>10} {'Надёжность':>15}")
    print("-" * 80)

    sources_data = []

    for source in sources:
        stats = StatsAggregator.get_stats_by_source(
            redis_client, strategy, symbol, tf, source
        )

        if stats:
            tp1_sl = int(stats.get("tp1_then_sl", 0))
            tp2_sl = int(stats.get("tp2_then_sl", 0))
            tp1_sl_rate = float(stats.get("tp1_then_sl_rate", 0))

            # Надёжность = 100% - процент разворотов
            reliability = 100.0 - tp1_sl_rate

            sources_data.append({
                "source": source,
                "tp1_sl": tp1_sl,
                "tp2_sl": tp2_sl,
                "reliability": reliability
            })

            print(f"{source:<25} {tp1_sl:>10} {tp2_sl:>10} {reliability:>14.1f}%")

    print("-" * 80 + "\n")

    # Рейтинг по надёжности
    if sources_data:
        sorted_sources = sorted(sources_data, key=lambda x: x["reliability"], reverse=True)

        print("🏆 Рейтинг по надёжности (меньше разворотов = лучше):\n")
        for i, item in enumerate(sorted_sources, 1):
            medal = "🥇" if i == 1 else ("🥈" if i == 2 else "🥉")
            print(f"{medal} {i}. {item['source']}: {item['reliability']:.1f}% надёжность")

        print()


def find_problematic_trades(strategy: str, symbol: str, tf: str, min_tp_level: int = 1):
    """
    Находит проблемные сделки с TP→SL.
    
    Args:
        strategy: Название стратегии
        symbol: Символ
        tf: Таймфрейм
        min_tp_level: Минимальный уровень TP для анализа (1, 2, или 3)
    """
    print("=" * 80)
    print(f"🔍 Поиск проблемных сделок (TP{min_tp_level}+ → SL)")
    print("=" * 80 + "\n")

    redis_client = get_redis()

    # Читаем закрытые сделки
    list_key = f"closed:{strategy}:{symbol}:{tf}"
    trade_ids = redis_client.lrange(list_key, 0, -1)

    problematic_trades = []

    for trade_id in trade_ids:
        trade = redis_client.hgetall(f"order:{trade_id}")

        if not trade:
            continue

        close_reason = trade.get("close_reason", "")
        tp_before = int(trade.get("tp_before_sl", 0))

        if close_reason == "SL" and tp_before >= min_tp_level:
            # Получаем детали
            signal = redis_client.hgetall(f"signal:{trade_id}")

            problematic_trades.append({
                "id": trade_id,
                "source": trade.get("source", "unknown"),
                "direction": trade.get("direction", "N/A"),
                "entry_price": float(trade.get("entry_price", 0)),
                "exit_price": float(trade.get("exit_price", 0)),
                "pnl": float(trade.get("pnl", 0)),
                "tp_reached": tp_before,
                "atr": float(signal.get("atr", 0)),
                "entry_time": int(trade.get("entry_time", 0)),
                "duration": int(trade.get("duration_sec", 0))
            })

    if not problematic_trades:
        print("✅ Проблемных сделок не найдено")
        return

    print(f"Найдено проблемных сделок: {len(problematic_trades)}\n")

    # Вывод деталей
    print(f"{'ID':12} {'Source':20} {'Dir':5} {'TP':3} {'Entry':10} {'Exit':10} {'P/L':10} {'ATR':8}")
    print("-" * 80)

    for trade in problematic_trades[:20]:  # Первые 20
        print(f"{trade['id'][:10]:12} "
              f"{trade['source']:20} "
              f"{trade['direction']:5} "
              f"{trade['tp_reached']:3} "
              f"{trade['entry_price']:10.2f} "
              f"{trade['exit_price']:10.2f} "
              f"{trade['pnl']:+10.2f} "
              f"{trade['atr']:8.4f}")

    if len(problematic_trades) > 20:
        print(f"\n... и ещё {len(problematic_trades) - 20} сделок")

    # Анализ паттернов
    print("\n" + "=" * 80)
    print("📊 Анализ паттернов:\n")

    # По источникам
    sources = Counter([t["source"] for t in problematic_trades])
    print("По источникам:")
    for source, count in sources.most_common():
        pct = count / len(problematic_trades) * 100
        print(f"  {source}: {count} ({pct:.1f}%)")

    # По направлению
    directions = Counter([t["direction"] for t in problematic_trades])
    print("\nПо направлению:")
    for direction, count in directions.most_common():
        pct = count / len(problematic_trades) * 100
        print(f"  {direction}: {count} ({pct:.1f}%)")

    # Средний ATR
    avg_atr = sum(t["atr"] for t in problematic_trades) / len(problematic_trades)
    print(f"\nСредний ATR: {avg_atr:.4f}")

    print()


def generate_optimization_report(strategy: str, symbol: str, tf: str):
    """Генерация отчёта с рекомендациями по оптимизации"""
    print("=" * 80)
    print(f"🎯 Отчёт по оптимизации: {strategy}/{symbol}/{tf}")
    print("=" * 80 + "\n")

    redis_client = get_redis()

    # Общая статистика
    stats = StatsAggregator.get_stats(redis_client, strategy, symbol, tf)

    total = int(stats.get("total_trades", 0))
    winrate = float(stats.get("winrate", 0))

    print("Базовые показатели:")
    print(f"  Всего сделок: {total}")
    print(f"  WinRate: {winrate:.1f}%")
    print(f"  Total P/L: {stats.get('total_pnl', 0):+.2f}\n")

    # Анализ упущенной прибыли
    tp1_sl = int(stats.get("tp1_then_sl", 0))
    tp2_sl = int(stats.get("tp2_then_sl", 0))
    tp1_hits = int(stats.get("tp1_hits", 0))
    tp2_hits = int(stats.get("tp2_hits", 0))

    tp1_reversal = (tp1_sl / tp1_hits * 100) if tp1_hits > 0 else 0
    tp2_reversal = (tp2_sl / tp2_hits * 100) if tp2_hits > 0 else 0

    print("Метрики разворотов:")
    print(f"  TP1→SL: {tp1_sl}/{tp1_hits} ({tp1_reversal:.1f}%)")
    print(f"  TP2→SL: {tp2_sl}/{tp2_hits} ({tp2_reversal:.1f}%)\n")

    # Текущая конфигурация
    current_tp_ratio = [0.50, 0.30, 0.20]
    print(f"Текущая конфигурация TP ratio: {current_tp_ratio}\n")

    # Рекомендации
    recommendations = []
    new_tp_ratio = list(current_tp_ratio)

    if tp1_reversal > 20:
        # Увеличиваем долю на TP1
        new_tp_ratio = [0.65, 0.25, 0.10]
        recommendations.append({
            "level": "HIGH",
            "type": "TP_RATIO",
            "message": f"Высокий TP1→SL ({tp1_reversal:.1f}%): увеличьте TP1 до 65%",
            "new_ratio": new_tp_ratio
        })
    elif tp1_reversal > 15:
        new_tp_ratio = [0.60, 0.25, 0.15]
        recommendations.append({
            "level": "MEDIUM",
            "type": "TP_RATIO",
            "message": f"Умеренный TP1→SL ({tp1_reversal:.1f}%): увеличьте TP1 до 60%",
            "new_ratio": new_tp_ratio
        })

    if tp2_reversal > 15:
        recommendations.append({
            "level": "HIGH",
            "type": "TRAILING_STOP",
            "message": f"Высокий TP2→SL ({tp2_reversal:.1f}%): используйте trailing stop",
        })
    elif tp2_reversal > 10:
        recommendations.append({
            "level": "MEDIUM",
            "type": "TRAILING_STOP",
            "message": f"Умеренный TP2→SL ({tp2_reversal:.1f}%): рассмотрите trailing stop",
        })

    if winrate < 55 and tp1_reversal > 15:
        recommendations.append({
            "level": "HIGH",
            "type": "STOP_LOSS",
            "message": "Низкий WinRate + высокий TP1→SL: стоп-лосс может быть слишком близко",
        })

    if not recommendations:
        recommendations.append({
            "level": "INFO",
            "type": "OK",
            "message": "Показатели в норме, изменения не требуются"
        })

    # Вывод рекомендаций
    print("Рекомендации:")
    print("-" * 80)

    for i, rec in enumerate(recommendations, 1):
        level_emoji = "🔴" if rec["level"] == "HIGH" else ("🟡" if rec["level"] == "MEDIUM" else "ℹ️")
        print(f"\n{i}. {level_emoji} {rec['message']}")

        if rec.get("new_ratio"):
            print(f"   Новая конфигурация: tp_ratio = {rec['new_ratio']}")

    print("\n" + "=" * 80 + "\n")


def compare_sources_reliability():
    """Сравнение надёжности источников (меньше TP→SL = лучше)"""
    print("=" * 80)
    print("🏆 Рейтинг источников по надёжности")
    print("=" * 80 + "\n")

    redis_client = get_redis()
    reporting = ReportingService()

    sources_summary = reporting.get_sources_summary()

    if not sources_summary:
        print("⚠️ Нет данных")
        return

    # Собираем метрики надёжности
    reliability_data = []

    for source in sources_summary.keys():
        stats = StatsAggregator.get_stats_by_source(
            redis_client, "orderflow", "tick", source
        )

        if stats and int(stats.get("total_trades", 0)) > 0:
            tp1_sl_rate = float(stats.get("tp1_then_sl_rate", 0))
            tp2_sl_rate = float(stats.get("tp2_then_sl_rate", 0))

            # Общая надёжность = среднее из (100% - reversal_rate)
            reliability = 100.0 - ((tp1_sl_rate + tp2_sl_rate) / 2.0)

            reliability_data.append({
                "source": source,
                "trades": int(stats.get("total_trades", 0)),
                "winrate": float(stats.get("winrate", 0)),
                "tp1_sl": int(stats.get("tp1_then_sl", 0)),
                "tp2_sl": int(stats.get("tp2_then_sl", 0)),
                "tp1_sl_rate": tp1_sl_rate,
                "tp2_sl_rate": tp2_sl_rate,
                "reliability": reliability
            })

    # Сортировка по надёжности
    reliability_data.sort(key=lambda x: x["reliability"], reverse=True)

    # Вывод таблицы
    print(f"{'Rank':5} {'Источник':25} {'Надёжность':12} {'TP1→SL':10} {'TP2→SL':10} {'WinRate':10}")
    print("-" * 80)

    medals = ["🥇", "🥈", "🥉"]

    for i, item in enumerate(reliability_data):
        medal = medals[i] if i < 3 else "  "
        print(f"{medal} {i+1:2}. "
              f"{item['source']:25} "
              f"{item['reliability']:11.1f}% "
              f"{item['tp1_sl']:10} "
              f"{item['tp2_sl']:10} "
              f"{item['winrate']:9.1f}%")

    print("\n" + "=" * 80 + "\n")

    # Лучший источник
    best = reliability_data[0]
    print(f"🏅 Самый надёжный источник: {best['source']}")
    print(f"   Надёжность: {best['reliability']:.1f}%")
    print(f"   WinRate: {best['winrate']:.1f}%")
    print(f"   TP1→SL: {best['tp1_sl_rate']:.1f}%")
    print(f"   TP2→SL: {best['tp2_sl_rate']:.1f}%\n")


def main():
    """Главная функция"""
    import argparse

    parser = argparse.ArgumentParser(description="Анализ упущенной прибыли")
    parser.add_argument("--strategy", default="orderflow", help="Стратегия")
    parser.add_argument("--symbol", help="Символ")
    parser.add_argument("--tf", default="tick", help="Таймфрейм")
    parser.add_argument("--mode", default="all",
                       choices=["metrics", "sources", "trades", "optimize", "all"],
                       help="Режим анализа")

    args = parser.parse_args()

    print("\n" + "=" * 80)
    print("🔬 Анализ упущенной прибыли (TP→SL Metrics)")
    print("=" * 80 + "\n")

    if args.mode in ["metrics", "all"]:
        analyze_missed_profit_metrics(args.strategy, args.symbol, args.tf)

    if args.mode in ["sources", "all"]:
        compare_sources_missed_profit(args.strategy, args.symbol, args.tf)

    if args.mode in ["trades", "all"]:
        find_problematic_trades(args.strategy, args.symbol, args.tf)

    if args.mode in ["optimize", "all"]:
        generate_optimization_report(args.strategy, args.symbol, args.tf)


if __name__ == "__main__":
    main()

