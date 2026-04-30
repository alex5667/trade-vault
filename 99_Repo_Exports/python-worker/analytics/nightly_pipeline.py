#!/usr/bin/env python3
"""
Nightly Pipeline - Полный ночной прогон аналитики.

Выполняет:
1. Экспорт партиционированного датасета
2. Тюнинг порогов для всех стратегий/символов
3. Сохранение ROC точек
4. Публикацию метрик для Grafana
5. Telegram отчёты с графиками

Использование:
    python -m analytics.nightly_pipeline \\
        --symbols XAUUSD \\
        --strategies aggregated,orderflow,ta \\
        --days 7

Запуск по расписанию (cron):
    0 2 * * * cd /path/to/python-worker && python -m analytics.nightly_pipeline --symbols XAUUSD --strategies aggregated
"""

from __future__ import annotations
import argparse
import time
import os
import json
import sys
from pathlib import Path

# Добавляем python-worker в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from analytics.repository import Repository, RepoConfig
from analytics.dataset_export import export_dataset_partitioned
from analytics.threshold_tuner import ThresholdTuner
from analytics.metrics_publisher import MetricsPublisher
from analytics.telegram_reporter_ext import TelegramReporterExt
from common.log import setup_logger


logger = setup_logger("NightlyPipeline")


def _ts_range(days: int) -> tuple[float, float]:
    """Вычисление временного диапазона"""
    now = time.time()
    return now - days * 86400, now


def main():
    """Главная функция ночного прогона"""
    parser = argparse.ArgumentParser(
        description="Nightly Analytics Pipeline - полный ночной прогон аналитики"
    )

    parser.add_argument(
        "--redis"
        default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        help="Redis URL"
    )
    parser.add_argument(
        "--symbols"
        required=True
        help="Comma-separated: XAUUSD,XAGUSD"
    )
    parser.add_argument(
        "--strategies"
        required=True
        help="Comma-separated: aggregated,orderflow,ta"
    )
    parser.add_argument(
        "--days"
        type=int
        default=7
        help="Количество дней истории"
    )
    parser.add_argument(
        "--skip-dataset"
        action="store_true"
        help="Пропустить экспорт датасета"
    )
    parser.add_argument(
        "--skip-telegram"
        action="store_true"
        help="Пропустить Telegram отчёты"
    )

    args = parser.parse_args()

    logger.info("=" * 70)
    logger.info("🌙 Nightly Analytics Pipeline")
    logger.info("=" * 70)
    logger.info(f"📊 Символы: {args.symbols}")
    logger.info(f"📊 Стратегии: {args.strategies}")
    logger.info(f"📅 Период: {args.days} дней")
    logger.info(f"📁 Датасет: {'Пропущен' if args.skip_dataset else 'Включён'}")
    logger.info(f"📱 Telegram: {'Пропущен' if args.skip_telegram else 'Включён'}")
    logger.info("")

    # Инициализация компонентов
    repo = Repository(RepoConfig(redis_url=args.redis))
    tuner = ThresholdTuner(repo)
    metrics_pub = MetricsPublisher(args.redis)
    reporter = TelegramReporterExt(args.redis) if not args.skip_telegram else None

    # Парсинг символов и стратегий
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    strategies = [s.strip() for s in args.strategies.split(",") if s.strip()]

    # Временной диапазон
    since, until = _ts_range(args.days)

    # Счётчики
    total_combinations = len(symbols) * len(strategies)
    processed = 0
    successful = 0

    # Обработка каждой комбинации
    for symbol in symbols:
        for strategy in strategies:
            processed += 1

            logger.info(f"\n{'=' * 70}")
            logger.info(f"[{processed}/{total_combinations}] {strategy}/{symbol}")
            logger.info("=" * 70)

            try:
                # 1) Получаем данные
                logger.info("📥 Получение данных...")

                orders = [
                    o for o in repo.read_closed_trades(100000)
                    if o.symbol == symbol
                    and (o.strategy or "").lower() == strategy.lower()
                    and o.entry_time
                    and since <= o.entry_time <= until
                ]

                signals = list(repo.iter_signals(
                    symbol=symbol
                    strategy=strategy
                    since_ts=since
                    until_ts=until
                ))

                logger.info(f"   Ордеров: {len(orders)}")
                logger.info(f"   Сигналов: {len(signals)}")

                if not orders or not signals:
                    logger.warning("   ⚠️ Недостаточно данных, пропуск")
                    continue

                # 2) Экспорт партиционированного датасета
                if not args.skip_dataset:
                    logger.info("📦 Экспорт датасета...")
                    dataset_path = export_dataset_partitioned(repo, orders, signals)
                    logger.info(f"   ✅ Датасет: {dataset_path}")

                # 3) Тюнинг порога и сохранение ROC
                logger.info("🔧 Тюнинг порога...")
                tune_res = tuner.tune_and_publish(
                    strategy=strategy
                    symbol=symbol
                    signals=signals
                    orders=orders
                    emit_telegram=False  # Отправим позже с графиками
                )

                if not tune_res:
                    logger.warning("   ⚠️ Не удалось настроить порог")
                    continue

                logger.info(f"   ✅ Порог: {tune_res['thr']:.2f}")

                # 4) Получаем ROC точки для отчёта
                logger.info("📊 Загрузка ROC точек...")
                roc_payload = repo.r.get(f"analytics:roc:{strategy}:{symbol}")

                auc = 0.0
                points = []

                if roc_payload:
                    d = json.loads(roc_payload)
                    auc = float(d.get("auc", 0.0))
                    points = d.get("points", [])
                    logger.info(f"   ✅ ROC точек: {len(points)}")

                # 5) Telegram отчёт с графиками
                if reporter:
                    logger.info("📱 Отправка Telegram отчёта...")
                    reporter.send_roc_report(
                        strategy=strategy
                        symbol=symbol
                        roc_points=points
                        auc=auc
                        summary=tune_res
                    )
                    logger.info("   ✅ Отчёт отправлен")

                # 6) Публикуем агрегированные метрики для Grafana
                logger.info("📈 Публикация метрик...")

                # Вычисляем метрики
                wins = sum(1 for o in orders if (o.pnl_usd is None or o.pnl_usd > 0))
                n = len(orders)
                winrate = (wins / n) if n > 0 else 0.0
                total_pnl = sum([o.pnl_usd or 0.0 for o in orders])
                avg_pnl = total_pnl / n if n > 0 else 0.0

                metrics_pub.publish(
                    strategy=strategy
                    symbol=symbol
                    metrics={
                        "total_trades": n
                        "wins": wins
                        "losses": n - wins
                        "winrate": winrate
                        "total_pnl": total_pnl
                        "avg_pnl_usd": avg_pnl
                        "auc": auc
                        "thr": tune_res.get("thr")
                        "youdenJ": tune_res.get("youdenJ")
                    }
                )

                logger.info("   ✅ Метрики опубликованы")

                successful += 1

            except Exception as e:
                logger.error(f"   ❌ Ошибка обработки {strategy}/{symbol}: {e}", exc_info=True)

    # Итоговая сводка
    logger.info("\n" + "=" * 70)
    logger.info("📊 Итоговая сводка")
    logger.info("=" * 70)
    logger.info(f"Обработано: {processed}/{total_combinations} комбинаций")
    logger.info(f"Успешно: {successful}")
    logger.info(f"Пропущено: {processed - successful}")
    logger.info("=" * 70 + "\n")


if __name__ == "__main__":
    main()

