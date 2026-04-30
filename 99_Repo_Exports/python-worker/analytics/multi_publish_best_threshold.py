#!/usr/bin/env python3
"""
Multi Publish Best Threshold - Мульти-тюнинг порога для нескольких символов/стратегий.

Использование:
    python -m analytics.multi_publish_best_threshold \\
        --symbols XAUUSD,XAGUSD \\
        --strategies aggregated,orderflow \\
        --days 7 \\
        --emit-telegram 1
        
Результат:
- Публикация порогов в hub:threshold:{strategy}:{symbol}
- ROC точки в analytics:roc:{strategy}:{symbol}
- Telegram уведомления
"""

from __future__ import annotations
import argparse
import time
import os
import sys
from pathlib import Path

# Добавляем python-worker в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from analytics.repository import Repository, RepoConfig
from analytics.threshold_tuner import ThresholdTuner
from common.log import setup_logger


logger = setup_logger("MultiPublishThreshold")


def _ts_range(days: int) -> tuple[float, float]:
    """Вычисление временного диапазона"""
    now = time.time()
    return now - days * 86400, now


def main():
    """Главная функция"""
    parser = argparse.ArgumentParser(
        description="Мульти-тюнинг порогов для нескольких символов/стратегий"
    )

    parser.add_argument(
        "--redis"
        default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        help="Redis URL"
    )
    parser.add_argument(
        "--symbols"
        required=True
        help="Comma-separated symbols: XAUUSD,XAGUSD,BTCUSD"
    )
    parser.add_argument(
        "--strategies"
        required=True
        help="Comma-separated strategies: aggregated,orderflow,ta"
    )
    parser.add_argument(
        "--days"
        type=int
        default=7
        help="Количество дней истории для анализа"
    )
    parser.add_argument(
        "--emit-telegram"
        type=int
        default=1
        help="Отправлять уведомления в Telegram (1=да, 0=нет)"
    )

    args = parser.parse_args()

    logger.info("=" * 70)
    logger.info("🔧 Multi Publish Best Threshold")
    logger.info("=" * 70)

    # Инициализация
    repo = Repository(RepoConfig(redis_url=args.redis))
    tuner = ThresholdTuner(repo)

    # Парсинг символов и стратегий
    symbols = [s.strip() for s in args.symbols.split(",") if s.strip()]
    strategies = [s.strip() for s in args.strategies.split(",") if s.strip()]

    logger.info(f"📊 Символы: {symbols}")
    logger.info(f"📊 Стратегии: {strategies}")
    logger.info(f"📅 Период: {args.days} дней")
    logger.info("")

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

            logger.info(f"[{processed}/{total_combinations}] Обработка {strategy}/{symbol}...")

            try:
                # Получаем ордера за период
                orders = [
                    o for o in repo.read_closed_trades(50000)
                    if o.symbol == symbol
                    and (o.strategy or "").lower() == strategy.lower()
                    and o.entry_time
                    and since <= o.entry_time <= until
                ]

                logger.info(f"   Найдено ордеров: {len(orders)}")

                # Получаем сигналы
                signals = list(repo.iter_signals(
                    symbol=symbol
                    strategy=strategy
                    since_ts=since
                    until_ts=until
                ))

                logger.info(f"   Найдено сигналов: {len(signals)}")

                if not orders or not signals:
                    logger.warning(f"   ⚠️ Недостаточно данных для {strategy}/{symbol}")
                    continue

                # Тюнинг и публикация
                result = tuner.tune_and_publish(
                    strategy=strategy
                    symbol=symbol
                    signals=signals
                    orders=orders
                    emit_telegram=bool(args.emit_telegram)
                )

                if result:
                    successful += 1
                    logger.info(
                        f"   ✅ Порог установлен: {result['thr']:.2f} | "
                        f"AUC={result['auc']:.3f} | J={result['youdenJ']:.3f}"
                    )
                else:
                    logger.warning(f"   ⚠️ Не удалось установить порог для {strategy}/{symbol}")

            except Exception as e:
                logger.error(f"   ❌ Ошибка обработки {strategy}/{symbol}: {e}")

    # Итоговая сводка
    logger.info("")
    logger.info("=" * 70)
    logger.info(f"✅ Обработано: {processed} комбинаций")
    logger.info(f"✅ Успешно: {successful} комбинаций")
    logger.info(f"⚠️ Пропущено: {processed - successful} комбинаций")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()

