#!/usr/bin/env python3
from __future__ import annotations

"""
A/B Compare - Статистическое сравнение стратегий с bootstrap доверительными интервалами.

Функции:
- Сравнение стратегий по winrate и avg P/L
- Bootstrap доверительные интервалы (95%)
- Вероятность превосходства A над B
- Публикация результатов в Redis
- Telegram уведомления

Использование:
    python -m analytics.ab_compare \\
        --symbol  \\
        --strategies aggregated,orderflow,ta \\
        --days 14 \\
        --pairs aggregated:orderflow,aggregated:ta
"""

import argparse
import json
import os
import sys
import time
from collections.abc import Callable
from pathlib import Path
from typing import Any

import numpy as np
import redis

# Добавляем python-worker в путь
sys.path.insert(0, str(Path(__file__).parent.parent))

from analytics.repository import Order, RepoConfig, Repository
from analytics.telegram_reporter_ext import TelegramReporterExt
from common.log import setup_logger

logger = setup_logger("ABCompare")

# Опциональный GPU сервис для квантилей
_GPU_AVAILABLE = False
try:
    from services.gpu_compute_service import get_gpu_service

    _gpu_service = get_gpu_service()
    _GPU_AVAILABLE = bool(_gpu_service and _gpu_service.is_gpu_available())
except Exception:
    _gpu_service = None
    _GPU_AVAILABLE = False


def _quantiles(values: np.ndarray, probs: list[float], use_gpu: bool) -> np.ndarray:
    """Безопасное вычисление квантилей с GPU fallback."""
    if values.size == 0:
        return np.zeros(len(probs), dtype=np.float32)
    probs_arr = np.asarray(probs, dtype=np.float32)
    if use_gpu and _GPU_AVAILABLE and _gpu_service:
        try:
            return _gpu_service.compute_quantiles(values.astype(np.float32), probs_arr.tolist())
        except Exception:
            pass
    return np.quantile(values, probs_arr).astype(np.float32)

def winrate(data: list[float]) -> float:
    """Вычисление winrate"""
    if not data:
        return 0.0
    return sum(1 for x in data if x >= 0) / float(len(data))


def avg(data: list[float]) -> float:
    """Вычисление среднего"""
    return float(np.mean(data)) if data else 0.0


def bootstrap_ci(
    values: list[float],
    stat_fn: Callable[[list[float]], float],
    n_boot: int = 2000,
    alpha: float = 0.05,
    use_gpu: bool = False
) -> tuple[float, float, float]:
    """
    Bootstrap доверительный интервал (vectorised NumPy implementation).

    Args:
        values: Исходные значения
        stat_fn: Функция для вычисления статистики
        n_boot: Количество bootstrap итераций
        alpha: Уровень значимости (0.05 = 95% CI)

    Returns:
        (point_estimate, lower_bound, upper_bound)
    """
    if not values:
        return (0.0, 0.0, 0.0)

    vals = np.asarray(values, dtype=np.float64)
    n = len(vals)

    # Vectorised: sample (n_boot, n) indices at once
    idx = np.random.randint(0, n, size=(n_boot, n))
    samples = vals[idx]  # shape (n_boot, n)

    # Fast-path for the two most common stat functions
    if stat_fn is winrate or stat_fn == winrate:
        boot_stats = (samples >= 0).mean(axis=1).astype(np.float64)
    elif stat_fn is avg or stat_fn == avg:
        boot_stats = samples.mean(axis=1)
    else:
        # Generic path: apply stat_fn row-by-row (slower, but correct)
        boot_stats = np.array([stat_fn(samples[i].tolist()) for i in range(n_boot)])

    qs = _quantiles(boot_stats, [alpha / 2, 1 - alpha / 2], use_gpu=use_gpu)
    lo, hi = float(qs[0]), float(qs[1])

    return (stat_fn(values), lo, hi)


def prob_A_beats_B(
    a_values: list[float],
    b_values: list[float],
    stat_fn: Callable[[list[float]], float],
    n_boot: int = 2000
) -> float:
    """
    Вероятность того, что стратегия A превосходит B (vectorised).

    Args:
        a_values: P/L значения для стратегии A
        b_values: P/L значения для стратегии B
        stat_fn: Функция для вычисления статистики (winrate или avg)
        n_boot: Количество bootstrap итераций

    Returns:
        Вероятность (0..1)
    """
    if not a_values or not b_values:
        return 0.0

    a = np.asarray(a_values, dtype=np.float64)
    b = np.asarray(b_values, dtype=np.float64)

    idx_a = np.random.randint(0, len(a), size=(n_boot, len(a)))
    idx_b = np.random.randint(0, len(b), size=(n_boot, len(b)))
    samples_a = a[idx_a]  # (n_boot, na)
    samples_b = b[idx_b]  # (n_boot, nb)

    # Fast-path
    if stat_fn is winrate or stat_fn == winrate:
        stats_a = (samples_a >= 0).mean(axis=1)
        stats_b = (samples_b >= 0).mean(axis=1)
    elif stat_fn is avg or stat_fn == avg:
        stats_a = samples_a.mean(axis=1)
        stats_b = samples_b.mean(axis=1)
    else:
        stats_a = np.array([stat_fn(samples_a[i].tolist()) for i in range(n_boot)])
        stats_b = np.array([stat_fn(samples_b[i].tolist()) for i in range(n_boot)])

    return float((stats_a > stats_b).mean())


def load_orders(
    repo: Repository,
    symbol: str,
    strategies: list[str],
    since: float,
    until: float
) -> dict[str, list[Order]]:
    """
    Загрузка ордеров для указанных стратегий.
    
    Args:
        repo: Repository объект
        symbol: Символ
        strategies: Список стратегий
        since: Начало периода (unix timestamp)
        until: Конец периода (unix timestamp)
        
    Returns:
        Словарь {strategy: [Order, ...]}
    """
    res: dict[str, list[Order]] = {s: [] for s in strategies}

    for o in repo.read_closed_trades(200000):
        if o.symbol != symbol:
            continue
        if not o.entry_time:
            continue
        if o.entry_time < since or o.entry_time > until:
            continue

        s = (o.strategy or "").lower()
        if s in res:
            res[s].append(o)

    return res


def summarize_orders(orders: list[Order]) -> dict[str, Any]:
    """
    Вычисление сводной статистики для ордеров.
    
    Args:
        orders: Список ордеров
        
    Returns:
        Словарь со статистикой
    """
    pnls = [(o.pnl_usd or 0.0) for o in orders]

    wr = winrate(pnls)
    ap = avg(pnls)
    med = float(np.median(pnls)) if pnls else 0.0
    std = float(np.std(pnls)) if pnls else 0.0
    sharpe = (ap / std) if std > 1e-9 else 0.0

    return {
        "n": len(pnls),
        "winrate": wr,
        "avg_pnl": ap,
        "median_pnl": med,
        "std_pnl": std,
        "sharpe_like": sharpe
    },


def publish_to_redis(r: redis.Redis, symbol: str, summary: dict[str, dict]):
    """Публикация результатов A/B сравнения в Redis"""
    try:
        key = f"analytics:ab:last:{symbol}"
        payload = {
            "symbol": symbol,
            "ts": time.time(),
            "summary": summary
        },

        r.set(key, json.dumps(payload))

        r.xadd(
            os.getenv("AB_METRICS_STREAM", "metrics:ab"),
            {
                "symbol": symbol,
                "payload": json.dumps(summary),
                "ts": time.time()
            },
            maxlen=50000,
            approximate=True
        )

        logger.info(f"✅ A/B результаты опубликованы: {symbol}")

    except Exception as e:
        logger.error(f"❌ Ошибка публикации A/B: {e}")


def make_telegram_text(
    symbol: str,
    summary: dict[str, dict],
    pairs: list[tuple[str, str]]
) -> str:
    """Формирование текста для Telegram"""
    lines = [f"<b>🧪 A/B Сравнение</b>  <code>{symbol}</code>\n"]

    # Сводка по каждой стратегии
    for strat, stats in summary.items():
        lines.append(
            f"<b>{strat}</b>\n"
            f"  • Trades: {stats['n']}\n"
            f"  • Winrate: {stats['winrate']:.1%}\n"
            f"  • Avg P/L: ${stats['avg_pnl']:.2f}\n"
            f"  • Median: ${stats['median_pnl']:.2f}\n"
            f"  • Std: ${stats['std_pnl']:.2f}\n"
            f"  • Sharpe: {stats['sharpe_like']:.2f}\n"
        )

        # Bootstrap CI
        if "wr_ci" in stats:
            wr_lo, wr_hi = stats["wr_ci"]
            lines.append(f"  • WR CI(95%): [{wr_lo:.1%}, {wr_hi:.1%}]")

        if "ap_ci" in stats:
            ap_lo, ap_hi = stats["ap_ci"]
            lines.append(f"  • P/L CI(95%): [${ap_lo:.2f}, ${ap_hi:.2f}]\n")

    # Попарные сравнения
    if pairs:
        lines.append("\n<b>📊 Попарные сравнения:</b>\n")

        for (a, b) in pairs:
            if a not in summary or b not in summary:
                continue

            lines.append(f"<b>{a}</b> vs <b>{b}</b>")

            p_wr = summary[a].get(f"p_wr_gt_{b}", 0.0)
            p_ap = summary[a].get(f"p_ap_gt_{b}", 0.0)

            lines.append(f"  • P(WR({a}) > WR({b})) = {p_wr:.1%}")
            lines.append(f"  • P(P/L({a}) > P/L({b})) = {p_ap:.1%}\n")

    return "\n".join(lines)


def main():
    """Главная функция"""
    parser = argparse.ArgumentParser(
        description="A/B сравнение стратегий с bootstrap доверительными интервалами"
    )

    parser.add_argument(
        "--redis",
        default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
        help="Redis URL"
    )
    parser.add_argument(
        "--symbol",
        required=True,
        help="Symbol (e.g., )"
    )
    parser.add_argument(
        "--strategies",
        required=True,
        help="Comma-separated: aggregated,orderflow,ta"
    )
    parser.add_argument(
        "--days",
        type=int,
        default=7,
        help="Number of days to analyze"
    )
    parser.add_argument(
        "--pairs",
        default="",
        help="Pairs to compare: A:B,A:C (e.g., aggregated:orderflow)"
    )
    parser.add_argument(
        "--n-boot",
        type=int,
        default=2000,
        help="Number of bootstrap iterations"
    )
    parser.add_argument(
        "--use-gpu",
        action="store_true",
        help="Enable GPU quantiles if available"
    )

    args = parser.parse_args()

    logger.info("=" * 70)
    logger.info("🧪 A/B Compare - Strategy Comparison")
    logger.info("=" * 70)

    # Инициализация
    repo = Repository(RepoConfig(redis_url=args.redis))
    r = repo.r
    reporter = TelegramReporterExt(args.redis)

    # Временной диапазон
    now = time.time()
    since = now - args.days * 86400
    until = now

    # Парсинг стратегий и пар
    strategies = [s.strip().lower() for s in args.strategies.split(",") if s.strip()]

    pairs = []
    if args.pairs:
        for token in args.pairs.split(","):
            t = token.strip()
            if ":" in t:
                a, b = t.split(":")
                pairs.append((a.strip().lower(), b.strip().lower()))

    logger.info(f"📊 Symbol: {args.symbol}")
    logger.info(f"📊 Strategies: {strategies}")
    logger.info(f"📊 Period: {args.days} days")
    logger.info(f"📊 Pairs: {pairs}")
    logger.info("")

    # Загрузка ордеров
    logger.info("📥 Loading orders...")
    grouped = load_orders(repo, args.symbol, strategies, since, until)

    for strat, orders in grouped.items():
        logger.info(f"   {strat}: {len(orders)} orders")

    # Вычисление статистики и bootstrap CI
    logger.info("\n📊 Computing statistics...")
    summary: dict[str, dict] = {}
    boot: dict[str, list[float]] = {}

    use_gpu = bool(args.use_gpu and _GPU_AVAILABLE)
    if args.use_gpu and not _GPU_AVAILABLE:
        logger.info("⚠️ GPU requested but not available, using CPU quantiles")
    elif use_gpu:
        logger.info("🚀 GPU quantiles enabled")

    for strat, orders in grouped.items():
        pnls = [(o.pnl_usd or 0.0) for o in orders]

        # Bootstrap CI
        wr, wr_lo, wr_hi = bootstrap_ci(pnls, winrate, n_boot=args.n_boot, use_gpu=use_gpu)
        ap, ap_lo, ap_hi = bootstrap_ci(pnls, avg, n_boot=args.n_boot, use_gpu=use_gpu)

        # Базовые метрики
        base = summarize_orders(orders)
        base.update({
            "wr_ci": [wr_lo, wr_hi],
            "ap_ci": [ap_lo, ap_hi]
        })

        summary[strat] = base
        boot[strat] = pnls

        logger.info(
            f"   {strat}: WR={wr:.1%} CI=[{wr_lo:.1%}, {wr_hi:.1%}], "
            f"P/L=${ap:.2f} CI=[${ap_lo:.2f}, ${ap_hi:.2f}]"
        )

    # Попарные сравнения
    if pairs:
        logger.info("\n🔬 Pairwise comparisons...")

        for (a, b) in pairs:
            if a in boot and b in boot:
                p_wr = prob_A_beats_B(boot[a], boot[b], winrate, args.n_boot)
                p_ap = prob_A_beats_B(boot[a], boot[b], avg, args.n_boot)

                summary[a][f"p_wr_gt_{b}"] = p_wr
                summary[a][f"p_ap_gt_{b}"] = p_ap

                logger.info(f"   {a} vs {b}:")
                logger.info(f"      P(WR({a}) > WR({b})) = {p_wr:.1%}")
                logger.info(f"      P(P/L({a}) > P/L({b})) = {p_ap:.1%}")

    # Публикация в Redis
    logger.info("\n📤 Publishing to Redis...")
    publish_to_redis(r, args.symbol, summary)

    # Telegram уведомление
    logger.info("📱 Sending Telegram notification...")
    text = make_telegram_text(args.symbol, summary, pairs)

    reporter._push_text(
        group_id=f"ab:{args.symbol}:{int(time.time())}",
        title="A/B Summary",
        lines=text.split("\n")
    )

    logger.info("\n✅ A/B comparison completed!")
    logger.info("=" * 70)


if __name__ == "__main__":
    main()

