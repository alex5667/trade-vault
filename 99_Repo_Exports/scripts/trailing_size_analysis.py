#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Trailing Size Analysis Script

Анализ рекомендуемого размера трейлинг-стопа по истории закрытых сделок.
Использует TrailingSizeRecommender для расчёта lock_r и TRAILING_TP1_OFFSET_ATR.

Примеры использования:

# Анализ ETHUSDT за последние 1000 сделок
python scripts/trailing_size_analysis.py \
  --redis-url "redis://localhost:6379/0" \
  --source CryptoOrderFlow \
  --symbol ETHUSDT \
  --count 1000 \
  --stop-atr-mult 0.6

# Анализ с фильтром по времени (последний месяц)
python scripts/trailing_size_analysis.py \
  --redis-url "redis://localhost:6379/0" \
  --source CryptoOrderFlow \
  --symbol BTCUSDT \
  --count 2000 \
  --from "2025-12-01" \
  --stop-atr-mult 0.6 \
  --per-entry-tag

# Анализ с пользовательским квантилем MFE
python scripts/trailing_size_analysis.py \
  --redis-url "redis://localhost:6379/0" \
  --source CryptoOrderFlow \
  --symbol ETHUSDT \
  --count 1500 \
  --stop-atr-mult 0.6 \
  --mfe-quantile 0.3
"""

from __future__ import annotations

import argparse
from collections import defaultdict
from datetime import datetime, timezone

try:
    import redis
except ImportError:
    redis = None


from services.trailing_size_recommender import (
    ClosedTradeSnapshot,
    recommend_trailing_size,
    TrailingSizeRecommendation,
)


# ----------------------------
# Утилиты
# ----------------------------
def _parse_ts_arg(val: str | None) -> int | None:
    """Парсер временных аргументов."""
    if not val:
        return None
    s = val.strip()
    if not s:
        return None
    if s.isdigit():
        return int(s)
    try:
        if len(s) == 10 and s[4] == "-" and s[7] == "-":
            dt = datetime.strptime(s, "%Y-%m-%d")
            dt = dt.replace(tzinfo=timezone.utc)
        else:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=timezone.utc)
            else:
                dt = dt.astimezone(timezone.utc)
        return int(dt.timestamp() * 1000)
    except Exception as e:
        raise ValueError(f"Не могу разобрать значение времени '{s}': {e}") from e


def _fmt_f(v: float, decimals: int = 3) -> str:
    """Форматирование float."""
    return f"{v:.{decimals}f}"


# ----------------------------
# Загрузка данных из Redis
# ----------------------------
def load_trades_from_redis(
    redis_url: str,
    stream: str,
    count: int,
    source_filter: str | None = None,
    symbol_filter: str | None = None,
    from_ts_ms: int | None = None,
    to_ts_ms: int | None = None,
) -> list[ClosedTradeSnapshot]:
    """
    Загружает сделки из Redis stream trades:closed.
    """
    if not redis:
        raise ImportError("redis package not available")

    r = redis.from_url(redis_url, decode_responses=True)
    entries = r.xrevrange(stream, max="+", min="-", count=count)

    trades: list[ClosedTradeSnapshot] = []

    for msg_id, fields in entries:
        # Проверяем фильтры до парсинга
        if source_filter and fields.get("source") != source_filter:
            continue
        if symbol_filter and fields.get("symbol") != symbol_filter:
            continue

        # Фильтр по времени
        exit_ts = int(fields.get("exit_ts_ms", 0))
        if from_ts_ms and exit_ts < from_ts_ms:
            continue
        if to_ts_ms and exit_ts > to_ts_ms:
            continue

        try:
            trade = ClosedTradeSnapshot.from_trade_closed_dict(fields)
            trades.append(trade)
        except Exception as e:
            print(f"⚠️  Ошибка парсинга сделки {msg_id}: {e}")
            continue

    return trades


# ----------------------------
# Анализ
# ----------------------------
def analyze_symbol(
    trades: list[ClosedTradeSnapshot],
    symbol: str,
    stop_atr_mult: float,
    mfe_quantile: float = 0.25,
    min_trades: int = 50,
    trailing_only: bool = False,
) -> TrailingSizeRecommendation | None:
    """Анализ для одного символа."""
    rec = recommend_trailing_size(
        trades,
        stop_atr_mult=stop_atr_mult,
        min_trades=min_trades,
        winners_only=True,
        mfe_quantile=mfe_quantile,
        trailing_only=trailing_only,
    )
    return rec


def analyze_per_entry_tag(
    trades: list[ClosedTradeSnapshot],
    stop_atr_mult: float,
    mfe_quantile: float = 0.25,
    min_trades: int = 30,
    trailing_only: bool = False,
) -> dict[str, TrailingSizeRecommendation]:
    """Анализ по каждому entry_tag отдельно."""
    by_tag: dict[str, list[ClosedTradeSnapshot]] = defaultdict(list)

    for trade in trades:
        tag = trade.entry_tag or "<untagged>"
        by_tag[tag].append(trade)

    results: dict[str, TrailingSizeRecommendation] = {}

    for tag, tag_trades in by_tag.items():
        rec = recommend_trailing_size(
            tag_trades,
            stop_atr_mult=stop_atr_mult,
            min_trades=min_trades,
            winners_only=True,
            mfe_quantile=mfe_quantile,
            trailing_only=trailing_only,
        )
        if rec:
            results[tag] = rec

    return results


# ----------------------------
# Форматирование вывода
# ----------------------------
def print_recommendation(
    symbol_or_tag: str,
    rec: TrailingSizeRecommendation,
    prefix: str = "",
) -> None:
    """Вывод рекомендации в консоль."""
    mode = "trailing_only" if rec.trailing_only else "all_wins"
    print(f"{prefix}=== {symbol_or_tag} (mode: {mode}) ===")
    print(f"{prefix}Рекомендуемый lock_r: {_fmt_f(rec.lock_r)}R "
          f"(диапазон {_fmt_f(rec.lock_r_low)}–{_fmt_f(rec.lock_r_high)}R)")
    print(f"{prefix}→ TRAILING_TP1_OFFSET_ATR: {_fmt_f(rec.trailing_tp1_offset_atr)} "
          f"(диапазон {_fmt_f(rec.trailing_tp1_offset_atr_low)}–{_fmt_f(rec.trailing_tp1_offset_atr_high)})")

    print(f"{prefix}Статистика по выборке:")
    print(f"{prefix}  - Всего сделок: {rec.sample_size_total}, выигрышных: {rec.sample_size_win}")
    print(f"{prefix}  - Средний R: {_fmt_f(rec.avg_r_win)}R")
    print(f"{prefix}  - Медианный R: {_fmt_f(rec.median_r_win)}R")
    print(f"{prefix}  - Медианный MFE: {_fmt_f(rec.median_mfe_r_win)}R")
    print(f"{prefix}  - Средний giveback: {_fmt_f(rec.avg_giveback_r_win)}R "
          f"({rec.avg_giveback_ratio_win:.1%} от MFE)")
    print(f"{prefix}  - Confidence: {_fmt_f(rec.confidence)} "
          f"(σ_MFE_R={_fmt_f(rec.std_mfe_r)}, σ_giveback_ratio={_fmt_f(rec.std_giveback_ratio)})")


# ----------------------------
# Основная функция
# ----------------------------
def main():
    parser = argparse.ArgumentParser(
        description="Анализ рекомендуемого размера трейлинг-стопа",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog=__doc__,
    )

    parser.add_argument(
        "--redis-url",
        default="redis://localhost:6379/0",
        help="Redis URL (default: redis://localhost:6379/0)",
    )

    parser.add_argument(
        "--stream",
        default="trades:closed",
        help="Redis stream name (default: trades:closed)",
    )

    parser.add_argument(
        "--source",
        required=True,
        help="Фильтр по source (обязательно)",
    )

    parser.add_argument(
        "--symbol",
        help="Фильтр по symbol (опционально, если --per-entry-tag)",
    )

    parser.add_argument(
        "--count",
        type=int,
        default=1000,
        help="Количество последних сделок для анализа (default: 1000)",
    )

    parser.add_argument(
        "--from",
        dest="from_ts",
        help="Начало периода (YYYY-MM-DD или ms since epoch)",
    )

    parser.add_argument(
        "--to",
        dest="to_ts",
        help="Конец периода (YYYY-MM-DD или ms since epoch)",
    )

    parser.add_argument(
        "--stop-atr-mult",
        type=float,
        required=True,
        help="Множитель ATR для SL (stop_atr_mult из конфига)",
    )

    parser.add_argument(
        "--mfe-quantile",
        type=float,
        default=0.25,
        help="Квантиль MFE_R для расчёта lock_r (default: 0.25)",
    )

    parser.add_argument(
        "--min-trades",
        type=int,
        default=50,
        help="Минимальное количество сделок для рекомендации (default: 50)",
    )

    parser.add_argument(
        "--per-entry-tag",
        action="store_true",
        help="Анализировать отдельно по каждому entry_tag",
    )

    parser.add_argument(
        "--trailing-only",
        action="store_true",
        help="Использовать только сделки, где трейлинг был запущен (trailing_started или trailing_active)",
    )

    args = parser.parse_args()

    # Парсинг времени
    from_ts_ms = _parse_ts_arg(args.from_ts) if args.from_ts else None
    to_ts_ms = _parse_ts_arg(args.to_ts) if args.to_ts else None

    print("🔍 Загружаем сделки из Redis...")
    print(f"   Stream: {args.stream}")
    print(f"   Source: {args.source}")
    print(f"   Symbol: {args.symbol or 'ALL'}")
    print(f"   Count: {args.count}")
    if from_ts_ms:
        print(f"   From: {datetime.fromtimestamp(from_ts_ms/1000, tz=timezone.utc)}")
    if to_ts_ms:
        print(f"   To: {datetime.fromtimestamp(to_ts_ms/1000, tz=timezone.utc)}")
    print()

    # Загружаем данные
    trades = load_trades_from_redis(
        redis_url=args.redis_url,
        stream=args.stream,
        count=args.count,
        source_filter=args.source,
        symbol_filter=args.symbol,
        from_ts_ms=from_ts_ms,
        to_ts_ms=to_ts_ms,
    )

    print(f"📊 Загружено {len(trades)} сделок")
    print()

    if not trades:
        print("❌ Нет сделок для анализа")
        return

    if args.per_entry_tag:
        # Анализ по entry_tag
        print("🎯 Анализ по entry_tag:")
        tag_results = analyze_per_entry_tag(
            trades,
            stop_atr_mult=args.stop_atr_mult,
            mfe_quantile=args.mfe_quantile,
            min_trades=args.min_trades,
            trailing_only=args.trailing_only,
        )

        if not tag_results:
            print("❌ Недостаточно данных для анализа по entry_tag")
            return

        for tag in sorted(tag_results.keys()):
            rec = tag_results[tag]
            print_recommendation(tag, rec, "  ")
            print()

    else:
        # Анализ для символа в целом
        if not args.symbol:
            print("❌ Для общего анализа нужен --symbol")
            return

        print(f"🎯 Анализ для {args.symbol}:")
        rec = analyze_symbol(
            trades,
            symbol=args.symbol,
            stop_atr_mult=args.stop_atr_mult,
            mfe_quantile=args.mfe_quantile,
            min_trades=args.min_trades,
            trailing_only=args.trailing_only,
        )

        if not rec:
            print(f"❌ Недостаточно данных для {args.symbol} "
                  f"(нужно минимум {args.min_trades} сделок)")
            return

        print_recommendation(args.symbol, rec)
        print()

    print("✅ Анализ завершён")


if __name__ == "__main__":
    main()
