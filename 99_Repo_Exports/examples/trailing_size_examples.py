#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Примеры использования TrailingSizeRecommender

1. Из Redis stream trades:closed
2. По entry_tag отдельно
3. С различными параметрами
"""

from __future__ import annotations

import os
import sys
import redis
from collections import defaultdict
from typing import List

# Add scanner_infra to path for imports
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

from services.trailing_size_recommender import (
    ClosedTradeSnapshot,
    recommend_trailing_size,
    TrailingSizeRecommendation,
)


# ----------------------------
# Пример 1: Загрузка из Redis
# ----------------------------
def load_trades_from_redis(
    redis_client: redis.Redis,
    source: str,
    symbol: str,
    count: int = 1000
) -> List[ClosedTradeSnapshot]:
    """Загружает сделки из Redis stream."""
    entries = redis_client.xrevrange("trades:closed", max="+", min="-", count=count)

    trades = []
    for msg_id, fields in entries:
        if fields.get("source") != source:
            continue
        if fields.get("symbol") != symbol:
            continue

        try:
            trade = ClosedTradeSnapshot.from_trade_closed_dict(fields)
            trades.append(trade)
        except Exception as e:
            print(f"Ошибка парсинга сделки {msg_id}: {e}")
            continue

    return trades


# ----------------------------
# Пример 2: Анализ по символу (с двумя режимами)
# ----------------------------
def recommend_for_symbol(
    redis_client: redis.Redis,
    source: str,
    symbol: str,
    stop_atr_mult: float,
    count: int = 1000
) -> tuple[TrailingSizeRecommendation | None, TrailingSizeRecommendation | None]:
    """Анализ для одного символа в двух режимах: все сделки и только trailing."""
    trades = load_trades_from_redis(redis_client, source, symbol, count)

    if not trades:
        print(f"Нет сделок для {source}/{symbol}")
        return None, None

    # Рекомендация по всем выигрышным сделкам
    rec_all = recommend_trailing_size(
        trades,
        stop_atr_mult=stop_atr_mult,
        min_trades=50,
        winners_only=True,
        mfe_quantile=0.25,
        trailing_only=False,
    )

    # Рекомендация только по трейлинговым сделкам
    rec_trailing = recommend_trailing_size(
        trades,
        stop_atr_mult=stop_atr_mult,
        min_trades=25,  # меньше требований для trailing-сделок
        winners_only=True,
        mfe_quantile=0.25,
        trailing_only=True,
    )

    return rec_all, rec_trailing


# ----------------------------
# Пример 3: Анализ по entry_tag
# ----------------------------
def recommend_per_entry_tag(
    trades: List[ClosedTradeSnapshot],
    stop_atr_mult: float,
    min_trades: int = 30
) -> dict[str, tuple[TrailingSizeRecommendation | None, TrailingSizeRecommendation | None]]:
    """Анализ по каждому entry_tag отдельно (оба режима)."""
    by_tag = defaultdict(list)

    for trade in trades:
        tag = trade.entry_tag or "<untagged>"
        by_tag[tag].append(trade)

    results = {}
    for tag, tag_trades in by_tag.items():
        rec_all = recommend_trailing_size(
            tag_trades,
            stop_atr_mult=stop_atr_mult,
            min_trades=min_trades,
            winners_only=True,
            mfe_quantile=0.25,
            trailing_only=False,
        )
        rec_trailing = recommend_trailing_size(
            tag_trades,
            stop_atr_mult=stop_atr_mult,
            min_trades=max(5, min_trades // 4),  # меньше требований для trailing
            winners_only=True,
            mfe_quantile=0.25,
            trailing_only=True,
        )
        if rec_all or rec_trailing:
            results[tag] = (rec_all, rec_trailing)

    return results


# ----------------------------
# Пример 4: Полный анализ
# ----------------------------
def full_analysis_example():
    """Полный пример анализа."""
    # Подключение к Redis
    redis_client = redis.from_url("redis://localhost:6379/0", decode_responses=True)

    source = "CryptoOrderFlow"
    symbols = ["ETHUSDT", "BTCUSDT"]
    stop_atr_mult = 0.6  # ATR множитель для SL

    print("🚀 Анализ рекомендуемого размера трейлинга")
    print(f"Source: {source}")
    print(f"Stop ATR mult: {stop_atr_mult}")
    print()

    # Анализ по символам
    for symbol in symbols:
        print(f"📊 Анализ {symbol}:")

        # Анализ в двух режимах
        rec_all, rec_trailing = recommend_for_symbol(redis_client, source, symbol, stop_atr_mult, count=1500)

        # Все выигрышные сделки
        if rec_all:
            print(f"  ✅ Все win-сделки: lock_r={rec_all.lock_r:.2f}R, "
                  f"atr_offset={rec_all.trailing_tp1_offset_atr:.2f}, "
                  f"confidence={rec_all.confidence:.2f} "
                  f"({rec_all.sample_size_win}/{rec_all.sample_size_total} сделок)")
        else:
            print("  ❌ Все win-сделки: недостаточно данных")

        # Только трейлинговые сделки
        if rec_trailing:
            print(f"  ✅ Трейлинговые win-сделки: lock_r={rec_trailing.lock_r:.2f}R, "
                  f"atr_offset={rec_trailing.trailing_tp1_offset_atr:.2f}, "
                  f"confidence={rec_trailing.confidence:.2f} "
                  f"({rec_trailing.sample_size_win}/{rec_trailing.sample_size_total} сделок)")
        else:
            print("  ❌ Трейлинговые win-сделки: недостаточно данных")

        # Выбор лучшей рекомендации
        if rec_all and rec_trailing:
            # Выбираем по confidence, если разница значительная
            if abs(rec_all.confidence - rec_trailing.confidence) > 0.1:
                chosen = rec_all if rec_all.confidence > rec_trailing.confidence else rec_trailing
                mode = "все сделки" if chosen == rec_all else "трейлинговые"
                print(f"  🎯 Выбрана рекомендация: {mode} (confidence {chosen.confidence:.2f})")
            else:
                print(f"  🎯 Обе рекомендации близки по уверенности")

        # Анализ по entry_tag
        trades = load_trades_from_redis(redis_client, source, symbol, 1500)
        tag_recs = recommend_per_entry_tag(trades, stop_atr_mult)

        if tag_recs:
            print("  🎯 По entry_tag:")
            for tag, (tag_rec_all, tag_rec_trailing) in sorted(tag_recs.items()):
                if tag_rec_all:
                    print(f"    {tag} (все): lock_r={tag_rec_all.lock_r:.2f}R, atr_offset={tag_rec_all.trailing_tp1_offset_atr:.2f}")
                if tag_rec_trailing:
                    print(f"    {tag} (trailing): lock_r={tag_rec_trailing.lock_r:.2f}R, atr_offset={tag_rec_trailing.trailing_tp1_offset_atr:.2f}")

        print()


# ----------------------------
# Пример 5: Интеграция в существующую систему
# ----------------------------
def integrate_with_existing_analysis():
    """
    Как интегрировать в analyze_trades_from_redis_advanced.py

    Добавьте в main() после загрузки данных:
    """
    # Псевдокод интеграции
    if args.stop_atr_mult:
        for key, gb in groups.items():
            # Получить snapshots для группы
            group_snapshots = [s for s in snapshots
                             if matches_group_criteria(s, key, args.group_by)]

            rec = recommend_trailing_size(
                group_snapshots,
                stop_atr_mult=args.stop_atr_mult,
                min_trades=30,
                winners_only=True,
                mfe_quantile=0.25,
            )

            # Передать rec в render_global_report
            render_global_report(gb.label, gb.global_stats, rec)


# ----------------------------
# Пример 6: Тестирование с синтетическими данными
# ----------------------------
def test_with_synthetic_data():
    """Тест с синтетическими данными."""
    from datetime import datetime

    # Создаём тестовые сделки
    base_time = int(datetime(2025, 12, 13, 7, 0).timestamp() * 1000)

    trades = [
        # Хорошие сделки с высоким MFE
        ClosedTradeSnapshot(
            source="CryptoOrderFlow", symbol="ETHUSDT", strategy="crypto", entry_tag="deltaSpike",
            exit_ts_ms=base_time, pnl_net=200, pnl_if_fixed_exit=150, one_r_money=100,
            mfe_pnl=350, mae_pnl=-20, giveback=150, missed_profit=0,
            trailing_started=True, trailing_active=False,
            close_reason="tp", close_reason_raw="tp", close_reason_detail=""
        ),
        ClosedTradeSnapshot(
            source="CryptoOrderFlow", symbol="ETHUSDT", strategy="crypto", entry_tag="deltaSpike",
            exit_ts_ms=base_time + 1000, pnl_net=300, pnl_if_fixed_exit=250, one_r_money=100,
            mfe_pnl=400, mae_pnl=-30, giveback=100, missed_profit=0,
            trailing_started=True, trailing_active=True,
            close_reason="trailing", close_reason_raw="trailing", close_reason_detail=""
        ),
        # Средние сделки
        ClosedTradeSnapshot(
            source="CryptoOrderFlow", symbol="ETHUSDT", strategy="crypto", entry_tag="pullback",
            exit_ts_ms=base_time + 2000, pnl_net=150, pnl_if_fixed_exit=180, one_r_money=100,
            mfe_pnl=250, mae_pnl=-40, giveback=100, missed_profit=0,
            trailing_started=True, trailing_active=False,
            close_reason="tp", close_reason_raw="tp", close_reason_detail=""
        ),
    ]

    # Анализ в двух режимах
    rec_all = recommend_trailing_size(
        trades,
        stop_atr_mult=0.6,
        min_trades=2,
        winners_only=True,
        mfe_quantile=0.25,
        trailing_only=False,
    )

    rec_trailing = recommend_trailing_size(
        trades,
        stop_atr_mult=0.6,
        min_trades=2,
        winners_only=True,
        mfe_quantile=0.25,
        trailing_only=True,
    )

    print("🎯 Результаты теста:")

    if rec_all:
        print("Все win-сделки:")
        print(f"  Lock R: {rec_all.lock_r:.2f}R")
        print(f"  TRAILING_TP1_OFFSET_ATR: {rec_all.trailing_tp1_offset_atr:.2f}")
        print(f"  Confidence: {rec_all.confidence:.2f}")
        print(f"  Выборка: {rec_all.sample_size_win}/{rec_all.sample_size_total} сделок")
    else:
        print("Все win-сделки: ❌ недостаточно данных")

    if rec_trailing:
        print("Только трейлинговые win-сделки:")
        print(f"  Lock R: {rec_trailing.lock_r:.2f}R")
        print(f"  TRAILING_TP1_OFFSET_ATR: {rec_trailing.trailing_tp1_offset_atr:.2f}")
        print(f"  Confidence: {rec_trailing.confidence:.2f}")
        print(f"  Выборка: {rec_trailing.sample_size_win}/{rec_trailing.sample_size_total} сделок")
    else:
        print("Трейлинговые win-сделки: ❌ недостаточно данных")


if __name__ == "__main__":
    print("Примеры использования TrailingSizeRecommender")
    print("=" * 50)

    # Запуск теста
    test_with_synthetic_data()

    print("\n" + "=" * 50)
    print("Для реального анализа запустите:")
    print("python scripts/trailing_size_analysis.py --help")
