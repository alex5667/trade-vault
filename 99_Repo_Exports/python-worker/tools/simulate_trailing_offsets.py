#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
"""
Симуляция различных TRAILING_TP1_OFFSET_ATR для калибровки.

Анализирует существующие трейлинговые сделки и оценивает,
как бы вели себя разные offset_mult на основе giveback/missed_profit.

Для полной симуляции требуется доступ к историческим данным цены.
Этот скрипт дает рекомендации на основе существующей статистики.

Использование:
    python simulate_trailing_offsets.py --dsn "postgresql://..." --source CryptoOrderFlow --symbol ETHUSDT --limit 200
"""


import argparse
from dataclasses import dataclass
from typing import List, Optional, Dict, Any, Tuple

import psycopg2


EPS = 1e-9


@dataclass
class TradeRow:
    symbol: str
    source: str
    entry_tag: str

    entry_price: float
    direction: str
    lot: float
    atr: float

    pnl_net: float
    one_r_money: float

    mfe_pnl: float
    giveback: float
    missed_profit: float

    trailing_started: bool
    close_reason_detail: str

    exit_ts_ms: int


def r_or_zero(pnl: float, one_r: float) -> float:
    if one_r is None or abs(one_r) < 1e-9:
        return 0.0
    return pnl / one_r


def load_trailing_trades(
    conn,
    source: str,
    symbol: str,
    limit: int = 200,
) -> List[TradeRow]:
    """
    Загружает сделки с запущенным трейлингом и достигнутым TP1.
    Фильтрует только те сделки, где был достигнут TP1 (tp1_hit = true).
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                source,
                symbol,
                COALESCE(entry_tag, '') AS entry_tag,
                entry_price,
                direction,
                lot,
                (signal_payload->>'atr')::float AS atr,
                pnl_net,
                one_r_money,
                mfe_pnl,
                giveback,
                missed_profit,
                trailing_started,
                close_reason_detail,
                exit_ts_ms
            FROM trades_closed
            WHERE source = %s
              AND symbol = %s
              AND trailing_started = true
              AND tp1_hit = true
              AND (signal_payload->>'atr')::float > 0
              AND one_r_money > 0
              AND mfe_pnl > 0
            ORDER BY exit_ts_ms DESC
            LIMIT %s
            """,
            (source, symbol, limit),
        )
        rows = cur.fetchall()

    result: List[TradeRow] = []
    for r in rows:
        result.append(
            TradeRow(
                source=str(r[0]),
                symbol=str(r[1]),
                entry_tag=str(r[2] or ""),
                entry_price=float(r[3] or 0.0),
                direction=str(r[4] or ""),
                lot=float(r[5] or 0.0),
                atr=float(r[6] or 0.0),
                pnl_net=float(r[7] or 0.0),
                one_r_money=float(r[8] or 0.0),
                mfe_pnl=float(r[9] or 0.0),
                giveback=float(r[10] or 0.0),
                missed_profit=float(r[11] or 0.0),
                trailing_started=bool(r[12]),
                close_reason_detail=str(r[13] or ""),
                exit_ts_ms=int(r[14] or 0),
            )
        )
    return result


@dataclass
class PriceTick:
    """Тик цены для симуляции."""
    ts_ms: int
    price: float


def load_historical_prices(
    conn,
    symbol: str,
    start_ts_ms: int,
    end_ts_ms: int,
    max_ticks: int = 50000,
) -> List[PriceTick]:
    """
    Загружает исторические тиковые данные цены для симуляции.

    Args:
        conn: Соединение с БД
        symbol: Символ (BTCUSDT, ETHUSDT)
        start_ts_ms: Начало периода (timestamp в ms)
        end_ts_ms: Конец периода (timestamp в ms)
        max_ticks: Максимальное количество тиков для загрузки

    Returns:
        Список PriceTick отсортированных по времени
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT ts_ms, price
            FROM ticks
            WHERE symbol = %s
              AND ts_ms >= %s
              AND ts_ms <= %s
              AND price > 0
            ORDER BY ts_ms ASC
            LIMIT %s
            """,
            (symbol, start_ts_ms, end_ts_ms, max_ticks),
        )
        rows = cur.fetchall()

    result = []
    for r in rows:
        result.append(PriceTick(ts_ms=int(r[0]), price=float(r[1])))

    return result


def simulate_offset_mult_with_historical_data(
    trade: TradeRow,
    offset_mult: float,
    historical_prices: List[PriceTick],
    max_simulation_hours: int = 24,
) -> Dict[str, Any]:
    """
    Симулирует результат сделки с заданным offset_mult на исторических данных цены.

    Args:
        trade: Данные сделки
        offset_mult: Множитель ATR для сдвига SL
        historical_prices: Исторические тиковые данные цены
        max_simulation_hours: Максимальное время симуляции в часах

    Returns:
        Результат симуляции с метриками
    """
    if trade.mfe_pnl <= 0 or trade.atr <= 0 or not historical_prices:
        return {
            "simulated_pnl": trade.pnl_net,
            "simulated_r": r_or_zero(trade.pnl_net, trade.one_r_money),
            "simulated_giveback_r": 0.0,
            "simulated_missed_r": 0.0,
            "would_be_better": False,
            "actual_r": r_or_zero(trade.pnl_net, trade.one_r_money),
            "mfe_r": trade.mfe_pnl / trade.one_r_money if trade.one_r_money > 0 else 0.0,
            "sl_hit": False,
            "time_to_sl_hit_hours": None,
            "ticks_analyzed": 0,
        }

    # Определяем TP1 уровень (где активируется трейлинг)
    tp1_price = None
    if trade.direction.lower() in ["long", "buy"]:
        # Для лонга TP1 выше entry_price
        tp1_price = trade.entry_price + (trade.one_r_money / trade.lot)  # +1R
    else:
        # Для шорта TP1 ниже entry_price
        tp1_price = trade.entry_price - (trade.one_r_money / trade.lot)  # -1R

    if tp1_price is None:
        return simulate_offset_mult(trade, offset_mult)  # fallback to old method

    # Рассчитываем новый SL после достижения TP1
    offset = trade.atr * offset_mult
    if trade.direction.lower() in ["long", "buy"]:
        new_sl = trade.entry_price + offset
    else:
        new_sl = trade.entry_price - offset

    # Находим время достижения TP1 в исторических данных
    tp1_reached_ts = None
    for tick in historical_prices:
        if trade.direction.lower() in ["long", "buy"]:
            if tick.price >= tp1_price:
                tp1_reached_ts = tick.ts_ms
                break
        else:
            if tick.price <= tp1_price:
                tp1_reached_ts = tick.ts_ms
                break

    if tp1_reached_ts is None:
        # TP1 не был достигнут в симуляции
        return simulate_offset_mult(trade, offset_mult)  # fallback

    # Симулируем движение цены после TP1
    max_simulation_ts = tp1_reached_ts + (max_simulation_hours * 60 * 60 * 1000)
    sl_hit = False
    sl_hit_ts = None
    max_price_after_tp1 = tp1_price
    min_price_after_tp1 = tp1_price

    for tick in historical_prices:
        if tick.ts_ms < tp1_reached_ts:
            continue
        if tick.ts_ms > max_simulation_ts:
            break

        # Обновляем экстремумы
        max_price_after_tp1 = max(max_price_after_tp1, tick.price)
        min_price_after_tp1 = min(min_price_after_tp1, tick.price)

        # Проверяем, не пробит ли SL
        if trade.direction.lower() in ["long", "buy"]:
            if tick.price <= new_sl:
                sl_hit = True
                sl_hit_ts = tick.ts_ms
                break
        else:
            if tick.price >= new_sl:
                sl_hit = True
                sl_hit_ts = tick.ts_ms
                break

    # Рассчитываем результат симуляции
    if sl_hit and sl_hit_ts:
        # Сделка закрыта по SL
        exit_price = new_sl
        time_to_exit_hours = (sl_hit_ts - tp1_reached_ts) / (1000 * 60 * 60)
    else:
        # Сделка не закрыта за период симуляции - используем последнюю цену
        exit_price = historical_prices[-1].price if historical_prices else trade.entry_price
        time_to_exit_hours = max_simulation_hours

    # Рассчитываем PnL от TP1 до выхода
    if trade.direction.lower() in ["long", "buy"]:
        pnl_from_tp1 = (exit_price - tp1_price) * trade.lot
    else:
        pnl_from_tp1 = (tp1_price - exit_price) * trade.lot

    # Общий PnL = PnL до TP1 + PnL после TP1
    pnl_to_tp1 = trade.one_r_money  # TP1 достигнут, так что +1R
    total_simulated_pnl = pnl_to_tp1 + pnl_from_tp1

    simulated_r = total_simulated_pnl / trade.one_r_money if trade.one_r_money > 0 else 0.0

    # Метрики giveback и missed_profit
    actual_r = trade.pnl_net / trade.one_r_money if trade.one_r_money > 0 else 0.0
    mfe_r = trade.mfe_pnl / trade.one_r_money if trade.one_r_money > 0 else 0.0

    simulated_giveback_r = max(0.0, mfe_r - simulated_r)
    simulated_missed_r = max(0.0, simulated_r - actual_r) if simulated_r > actual_r else 0.0

    # Оцениваем, было бы лучше
    would_be_better = simulated_r > actual_r * 1.05  # Улучшение >5%

    return {
        "simulated_pnl": total_simulated_pnl,
        "simulated_r": simulated_r,
        "simulated_giveback_r": simulated_giveback_r,
        "simulated_missed_r": simulated_missed_r,
        "would_be_better": would_be_better,
        "actual_r": actual_r,
        "mfe_r": mfe_r,
        "sl_hit": sl_hit,
        "time_to_sl_hit_hours": time_to_exit_hours if sl_hit else None,
        "ticks_analyzed": len([t for t in historical_prices if tp1_reached_ts <= t.ts_ms <= max_simulation_ts]),
        "tp1_price": tp1_price,
        "new_sl": new_sl,
        "max_price_after_tp1": max_price_after_tp1,
        "min_price_after_tp1": min_price_after_tp1,
    }


def simulate_offset_mult(trade: TradeRow, offset_mult: float) -> Dict[str, Any]:
    """
    Упрощенная симуляция для обратной совместимости.
    Используется, когда нет исторических данных.
    """
    if trade.mfe_pnl <= 0 or trade.atr <= 0:
        return {"would_stop_early": False, "simulated_pnl": trade.pnl_net, "simulated_r": r_or_zero(trade.pnl_net, trade.one_r_money)}

    mfe_r = trade.mfe_pnl / trade.one_r_money
    current_r = trade.pnl_net / trade.one_r_money

    # Упрощенная модель на основе giveback/missed_profit
    giveback_r = (trade.giveback / trade.one_r_money) if trade.giveback > 0 else 0.0
    missed_r = (trade.missed_profit / trade.one_r_money) if trade.missed_profit > 0 else 0.0

    simulated_giveback_r = giveback_r * (1.0 / offset_mult)  # меньше offset = больше giveback
    simulated_missed_r = missed_r * offset_mult  # больше offset = больше missed

    simulated_r = mfe_r - simulated_giveback_r - simulated_missed_r
    simulated_pnl = simulated_r * trade.one_r_money

    would_be_better = (simulated_giveback_r < giveback_r * 0.7) and (simulated_missed_r < missed_r * 0.7)

    return {
        "simulated_pnl": simulated_pnl,
        "simulated_r": simulated_r,
        "simulated_giveback_r": simulated_giveback_r,
        "simulated_missed_r": simulated_missed_r,
        "would_be_better": would_be_better,
        "actual_r": current_r,
        "mfe_r": mfe_r,
        "sl_hit": None,  # unknown in simplified model
        "time_to_sl_hit_hours": None,
        "ticks_analyzed": 0,
    }


def analyze_offset_mult_range(
    conn,
    trades: List[TradeRow],
    offset_mults: List[float],
    symbol: str,
    max_simulation_hours: int = 24,
) -> Dict[float, Dict[str, Any]]:
    """
    Анализирует различные offset_mult на выборке сделок с использованием исторических данных.
    """
    results: Dict[float, Dict[str, Any]] = {}

    # Кэш исторических данных по сделкам (чтобы не загружать многократно)
    price_cache: Dict[int, List[PriceTick]] = {}

    for offset_mult in offset_mults:
        simulated_rs: List[float] = []
        giveback_rs: List[float] = []
        missed_rs: List[float] = []
        better_count = 0
        sl_hit_rates: List[bool] = []
        avg_time_to_sl_hit: List[float] = []

        for trade in trades:
            # Загружаем исторические данные для сделки (если не в кэше)
            if trade.exit_ts_ms not in price_cache:
                # Период: от entry до exit + запас на симуляцию
                start_ts = trade.exit_ts_ms - (7 * 24 * 60 * 60 * 1000)  # 7 дней назад
                end_ts = trade.exit_ts_ms + (max_simulation_hours * 60 * 60 * 1000)  # вперед на время симуляции

                price_cache[trade.exit_ts_ms] = load_historical_prices(
                    conn, symbol, start_ts, end_ts, max_ticks=50000
                )

            historical_prices = price_cache[trade.exit_ts_ms]

            # Используем полноценную симуляцию
            sim = simulate_offset_mult_with_historical_data(
                trade, offset_mult, historical_prices, max_simulation_hours
            )

            simulated_rs.append(sim["simulated_r"])
            giveback_rs.append(sim["simulated_giveback_r"])
            missed_rs.append(sim["simulated_missed_r"])

            if sim["would_be_better"]:
                better_count += 1

            if sim["sl_hit"] is not None:
                sl_hit_rates.append(sim["sl_hit"])
                if sim["time_to_sl_hit_hours"] is not None:
                    avg_time_to_sl_hit.append(sim["time_to_sl_hit_hours"])

        n = len(trades)
        avg_simulated_r = sum(simulated_rs) / n if n > 0 else 0.0
        avg_giveback_r = sum(giveback_rs) / n if n > 0 else 0.0
        avg_missed_r = sum(missed_rs) / n if n > 0 else 0.0
        sl_hit_rate = sum(sl_hit_rates) / len(sl_hit_rates) if sl_hit_rates else 0.0
        avg_time_to_hit = sum(avg_time_to_sl_hit) / len(avg_time_to_sl_hit) if avg_time_to_sl_hit else None

        results[offset_mult] = {
            "avg_expectancy_r": avg_simulated_r,
            "avg_giveback_r": avg_giveback_r,
            "avg_missed_r": avg_missed_r,
            "share_better": better_count / n if n > 0 else 0.0,
            "sample_size": n,
            "sl_hit_rate": sl_hit_rate,
            "avg_time_to_sl_hit_hours": avg_time_to_hit,
        }

    return results


def get_offset_mult_grid(symbol: str) -> List[float]:
    """
    Возвращает сетку offset_mult для тестирования в зависимости от символа.

    Для более волатильных символов (ETHUSDT) - шире диапазон и чаще значения
    Для менее волатильных (BTCUSDT) - уже диапазон
    """
    symbol_upper = symbol.upper()

    if symbol_upper == "ETHUSDT":
        # Более волатильный - шире диапазон, больше offset
        return [0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1.0, 1.2]
    elif symbol_upper == "BTCUSDT":
        # Менее волатильный - уже диапазон, меньший offset
        return [0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 1.0]
    elif symbol_upper in ["SOLUSDT", "ADAUSDT", "DOTUSDT"]:
        # Высоковолатильные альткоины
        return [0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 1.0, 1.2, 1.5]
    else:
        # Дефолтный диапазон
        return [0.2, 0.3, 0.4, 0.6, 0.8, 1.0, 1.2]


def recommend_offset_mult(analysis: Dict[float, Dict[str, Any]]) -> Dict[str, Any]:
    """
    Рекомендует оптимальный offset_mult на основе анализа с учетом SL hit rate.
    """
    if not analysis:
        return {"recommended": 0.6, "reason": "no_data"}

    # Критерии выбора:
    # 1. Максимальный expectancy_r
    # 2. Минимальный giveback_r
    # 3. Приемлемый missed_r (не слишком высокий)
    # 4. SL hit rate не слишком высокий (не хотим частые стопы)

    best_offset = 0.6  # default
    best_score = -float('inf')

    for offset, stats in analysis.items():
        expectancy = stats["avg_expectancy_r"]
        giveback = stats["avg_giveback_r"]
        missed = stats["avg_missed_r"]
        better_share = stats["share_better"]
        sl_hit_rate = stats.get("sl_hit_rate", 0.0)

        # Составной скор: expectancy - giveback_penalty - missed_penalty + better_bonus
        giveback_penalty = giveback * 0.5  # giveback хуже, чем упущенная прибыль
        missed_penalty = missed * 0.3
        sl_hit_penalty = sl_hit_rate * 0.1  # штраф за частые стопы

        score = expectancy - giveback_penalty - missed_penalty - sl_hit_penalty + better_share * 0.2

        if score > best_score:
            best_score = score
            best_offset = offset

    return {
        "recommended": best_offset,
        "reason": "max_expectancy_min_giveback_with_sl_penalty",
        "analysis": analysis,
    }


def print_simulation_report(symbol: str, source: str, analysis: Dict[float, Dict[str, Any]], recommendation: Dict[str, Any]) -> None:
    print(f"=== Полноценная симуляция TRAILING_TP1_OFFSET_ATR для {symbol} ({source}) ===")
    print(f"Рекомендация: {recommendation['recommended']:.2f} (причина: {recommendation['reason']})")
    print()

    print("📊 Анализ различных offset_mult (с историческими данными):")
    print("offset | exp_R | giveback_R | missed_R | better% | SL_hit% | time_h | n")
    print("-" * 75)

    for offset in sorted(analysis.keys()):
        stats = analysis[offset]
        time_str = f"{stats['avg_time_to_sl_hit_hours']:.1f}" if stats['avg_time_to_sl_hit_hours'] else "N/A"
        print(
            f"{offset:6.2f} | "
            f"{stats['avg_expectancy_r']:5.3f} | "
            f"{stats['avg_giveback_r']:10.3f} | "
            f"{stats['avg_missed_r']:9.3f} | "
            f"{stats['share_better']*100:6.1f}% | "
            f"{stats['sl_hit_rate']*100:6.1f}% | "
            f"{time_str:6s} | "
            f"{stats['sample_size']:2d}"
        )

    print()
    print("💡 Redis-конфиг для symbol_specs:")
    print(f"  {symbol}:")
    print("    trailing:")
    print(f"      tp1_offset_atr: {recommendation['recommended']:.2f}")
    print()
    print("✅ Фильтр: только сделки с tp1_hit = true")
    print("✅ Симуляция: на исторических тиковых данных цены")
    print("✅ Анализ: временная динамика возврата цены к new_sl")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Simulate TRAILING_TP1_OFFSET_ATR for symbol")
    parser.add_argument("--dsn", type=str, required=True, help="PostgreSQL DSN")
    parser.add_argument("--source", type=str, default="CryptoOrderFlow", help="Source")
    parser.add_argument("--symbol", type=str, required=True, help="Symbol (ETHUSDT, BTCUSDT)")
    parser.add_argument("--limit", type=int, default=200, help="Number of trailing trades to analyze")
    parser.add_argument("--offsets", type=str, help="Comma-separated offset_mult values to test (default: auto by symbol)")
    parser.add_argument("--max-hours", type=int, default=24, help="Max simulation hours after TP1")

    args = parser.parse_args()

    conn = psycopg2.connect(args.dsn)

    trades = load_trailing_trades(conn, args.source, args.symbol, args.limit)

    if not trades:
        print(f"Нет трейлинговых сделок с tp1_hit=true для анализа {args.symbol}")
        conn.close()
        exit(1)

    # Используем заданную сетку или авто по символу
    if args.offsets:
        offset_mults = [float(x.strip()) for x in args.offsets.split(",") if x.strip()]
    else:
        offset_mults = get_offset_mult_grid(args.symbol)

    print(f"Анализ {len(trades)} сделок с tp1_hit=true для {args.symbol}")
    print(f"Тестируем offset_mult: {offset_mults}")
    print(f"Макс. время симуляции: {args.max_hours} часов")
    print()

    analysis = analyze_offset_mult_range(conn, trades, offset_mults, args.symbol, args.max_hours)
    recommendation = recommend_offset_mult(analysis)

    print_simulation_report(args.symbol, args.source, analysis, recommendation)

    conn.close()
