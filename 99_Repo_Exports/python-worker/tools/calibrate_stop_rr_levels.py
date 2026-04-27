#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Калибровка stop_atr_mult и rr_levels под волатильность символа.

На основе рекомендаций:
1. Рассчитываем mae_atr_ratio = |mae_price - entry_price| / atr
2. stop_atr_mult ≈ 75-й перцентиль mae_atr_ratio
3. rr_levels на основе распределения mfe_r (медиана, 75-й, 90-й перцентили)

Использование:
    python calibrate_stop_rr_levels.py --dsn "postgresql://..." --source CryptoOrderFlow --symbol ETHUSDT --limit 1000
"""

from __future__ import annotations

import argparse
import statistics as stats
from dataclasses import dataclass
from typing import List, Optional, Dict, Any

import psycopg2


EPS = 1e-9


def get_conn(dsn: str):
    """Создать соединение с базой данных по DSN."""
    return psycopg2.connect(dsn)


@dataclass
class TradeRow:
    symbol: str
    source: str
    entry_tag: str

    entry_price: float
    direction: str  # LONG/SHORT
    lot: float

    atr: float

    pnl_net: float
    pnl_if_fixed_exit: float
    one_r_money: float

    mfe_pnl: float
    mae_pnl: float

    min_price_seen: float
    max_price_seen: float

    exit_ts_ms: int


def r_or_zero(pnl: float, one_r: float) -> float:
    if one_r is None or abs(one_r) < 1e-9:
        return 0.0
    return pnl / one_r


def load_calibration_data(
    conn,
    source: str,
    symbol: str,
    limit: int = 500,
) -> List[TradeRow]:
    """
    Загружает данные для калибровки из trades_closed.
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
                pnl_if_fixed_exit,
                one_r_money,
                mfe_pnl,
                mae_pnl,
                min_price_seen,
                max_price_seen,
                exit_ts_ms
            FROM trades_closed
            WHERE source = %s
              AND symbol = %s
              AND (signal_payload->>'atr')::float > 0
              AND one_r_money > 0
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
                pnl_if_fixed_exit=float(r[8] or 0.0),
                one_r_money=float(r[9] or 0.0),
                mfe_pnl=float(r[10] or 0.0),
                mae_pnl=float(r[11] or 0.0),
                min_price_seen=float(r[12] or 0.0),
                max_price_seen=float(r[13] or 0.0),
                exit_ts_ms=int(r[14] or 0),
            )
        )
    return result


def calculate_mae_price(entry_price: float, direction: str, min_price: float, max_price: float) -> float:
    """
    Вычисляем mae_price на основе направления позиции.
    Для LONG: mae_price = min_price_seen (самая низкая цена)
    Для SHORT: mae_price = max_price_seen (самая высокая цена)
    """
    if direction.upper() == "LONG":
        return min_price
    elif direction.upper() == "SHORT":
        return max_price
    else:
        return entry_price


def quantile(values: List[float], q: float) -> float:
    """Вычисляет квантиль (0.0-1.0) для списка значений."""
    if not values:
        return 0.0
    return float(stats.quantiles(values, n=100)[int(q * 99)])


def calibrate_stop_atr_mult(trades: List[TradeRow]) -> Dict[str, Any]:
    """
    Калибровка stop_atr_mult на основе mae_atr_ratio.
    """
    mae_atr_ratios: List[float] = []

    for t in trades:
        if t.atr <= 0 or t.one_r_money <= 0:
            continue

        mae_price = calculate_mae_price(t.entry_price, t.direction, t.min_price_seen, t.max_price_seen)
        mae_px = abs(mae_price - t.entry_price)

        if mae_px > 0:
            mae_atr_ratio = mae_px / t.atr
            mae_atr_ratios.append(mae_atr_ratio)

    if not mae_atr_ratios:
        return {"stop_atr_mult": 1.0, "stats": {}}

    # Статистика распределения
    median_ratio = quantile(mae_atr_ratios, 0.5)
    q75_ratio = quantile(mae_atr_ratios, 0.75)
    q90_ratio = quantile(mae_atr_ratios, 0.9)

    # Рекомендация: 75-й перцентиль, но не меньше 0.5 и не больше 2.0
    recommended_stop_atr_mult = max(0.5, min(2.0, q75_ratio))

    return {
        "stop_atr_mult": recommended_stop_atr_mult,
        "stats": {
            "count": len(mae_atr_ratios),
            "median_mae_atr_ratio": median_ratio,
            "q75_mae_atr_ratio": q75_ratio,
            "q90_mae_atr_ratio": q90_ratio,
            "min_mae_atr_ratio": min(mae_atr_ratios),
            "max_mae_atr_ratio": max(mae_atr_ratios),
        }
    }


def calibrate_rr_levels(trades: List[TradeRow]) -> Dict[str, Any]:
    """
    Калибровка rr_levels на основе распределения mfe_r.
    """
    mfe_r_values: List[float] = []

    for t in trades:
        if t.one_r_money <= 0:
            continue

        mfe_r = r_or_zero(t.mfe_pnl, t.one_r_money)
        if mfe_r > 0:
            mfe_r_values.append(mfe_r)

    if not mfe_r_values:
        return {"rr_levels": [1.0, 2.0, 3.0], "stats": {}}

    # Распределение mfe_r
    median_mfe_r = quantile(mfe_r_values, 0.5)
    q75_mfe_r = quantile(mfe_r_values, 0.75)
    q90_mfe_r = quantile(mfe_r_values, 0.9)

    # RR уровни: около медианы, 75-го и 90-го перцентилей
    # Округляем до "красивых" значений (1R, 1.5R, 2R, 3R и т.д.)
    def round_to_nice(r_val: float) -> float:
        nice_values = [0.5, 0.8, 1.0, 1.5, 2.0, 2.5, 3.0, 4.0, 5.0]
        return min(nice_values, key=lambda x: abs(x - r_val))

    tp1 = max(0.5, min(2.0, round_to_nice(median_mfe_r * 0.9)))  # немного ниже медианы
    tp2 = max(1.0, min(3.0, round_to_nice(q75_mfe_r)))
    tp3 = max(1.5, min(5.0, round_to_nice(q90_mfe_r)))

    # Убеждаемся, что TP1 < TP2 < TP3
    if tp1 >= tp2:
        tp1 = tp2 * 0.7
    if tp2 >= tp3:
        tp3 = tp2 * 1.5

    rr_levels = [round(tp1, 1), round(tp2, 1), round(tp3, 1)]

    return {
        "rr_levels": rr_levels,
        "stats": {
            "count": len(mfe_r_values),
            "median_mfe_r": median_mfe_r,
            "q75_mfe_r": q75_mfe_r,
            "q90_mfe_r": q90_mfe_r,
            "min_mfe_r": min(mfe_r_values),
            "max_mfe_r": max(mfe_r_values),
        }
    }


def print_calibration_report(symbol: str, source: str, stop_cal: Dict[str, Any], rr_cal: Dict[str, Any]) -> None:
    print(f"=== Калибровка параметров для {symbol} ({source}) ===")
    print()

    print("📊 STOP_ATR_MULT калибровка:")
    stop_stats = stop_cal.get("stats", {})
    if stop_stats:
        print(f"  Выборка: {stop_stats['count']} сделок")
        print(f"  MAE_ATR_RATIO - медиана: {stop_stats['median_mae_atr_ratio']:.3f}")
        print(f"  MAE_ATR_RATIO - 75-й перц: {stop_stats['q75_mae_atr_ratio']:.3f}")
        print(f"  MAE_ATR_RATIO - 90-й перц: {stop_stats['q90_mae_atr_ratio']:.3f}")
        print(f"  Диапазон: [{stop_stats['min_mae_atr_ratio']:.3f}, {stop_stats['max_mae_atr_ratio']:.3f}]")
    print(f"  ✅ Рекомендуемый STOP_ATR_MULT: {stop_cal['stop_atr_mult']:.3f}")
    print()

    print("📊 RR_LEVELS калибровка:")
    rr_stats = rr_cal.get("stats", {})
    if rr_stats:
        print(f"  Выборка: {rr_stats['count']} сделок")
        print(f"  MFE_R - медиана: {rr_stats['median_mfe_r']:.3f}")
        print(f"  MFE_R - 75-й перц: {rr_stats['q75_mfe_r']:.3f}")
        print(f"  MFE_R - 90-й перц: {rr_stats['q90_mfe_r']:.3f}")
        print(f"  Диапазон: [{rr_stats['min_mfe_r']:.3f}, {rr_stats['max_mfe_r']:.3f}]")
    rr_levels = rr_cal["rr_levels"]
    print(f"  ✅ Рекомендуемые RR_LEVELS: {rr_levels}")
    print()

    print("💡 Redis-конфиг для symbol_specs:")
    print(f"  {symbol}:")
    print(f"    trailing:")
    print(f"      stop_atr_mult: {stop_cal['stop_atr_mult']:.3f}")
    print(f"      rr_levels: {rr_levels}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Calibrate stop_atr_mult and rr_levels for symbol")
    parser.add_argument("--dsn", type=str, required=True, help="PostgreSQL DSN")
    parser.add_argument("--source", type=str, default="CryptoOrderFlow", help="Source")
    parser.add_argument("--symbol", type=str, required=True, help="Symbol (ETHUSDT, BTCUSDT)")
    parser.add_argument("--limit", type=int, default=500, help="Number of trades to analyze")

    args = parser.parse_args()

    conn = psycopg2.connect(args.dsn)

    trades = load_calibration_data(conn, args.source, args.symbol, args.limit)

    if not trades:
        print(f"Нет данных для калибровки {args.symbol}")
        conn.close()
        exit(1)

    stop_calibration = calibrate_stop_atr_mult(trades)
    rr_calibration = calibrate_rr_levels(trades)

    print_calibration_report(args.symbol, args.source, stop_calibration, rr_calibration)

    conn.close()
