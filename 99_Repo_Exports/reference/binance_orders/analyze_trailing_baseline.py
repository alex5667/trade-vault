#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
Mini-анализатор pnl_if_fixed_exit vs pnl_net (edge трейлинга).

Анализирует последние N сделок по символу и считает edge трейлинга:
- expectancy managed vs baseline
- доля сделок, где трейлинг улучшил/ухудшил результат
- метрики giveback/missed по трейлинговым сделкам

Использование:
    python analyze_trailing_baseline.py --dsn "postgresql://..." --source CryptoOrderFlow --symbol ETHUSDT --limit 200
"""

from __future__ import annotations

import math
from dataclasses import dataclass
from typing import List, Optional, Dict, Any

import psycopg2


@dataclass
class TradeRow:
    symbol: str
    source: str
    entry_tag: str
    pnl_net: float
    pnl_fixed: float
    one_r_money: float
    trailing_started: bool
    trailing_active: bool
    close_reason: str
    close_reason_raw: str
    exit_ts_ms: int


def r_or_zero(pnl: float, one_r: float) -> float:
    if one_r is None or abs(one_r) < 1e-9:
        return 0.0
    return pnl / one_r


def load_last_trades_from_db(
    conn,
    source: str,
    symbol: str,
    limit: int = 100,
) -> List[TradeRow]:
    """
    Загружает последние N сделок из trades_closed.
    """
    with conn.cursor() as cur:
        cur.execute(
            """
            SELECT
                source,
                symbol,
                COALESCE(entry_tag, '') AS entry_tag,
                pnl_net,
                pnl_if_fixed_exit,
                one_r_money,
                trailing_started,
                trailing_active,
                close_reason,
                close_reason_raw,
                exit_ts_ms
            FROM trades_closed
            WHERE source = %s
              AND symbol = %s
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
                source=r[0],
                symbol=r[1],
                entry_tag=r[2],
                pnl_net=float(r[3] or 0.0),
                pnl_fixed=float(r[4] or 0.0),
                one_r_money=float(r[5] or 0.0),
                trailing_started=bool(r[6]),
                trailing_active=bool(r[7]),
                close_reason=str(r[8] or ""),
                close_reason_raw=str(r[9] or ""),
                exit_ts_ms=int(r[10] or 0),
            )
        )
    return result


def analyze_trailing_edge(trades: List[TradeRow]) -> Dict[str, Any]:
    if not trades:
        return {"n": 0}

    def calc_stats(subset: List[TradeRow]) -> Dict[str, Any]:
        if not subset:
            return {"n": 0}

        r_managed = []
        r_baseline = []
        diffs_r = []
        diffs_usd = []

        better = worse = equal = 0

        for t in subset:
            r_m = r_or_zero(t.pnl_net, t.one_r_money)
            r_b = r_or_zero(t.pnl_fixed, t.one_r_money)
            r_managed.append(r_m)
            r_baseline.append(r_b)
            diffs_r.append(r_m - r_b)
            diffs_usd.append(t.pnl_net - t.pnl_fixed)

            if t.pnl_net > t.pnl_fixed + 1e-9:
                better += 1
            elif t.pnl_net < t.pnl_fixed - 1e-9:
                worse += 1
            else:
                equal += 1

        n = len(subset)
        def mean(xs: List[float]) -> float:
            return sum(xs) / len(xs) if xs else 0.0

        return {
            "n": n,
            "expectancy_managed_R": mean(r_managed),
            "expectancy_baseline_R": mean(r_baseline),
            "delta_expectancy_R": mean(diffs_r),
            "avg_diff_usd": mean(diffs_usd),
            "share_better": better / n,
            "share_worse": worse / n,
            "share_equal": equal / n,
        }

    total_stats = calc_stats(trades)
    trailing_trades = [t for t in trades if t.trailing_started or t.trailing_active]
    trailing_stats = calc_stats(trailing_trades)

    return {
        "total": total_stats,
        "trailing_only": trailing_stats,
    }


def print_report(symbol: str, source: str, stats: Dict[str, Any]) -> None:
    total = stats.get("total", {})
    trailing = stats.get("trailing_only", {})

    print(f"=== Edge трейлинга vs baseline ===")
    print(f"source={source}, symbol={symbol}")
    print()
    print(f"Всего сделок: {total.get('n', 0)}")
    print(
        f"Exp_R (managed) : {total.get('expectancy_managed_R', 0):+.3f}, "
        f"Exp_R (baseline): {total.get('expectancy_baseline_R', 0):+.3f}, "
        f"ΔExp_R: {total.get('delta_expectancy_R', 0):+.3f}"
    )
    print(
        f"Доля better/worse/equal (все): "
        f"{total.get('share_better', 0)*100:.1f}% / "
        f"{total.get('share_worse', 0)*100:.1f}% / "
        f"{total.get('share_equal', 0)*100:.1f}%"
    )
    print(f"Средняя разница pnl_net - pnl_fixed (USD): {total.get('avg_diff_usd', 0):+.3f}")
    print()

    print(f"Только трейлинговые сделки: {trailing.get('n', 0)}")
    print(
        f"Exp_R (managed, trailing) : {trailing.get('expectancy_managed_R', 0):+.3f}, "
        f"Exp_R (baseline, trailing): {trailing.get('expectancy_baseline_R', 0):+.3f}, "
        f"ΔExp_R: {trailing.get('delta_expectancy_R', 0):+.3f}"
    )
    print(
        f"Доля better/worse/equal (trailing): "
        f"{trailing.get('share_better', 0)*100:.1f}% / "
        f"{trailing.get('share_worse', 0)*100:.1f}% / "
        f"{trailing.get('share_equal', 0)*100:.1f}%"
    )
    print(f"Средняя разница pnl_net - pnl_fixed (USD, trailing): {trailing.get('avg_diff_usd', 0):+.3f}")


if __name__ == "__main__":
    import argparse

    parser = argparse.ArgumentParser(description="Analyze trailing edge vs baseline")
    parser.add_argument("--dsn", type=str, required=True, help="PostgreSQL DSN")
    parser.add_argument("--source", type=str, default="CryptoOrderFlow", help="Source")
    parser.add_argument("--symbol", type=str, required=True, help="Symbol (ETHUSDT, BTCUSDT)")
    parser.add_argument("--limit", type=int, default=200, help="Number of trades to analyze")

    args = parser.parse_args()

    conn = psycopg2.connect(args.dsn)

    trades = load_last_trades_from_db(conn, args.source, args.symbol, args.limit)
    stats = analyze_trailing_edge(trades)
    print_report(args.symbol, args.source, stats)

    conn.close()
