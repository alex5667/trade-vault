#!/usr/bin/env python
"""
Полноценная симуляция TRAILING_TP1_OFFSET_ATR для калибровки.

Анализирует сделки с tp1_hit=true и симулирует трейлинг на реальных
исторических данных из таблицы ticks, оценивая оптимальный offset_mult.

Использование:
    python simulate_trailing_tp1_full.py --dsn "postgresql://..." --source CryptoOrderFlow --symbol ETHUSDT --limit 200
"""

from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple

import psycopg2
import psycopg2.extras


@dataclass
class TradeRow:
    id: int
    symbol: str
    source: str
    direction: str  # 'LONG' / 'SHORT'
    entry_ts_ms: int
    exit_ts_ms: int
    entry_price: float
    exit_price: float
    initial_sl_price: float
    tp1_hit: bool
    trailing_started: bool
    tp1_price: float
    atr_entry: float


@dataclass
class Tick:
    ts_ms: int
    price: float


@dataclass
class SimResult:
    offset_mult: float
    trade_id: int
    r_orig: float
    r_mfe: float
    r_trail: float
    giveback_r: float
    missed_r: float
    fake_stopout: bool
    exit_reason: str  # 'trailing_stop' / 'original_exit' / 'mfe_exit'


@dataclass
class OffsetStats:
    offset_mult: float
    expectancy_r: float
    avg_giveback_r: float
    avg_missed_r: float
    share_fake_stopout: float
    count: int


def _sign(direction: str) -> int:
    return 1 if direction.upper() == "LONG" else -1


def fetch_trades(
    conn,
    source: str,
    symbol: str,
    limit: int,
) -> List[TradeRow]:
    """
    Берём только сделки с tp1_hit = TRUE и trailing_started = TRUE.
    Используем trades_closed таблицу.
    """
    sql = """
    SELECT
        id,
        symbol,
        source,
        direction,
        entry_ts_ms,
        exit_ts_ms,
        entry_price,
        exit_price,
        initial_sl_price,
        tp1_hit,
        trailing_started,
        tp1_price,
        (signal_payload->>'atr')::float AS atr_entry
    FROM trades_closed
    WHERE source = %(source)s
      AND symbol = %(symbol)s
      AND tp1_hit = TRUE
      AND trailing_started = TRUE
      AND exit_ts_ms IS NOT NULL
      AND (signal_payload->>'atr')::float > 0
      AND one_r_money > 0
    ORDER BY exit_ts_ms DESC
    LIMIT %(limit)s;
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, {"source": source, "symbol": symbol, "limit": limit})
        rows = cur.fetchall()

    trades: List[TradeRow] = []
    for r in rows:
        trades.append(
            TradeRow(
                id=r["id"],
                symbol=r["symbol"],
                source=r["source"],
                direction=r["direction"],
                entry_ts_ms=int(r["entry_ts_ms"]),
                exit_ts_ms=int(r["exit_ts_ms"]),
                entry_price=float(r["entry_price"]),
                exit_price=float(r["exit_price"]),
                initial_sl_price=float(r["initial_sl_price"] or 0.0),
                tp1_hit=bool(r["tp1_hit"]),
                trailing_started=bool(r["trailing_started"]),
                tp1_price=float(r["tp1_price"] or 0.0),
                atr_entry=float(r["atr_entry"] or 0.0),
            )
        )
    return trades


def fetch_ticks(
    conn,
    symbol: str,
    start_ts_ms: int,
    end_ts_ms: int,
) -> List[Tick]:
    """
    Загружаем тики из таблицы ticks между start_ts_ms и end_ts_ms.
    """
    sql = """
    SELECT ts_ms, price
    FROM ticks
    WHERE symbol = %(symbol)s
      AND ts_ms >= %(start_ts_ms)s
      AND ts_ms <= %(end_ts_ms)s
    ORDER BY ts_ms ASC;
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, {"symbol": symbol, "start_ts_ms": start_ts_ms, "end_ts_ms": end_ts_ms})
        rows = cur.fetchall()

    ticks: List[Tick] = []
    for r in rows:
        ticks.append(Tick(ts_ms=int(r["ts_ms"]), price=float(r["price"])))
    return ticks


def compute_r(
    direction: str,
    entry_price: float,
    price: float,
    initial_sl_price: float,
    eps: float = 1e-8,
) -> float:
    risk_per_unit = max(abs(entry_price - initial_sl_price), eps)
    sign = _sign(direction)
    return sign * (price - entry_price) / risk_per_unit


def find_tp1_hit_ts(ticks: List[Tick], trade: TradeRow) -> int:
    """
    Находим timestamp достижения TP1 в исторических данных.
    """
    tp1_price = trade.tp1_price
    if tp1_price <= 0:
        # Если tp1_price не задан, используем entry_ts_ms как отправную точку
        return trade.entry_ts_ms

    for tick in ticks:
        if trade.direction.upper() == "LONG":
            if tick.price >= tp1_price:
                return tick.ts_ms
        else:  # SHORT
            if tick.price <= tp1_price:
                return tick.ts_ms

    # Если TP1 не достигнут в данных, возвращаем entry_ts_ms
    return trade.entry_ts_ms


def simulate_trade_for_offset(
    trade: TradeRow,
    offset_mult: float,
    ticks: List[Tick],
    use_mfe_exit: bool = False,
    eps: float = 1e-8,
) -> SimResult:
    """
    Полноценная симуляция трейлинга на исторических тиках.
    """
    direction = trade.direction
    entry_price = trade.entry_price
    exit_price_orig = trade.exit_price
    initial_sl_price = trade.initial_sl_price

    # Исходные метрики
    r_orig = compute_r(direction, entry_price, exit_price_orig, initial_sl_price, eps)

    # ATR и offset
    atr = float(trade.atr_entry or 0.0)
    offset = max(0.0, atr * float(offset_mult))

    if offset <= 0.0 or atr <= 0.0:
        # Если нет ATR, просто возвращаем исходную сделку
        r_mfe = r_orig
        return SimResult(
            offset_mult=offset_mult,
            trade_id=trade.id,
            r_orig=r_orig,
            r_mfe=r_mfe,
            r_trail=r_orig,
            giveback_r=0.0,
            missed_r=0.0,
            fake_stopout=False,
            exit_reason="no_atr",
        )

    if direction.upper() == "LONG":
        new_sl = entry_price + offset
    else:
        new_sl = entry_price - offset

    # Находим время достижения TP1
    tp1_hit_ts = find_tp1_hit_ts(ticks, trade)

    # Пробегаем тики только после TP1
    r_mfe = r_orig
    r_trail = r_orig
    exit_reason = "original_exit"
    trailing_exit = False

    for tick in ticks:
        if tick.ts_ms < tp1_hit_ts:
            continue
        if tick.ts_ms > trade.exit_ts_ms:
            break

        # Обновляем MFE
        r_tick = compute_r(direction, entry_price, tick.price, initial_sl_price, eps)
        if r_tick > r_mfe:
            r_mfe = r_tick

        # Проверяем трейлинг
        if direction.upper() == "LONG":
            if tick.price <= new_sl:
                r_trail = compute_r(direction, entry_price, new_sl, initial_sl_price, eps)
                exit_reason = "trailing_stop"
                trailing_exit = True
                break
        else:  # SHORT
            if tick.price >= new_sl:
                r_trail = compute_r(direction, entry_price, new_sl, initial_sl_price, eps)
                exit_reason = "trailing_stop"
                trailing_exit = True
                break

    # Если трейлинг не сработал
    if not trailing_exit:
        if use_mfe_exit:
            r_trail = r_mfe
            exit_reason = "mfe_exit"
        else:
            r_trail = r_orig
            exit_reason = "original_exit"

    giveback_r = max(r_mfe - r_trail, 0.0)
    missed_r = max(r_orig - r_trail, 0.0)

    fake_stopout = False
    if trailing_exit and r_mfe > r_trail + 0.1:  # 0.1R запас
        fake_stopout = True

    return SimResult(
        offset_mult=offset_mult,
        trade_id=trade.id,
        r_orig=r_orig,
        r_mfe=r_mfe,
        r_trail=r_trail,
        giveback_r=giveback_r,
        missed_r=missed_r,
        fake_stopout=fake_stopout,
        exit_reason=exit_reason,
    )


def aggregate_stats(results: List[SimResult]) -> OffsetStats:
    offset_mult = results[0].offset_mult if results else 0.0
    n = len(results)
    if n == 0:
        return OffsetStats(
            offset_mult=offset_mult,
            expectancy_r=0.0,
            avg_giveback_r=0.0,
            avg_missed_r=0.0,
            share_fake_stopout=0.0,
            count=0,
        )

    avg_expectancy = sum(r.r_trail for r in results) / n
    avg_giveback = sum(r.giveback_r for r in results) / n
    avg_missed = sum(r.missed_r for r in results) / n
    share_fake = sum(1 for r in results if r.fake_stopout) / n

    return OffsetStats(
        offset_mult=offset_mult,
        expectancy_r=avg_expectancy,
        avg_giveback_r=avg_giveback,
        avg_missed_r=avg_missed,
        share_fake_stopout=share_fake,
        count=n,
    )


def score_offset(stats: OffsetStats) -> float:
    """
    Скоринг offset_mult. Можно тюнить веса под вашу философию.
    """
    if stats.count == 0:
        return -1e9

    w_exp = 1.0    # expectancy_r
    w_gb = 0.4     # giveback_r (penalty)
    w_mis = 0.3    # missed_r (penalty)
    w_fake = 0.7   # fake_stopout (penalty)

    return (
        w_exp * stats.expectancy_r
        - w_gb * stats.avg_giveback_r
        - w_mis * stats.avg_missed_r
        - w_fake * stats.share_fake_stopout
    )


def run_calibration(
    dsn: str,
    source: str,
    symbol: str,
    offset_mult_list: List[float],
    limit: int,
    use_mfe_exit: bool,
) -> None:
    conn = psycopg2.connect(dsn)
    try:
        trades = fetch_trades(conn, source, symbol, limit)
        print(f"Loaded {len(trades)} trades with tp1_hit=TRUE and trailing_started=TRUE for {symbol} / {source}")

        if not trades:
            print("No trades found with the required criteria.")
            return

        # Для каждой сделки заранее грузим тики
        trade_ticks: Dict[int, List[Tick]] = {}
        for t in trades:
            # Добавляем буфер в 5 минут после exit_ts_ms
            end_ts_ms = t.exit_ts_ms + (5 * 60 * 1000)  # 5 minutes in ms
            ticks = fetch_ticks(conn, t.symbol, t.entry_ts_ms, end_ts_ms)
            trade_ticks[t.id] = ticks
            print(f"Trade {t.id}: loaded {len(ticks)} ticks from {t.entry_ts_ms} to {end_ts_ms}")

        stats_per_offset: List[OffsetStats] = []

        for offset_mult in offset_mult_list:
            results_for_offset: List[SimResult] = []
            for t in trades:
                ticks = trade_ticks.get(t.id, [])
                if not ticks:
                    continue
                res = simulate_trade_for_offset(
                    trade=t,
                    offset_mult=offset_mult,
                    ticks=ticks,
                    use_mfe_exit=use_mfe_exit,
                )
                results_for_offset.append(res)

            stats = aggregate_stats(results_for_offset)
            stats_per_offset.append(stats)

        # Выбор лучшего offset_mult
        best_stats: OffsetStats | None = None
        best_score = -1e9
        for s in stats_per_offset:
            sc = score_offset(s)
            if sc > best_score:
                best_score = sc
                best_stats = s

        print("\n=== Results per offset_mult ===")
        print("offset | exp_R | giveback_R | missed_R | fake% | count | score")
        print("-" * 65)

        for s in stats_per_offset:
            sc = score_offset(s)
            print(
                f"{s.offset_mult:6.2f} | "
                f"{s.expectancy_r:5.3f} | "
                f"{s.avg_giveback_r:10.3f} | "
                f"{s.avg_missed_r:9.3f} | "
                f"{s.share_fake_stopout*100:5.1f}% | "
                f"{s.count:5d} | "
                f"{sc:6.3f}"
            )

        print("\n=== Recommended offset_mult ===")
        if best_stats:
            print(
                f"symbol={symbol} source={source} "
                f"offset_mult={best_stats.offset_mult:.3f} "
                f"(score={best_score:.3f}, "
                f"expR={best_stats.expectancy_r:.3f}, "
                f"giveback={best_stats.avg_giveback_r:.3f}, "
                f"missed={best_stats.avg_missed_r:.3f}, "
                f"fake={best_stats.share_fake_stopout:.3f}, "
                f"count={best_stats.count})"
            )

            # Сохраняем в Redis
            try:
                import redis
                r = redis.from_url("redis://localhost:6379/0", decode_responses=True)
                key = f"symbol:{symbol}:spec"
                r.hset(key, "trailing_tp1_offset_atr", best_stats.offset_mult)
                print(f"✅ Saved to Redis: {key} -> trailing_tp1_offset_atr = {best_stats.offset_mult:.3f}")
            except Exception as e:
                print(f"⚠️ Failed to save to Redis: {e}")

        else:
            print("No stats computed")

    finally:
        conn.close()


def parse_offset_list(s: str) -> List[float]:
    return [float(x.strip()) for x in s.split(",") if x.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Full trailing TP1 offset ATR calibration with historical ticks"
    )
    parser.add_argument("--dsn", required=True, help="PostgreSQL DSN")
    parser.add_argument("--source", required=True, help="Strategy/source name")
    parser.add_argument("--symbol", required=True, help="Symbol, e.g. ETHUSDT")
    parser.add_argument(
        "--offsets",
        default="0.3,0.4,0.5,0.6,0.7",
        help="Comma-separated list of offset_mult, e.g. '0.3,0.4,0.5,0.6'",
    )
    parser.add_argument(
        "--limit",
        type=int,
        default=200,
        help="Number of trades with tp1_hit to analyze",
    )
    parser.add_argument(
        "--use-mfe-exit",
        action="store_true",
        help="If set, simulate exit at MFE when trailing not hit; otherwise use original exit",
    )

    args = parser.parse_args()
    offset_list = parse_offset_list(args.offsets)

    run_calibration(
        dsn=args.dsn,
        source=args.source,
        symbol=args.symbol,
        offset_mult_list=offset_list,
        limit=args.limit,
        use_mfe_exit=args.use_mfe_exit,
    )


if __name__ == "__main__":
    main()
