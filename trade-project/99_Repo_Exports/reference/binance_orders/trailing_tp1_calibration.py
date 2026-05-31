#!/usr/bin/env python
from __future__ import annotations

import argparse
import math
from dataclasses import dataclass
from datetime import timedelta
from typing import Any, Dict, List, Optional, Tuple

import psycopg2
import psycopg2.extras


@dataclass
class TradeRow:
    id: int
    symbol: str
    source: str
    side: str  # 'LONG' / 'SHORT'
    entry_ts: Any
    exit_ts: Any
    entry_price: float
    exit_price: float
    initial_sl_price: float
    tp1_hit: bool
    tp1_hit_ts: Any
    tp1_price: float
    atr_entry: float


@dataclass
class Candle:
    ts: Any
    high: float
    low: float


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
    exit_reason: str  # 'trailing_stop' / 'original_exit' / 'mfe_exit' / 'no_atr'


@dataclass
class OffsetStats:
    offset_mult: float
    expectancy_r: float
    avg_giveback_r: float
    avg_missed_r: float
    share_fake_stopout: float
    count: int


def _sign(side: str) -> int:
    return 1 if side.upper() == "LONG" else -1


def fetch_trades(
    conn,
    source: str,
    symbol: str,
    limit: int,
) -> List[TradeRow]:
    """
    Берём только сделки, где реально был TP1 из таблицы trades_closed.
    Используем one_r_money и lot для восстановления риска (ATR).
    """
    sql = """
    SELECT
        id,
        symbol,
        source,
        direction as side,
        entry_ts,
        exit_ts,
        entry_price,
        exit_price,
        one_r_money,
        lot,
        tp1_hit,
        entry_ts as tp1_hit_ts_proxy
    FROM trades_closed
    WHERE source = %(source)s
      AND symbol = %(symbol)s
      AND tp1_hit = TRUE
      AND exit_ts IS NOT NULL
      AND one_r_money > 0
    ORDER BY entry_ts DESC
    LIMIT %(limit)s;
    """
    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql, {"source": source, "symbol": symbol, "limit": limit})
        rows = cur.fetchall()
        print(f"DEBUG: Fetched {len(rows)} rows")

    trades: List[TradeRow] = []
    for r in rows:
        # Восстанавливаем данные
        # ATR (risk per unit) = one_r_money / lot
        # Initial SL = entry - sign * risk_per_unit
        
        entry_price = float(r["entry_price"])
        lot = float(r["lot"] or 0.0)
        one_r_money = float(r["one_r_money"] or 0.0)
        
        if lot <= 1e-9:
             continue
             
        risk_per_unit = one_r_money / lot
        atr_entry = risk_per_unit
        
        side = r["side"]
        sign = _sign(side)
        
        # SL = entry - sign * 1.0 * risk (предполагаем исходный стоп = 1R)
        initial_sl_price = entry_price - sign * risk_per_unit

        # Для tp1_hit_ts используем entry_ts (приблизительно), так как точного ts нет в таблице
        tp1_hit_ts = r["tp1_hit_ts_proxy"]

        trades.append(
            TradeRow(
                id=r["id"],
                symbol=r["symbol"],
                source=r["source"],
                side=r["side"],
                entry_ts=r["entry_ts"],
                exit_ts=r["exit_ts"],
                entry_price=entry_price,
                exit_price=float(r["exit_price"]),
                initial_sl_price=initial_sl_price,
                tp1_hit=bool(r["tp1_hit"]),
                tp1_hit_ts=tp1_hit_ts,
                tp1_price=0.0, # Не критично для симуляции трейлинга (стартуем от entry/current)
                atr_entry=atr_entry,
            )
        )
    return trades


def fetch_candles(
    conn,
    symbol: str,
    start_ts,
    end_ts,
) -> List[Candle]:
    """
    Загружаем минутные свечи по символу между start_ts и end_ts.
    Агрегируем из таблицы ticks, если нет готовых минутных данных.
    """
    # Сначала попробуем найти таблицу с минутными данными
    sql_minute = """
    SELECT ts, high, low
    FROM ohlcv_1m
    WHERE symbol = %(symbol)s
      AND ts >= %(start_ts)s
      AND ts <= %(end_ts)s
    ORDER BY ts ASC;
    """

    try:
        with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            cur.execute(sql_minute, {"symbol": symbol, "start_ts": start_ts, "end_ts": end_ts})
            rows = cur.fetchall()
            if rows:
                candles: List[Candle] = []
                for r in rows:
                    candles.append(Candle(ts=r["ts"], high=float(r["high"]), low=float(r["low"])))
                return candles
    except:
        conn.rollback()
        # Таблица ohlcv_1m может не существовать, пробуем ticks
        pass

    # Fallback: агрегируем из тиков
    sql_ticks = """
    SELECT
        date_trunc('minute', ts) as minute_ts,
        max(price) as high,
        min(price) as low
    FROM ticks
    WHERE symbol = %(symbol)s
      AND ts >= %(start_ts)s
      AND ts <= %(end_ts)s
      AND price > 0
    GROUP BY date_trunc('minute', ts)
    ORDER BY minute_ts ASC;
    """

    with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
        cur.execute(sql_ticks, {"symbol": symbol, "start_ts": start_ts, "end_ts": end_ts})
        rows = cur.fetchall()

    candles: List[Candle] = []
    for r in rows:
        candles.append(Candle(ts=r["minute_ts"], high=float(r["high"]), low=float(r["low"])))
    return candles


def compute_r(
    side: str,
    entry_price: float,
    price: float,
    initial_sl_price: float,
    eps: float = 1e-8,
) -> float:
    risk_per_unit = max(abs(entry_price - initial_sl_price), eps)
    sign = _sign(side)
    return sign * (price - entry_price) / risk_per_unit


def simulate_trade_for_offset(
    trade: TradeRow,
    offset_mult: float,
    candles: List[Candle],
    use_mfe_exit: bool = False,
    eps: float = 1e-8,
) -> SimResult:
    """
    Полноценная симуляция трейлинга:
    - трейлинг активен только после TP1
    - если выбивает по new_sl раньше, чем достигается exit/MFE → трейлинг-выход
    - иначе → сидим до исходного exit или до MFE (режим use_mfe_exit)
    """
    side = trade.side
    entry_price = trade.entry_price
    exit_price_orig = trade.exit_price
    initial_sl_price = trade.initial_sl_price

    # Если initial_sl_price не задан, рассчитаем приблизительно
    if initial_sl_price <= eps:
        # Используем one_r_money из trades_closed, если доступно
        # Для простоты: SL на расстоянии 1 ATR от entry
        atr = float(trade.atr_entry or 0.0)
        if side.upper() == "LONG":
            initial_sl_price = entry_price - atr
        else:
            initial_sl_price = entry_price + atr

    r_orig = compute_r(side, entry_price, exit_price_orig, initial_sl_price, eps)

    atr = float(trade.atr_entry or 0.0)
    offset = max(0.0, atr * float(offset_mult))

    if offset <= 0.0 or atr <= 0.0:
        # нет ATR — считаем, что трейлинг не даёт эффекта
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

    if side.upper() == "LONG":
        new_sl = entry_price + offset
    else:
        new_sl = entry_price - offset

    # обходим путь цены после TP1
    r_mfe = r_orig
    r_trail = r_orig
    exit_reason = "original_exit"
    trailing_exit = False

    for c in candles:
        if side.upper() == "LONG":
            r_candle_high = compute_r(side, entry_price, c.high, initial_sl_price, eps)
            if r_candle_high > r_mfe:
                r_mfe = r_candle_high

            if c.low <= new_sl:
                r_trail = compute_r(side, entry_price, new_sl, initial_sl_price, eps)
                exit_reason = "trailing_stop"
                trailing_exit = True
                break
        else:
            r_candle_low = compute_r(side, entry_price, c.low, initial_sl_price, eps)
            if r_candle_low > r_mfe:
                r_mfe = r_candle_low

            if c.high >= new_sl:
                r_trail = compute_r(side, entry_price, new_sl, initial_sl_price, eps)
                exit_reason = "trailing_stop"
                trailing_exit = True
                break

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
    if trailing_exit and r_mfe > r_trail + 0.1:  # 0.1R как «значимое» улучшение
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
    Функция качества offset_mult.
    Весами можно играть позже.
    """
    if stats.count == 0:
        return -1e9

    w_exp = 1.0
    w_gb = 0.4
    w_mis = 0.3
    w_fake = 0.7

    return (
        w_exp * stats.expectancy_r
        - w_gb * stats.avg_giveback_r
        - w_mis * stats.avg_missed_r
        - w_fake * stats.share_fake_stopout
    )


def calibrate_trailing_offset(
    conn,
    source: str,
    symbol: str,
    offset_mult_list: List[float],
    limit: int = 200,
    use_mfe_exit: bool = False,
) -> Tuple[Optional[OffsetStats], List[OffsetStats]]:
    """
    Главная точка: калибровка offset_mult по историческим сделкам.
    Возвращает:
      best_stats  — лучший offset_mult по score_offset
      all_stats   — список метрик по всем offset_mult
    """
    trades = fetch_trades(conn, source, symbol, limit)
    if not trades:
        return None, []

    trade_paths: Dict[int, List[Candle]] = {}
    for t in trades:
        # небольшой хвост после реального выхода, чтобы увидеть продолжение
        # Используем tp1_hit_ts, если доступен, иначе entry_ts + приблизительное время достижения TP1
        start_ts = t.tp1_hit_ts if t.tp1_hit_ts else t.entry_ts
        end_ts = t.exit_ts + timedelta(minutes=5)
        candles = fetch_candles(conn, t.symbol, start_ts, end_ts)
        trade_paths[t.id] = candles

    stats_per_offset: List[OffsetStats] = []

    for offset_mult in offset_mult_list:
        results_for_offset: List[SimResult] = []
        for t in trades:
            path = trade_paths.get(t.id, [])
            if not path:
                continue
            res = simulate_trade_for_offset(
                trade=t,
                offset_mult=offset_mult,
                candles=path,
                use_mfe_exit=use_mfe_exit,
            )
            results_for_offset.append(res)

        stats = aggregate_stats(results_for_offset)
        stats_per_offset.append(stats)

    best_stats: Optional[OffsetStats] = None
    best_score = -1e9
    for s in stats_per_offset:
        sc = score_offset(s)
        if sc > best_score:
            best_score = sc
            best_stats = s

    return best_stats, stats_per_offset


# ----- Walk-Forward variant -----


def calibrate_trailing_offset_wf(
    conn,
    source: str,
    symbol: str,
    offset_mult_list: List[float],
    limit: int = 300,
    use_mfe_exit: bool = False,
    min_train_trades: int = 100,
    test_trades: int = 30,
    step_trades: int = 20,
    stability_threshold: float = 0.5,
    min_oos_pf: float = 1.0,
):
    """
    Walk-Forward variant of calibrate_trailing_offset.

    Instead of picking the best offset_mult on the entire sample (in-sample),
    this uses expanding-window walk-forward validation to select a robust
    offset_mult with out-of-sample stability guarantees.

    Returns:
        WalkForwardResult from calibrate.walk_forward_calibrator
    """
    from calibrate.walk_forward_calibrator import (
        WalkForwardCalibrator,
        OOSMetrics,
        WalkForwardResult,
    )

    trades = fetch_trades(conn, source, symbol, limit)
    if not trades:
        return WalkForwardResult(
            symbol=symbol, robust_param=0.0, stability_score=999.0,
            deploy=False, n_folds=0, n_stable_folds=0,
        )

    # Sort trades chronologically (oldest first) — fetch returns DESC order
    trades = list(reversed(trades))

    # Pre-fetch candle paths for all trades (expensive I/O, do once)
    trade_paths: Dict[int, List[Candle]] = {}
    for t in trades:
        start_ts = t.tp1_hit_ts if t.tp1_hit_ts else t.entry_ts
        end_ts = t.exit_ts + timedelta(minutes=5)
        candles = fetch_candles(conn, t.symbol, start_ts, end_ts)
        trade_paths[t.id] = candles

    def _objective(trade_slice, param: float) -> float:
        """Score an offset_mult on a subset of trades (in-sample)."""
        results: List[SimResult] = []
        for t in trade_slice:
            path = trade_paths.get(t.id, [])
            if not path:
                continue
            res = simulate_trade_for_offset(
                trade=t, offset_mult=param,
                candles=path, use_mfe_exit=use_mfe_exit,
            )
            results.append(res)
        if not results:
            return -1e9
        stats = aggregate_stats(results)
        return score_offset(stats)

    def _evaluate(trade_slice, param: float) -> OOSMetrics:
        """Evaluate an offset_mult on OOS trades."""
        results: List[SimResult] = []
        for t in trade_slice:
            path = trade_paths.get(t.id, [])
            if not path:
                continue
            res = simulate_trade_for_offset(
                trade=t, offset_mult=param,
                candles=path, use_mfe_exit=use_mfe_exit,
            )
            results.append(res)

        if not results:
            return OOSMetrics()

        stats = aggregate_stats(results)
        sc = score_offset(stats)

        n = len(results)
        wins = sum(1 for r in results if r.r_trail > 0)
        total_pos = sum(r.r_trail for r in results if r.r_trail > 0)
        total_neg = abs(sum(r.r_trail for r in results if r.r_trail <= 0))

        win_rate = wins / n if n else 0.0
        pf = total_pos / total_neg if total_neg > 1e-9 else (
            10.0 if total_pos > 0 else 0.0
        )

        # Sharpe of R-returns
        r_returns = [r.r_trail for r in results]
        mu = sum(r_returns) / n
        var = sum((x - mu) ** 2 for x in r_returns) / max(n - 1, 1)
        std = var ** 0.5
        sharpe = mu / std if std > 1e-9 else 0.0

        return OOSMetrics(
            sharpe=sharpe,
            win_rate=win_rate,
            profit_factor=pf,
            expectancy_r=stats.expectancy_r,
            n_trades=n,
            score=sc,
        )

    wfc = WalkForwardCalibrator(
        min_train_trades=min_train_trades,
        test_trades=test_trades,
        step_trades=step_trades,
        stability_threshold=stability_threshold,
        min_oos_pf=min_oos_pf,
    )

    return wfc.run(
        trades=trades,
        param_candidates=offset_mult_list,
        objective_fn=_objective,
        evaluate_fn=_evaluate,
        symbol=symbol,
    )


# ----- CLI-обёртка для ручного запуска -----


def parse_offset_list(s: str) -> List[float]:
    return [float(x) for x in s.split(",") if x.strip()]


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Full trailing TP1 offset ATR calibration"
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

    conn = psycopg2.connect(args.dsn)
    try:
        best_stats, all_stats = calibrate_trailing_offset(
            conn=conn,
            source=args.source,
            symbol=args.symbol,
            offset_mult_list=offset_list,
            limit=args.limit,
            use_mfe_exit=args.use_mfe_exit,
        )
    finally:
        conn.close()

    print("=== Results per offset_mult ===")
    for s in all_stats:
        sc = score_offset(s)
        print(
            f"offset={s.offset_mult:.2f} "
            f"count={s.count} "
            f"expR={s.expectancy_r:.3f} "
            f"giveback={s.avg_giveback_r:.3f} "
            f"missed={s.avg_missed_r:.3f} "
            f"fake={s.share_fake_stopout:.3f} "
            f"score={sc:.3f}"
        )

    print("\n=== Recommended offset_mult ===")
    if best_stats is None:
        print("No trades found")
    else:
        print(
            f"offset={best_stats.offset_mult:.3f} "
            f"expR={best_stats.expectancy_r:.3f} "
            f"giveback={best_stats.avg_giveback_r:.3f} "
            f"missed={best_stats.avg_missed_r:.3f} "
            f"fake={best_stats.share_fake_stopout:.3f} "
            f"count={best_stats.count}"
        )


if __name__ == "__main__":
    main()
