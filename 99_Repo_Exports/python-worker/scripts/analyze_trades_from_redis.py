#!/usr/bin/env python3
from __future__ import annotations
from core.redis_keys import RedisStreams as RS

"""
Быстрый анализ baseline vs managed + трейлинг по сделкам, прочитанным из Redis Stream.

Пример:
    python analyze_trades_from_redis.py \
        --redis-url redis://localhost:6379/0 \
        --stream trades:closed \
        --symbol ETHUSDT \
        --source CryptoOrderFlow \
        --limit 1000
"""


import argparse
import math
import os
from dataclasses import dataclass

import redis


@dataclass
class Trade:
    source: str
    symbol: str
    exit_ts_ms: int
    pnl_net: float
    pnl_if_fixed_exit: float
    one_r_money: float
    giveback: float
    missed_profit: float
    mfe_pnl: float
    mae_pnl: float
    trailing_started: bool
    trailing_active: bool
    close_reason: str
    close_reason_raw: str
    entry_tag: str


def _to_float(value, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except Exception:
        return default


def _to_bool(value) -> bool:
    if value is None:
        return False
    normalized = str(value).strip().lower()
    return normalized in ("1", "true", "t", "yes", "y")


def load_trades_from_redis(
    redis_url: str,
    stream: str,
    limit: int,
    source: str | None = None,
    symbol: str | None = None,
) -> list[Trade]:
    """
    Читает последние `limit` записей из stream, фильтрует по source / symbol.
    Использует XREVRANGE, чтобы взять последние события.
    """
    client = redis.from_url(redis_url, decode_responses=True)

    entries = client.xrevrange(stream, max="+", min="-", count=limit * 3)
    trades: list[Trade] = []

    for _stream_id, fields in entries:
        s_source = (fields.get("source") or "")
        s_symbol = (fields.get("symbol") or "")

        if source and s_source != source:
            continue
        if symbol and s_symbol != symbol:
            continue

        trade = Trade(
            source=s_source,
            symbol=s_symbol,
            exit_ts_ms=int(_to_float(fields.get("exit_ts_ms"), 0)),
            pnl_net=_to_float(fields.get("pnl_net"), 0.0),
            pnl_if_fixed_exit=_to_float(fields.get("pnl_if_fixed_exit"), 0.0),
            one_r_money=_to_float(fields.get("one_r_money"), 0.0),
            giveback=_to_float(fields.get("giveback"), 0.0),
            missed_profit=_to_float(fields.get("missed_profit"), 0.0),
            mfe_pnl=_to_float(fields.get("mfe_pnl"), 0.0),
            mae_pnl=_to_float(fields.get("mae_pnl"), 0.0),
            trailing_started=_to_bool(fields.get("trailing_started")),
            trailing_active=_to_bool(fields.get("trailing_active")),
            close_reason=(fields.get("close_reason") or ""),
            close_reason_raw=(fields.get("close_reason_raw") or ""),
            entry_tag=(fields.get("entry_tag") or ""),
        )
        trades.append(trade)

        if len(trades) >= limit:
            break

    trades.sort(key=lambda x: x.exit_ts_ms)
    return trades


def _split_r(
    trades: list[Trade],
) -> tuple[list[float], list[float], list[float], list[float]]:
    """
    Возвращает:
        r_managed, r_baseline, r_wins, r_losses
    где:
        r_managed = pnl_net / one_r_money
        r_baseline = pnl_if_fixed_exit / one_r_money
    """
    r_managed: list[float] = []
    r_baseline: list[float] = []
    r_wins: list[float] = []
    r_losses: list[float] = []

    for trade in trades:
        if trade.one_r_money <= 1e-12:
            continue
        r_m = trade.pnl_net / trade.one_r_money
        r_b = trade.pnl_if_fixed_exit / trade.one_r_money
        r_managed.append(r_m)
        r_baseline.append(r_b)
        if r_m > 0:
            r_wins.append(r_m)
        elif r_m < 0:
            r_losses.append(r_m)

    return r_managed, r_baseline, r_wins, r_losses


def _mean(values: list[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def _std(values: list[float]) -> float:
    if len(values) < 2:
        return 0.0
    mean_val = _mean(values)
    variance = sum((value - mean_val) ** 2 for value in values) / (len(values) - 1)
    return math.sqrt(variance)


def compute_global_metrics(trades: list[Trade]) -> dict[str, float]:
    total = len(trades)
    if total == 0:
        return {}

    pnl = [trade.pnl_net for trade in trades]
    pnl_fixed = [trade.pnl_if_fixed_exit for trade in trades]

    wins = [trade for trade in trades if trade.pnl_net > 0]
    losses = [trade for trade in trades if trade.pnl_net < 0]

    wr_managed = len(wins) / total
    wr_baseline = sum(1 for trade in trades if trade.pnl_if_fixed_exit > 0) / total

    r_m, r_b, r_wins, r_losses = _split_r(trades)

    expectancy_r = _mean(r_m)
    expectancy_fixed_r = _mean(r_b)

    avg_win_r = _mean([value for value in r_m if value > 0])
    avg_loss_r = _mean([value for value in r_m if value < 0])
    payoff_r = (avg_win_r / abs(avg_loss_r)) if avg_loss_r < 0 else 0.0

    avg_win_usd = _mean([trade.pnl_net for trade in wins])
    avg_loss_usd = _mean([trade.pnl_net for trade in losses])
    payoff_usd = (avg_win_usd / abs(avg_loss_usd)) if avg_loss_usd < 0 else 0.0

    payoff_fixed_r = 0.0
    payoff_fixed_usd = 0.0
    if pnl_fixed:
        fixed_wins = [trade for trade in trades if trade.pnl_if_fixed_exit > 0]
        fixed_losses = [trade for trade in trades if trade.pnl_if_fixed_exit < 0]
        r_fixed = [
            trade.pnl_if_fixed_exit / trade.one_r_money
            for trade in trades
            if trade.one_r_money > 1e-12
        ]
        expectancy_fixed_r = _mean(r_fixed)
        fw_r = _mean(
            [
                trade.pnl_if_fixed_exit / trade.one_r_money
                for trade in fixed_wins
                if trade.one_r_money > 1e-12
            ]
        )
        fl_r = _mean(
            [
                trade.pnl_if_fixed_exit / trade.one_r_money
                for trade in fixed_losses
                if trade.one_r_money > 1e-12
            ]
        )
        payoff_fixed_r = (fw_r / abs(fl_r)) if fl_r < 0 else 0.0

        fw_usd = _mean([trade.pnl_if_fixed_exit for trade in fixed_wins])
        fl_usd = _mean([trade.pnl_if_fixed_exit for trade in fixed_losses])
        payoff_fixed_usd = (fw_usd / abs(fl_usd)) if fl_usd < 0 else 0.0

    std_r = _std(r_m)
    sharpe = (expectancy_r / std_r) if std_r > 1e-9 else 0.0

    equity = 0.0
    peak = 0.0
    mdd = 0.0
    for trade in trades:
        equity += trade.pnl_net
        if equity > peak:
            peak = equity
        drawdown = peak - equity
        if drawdown > mdd:
            mdd = drawdown

    delta_expectancy_r = expectancy_r - expectancy_fixed_r

    trailing_trades = [
        trade for trade in trades if trade.trailing_started or trade.trailing_active
    ]
    trailing_total = len(trailing_trades)
    trailing_wr = (
        sum(1 for trade in trailing_trades if trade.pnl_net > 0) / trailing_total
        if trailing_total > 0
        else 0.0
    )

    r_m_tr, r_b_tr, _r_wins_tr, _r_losses_tr = _split_r(trailing_trades)
    trailing_expectancy_r = _mean(r_m_tr)
    trailing_expectancy_fixed_r = _mean(r_b_tr)
    trailing_delta_expectancy_r = trailing_expectancy_r - trailing_expectancy_fixed_r

    return {
        "n": total,
        "pnl_net_sum": sum(pnl),
        "pnl_net_avg": _mean(pnl),
        "wr_managed": wr_managed,
        "wr_baseline": wr_baseline,
        "expectancy_r": expectancy_r,
        "expectancy_fixed_r": expectancy_fixed_r,
        "delta_expectancy_r": delta_expectancy_r,
        "payoff_r": payoff_r,
        "payoff_usd": payoff_usd,
        "payoff_fixed_r": payoff_fixed_r,
        "payoff_fixed_usd": payoff_fixed_usd,
        "std_r": std_r,
        "sharpe": sharpe,
        "mdd_usd": mdd,
        "trailing_share": trailing_total / total if total > 0 else 0.0,
        "trailing_wr": trailing_wr,
        "trailing_expectancy_r": trailing_expectancy_r,
        "trailing_expectancy_fixed_r": trailing_expectancy_fixed_r,
        "trailing_delta_expectancy_r": trailing_delta_expectancy_r,
    }


def print_report(trades: list[Trade], metrics: dict[str, float], source: str, symbol: str) -> None:
    if not trades:
        print("Нет сделок по заданному фильтру.")
        return

    total = metrics["n"]

    print("========================================")
    print(f"📊 Redis-отчет: {source} / {symbol}")
    print(f"Сделок в выборке: {total}")
    print("----------------------------------------")
    print("📈 Managed (фактические выходы)")
    print(f"P/L net: {metrics['pnl_net_sum']:+.2f} | Avg: {metrics['pnl_net_avg']:+.3f}")
    print(f"WR(managed): {metrics['wr_managed']*100:.1f}%")
    print(f"Expectancy R: {metrics['expectancy_r']:+.3f}")
    print(f"Payoff(R): {metrics['payoff_r']:.3f} | Payoff(USD): {metrics['payoff_usd']:.3f}")
    print(f"Sharpe*(по R): {metrics['sharpe']:.2f} | Std(R): {metrics['std_r']:.3f}")
    print(f"MDD (USD по pnl_net): {metrics['mdd_usd']:.2f}")
    print("----------------------------------------")
    print("📉 Baseline (pnl_if_fixed_exit)")
    print(f"WR(baseline): {metrics['wr_baseline']*100:.1f}%")
    print(f"Expectancy_fixed R: {metrics['expectancy_fixed_r']:+.3f}")
    print(f"Payoff_fixed(R): {metrics['payoff_fixed_r']:.3f} | Payoff_fixed(USD): {metrics['payoff_fixed_usd']:.3f}")
    print(f"ΔExpectancy R (managed - baseline): {metrics['delta_expectancy_r']:+.3f}")
    print("----------------------------------------")
    print("🧷 Трейлинг")
    print(f"trailing_share: {metrics['trailing_share']*100:.1f}% (доля сделок с трейлингом)")
    print(f"trailing_WR: {metrics['trailing_wr']*100:.1f}%")
    print(f"trailing_Exp_R(managed): {metrics['trailing_expectancy_r']:+.3f}")
    print(f"trailing_Exp_R(baseline): {metrics['trailing_expectancy_fixed_r']:+.3f}")
    print(f"trailing_ΔExp_R: {metrics['trailing_delta_expectancy_r']:+.3f}")
    print("========================================")


def main() -> None:
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--stream", default=os.getenv("TRADES_CLOSED_STREAM_NAME", RS.TRADES_CLOSED))
    parser.add_argument("--source", required=True, help="Например, CryptoOrderFlow")
    parser.add_argument("--symbol", required=True, help="Например, ETHUSDT")
    parser.add_argument("--limit", type=int, default=1000, help="Сколько последних сделок читать")
    args = parser.parse_args()

    trades = load_trades_from_redis(
        redis_url=args.redis_url,
        stream=args.stream,
        limit=args.limit,
        source=args.source,
        symbol=args.symbol,
    )

    metrics = compute_global_metrics(trades)
    print_report(trades, metrics, source=args.source, symbol=args.symbol)


if __name__ == "__main__":
    main()

