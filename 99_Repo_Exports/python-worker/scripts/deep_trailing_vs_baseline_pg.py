#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
"""
Deep trailing vs baseline analyzer for trades_closed in Redis Stream.

Ожидается stream (по умолчанию trades:closed) с полями минимум:
  source, symbol, entry_tag, pnl_net, pnl_if_fixed_exit, one_r_money,
  mfe_pnl, mae_pnl, giveback, missed_profit,
  trailing_started, trailing_active,
  close_reason, close_reason_raw, close_reason_detail,
  notional_usd, exit_ts_ms

Если имена другие — поправьте маппинг в load_trades.
"""

from utils.time_utils import get_ny_time_millis

import argparse
import statistics as stats
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

import os
import redis


EPS = 1e-9


@dataclass
class TradeRow:
    symbol: str
    source: str
    entry_tag: str

    pnl_net: float
    pnl_fixed: float
    one_r: float

    mfe_pnl: float
    mae_pnl: float
    giveback: float
    missed_profit: float

    trailing_started: bool
    trailing_active: bool
    close_reason: str
    close_reason_raw: str
    close_reason_detail: str

    notional_usd: float
    exit_ts_ms: int

    @property
    def r_managed(self) -> float:
        if abs(self.one_r) < EPS:
            return 0.0
        return self.pnl_net / self.one_r

    @property
    def r_baseline(self) -> float:
        if abs(self.one_r) < EPS:
            return 0.0
        return self.pnl_fixed / self.one_r

    @property
    def mfe_r(self) -> float:
        if abs(self.one_r) < EPS:
            return 0.0
        return self.mfe_pnl / self.one_r

    @property
    def mae_r(self) -> float:
        if abs(self.one_r) < EPS:
            return 0.0
        return self.mae_pnl / self.one_r

    @property
    def giveback_r(self) -> float:
        if abs(self.one_r) < EPS:
            return 0.0
        return self.giveback / self.one_r

    @property
    def missed_r(self) -> float:
        if abs(self.one_r) < EPS:
            return 0.0
        return self.missed_profit / self.one_r

    @property
    def giveback_ratio(self) -> float:
        if self.mfe_pnl > EPS:
            return max(0.0, self.giveback) / self.mfe_pnl
        return 0.0

    @property
    def missed_ratio(self) -> float:
        if self.mfe_pnl > EPS:
            return max(0.0, self.missed_profit) / self.mfe_pnl
        return 0.0

    @property
    def is_trailing_trade(self) -> bool:
        return bool(self.trailing_started or self.trailing_active)

    @property
    def is_trailing_close(self) -> bool:
        return self.close_reason_detail in ("TRAILING_PROFIT", "TRAILING_STOP")

    @property
    def is_win(self) -> bool:
        return self.pnl_net > EPS

    @property
    def is_loss(self) -> bool:
        return self.pnl_net < -EPS

    @property
    def is_be(self) -> bool:
        return not self.is_win and not self.is_loss

    @property
    def is_win_fixed(self) -> bool:
        return self.pnl_fixed > EPS

    @property
    def is_loss_fixed(self) -> bool:
        return self.pnl_fixed < -EPS

    @property
    def is_be_fixed(self) -> bool:
        return not self.is_win_fixed and not self.is_loss_fixed


def mean(values: List[float]) -> float:
    return sum(values) / len(values) if values else 0.0


def stddev(values: List[float]) -> float:
    if len(values) < 2:
        return 0.0
    return float(stats.pstdev(values))


def downside_std(values: List[float]) -> float:
    negatives = [value for value in values if value < 0]
    if len(negatives) < 2:
        return 0.0
    return float(stats.pstdev(negatives))


def max_drawdown(equity: List[float]) -> float:
    if not equity:
        return 0.0
    peak = equity[0]
    mdd = 0.0
    for value in equity:
        peak = max(peak, value)
        drawdown = peak - value
        if drawdown > mdd:
            mdd = drawdown
    return mdd


def compute_equity_curve(trades: List[TradeRow], use_net: bool = True) -> List[float]:
    equity = 0.0
    curve: List[float] = []
    for trade in sorted(trades, key=lambda x: x.exit_ts_ms):
        equity += trade.pnl_net if use_net else trade.pnl_fixed
        curve.append(equity)
    return curve


def _to_float(value: Any, default: float = 0.0) -> float:
    if value is None:
        return default
    try:
        return float(value)
    except Exception:
        return default


def _to_int(value: Any, default: int = 0) -> int:
    if value is None:
        return default
    try:
        return int(float(value))
    except Exception:
        return default


def _to_bool(value: Any) -> bool:
    if value is None:
        return False
    s = str(value).strip().lower()
    return s in ("1", "true", "t", "yes", "y")


def load_trades(
    redis_client,
    stream: str,
    source: str,
    symbol: str,
    limit: int = 200,
    since_days: Optional[int] = None,
) -> List[TradeRow]:
    threshold_ms: Optional[int] = None
    if since_days is not None and since_days > 0:
        threshold_ms = int(get_ny_time_millis() - since_days * 86400 * 1000)

    # Берем с запасом, чтобы отфильтровать по source/symbol.
    entries = redis_client.xrevrange(stream, max="+", min="-", count=limit * 4)
    trades: List[TradeRow] = []

    for _stream_id, fields in entries:
        s_source = str(fields.get("source") or "")
        s_symbol = str(fields.get("symbol") or "")
        if source and s_source != source:
            continue
        if symbol and s_symbol != symbol:
            continue

        exit_ts_ms = _to_int(fields.get("exit_ts_ms"))
        if threshold_ms is not None and exit_ts_ms < threshold_ms:
            continue

        trade = TradeRow(
            source=s_source,
            symbol=s_symbol,
            entry_tag=str(fields.get("entry_tag") or ""),
            pnl_net=_to_float(fields.get("pnl_net")),
            pnl_fixed=_to_float(fields.get("pnl_if_fixed_exit")),
            one_r=_to_float(fields.get("one_r_money")),
            mfe_pnl=_to_float(fields.get("mfe_pnl")),
            mae_pnl=_to_float(fields.get("mae_pnl")),
            giveback=_to_float(fields.get("giveback")),
            missed_profit=_to_float(fields.get("missed_profit")),
            trailing_started=_to_bool(fields.get("trailing_started")),
            trailing_active=_to_bool(fields.get("trailing_active")),
            close_reason=str(fields.get("close_reason") or ""),
            close_reason_raw=str(fields.get("close_reason_raw") or ""),
            close_reason_detail=str(fields.get("close_reason_detail") or ""),
            notional_usd=_to_float(fields.get("notional_usd")),
            exit_ts_ms=exit_ts_ms,
        )
        trades.append(trade)

        if len(trades) >= limit:
            break

    trades.sort(key=lambda t: t.exit_ts_ms)
    return trades


@dataclass
class TagStats:
    tag: str
    trades: List[TradeRow]

    def finalize(self) -> Dict[str, Any]:
        total = len(self.trades)
        if total == 0:
            return {"n": 0}

        r_m = [trade.r_managed for trade in self.trades]
        r_b = [trade.r_baseline for trade in self.trades]
        diffs_r = [trade.r_managed - trade.r_baseline for trade in self.trades]
        diffs_usd = [trade.pnl_net - trade.pnl_fixed for trade in self.trades]

        wins = sum(1 for trade in self.trades if trade.is_win)
        losses = sum(1 for trade in self.trades if trade.is_loss)
        be = total - wins - losses

        wins_fixed = sum(1 for trade in self.trades if trade.is_win_fixed)
        losses_fixed = sum(1 for trade in self.trades if trade.is_loss_fixed)
        be_fixed = total - wins_fixed - losses_fixed

        trailing_trades = [trade for trade in self.trades if trade.is_trailing_trade]
        trailing_closes = [trade for trade in self.trades if trade.is_trailing_close]

        r_m_tr = [trade.r_managed for trade in trailing_trades]
        r_b_tr = [trade.r_baseline for trade in trailing_trades]
        diffs_r_tr = [trade.r_managed - trade.r_baseline for trade in trailing_trades]

        giveback_vals_usd = [max(0.0, trade.giveback) for trade in self.trades]
        giveback_vals_r = [max(0.0, trade.giveback_r) for trade in self.trades]
        giveback_ratios = [
            trade.giveback_ratio for trade in self.trades if trade.mfe_pnl > EPS
        ]
        giveback_count = sum(1 for trade in self.trades if trade.giveback > EPS)

        missed_vals_usd = [max(0.0, trade.missed_profit) for trade in self.trades]
        missed_vals_r = [max(0.0, trade.missed_r) for trade in self.trades]
        missed_ratios = [
            trade.missed_ratio for trade in self.trades if trade.mfe_pnl > EPS
        ]
        missed_count = sum(1 for trade in self.trades if trade.missed_profit > EPS)

        mfe_vals_r = [trade.mfe_r for trade in self.trades]
        mae_vals_r = [trade.mae_r for trade in self.trades]

        trailing_total = len(trailing_trades)
        trailing_close_total = len(trailing_closes)

        trailing_wins = sum(1 for trade in trailing_closes if trade.is_win)
        trailing_losses = sum(1 for trade in trailing_closes if trade.is_loss)

        return {
            "tag": self.tag,
            "n": total,
            "wins": wins,
            "losses": losses,
            "be": be,
            "wr": wins / total if total > 0 else 0.0,
            "wins_fixed": wins_fixed,
            "losses_fixed": losses_fixed,
            "be_fixed": be_fixed,
            "wr_fixed": wins_fixed / total if total > 0 else 0.0,
            "expectancy_managed_r": mean(r_m),
            "expectancy_baseline_r": mean(r_b),
            "delta_expectancy_r": mean(diffs_r),
            "avg_diff_usd": mean(diffs_usd),
            "giveback_avg_usd": mean(giveback_vals_usd),
            "giveback_avg_r": mean(giveback_vals_r),
            "giveback_avg_ratio": mean(giveback_ratios),
            "giveback_share": giveback_count / total if total > 0 else 0.0,
            "missed_avg_usd": mean(missed_vals_usd),
            "missed_avg_r": mean(missed_vals_r),
            "missed_avg_ratio": mean(missed_ratios),
            "missed_share": missed_count / total if total > 0 else 0.0,
            "mfe_avg_r": mean(mfe_vals_r),
            "mae_avg_r": mean(mae_vals_r),
            "trailing_share": trailing_total / total if total > 0 else 0.0,
            "trailing_close_share": trailing_close_total / total if total > 0 else 0.0,
            "trailing_wr": trailing_wins / trailing_close_total
            if trailing_close_total > 0
            else 0.0,
            "trailing_expectancy_r": mean(r_m_tr) if trailing_total > 0 else 0.0,
            "trailing_expectancy_fixed_r": mean(r_b_tr) if trailing_total > 0 else 0.0,
            "trailing_delta_expectancy_r": mean(diffs_r_tr)
            if trailing_total > 0
            else 0.0,
        }


def analyze_global(trades: List[TradeRow]) -> Dict[str, Any]:
    stats_tag = TagStats(tag="__ALL__", trades=trades).finalize()

    r_m = [trade.r_managed for trade in trades]
    mu = mean(r_m)
    sigma = stddev(r_m)
    downside_sigma = downside_std(r_m)
    sharpe = mu / sigma if sigma > EPS else 0.0
    sortino = mu / downside_sigma if downside_sigma > EPS else 0.0

    eq_net = compute_equity_curve(trades, use_net=True)
    eq_baseline = compute_equity_curve(trades, use_net=False)
    mdd_net = max_drawdown(eq_net)
    mdd_baseline = max_drawdown(eq_baseline)

    out: Dict[str, Any] = dict(stats_tag)
    out.update(
        {
            "sharpe_r": sharpe,
            "sortino_r": sortino,
            "mdd_net_usd": mdd_net,
            "mdd_baseline_usd": mdd_baseline,
        }
    )
    return out


def analyze_by_tag(trades: List[TradeRow], min_trades: int = 10) -> List[Dict[str, Any]]:
    by_tag: Dict[str, List[TradeRow]] = {}
    for trade in trades:
        tag = trade.entry_tag or "__EMPTY__"
        by_tag.setdefault(tag, []).append(trade)

    stats_list: List[Dict[str, Any]] = []
    for tag, tagged_trades in by_tag.items():
        if len(tagged_trades) < min_trades:
            continue
        stats_list.append(TagStats(tag=tag, trades=tagged_trades).finalize())

    stats_list.sort(key=lambda x: x["n"], reverse=True)
    return stats_list


def print_global_report(symbol: str, source: str, stats_glob: Dict[str, Any]) -> None:
    print("========================================")
    print(f"Global stats: source={source}, symbol={symbol}")
    print(f"Сделок: {stats_glob['n']}")
    print(
        f"W/L/BE(managed): {stats_glob['wins']}/{stats_glob['losses']}/{stats_glob['be']} | "
        f"WR(managed): {stats_glob['wr']*100:.1f}%"
    )
    print(
        f"W/L/BE(baseline): {stats_glob['wins_fixed']}/{stats_glob['losses_fixed']}/{stats_glob['be_fixed']} | "
        f"WR(baseline): {stats_glob['wr_fixed']*100:.1f}%"
    )
    print(
        f"Expectancy R: managed={stats_glob['expectancy_managed_r']:+.3f}, "
        f"baseline={stats_glob['expectancy_baseline_r']:+.3f}, "
        f"ΔExp_R={stats_glob['delta_expectancy_r']:+.3f}"
    )
    print(f"Avg diff (pnl_net - pnl_fixed), USD: {stats_glob['avg_diff_usd']:+.3f}")
    print(
        f"Sharpe(R)={stats_glob['sharpe_r']:+.2f}, "
        f"Sortino(R)={stats_glob['sortino_r']:+.2f}"
    )
    print(
        f"MDD net / baseline (USD): {stats_glob['mdd_net_usd']:.2f} / {stats_glob['mdd_baseline_usd']:.2f}"
    )
    print()
    print("Giveback / Missed / Excursions (в среднем):")
    print(
        f"  Giveback: {stats_glob['giveback_avg_usd']:+.3f} USD, "
        f"{stats_glob['giveback_avg_r']:+.3f} R, "
        f"ratio={stats_glob['giveback_avg_ratio']*100:.1f}% "
        f"(share={stats_glob['giveback_share']*100:.1f}%)"
    )
    print(
        f"  Missed : {stats_glob['missed_avg_usd']:+.3f} USD, "
        f"{stats_glob['missed_avg_r']:+.3f} R, "
        f"ratio={stats_glob['missed_avg_ratio']*100:.1f}% "
        f"(share={stats_glob['missed_share']*100:.1f}%)"
    )
    print(
        f"  MFE/MAE: mfe_avg={stats_glob['mfe_avg_r']:+.3f} R, "
        f"mae_avg={stats_glob['mae_avg_r']:+.3f} R"
    )
    print()
    print("Trailing:")
    print(
        f"  trailing_share={stats_glob['trailing_share']*100:.1f}% "
        f"(запущен трейл)"
    )
    print(
        f"  trailing_close_share={stats_glob['trailing_close_share']*100:.1f}% "
        f"(закрыто по трейлу)"
    )
    print(
        f"  trailing_WR={stats_glob['trailing_wr']*100:.1f}% "
        f"(среди трейлинговых закрытий)"
    )
    print(
        f"  trailing Exp_R (managed)={stats_glob['trailing_expectancy_r']:+.3f}, "
        f"(baseline)={stats_glob['trailing_expectancy_fixed_r']:+.3f}, "
        f"Δ={stats_glob['trailing_delta_expectancy_r']:+.3f}"
    )
    print()


def print_tag_report(tag_stats: List[Dict[str, Any]], max_tags: int = 15) -> None:
    if not tag_stats:
        print("Нет тегов с достаточным числом сделок.")
        return

    print("=== Per-entry_tag stats (top by trades) ===")
    print(
        "tag | n | WR_managed | Exp_R_managed | Exp_R_baseline | ΔExp_R | trailing_share | trailing_WR | giveback_avg_R | missed_avg_R"
    )
    print("-" * 120)
    for stats_row in tag_stats[:max_tags]:
        print(
            f"{stats_row['tag'][:20]:20s} | "
            f"{stats_row['n']:4d} | "
            f"{stats_row['wr']*100:10.1f}% | "
            f"{stats_row['expectancy_managed_r']:+.3f} | "
            f"{stats_row['expectancy_baseline_r']:+.3f} | "
            f"{stats_row['delta_expectancy_r']:+.3f} | "
            f"{stats_row['trailing_share']*100:13.1f}% | "
            f"{stats_row['trailing_wr']*100:10.1f}% | "
            f"{stats_row['giveback_avg_r']:+.3f} | "
            f"{stats_row['missed_avg_r']:+.3f}"
        )
    print()


def main() -> None:
    parser = argparse.ArgumentParser(
        description="Deep trailing vs baseline analyzer for trades_closed in Redis Stream."
    )
    parser.add_argument(
        "--redis-url",
        type=str,
        default=None,
        help="Redis URL, e.g. redis://localhost:6379/0 (по умолчанию REDIS_URL)",
    )
    parser.add_argument(
        "--stream",
        type=str,
        default=None,
        help="Имя stream (по умолчанию TRADES_CLOSED_STREAM_NAME или trades:closed)",
    )
    parser.add_argument(
        "--source", type=str, default="CryptoOrderFlow", help="source (strategy source)"
    )
    parser.add_argument(
        "--symbols",
        type=str,
        default="ETHUSDT,BTCUSDT",
        help="comma-separated list of symbols",
    )
    parser.add_argument("--limit", type=int, default=200, help="max trades per symbol")
    parser.add_argument(
        "--since-days",
        type=int,
        default=0,
        help="optional, cut window by last N days",
    )
    parser.add_argument(
        "--min-trades-per-tag",
        type=int,
        default=10,
        help="min trades per entry_tag to show stats",
    )

    args = parser.parse_args()

    redis_url = args.redis_url or os.getenv("REDIS_URL", "redis://localhost:6379/0")
    stream_name = args.stream or os.getenv("TRADES_CLOSED_STREAM_NAME", "trades:closed")

    r = redis.from_url(redis_url, decode_responses=True)

    symbols = [symbol.strip() for symbol in args.symbols.split(",") if symbol.strip()]

    for sym in symbols:
        trades = load_trades(
            r,
            stream_name,
            source=args.source,
            symbol=sym,
            limit=args.limit,
            since_days=args.since_days if args.since_days > 0 else None,
        )
        if not trades:
            print(f"Нет сделок для source={args.source}, symbol={sym}")
            continue

        global_stats = analyze_global(trades)
        print_global_report(sym, args.source, global_stats)

        per_tag = analyze_by_tag(trades, min_trades=args.min_trades_per_tag)
        print_tag_report(per_tag, max_tags=20)

if __name__ == "__main__":
    main()

