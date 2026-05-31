from __future__ import annotations

import argparse

import redis  # pip install redis


from analysis.tag_stats import Trade, TagStats


# ----------------------------
# Утилиты приведения типов
# ----------------------------

def _to_float(v, default: float = 0.0) -> float:
    if v is None:
        return default
    try:
        return float(v)
    except Exception:
        return default


def _to_int(v, default: int = 0) -> int:
    if v is None:
        return default
    try:
        return int(float(v))
    except Exception:
        return default


def _to_bool(v) -> bool:
    if v is None:
        return False
    s = str(v).strip().lower()
    return s in ("1", "true", "t", "yes", "y")


# ----------------------------
# Загрузка сделок из Redis
# ----------------------------

def load_trades_from_redis(
    redis_url: str,
    stream_key: str,
    count: int,
    source_filter: str | None = None,
    symbol_filter: str | None = None,
) -> list[Trade]:
    r = redis.from_url(redis_url, decode_responses=True)
    entries = r.xrevrange(stream_key, max="+", min="-", count=count)

    trades: list[Trade] = []

    for entry_id, fields in entries:  # noqa: B007
        source = fields.get("source") or fields.get("strategy_source") or "Unknown"
        symbol = fields.get("symbol") or "UNKNOWN"

        if source_filter and source != source_filter:
            continue
        if symbol_filter and symbol != symbol_filter:
            continue

        trade = Trade(
            source=source,
            symbol=symbol,
            exit_ts_ms=_to_int(fields.get("exit_ts_ms") or fields.get("exit_ts") or fields.get("ts")),
            pnl_net=_to_float(fields.get("pnl_net") or fields.get("pnl")),
            pnl_if_fixed_exit=_to_float(fields.get("pnl_if_fixed_exit") or fields.get("pnl_fixed")),
            one_r_money=_to_float(fields.get("one_r_money") or fields.get("one_r")),
            giveback=_to_float(fields.get("giveback")),
            missed_profit=_to_float(fields.get("missed_profit")),
            mfe_pnl=_to_float(fields.get("mfe_pnl")),
            mae_pnl=_to_float(fields.get("mae_pnl")),
            trailing_started=_to_bool(fields.get("trailing_started")),
            trailing_active=_to_bool(fields.get("trailing_active")),
            close_reason=fields.get("close_reason") or "",
            close_reason_raw=fields.get("close_reason_raw") or "",
            close_reason_detail=fields.get("close_reason_detail") or "",
            entry_tag=fields.get("entry_tag") or "",
            strategy=fields.get("strategy") or "",
        )
        trades.append(trade)

    trades.sort(key=lambda t: t.exit_ts_ms)
    return trades


# ----------------------------
# Печать отчётов
# ----------------------------

def _fmt_pct(x: float) -> str:
    return f"{x * 100:.1f}%"


def _fmt_f(x: float) -> str:
    return f"{x:.3f}"


def print_global_report(source: str, symbol: str, stats: TagStats) -> None:
    m = stats.finalize()
    print(f"\n=== Global metrics (source={source}, symbol={symbol}) ===")
    print(f"Trades: {int(m['n'])}")
    print(f"P/L net sum: {m['pnl_net_sum']:.2f} | avg: {m['pnl_net_avg']:.3f}")
    print(f"P/L fixed sum: {m['pnl_fixed_sum']:.2f} | avg: {m['pnl_fixed_avg']:.3f}")
    print(f"WR (managed): {_fmt_pct(m['wr_managed'])} | WR (baseline): {_fmt_pct(m['wr_baseline'])}")
    print(
        f"Expectancy R (managed): {_fmt_f(m['expectancy_r'])} | "
        f"baseline: {_fmt_f(m['expectancy_fixed_r'])} | "
        f"Δ: {_fmt_f(m['delta_expectancy_r'])}"
    )
    print(
        f"Payoff R (managed): {_fmt_f(m['payoff_r'])} | "
        f"baseline: {_fmt_f(m['payoff_fixed_r'])}"
    )
    print(
        f"Payoff USD (managed): {_fmt_f(m['payoff_usd'])} | "
        f"baseline: {_fmt_f(m['payoff_fixed_usd'])}"
    )
    print(f"Std(R): {_fmt_f(m['std_r'])} | Sharpe*: {_fmt_f(m['sharpe'])}")
    print(f"MDD (USD): {m['mdd_usd']:.2f}")

    print("\nGiveback / Missed / Excursions:")
    print(
        f"Giveback_avg: {m['giveback_avg_usd']:.3f} USD | "
        f"{m['giveback_avg_r']:.3f} R | "
        f"ratio vs MFE: {m['giveback_avg_ratio']:.3f} | "
        f"share: {_fmt_pct(m['giveback_share'])}"
    )
    print(
        f"Missed_avg: {m['missed_avg_usd']:.3f} USD | "
        f"{m['missed_avg_r']:.3f} R | "
        f"ratio vs MFE: {m['missed_avg_ratio']:.3f} | "
        f"share: {_fmt_pct(m['missed_share'])}"
    )
    print(
        f"MFE_avg: {m['mfe_avg_r']:.3f} R | "
        f"MAE_avg: {m['mae_avg_r']:.3f} R"
    )

    print("\nTrailing:")
    print(
        f"Trailing_share: {_fmt_pct(m['trailing_share'])} | "
        f"close_share: {_fmt_pct(m['trailing_close_share'])} | "
        f"WR(trail_closes): {_fmt_pct(m['trailing_wr'])}"
    )
    print(
        f"Trailing Expectancy R (managed): {_fmt_f(m['trailing_expectancy_r'])} | "
        f"baseline: {_fmt_f(m['trailing_expectancy_fixed_r'])} | "
        f"Δ: {_fmt_f(m['trailing_delta_expectancy_r'])}"
    )
    print(
        f"ΔExp_R: {_fmt_f(m['delta_expectancy_r'])} | "
        f"better: {_fmt_pct(m['share_better'])}, worse: {_fmt_pct(m['share_worse'])}"
    )


def print_entry_tag_report(stats_by_tag: dict[str, TagStats], min_trades: int = 5) -> None:
    print(f"\n=== Entry-tag metrics (only tags with n >= {min_trades}) ===")
    rows: list[tuple[str, dict[str, float]]] = []
    for tag, s in stats_by_tag.items():
        m = s.finalize()
        if m["n"] >= min_trades:
            rows.append((tag, m))

    # Сортируем по delta_expectancy_r (где управление даёт максимальный прирост к baseline)
    rows.sort(key=lambda x: x[1].get("delta_expectancy_r", 0.0), reverse=True)

    if not rows:
        print("No tags with enough trades.")
        return

    header = (
        f"{'tag':20s}  {'n':>4s}  {'WR':>6s}  {'ExpR':>7s}  {'ExpR_fix':>9s}  "
        f"{'ΔExpR':>7s}  {'better':>7s}  {'worse':>6s}  {'PayoffR':>8s}  {'Trail%':>7s}  "
        f"{'ΔExpR_trail':>11s}  {'GB_R':>6s}  {'Miss_R':>7s}  {'MFE_R':>7s}"
    )
    print(header)
    print("-" * len(header))

    for tag, m in rows:
        print(
            f"{tag[:20]:20s}  "
            f"{int(m['n']):4d}  "
            f"{_fmt_pct(m['wr_managed']):>6s}  "
            f"{_fmt_f(m['expectancy_r']):>7s}  "
            f"{_fmt_f(m['expectancy_fixed_r']):>9s}  "
            f"{_fmt_f(m['delta_expectancy_r']):>7s}  "
            f"{_fmt_pct(m['share_better']):>7s}  "
            f"{_fmt_pct(m['share_worse']):>6s}  "
            f"{_fmt_f(m['payoff_r']):>8s}  "
            f"{_fmt_pct(m['trailing_share']):>7s}  "
            f"{_fmt_f(m['trailing_delta_expectancy_r']):>11s}  "
            f"{_fmt_f(m['giveback_avg_r']):>6s}  "
            f"{_fmt_f(m['missed_avg_r']):>7s}  "
            f"{_fmt_f(m['mfe_avg_r']):>7s}"
        )


# ----------------------------
# CLI
# ----------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Advanced analysis of trades:closed from Redis (managed vs baseline, per entry_tag)."
    )
    parser.add_argument("--redis-url", default="redis://localhost:6379/0", help="Redis DSN (default: redis://localhost:6379/0)")
    parser.add_argument("--stream", default="trades:closed", help="Redis stream key (default: trades:closed)")
    parser.add_argument("--count", type=int, default=1000, help="How many latest trades to read (default: 1000)")
    parser.add_argument("--source", default=None, help="Filter by source (e.g. CryptoOrderFlow)")
    parser.add_argument("--symbol", default=None, help="Filter by symbol (e.g. ETHUSDT)")
    parser.add_argument("--min-trades-per-tag", type=int, default=5, help="Min trades per tag to show in tag-table (default: 5)")

    args = parser.parse_args()

    trades = load_trades_from_redis(
        redis_url=args.redis_url,
        stream_key=args.stream,
        count=args.count,
        source_filter=args.source,
        symbol_filter=args.symbol,
    )

    if not trades:
        print("No trades found for given filters.")
        return

    source = trades[0].source
    symbol = trades[0].symbol

    stats_global = TagStats(tag="__ALL__")
    stats_by_tag: dict[str, TagStats] = {}

    for t in trades:
        stats_global.add_trade(t)
        tag = t.entry_tag or "<untagged>"
        s = stats_by_tag.get(tag)
        if s is None:
            s = TagStats(tag=tag)
            stats_by_tag[tag] = s
        s.add_trade(t)

    print_global_report(source, symbol, stats_global)
    print_entry_tag_report(stats_by_tag, min_trades=args.min_trades_per_tag)


if __name__ == "__main__":
    main()
