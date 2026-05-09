#!/usr/bin/env python3
from __future__ import annotations

"""
Advanced analysis of trades_closed from Postgres/Timescale:
- managed vs baseline metrics (global and per entry_tag)
- grouping modes: none / source_symbol / strategy
- time filters: --from / --to (ms or date/ISO)
- markdown output for Telegram

Example:
  python -m scripts.analyze_trades_from_postgres_advanced \
    --dsn "postgresql://postgres:postgres@localhost:5432/scanner_analytics" \
    --source CryptoOrderFlow \
    --symbol ETHUSDT \
    --limit 2000 \
    --from "2025-12-01" \
    --markdown
"""

import argparse
from dataclasses import dataclass
from datetime import UTC, datetime

import psycopg2

from analytics.tag_stats import TagStats, Trade


# ----------------------------
# Утилиты приведения/парсинга типов
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
    if isinstance(v, bool):
        return v
    s = str(v).strip().lower()
    return s in ("1", "t", "true", "yes", "y")


def parse_ts_arg(val: str | None) -> int | None:
    """
    --from / --to parser:
    - digits => treat as exit_ts_ms (epoch ms)
    - else parse date/ISO and convert to UTC ms
    """
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
            dt = dt.replace(tzinfo=UTC)
        else:
            dt = datetime.fromisoformat(s)
            if dt.tzinfo is None:
                dt = dt.replace(tzinfo=UTC)
            else:
                dt = dt.astimezone(UTC)
        return int(dt.timestamp() * 1000)
    except Exception as e:
        raise ValueError(f"Не могу разобрать значение времени '{s}': {e}") from e


# ----------------------------
# Загрузка сделок из Postgres
# ----------------------------
def load_trades_from_postgres(
    dsn: str,
    limit: int,
    table: str = "trades_closed",
    source_filter: str | None = None,
    symbol_filter: str | None = None,
    from_ts_ms: int | None = None,
    to_ts_ms: int | None = None,
) -> list[Trade]:
    conn = psycopg2.connect(dsn)
    try:
        cur = conn.cursor()

        cols = """
            source,
            symbol,
            exit_ts_ms,
            pnl_net,
            pnl_if_fixed_exit,
            one_r_money,
            giveback,
            missed_profit,
            mfe_pnl,
            mae_pnl,
            trailing_started,
            trailing_active,
            close_reason,
            close_reason_raw,
            close_reason_detail,
            entry_tag,
            strategy,
        """

        sql = f"SELECT {cols} FROM {table}"
        conds: list[str] = []
        params: list[object] = []

        if source_filter:
            conds.append("source = %s")
            params.append(source_filter)
        if symbol_filter:
            conds.append("symbol = %s")
            params.append(symbol_filter)
        if from_ts_ms is not None:
            conds.append("exit_ts_ms >= %s")
            params.append(from_ts_ms)
        if to_ts_ms is not None:
            conds.append("exit_ts_ms <= %s")
            params.append(to_ts_ms)

        if conds:
            sql += " WHERE " + " AND ".join(conds)

        sql += " ORDER BY exit_ts_ms DESC LIMIT %s"
        params.append(limit)

        cur.execute(sql, params)
        rows = cur.fetchall()
    finally:
        conn.close()

    trades: list[Trade] = []
    for row in rows:
        (
            source,
            symbol,
            exit_ts_ms,
            pnl_net,
            pnl_if_fixed_exit,
            one_r_money,
            giveback,
            missed_profit,
            mfe_pnl,
            mae_pnl,
            trailing_started,
            trailing_active,
            close_reason,
            close_reason_raw,
            close_reason_detail,
            entry_tag,
            strategy,
        ) = row

        trades.append(
            Trade(
                source=source or "Unknown",
                symbol=symbol or "UNKNOWN",
                exit_ts_ms=_to_int(exit_ts_ms),
                pnl_net=_to_float(pnl_net),
                pnl_if_fixed_exit=_to_float(pnl_if_fixed_exit),
                one_r_money=_to_float(one_r_money),
                giveback=_to_float(giveback),
                missed_profit=_to_float(missed_profit),
                mfe_pnl=_to_float(mfe_pnl),
                mae_pnl=_to_float(mae_pnl),
                trailing_started=_to_bool(trailing_started),
                trailing_active=_to_bool(trailing_active),
                close_reason=close_reason or "",
                close_reason_raw=close_reason_raw or "",
                close_reason_detail=close_reason_detail or "",
                entry_tag=entry_tag or "",
                strategy=strategy or "",
            )
        )

    trades.sort(key=lambda t: t.exit_ts_ms)
    return trades


# ----------------------------
# Рендер отчётов (plain / markdown)
# ----------------------------
def _fmt_pct(x: float) -> str:
    return f"{x * 100:.1f}%"


def _fmt_f(x: float) -> str:
    return f"{x:.3f}"


def render_global_report(label: str, stats: TagStats) -> str:
    m = stats.finalize()
    lines: list[str] = []
    lines.append(f"=== Global metrics [{label}] ===")
    lines.append(f"Trades: {int(m['n'])}")
    lines.append(f"P/L net sum: {m['pnl_net_sum']:.2f} | avg: {m['pnl_net_avg']:.3f}")
    lines.append(f"P/L fixed sum: {m['pnl_fixed_sum']:.2f} | avg: {m['pnl_fixed_avg']:.3f}")
    lines.append(
        f"WR (managed): {_fmt_pct(m['wr_managed'])} | "
        f"WR (baseline): {_fmt_pct(m['wr_baseline'])}"
    )
    lines.append(
        f"Expectancy R (managed): {_fmt_f(m['expectancy_r'])} | "
        f"baseline: {_fmt_f(m['expectancy_fixed_r'])} | "
        f"Δ: {_fmt_f(m['delta_expectancy_r'])}"
    )
    lines.append(
        f"Payoff R (managed): {_fmt_f(m['payoff_r'])} | "
        f"baseline: {_fmt_f(m['payoff_fixed_r'])}"
    )
    lines.append(
        f"Payoff USD (managed): {_fmt_f(m['payoff_usd'])} | "
        f"baseline: {_fmt_f(m['payoff_fixed_usd'])}"
    )
    lines.append(f"Std(R): {_fmt_f(m['std_r'])} | Sharpe*: {_fmt_f(m['sharpe'])}")
    lines.append(f"MDD (USD): {m['mdd_usd']:.2f}")

    lines.append("")
    lines.append("Giveback / Missed / Excursions:")
    lines.append(
        f"Giveback_avg: {m['giveback_avg_usd']:.3f} USD | "
        f"{m['giveback_avg_r']:.3f} R | "
        f"ratio vs MFE: {m['giveback_avg_ratio']:.3f} | "
        f"share: {_fmt_pct(m['giveback_share'])}"
    )
    lines.append(
        f"Missed_avg: {m['missed_avg_usd']:.3f} USD | "
        f"{m['missed_avg_r']:.3f} R | "
        f"ratio vs MFE: {m['missed_avg_ratio']:.3f} | "
        f"share: {_fmt_pct(m['missed_share'])}"
    )
    lines.append(
        f"MFE_avg: {m['mfe_avg_r']:.3f} R | "
        f"MAE_avg: {m['mae_avg_r']:.3f} R"
    )

    lines.append("")
    lines.append("Trailing:")
    lines.append(
        f"Trailing_share: {_fmt_pct(m['trailing_share'])} | "
        f"close_share: {_fmt_pct(m['trailing_close_share'])} | "
        f"WR(trail_closes): {_fmt_pct(m['trailing_wr'])}"
    )
    lines.append(
        f"Trailing Expectancy R (managed): {_fmt_f(m['trailing_expectancy_r'])} | "
        f"baseline: {_fmt_f(m['trailing_expectancy_fixed_r'])} | "
        f"Δ: {_fmt_f(m['trailing_delta_expectancy_r'])}"
    )
    return "\n".join(lines)


def render_entry_tag_report(
    stats_by_tag: dict[str, TagStats],
    min_trades: int = 5,
) -> str:
    lines: list[str] = []
    lines.append(f"=== Entry-tag metrics (n >= {min_trades}) ===")

    rows: list[tuple[str, dict[str, float]]] = []
    for tag, s in stats_by_tag.items():
        m = s.finalize()
        if m["n"] >= min_trades:
            rows.append((tag, m))

    rows.sort(key=lambda x: x[1].get("delta_expectancy_r", 0.0), reverse=True)

    if not rows:
        lines.append("No tags with enough trades.")
        return "\n".join(lines)

    header = (
        f"{'tag':20s}  {'n':>4s}  {'WR':>6s}  {'ExpR':>7s}  {'ExpR_fix':>9s}  "
        f"{'ΔExpR':>7s}  {'PayoffR':>8s}  {'Trail%':>7s}  {'ΔExpR_trail':>11s}  "
        f"{'GB_R':>6s}  {'Miss_R':>7s}  {'MFE_R':>7s}"
    )
    lines.append(header)
    lines.append("-" * len(header))

    for tag, m in rows:
        lines.append(
            f"{tag[:20]:20s}  "
            f"{int(m['n']):4d}  "
            f"{_fmt_pct(m['wr_managed']):>6s}  "
            f"{_fmt_f(m['expectancy_r']):>7s}  "
            f"{_fmt_f(m['expectancy_fixed_r']):>9s}  "
            f"{_fmt_f(m['delta_expectancy_r']):>7s}  "
            f"{_fmt_f(m['payoff_r']):>8s}  "
            f"{_fmt_pct(m['trailing_share']):>7s}  "
            f"{_fmt_f(m['trailing_delta_expectancy_r']):>11s}  "
            f"{_fmt_f(m['giveback_avg_r']):>6s}  "
            f"{_fmt_f(m['missed_avg_r']):>7s}  "
            f"{_fmt_f(m['mfe_avg_r']):>7s}"
        )

    return "\n".join(lines)


# ----------------------------
# Группировка
# ----------------------------
@dataclass
class GroupBucket:
    label: str
    global_stats: TagStats
    by_tag: dict[str, TagStats]


def build_groups(
    trades: list[Trade],
    group_by: str,
) -> dict[str, GroupBucket]:
    """
    group_by:
      - "none"
      - "source_symbol"
      - "strategy"
    """
    groups: dict[str, GroupBucket] = {}

    for t in trades:
        if group_by == "none":
            key = "GLOBAL"
            label = f"{t.source} / {t.symbol}"
        elif group_by == "source_symbol":
            key = f"{t.source}::{t.symbol}"
            label = f"{t.source} / {t.symbol}"
        elif group_by == "strategy":
            strat = t.strategy or "unknown"
            key = strat
            label = f"strategy={strat}"
        else:
            key = "GLOBAL"
            label = f"{t.source} / {t.symbol}"

        gb = groups.get(key)
        if gb is None:
            gb = GroupBucket(
                label=label,
                global_stats=TagStats(tag="__ALL__"),
                by_tag={},
            )
            groups[key] = gb

        gb.global_stats.add_trade(t)
        tag_key = t.entry_tag or "<untagged>"
        ts = gb.by_tag.get(tag_key)
        if ts is None:
            ts = TagStats(tag=tag_key)
            gb.by_tag[tag_key] = ts
        ts.add_trade(t)

    return groups


# ----------------------------
# CLI
# ----------------------------
def main() -> None:
    parser = argparse.ArgumentParser(
        description="Advanced analysis of trades_closed from Postgres (managed vs baseline, per entry_tag, grouping, time filters)."
    )
    parser.add_argument(
        "--dsn",
        required=True,
        help="Postgres DSN, e.g. postgres://user:pass@localhost:5432/scanner_analytics",
    )
    parser.add_argument("--table", default="trades_closed")
    parser.add_argument("--limit", type=int, default=1000)
    parser.add_argument("--source", default=None)
    parser.add_argument("--symbol", default=None)

    parser.add_argument(
        "--from",
        dest="from_ts",
        default=None,
        help="Lower bound for exit_ts_ms (inclusive). Either ms since epoch or date 'YYYY-MM-DD' / ISO.",
    )
    parser.add_argument(
        "--to",
        dest="to_ts",
        default=None,
        help="Upper bound for exit_ts_ms (inclusive). Either ms since epoch or date 'YYYY-MM-DD' / ISO.",
    )

    parser.add_argument(
        "--group-by",
        choices=["none", "source_symbol", "strategy"],
        default="none",
        help="Grouping mode: none (one bucket), source_symbol, strategy.",
    )

    parser.add_argument(
        "--min-trades-per-tag",
        type=int,
        default=5,
        help="Minimal trades per entry_tag to show in tag-table.",
    )

    parser.add_argument(
        "--markdown",
        action="store_true",
        help="Wrap output into ``` for Telegram Markdown.",
    )

    args = parser.parse_args()

    from_ts_ms = parse_ts_arg(args.from_ts) if args.from_ts else None
    to_ts_ms = parse_ts_arg(args.to_ts) if args.to_ts else None

    trades = load_trades_from_postgres(
        dsn=args.dsn,
        limit=args.limit,
        table=args.table,
        source_filter=args.source,
        symbol_filter=args.symbol,
        from_ts_ms=from_ts_ms,
        to_ts_ms=to_ts_ms,
    )

    if not trades:
        print("No trades found for given filters.")
        return

    groups = build_groups(trades, group_by=args.group_by)

    out_chunks: list[str] = []
    for key in sorted(groups.keys()):
        gb = groups[key]
        out_chunks.append(render_global_report(gb.label, gb.global_stats))
        out_chunks.append("")
        out_chunks.append(render_entry_tag_report(gb.by_tag, min_trades=args.min_trades_per_tag))
        out_chunks.append("")

    full_text = "\n".join(out_chunks).strip()

    if args.markdown:
        print("```")
        print(full_text)
        print("```")
    else:
        print(full_text)


if __name__ == "__main__":
    main()

