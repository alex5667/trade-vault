from __future__ import annotations

"""
analyze_trades_from_redis_advanced.py

Глубокий анализ закрытых сделок из Redis (stream trades:closed):

- глобальные метрики managed vs baseline (pnl_net vs pnl_if_fixed_exit)
- подробные трейлинговые/экскурсионные метрики
- анализ по entry_tag (deltaSpikeZ / breakout_ob / pullback_to_fvg и т.п.)

Предполагается, что в stream trades:closed есть поля:

- source, symbol, exit_ts_ms
- pnl_net, pnl_if_fixed_exit, one_r_money
- giveback, missed_profit
- mfe_pnl, mae_pnl
- trailing_started, trailing_active
- close_reason, close_reason_raw, close_reason_detail (опционально)
- entry_tag
"""


import argparse
import math
from dataclasses import dataclass

import redis  # pip install redis

# ----------------------------
# Модели и утилиты
# ----------------------------

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
    close_reason_detail: str
    entry_tag: str


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
# Агрегатор по entry_tag / глобальный
# ----------------------------

@dataclass
class TagStats:
    tag: str

    n: int = 0
    sum_pnl_net: float = 0.0
    sum_pnl_fixed: float = 0.0

    n_win: int = 0
    n_loss: int = 0
    sum_win_usd: float = 0.0
    sum_loss_usd: float = 0.0

    n_win_fixed: int = 0
    n_loss_fixed: int = 0
    sum_win_fixed_usd: float = 0.0
    sum_loss_fixed_usd: float = 0.0

    # R-метрики (managed)
    n_r: int = 0
    sum_r_managed: float = 0.0
    sum_r2_managed: float = 0.0
    sum_r_win: float = 0.0
    n_r_win: int = 0
    sum_r_loss: float = 0.0
    n_r_loss: int = 0

    # R-метрики (baseline)
    n_r_fixed: int = 0
    sum_r_baseline: float = 0.0
    sum_r_fixed_win: float = 0.0
    n_r_fixed_win: int = 0
    sum_r_fixed_loss: float = 0.0
    n_r_fixed_loss: int = 0

    # Giveback
    sum_giveback_usd: float = 0.0
    sum_giveback_r: float = 0.0
    sum_giveback_ratio: float = 0.0
    n_giveback_pos: int = 0
    n_giveback_r: int = 0
    n_giveback_ratio: int = 0

    # Missed profit
    sum_missed_usd: float = 0.0
    sum_missed_r: float = 0.0
    sum_missed_ratio: float = 0.0
    n_missed_pos: int = 0
    n_missed_r: int = 0
    n_missed_ratio: int = 0

    # Экскурсии (в R)
    sum_mfe_r: float = 0.0
    sum_mae_r: float = 0.0
    n_mfe_r: int = 0
    n_mae_r: int = 0

    # Трейлинг
    n_trailing: int = 0
    n_trailing_closed: int = 0
    n_trailing_closed_win: int = 0
    sum_r_trailing: float = 0.0
    n_r_trailing: int = 0
    sum_r_fixed_trailing: float = 0.0
    n_r_fixed_trailing: int = 0

    # Эквити и MDD
    eq: float = 0.0
    peak: float = 0.0
    mdd: float = 0.0

    def add_trade(self, t: Trade) -> None:
        self.n += 1
        self.sum_pnl_net += t.pnl_net
        self.sum_pnl_fixed += t.pnl_if_fixed_exit

        # Win / Loss (managed)
        if t.pnl_net > 0:
            self.n_win += 1
            self.sum_win_usd += t.pnl_net
        elif t.pnl_net < 0:
            self.n_loss += 1
            self.sum_loss_usd += t.pnl_net

        # Win / Loss (baseline)
        if t.pnl_if_fixed_exit > 0:
            self.n_win_fixed += 1
            self.sum_win_fixed_usd += t.pnl_if_fixed_exit
        elif t.pnl_if_fixed_exit < 0:
            self.n_loss_fixed += 1
            self.sum_loss_fixed_usd += t.pnl_if_fixed_exit

        # Всё, что зависит от 1R
        if t.one_r_money > 1e-12:
            r_m = t.pnl_net / t.one_r_money
            r_b = t.pnl_if_fixed_exit / t.one_r_money

            self.n_r += 1
            self.sum_r_managed += r_m
            self.sum_r2_managed += r_m * r_m

            self.n_r_fixed += 1
            self.sum_r_baseline += r_b

            if r_m > 0:
                self.n_r_win += 1
                self.sum_r_win += r_m
            elif r_m < 0:
                self.n_r_loss += 1
                self.sum_r_loss += r_m

            if r_b > 0:
                self.n_r_fixed_win += 1
                self.sum_r_fixed_win += r_b
            elif r_b < 0:
                self.n_r_fixed_loss += 1
                self.sum_r_fixed_loss += r_b

            if t.giveback > 0:
                self.sum_giveback_r += t.giveback / t.one_r_money
                self.n_giveback_r += 1

            if t.missed_profit > 0:
                self.sum_missed_r += t.missed_profit / t.one_r_money
                self.n_missed_r += 1

            if t.mfe_pnl != 0:
                self.sum_mfe_r += t.mfe_pnl / t.one_r_money
                self.n_mfe_r += 1
            if t.mae_pnl != 0:
                self.sum_mae_r += t.mae_pnl / t.one_r_money
                self.n_mae_r += 1

        # Giveback в USD и доле MFE
        if t.giveback > 0:
            self.n_giveback_pos += 1
            self.sum_giveback_usd += t.giveback
            if t.mfe_pnl > 1e-12:
                self.sum_giveback_ratio += t.giveback / t.mfe_pnl
                self.n_giveback_ratio += 1

        # Missed-profit в USD и доле MFE
        if t.missed_profit > 0:
            self.n_missed_pos += 1
            self.sum_missed_usd += t.missed_profit
            if t.mfe_pnl > 1e-12:
                self.sum_missed_ratio += t.missed_profit / t.mfe_pnl
                self.n_missed_ratio += 1

        # Трейлинг
        trailing_flag = t.trailing_started or t.trailing_active
        if trailing_flag:
            self.n_trailing += 1
            if t.one_r_money > 1e-12:
                r_m = t.pnl_net / t.one_r_money
                r_b = t.pnl_if_fixed_exit / t.one_r_money
                self.sum_r_trailing += r_m
                self.n_r_trailing += 1
                self.sum_r_fixed_trailing += r_b
                self.n_r_fixed_trailing += 1

        # Было ли закрытие именно трейлингом
        is_trailing_close = False
        cr = (t.close_reason_raw or "").upper()
        crd = (t.close_reason_detail or "").upper()
        if "TRAILING" in cr or "TRAILING" in crd:
            is_trailing_close = True

        if trailing_flag and is_trailing_close:
            self.n_trailing_closed += 1
            if t.pnl_net > 0:
                self.n_trailing_closed_win += 1

        # Эквити / MDD
        self.eq += t.pnl_net
        if self.eq > self.peak:
            self.peak = self.eq
        dd = self.peak - self.eq
        if dd > self.mdd:
            self.mdd = dd

    def finalize(self) -> dict[str, float]:
        if self.n == 0:
            return {"tag": self.tag, "n": 0}

        res: dict[str, float] = {"tag": self.tag, "n": float(self.n)}

        # Базовые суммы
        res["pnl_net_sum"] = self.sum_pnl_net
        res["pnl_net_avg"] = self.sum_pnl_net / self.n

        res["pnl_fixed_sum"] = self.sum_pnl_fixed
        res["pnl_fixed_avg"] = self.sum_pnl_fixed / self.n

        # Win-rate
        res["wr_managed"] = self.n_win / self.n if self.n > 0 else 0.0
        res["wr_baseline"] = self.n_win_fixed / self.n if self.n > 0 else 0.0

        # Expectancy (R)
        expectancy_r = self.sum_r_managed / self.n_r if self.n_r > 0 else 0.0
        expectancy_fixed_r = self.sum_r_baseline / self.n_r_fixed if self.n_r_fixed > 0 else 0.0
        res["expectancy_r"] = expectancy_r
        res["expectancy_fixed_r"] = expectancy_fixed_r
        res["delta_expectancy_r"] = expectancy_r - expectancy_fixed_r

        # Payoff (R)
        avg_win_r = self.sum_r_win / self.n_r_win if self.n_r_win > 0 else 0.0
        avg_loss_r = self.sum_r_loss / self.n_r_loss if self.n_r_loss > 0 else 0.0
        payoff_r = (avg_win_r / abs(avg_loss_r)) if avg_loss_r < 0 else 0.0
        res["payoff_r"] = payoff_r

        # Payoff (USD, managed)
        avg_win_usd = self.sum_win_usd / self.n_win if self.n_win > 0 else 0.0
        avg_loss_usd = self.sum_loss_usd / self.n_loss if self.n_loss > 0 else 0.0
        payoff_usd = (avg_win_usd / abs(avg_loss_usd)) if avg_loss_usd < 0 else 0.0
        res["payoff_usd"] = payoff_usd

        # Payoff (USD, baseline)
        avg_win_fixed_usd = self.sum_win_fixed_usd / self.n_win_fixed if self.n_win_fixed > 0 else 0.0
        avg_loss_fixed_usd = self.sum_loss_fixed_usd / self.n_loss_fixed if self.n_loss_fixed > 0 else 0.0
        payoff_fixed_usd = (avg_win_fixed_usd / abs(avg_loss_fixed_usd)) if avg_loss_fixed_usd < 0 else 0.0
        res["payoff_fixed_usd"] = payoff_fixed_usd

        # Payoff (R, baseline)
        avg_win_fixed_r = self.sum_r_fixed_win / self.n_r_fixed_win if self.n_r_fixed_win > 0 else 0.0
        avg_loss_fixed_r = self.sum_r_fixed_loss / self.n_r_fixed_loss if self.n_r_fixed_loss > 0 else 0.0
        payoff_fixed_r = (avg_win_fixed_r / abs(avg_loss_fixed_r)) if avg_loss_fixed_r < 0 else 0.0
        res["payoff_fixed_r"] = payoff_fixed_r

        # Std(R) и Sharpe
        if self.n_r > 1:
            mean_r = expectancy_r
            var = (self.sum_r2_managed - self.n_r * mean_r * mean_r) / (self.n_r - 1)
            std_r = math.sqrt(max(var, 0.0))
        else:
            std_r = 0.0
        res["std_r"] = std_r
        res["sharpe"] = (expectancy_r / std_r) if std_r > 1e-9 else 0.0

        # MDD
        res["mdd_usd"] = self.mdd

        # Giveback
        res["giveback_avg_usd"] = self.sum_giveback_usd / self.n_giveback_pos if self.n_giveback_pos > 0 else 0.0
        res["giveback_avg_r"] = self.sum_giveback_r / self.n_giveback_r if self.n_giveback_r > 0 else 0.0
        res["giveback_avg_ratio"] = self.sum_giveback_ratio / self.n_giveback_ratio if self.n_giveback_ratio > 0 else 0.0
        res["giveback_share"] = self.n_giveback_pos / self.n if self.n > 0 else 0.0

        # Missed profit
        res["missed_avg_usd"] = self.sum_missed_usd / self.n_missed_pos if self.n_missed_pos > 0 else 0.0
        res["missed_avg_r"] = self.sum_missed_r / self.n_missed_r if self.n_missed_r > 0 else 0.0
        res["missed_avg_ratio"] = self.sum_missed_ratio / self.n_missed_ratio if self.n_missed_ratio > 0 else 0.0
        res["missed_share"] = self.n_missed_pos / self.n if self.n > 0 else 0.0

        # Экскурсии
        res["mfe_avg_r"] = self.sum_mfe_r / self.n_mfe_r if self.n_mfe_r > 0 else 0.0
        res["mae_avg_r"] = self.sum_mae_r / self.n_mae_r if self.n_mae_r > 0 else 0.0

        # Трейлинг
        res["trailing_share"] = self.n_trailing / self.n if self.n > 0 else 0.0
        res["trailing_close_share"] = (self.n_trailing_closed / self.n_trailing) if self.n_trailing > 0 else 0.0
        res["trailing_wr"] = (self.n_trailing_closed_win / self.n_trailing_closed) if self.n_trailing_closed > 0 else 0.0

        trailing_expectancy_r = self.sum_r_trailing / self.n_r_trailing if self.n_r_trailing > 0 else 0.0
        trailing_expectancy_fixed_r = self.sum_r_fixed_trailing / self.n_r_fixed_trailing if self.n_r_fixed_trailing > 0 else 0.0
        res["trailing_expectancy_r"] = trailing_expectancy_r
        res["trailing_expectancy_fixed_r"] = trailing_expectancy_fixed_r
        res["trailing_delta_expectancy_r"] = trailing_expectancy_r - trailing_expectancy_fixed_r

        return res


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

    for entry_id, fields in entries:
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
    print(f"Expectancy R (managed): {_fmt_f(m['expectancy_r'])} | baseline: {_fmt_f(m['expectancy_fixed_r'])} | Δ: {_fmt_f(m['delta_expectancy_r'])}")
    print(f"Payoff R (managed): {_fmt_f(m['payoff_r'])} | baseline: {_fmt_f(m['payoff_fixed_r'])}")
    print(f"Payoff USD (managed): {_fmt_f(m['payoff_usd'])} | baseline: {_fmt_f(m['payoff_fixed_usd'])}")
    print(f"Std(R): {_fmt_f(m['std_r'])} | Sharpe*: {_fmt_f(m['sharpe'])}")
    print(f"MDD (USD): {m['mdd_usd']:.2f}")

    print("\nGiveback / Missed / Excursions:")
    print(
        f"Giveback_avg: {m['giveback_avg_usd']:.3f} USD | {m['giveback_avg_r']:.3f} R | ratio vs MFE: {m['giveback_avg_ratio']:.3f} | share: {_fmt_pct(m['giveback_share'])}"
    )
    print(
        f"Missed_avg: {m['missed_avg_usd']:.3f} USD | {m['missed_avg_r']:.3f} R | ratio vs MFE: {m['missed_avg_ratio']:.3f} | share: {_fmt_pct(m['missed_share'])}"
    )
    print(
        f"MFE_avg: {m['mfe_avg_r']:.3f} R | MAE_avg: {m['mae_avg_r']:.3f} R"
    )

    print("\nTrailing:")
    print(
        f"Trailing_share: {_fmt_pct(m['trailing_share'])} | close_share: {_fmt_pct(m['trailing_close_share'])} | WR(trail_closes): {_fmt_pct(m['trailing_wr'])}"
    )
    print(
        f"Trailing Expectancy R (managed): {_fmt_f(m['trailing_expectancy_r'])} | baseline: {_fmt_f(m['trailing_expectancy_fixed_r'])} | Δ: {_fmt_f(m['trailing_delta_expectancy_r'])}"
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
        f"{'ΔExpR':>7s}  {'PayoffR':>8s}  {'Trail%':>7s}  {'ΔExpR_trail':>11s}  "
        f"{'GB_R':>6s}  {'Miss_R':>7s}  {'MFE_R':>7s}"
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
