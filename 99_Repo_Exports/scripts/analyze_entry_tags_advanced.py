#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Анализ baseline vs managed по entry_tag.

Берёт последние N сделок из Redis-стрима trades:closed,
фильтрует по source / symbol и считает:
- WR / Expectancy / Payoff по фактическому pnl_net (managed)
- WR_fixed / Expectancy_fixed / Payoff_fixed по pnl_if_fixed_exit (baseline)
- ΔExp_R = Exp_R(managed) - Exp_R(baseline)
- Giveback / Missed profit / MFE/MAE / трейлинг по каждому entry_tag
"""
from __future__ import annotations

import os
import json
import argparse
from dataclasses import dataclass
import math

import redis


try:
    from services.trailing_size_recommender import (
        ClosedTradeSnapshot,
        recommend_trailing_size,
    )
except ImportError:
    # Fallback for testing - define dummy classes
    @dataclass
    class ClosedTradeSnapshot:
        source: str
        symbol: str
        pnl_net: float
        one_r_money: float
        mfe_pnl: float
        giveback: float
        trailing_started: bool
        trailing_active: bool
        exit_ts_ms: int
        entry_tag: str

    def recommend_trailing_size(*args, **kwargs):
        return None


@dataclass
class Trade:
    """
    Унифицированное представление сделки для аналитики.
    Все источники (Redis/Postgres) приводят записи к этому виду.
    """

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
    strategy: str = ""


@dataclass
class TagStats:
    """
    Агрегатор метрик по тегу (entry_tag) или для глобала (__ALL__).
    Можно использовать для любых группировок (entry_tag, strategy и т.д.).
    """

    tag: str

    # Базовые суммы/счетчики
    n: int = 0

    # Счётчики сравнения managed vs baseline
    better_count: int = 0  # managed > baseline
    worse_count: int = 0   # managed < baseline
    equal_count: int = 0   # managed ≈ baseline
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

        if t.pnl_net > 0:
            self.n_win += 1
            self.sum_win_usd += t.pnl_net
        elif t.pnl_net < 0:
            self.n_loss += 1
            self.sum_loss_usd += t.pnl_net

        if t.pnl_if_fixed_exit > 0:
            self.n_win_fixed += 1
            self.sum_win_fixed_usd += t.pnl_if_fixed_exit
        elif t.pnl_if_fixed_exit < 0:
            self.n_loss_fixed += 1
            self.sum_loss_fixed_usd += t.pnl_if_fixed_exit

        if t.one_r_money > 1e-12:
            r_m = t.pnl_net / t.one_r_money
            r_b = t.pnl_if_fixed_exit / t.one_r_money

            self.n_r += 1
            self.sum_r_managed += r_m
            self.sum_r2_managed += r_m * r_m
            self.n_r_fixed += 1
            self.sum_r_baseline += r_b

            # Считаем сравнение managed vs baseline
            delta = r_m - r_b
            eps = 1e-6
            if delta > eps:
                self.better_count += 1
            elif delta < -eps:
                self.worse_count += 1
            else:
                self.equal_count += 1

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

        if t.giveback > 0:
            self.n_giveback_pos += 1
            self.sum_giveback_usd += t.giveback
            if t.mfe_pnl > 1e-12:
                self.sum_giveback_ratio += t.giveback / t.mfe_pnl
                self.n_giveback_ratio += 1

        if t.missed_profit > 0:
            self.n_missed_pos += 1
            self.sum_missed_usd += t.missed_profit
            if t.mfe_pnl > 1e-12:
                self.sum_missed_ratio += t.missed_profit / t.mfe_pnl
                self.n_missed_ratio += 1

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

        is_trailing_close = False
        cr = (t.close_reason_raw or "").upper()
        crd = (t.close_reason_detail or "").upper()
        if "TRAILING" in cr or "TRAILING" in crd:
            is_trailing_close = True

        if trailing_flag and is_trailing_close:
            self.n_trailing_closed += 1
            if t.pnl_net > 0:
                self.n_trailing_closed_win += 1

        self.eq += t.pnl_net
        if self.eq > self.peak:
            self.peak = self.eq
        dd = self.peak - self.eq
        if dd > self.mdd:
            self.mdd = dd

    def finalize(self) -> dict[str, float]:
        if self.n == 0:
            return {"tag": self.tag, "n": 0.0}

        res: dict[str, float] = {"tag": self.tag, "n": float(self.n)}

        res["pnl_net_sum"] = self.sum_pnl_net
        res["pnl_net_avg"] = self.sum_pnl_net / self.n

        res["pnl_fixed_sum"] = self.sum_pnl_fixed
        res["pnl_fixed_avg"] = self.sum_pnl_fixed / self.n

        res["wr_managed"] = self.n_win / self.n if self.n > 0 else 0.0
        res["wr_baseline"] = self.n_win_fixed / self.n if self.n > 0 else 0.0

        expectancy_r = self.sum_r_managed / self.n_r if self.n_r > 0 else 0.0
        expectancy_fixed_r = self.sum_r_baseline / self.n_r_fixed if self.n_r_fixed > 0 else 0.0
        res["expectancy_r"] = expectancy_r
        res["expectancy_fixed_r"] = expectancy_fixed_r
        res["delta_expectancy_r"] = expectancy_r - expectancy_fixed_r

        avg_win_r = self.sum_r_win / self.n_r_win if self.n_r_win > 0 else 0.0
        avg_loss_r = self.sum_r_loss / self.n_r_loss if self.n_r_loss > 0 else 0.0
        payoff_r = (avg_win_r / abs(avg_loss_r)) if avg_loss_r < 0 else 0.0
        res["payoff_r"] = payoff_r

        avg_win_usd = self.sum_win_usd / self.n_win if self.n_win > 0 else 0.0
        avg_loss_usd = self.sum_loss_usd / self.n_loss if self.n_loss > 0 else 0.0
        payoff_usd = (avg_win_usd / abs(avg_loss_usd)) if avg_loss_usd < 0 else 0.0
        res["payoff_usd"] = payoff_usd

        avg_win_fixed_usd = self.sum_win_fixed_usd / self.n_win_fixed if self.n_win_fixed > 0 else 0.0
        avg_loss_fixed_usd = self.sum_loss_fixed_usd / self.n_loss_fixed if self.n_loss_fixed > 0 else 0.0
        payoff_fixed_usd = (avg_win_fixed_usd / abs(avg_loss_fixed_usd)) if avg_loss_fixed_usd < 0 else 0.0
        res["payoff_fixed_usd"] = payoff_fixed_usd

        avg_win_fixed_r = self.sum_r_fixed_win / self.n_r_fixed_win if self.n_r_fixed_win > 0 else 0.0
        avg_loss_fixed_r = self.sum_r_fixed_loss / self.n_r_fixed_loss if self.n_r_fixed_loss > 0 else 0.0
        payoff_fixed_r = (avg_win_fixed_r / abs(avg_loss_fixed_r)) if avg_loss_fixed_r < 0 else 0.0
        res["payoff_fixed_r"] = payoff_fixed_r

        if self.n_r > 1:
            mean_r = expectancy_r
            var = (self.sum_r2_managed - self.n_r * mean_r * mean_r) / (self.n_r - 1)
            std_r = math.sqrt(max(var, 0.0))
        else:
            std_r = 0.0
        res["std_r"] = std_r
        res["sharpe"] = (expectancy_r / std_r) if std_r > 1e-9 else 0.0

        res["mdd_usd"] = self.mdd

        res["giveback_avg_usd"] = self.sum_giveback_usd / self.n_giveback_pos if self.n_giveback_pos > 0 else 0.0
        res["giveback_avg_r"] = self.sum_giveback_r / self.n_giveback_r if self.n_giveback_r > 0 else 0.0
        res["giveback_avg_ratio"] = self.sum_giveback_ratio / self.n_giveback_ratio if self.n_giveback_ratio > 0 else 0.0
        res["giveback_share"] = self.n_giveback_pos / self.n if self.n > 0 else 0.0

        res["missed_avg_usd"] = self.sum_missed_usd / self.n_missed_pos if self.n_missed_pos > 0 else 0.0
        res["missed_avg_r"] = self.sum_missed_r / self.n_missed_r if self.n_missed_r > 0 else 0.0
        res["missed_avg_ratio"] = self.sum_missed_ratio / self.n_missed_ratio if self.n_missed_ratio > 0 else 0.0
        res["missed_share"] = self.n_missed_pos / self.n if self.n > 0 else 0.0

        res["mfe_avg_r"] = self.sum_mfe_r / self.n_mfe_r if self.n_mfe_r > 0 else 0.0
        res["mae_avg_r"] = self.sum_mae_r / self.n_mae_r if self.n_mae_r > 0 else 0.0

        res["trailing_share"] = self.n_trailing / self.n if self.n > 0 else 0.0
        res["trailing_close_share"] = (self.n_trailing_closed / self.n_trailing) if self.n_trailing > 0 else 0.0
        res["trailing_wr"] = (self.n_trailing_closed_win / self.n_trailing_closed) if self.n_trailing_closed > 0 else 0.0

        trailing_expectancy_r = self.sum_r_trailing / self.n_r_trailing if self.n_r_trailing > 0 else 0.0
        trailing_expectancy_fixed_r = self.sum_r_fixed_trailing / self.n_r_fixed_trailing if self.n_r_fixed_trailing > 0 else 0.0
        res["trailing_expectancy_r"] = trailing_expectancy_r
        res["trailing_expectancy_fixed_r"] = trailing_expectancy_fixed_r
        res["trailing_delta_expectancy_r"] = trailing_expectancy_r - trailing_expectancy_fixed_r

        # Доли сравнения managed vs baseline
        n_total = float(self.n_r)
        if n_total > 0:
            res["share_better"] = self.better_count / n_total
            res["share_worse"] = self.worse_count / n_total
            res["share_equal"] = self.equal_count / n_total
        else:
            res["share_better"] = 0.0
            res["share_worse"] = 0.0
            res["share_equal"] = 0.0

        return res


EPS = 1e-9
NO_TAG = "__NO_TAG__"


# Using unified TagStats from analytics.tag_stats


def _build_trailing_snapshots_for_group(group_trades: list[dict]) -> list[ClosedTradeSnapshot]:
    snaps: list[ClosedTradeSnapshot] = []
    for t in group_trades:
        try:
            snap = ClosedTradeSnapshot(
                source=str(t.get("source") or t.get("strategy_source") or "Unknown"),
                symbol=str(t.get("symbol") or "UNKNOWN").upper(),
                pnl_net=float(t.get("pnl_net") or 0.0),
                one_r_money=float(t.get("one_r_money") or 0.0),
                mfe_pnl=float(t.get("mfe_pnl") or 0.0),
                giveback=float(t.get("giveback") or 0.0),
                trailing_started=bool(t.get("trailing_started") or t.get("trailing_active") or False),
                trailing_active=bool(t.get("trailing_active") or False),
                exit_ts_ms=int(t.get("exit_ts_ms") or 0),
                entry_tag=str(t.get("entry_tag") or ""),
            )
            snaps.append(snap)
        except Exception:
            continue
    return snaps


def _format_trailing_rec_for_tag(
    source: str,
    symbol: str,
    entry_tag: str,
    snaps: list[ClosedTradeSnapshot],
    stop_atr_mult: float,
    min_trades: int = 30,
    mfe_quantile: float = 0.25,
) -> str:
    """
    Возвращает Markdown-блок с рекомендацией по трейлингу для конкретного entry_tag.
    """
    if not snaps:
        return ""

    rec_all = recommend_trailing_size(
        snaps,
        stop_atr_mult=stop_atr_mult,
        min_trades=min_trades,
        mfe_quantile=mfe_quantile,
        trailing_only=False,
    )
    rec_tr = recommend_trailing_size(
        snaps,
        stop_atr_mult=stop_atr_mult,
        min_trades=max(10, min_trades // 2),
        mfe_quantile=mfe_quantile,
        trailing_only=True,
    )

    lines: list[str] = []
    lines.append(f"- Trailing recommendation for tag `{entry_tag}`:")

    if not rec_all and not rec_tr:
        lines.append("  - недостаточно данных для оценки.\n")
        return "\n".join(lines)

    def fmt(rec, label: str) -> str:
        return (
            f"  - {label}: n_total={rec.sample_size_total}, n_wins={rec.sample_size_win}, "
            f"lock_r≈{rec.lock_r:.2f}R → TP1_OFFSET_ATR≈{rec.trailing_tp1_offset_atr:.2f}; "
            f"MFE_R avg/median≈{rec.avg_mfe_r_win:.2f}/{rec.median_mfe_r_win:.2f}, "
            f"giveback_R≈{rec.avg_giveback_r_win:.2f}, ratio≈{rec.avg_giveback_ratio_win:.2f}, "
            f"confidence≈{rec.confidence:.2f}"
        )

    if rec_all:
        lines.append(fmt(rec_all, "все win-сделки"))
    if rec_tr:
        lines.append(fmt(rec_tr, "только трейлинговые win-сделки"))

    return "\n".join(lines) + "\n"


def _parse_trade(fields: dict) -> dict:
    """
    Универсальный парсер записи из XSTREAM/HASH.
    Если внутри есть json-поле - пытаемся распарсить.
    Иначе считаем, что fields уже есть dict TradeClosed.
    """
    # Вариант 1: всё плоско (как asdict(TradeClosed))
    if "pnl_net" in fields or "symbol" in fields:
        return {k: _try_float_or_str(v) for k, v in fields.items()}

    # Вариант 2: payload/obj как json
    for key in ("data", "payload", "trade", "obj"):
        if key in fields:
            try:
                inner = json.loads(fields[key])
                if isinstance(inner, dict):
                    return inner
            except Exception:
                continue

    # fallback: вернуть как есть
    return {k: _try_float_or_str(v) for k, v in fields.items()}


def _try_float_or_str(v: str):
    try:
        if isinstance(v, (int, float)):
            return v
        if v is None:
            return 0.0
        s = str(v)
        if not s:
            return 0.0
        return float(s)
    except Exception:
        return v


def load_trades_from_redis(r: redis.Redis, limit: int) -> list[dict]:
    """
    Берём последние limit записей из стрима trades:closed в обратном порядке (свежее → старое).
    """
    entries = r.xrevrange("trades:closed", max="+", min="-", count=limit)
    trades: list[dict] = []
    for _id, fields in entries:
        if isinstance(fields, dict):
            t = _parse_trade(fields)
            t["_stream_id"] = _id
            trades.append(t)
    return trades


def analyze_by_entry_tag(
    trades: list[dict],
    source: str | None = None,
    symbol: str | None = None,
    min_trades: int = 5,
    include_untagged: bool = False,
) -> list[dict]:
    buckets: dict[str, TagStats] = {}

    source = (source or "").lower().strip()
    symbol_up = (symbol or "").upper().strip()

    for t_dict in trades:
        t_source = str(t_dict.get("source") or "").lower()
        t_symbol = str(t_dict.get("symbol") or "").upper()

        if source and t_source != source:
            continue
        if symbol_up and t_symbol != symbol_up:
            continue

        entry_tag = str(t_dict.get("entry_tag") or "").strip()
        if not entry_tag:
            entry_tag = NO_TAG

        if entry_tag == NO_TAG and not include_untagged:
            continue

        # Convert dict to Trade object
        trade = Trade(
            source=t_dict.get("source") or "Unknown",
            symbol=t_dict.get("symbol") or "UNKNOWN",
            exit_ts_ms=int(t_dict.get("exit_ts_ms") or 0),
            pnl_net=float(t_dict.get("pnl_net") or 0.0),
            pnl_if_fixed_exit=float(t_dict.get("pnl_if_fixed_exit") or 0.0),
            one_r_money=float(t_dict.get("one_r_money") or 0.0),
            giveback=float(t_dict.get("giveback") or 0.0),
            missed_profit=float(t_dict.get("missed_profit") or 0.0),
            mfe_pnl=float(t_dict.get("mfe_pnl") or 0.0),
            mae_pnl=float(t_dict.get("mae_pnl") or 0.0),
            trailing_started=bool(t_dict.get("trailing_started") or False),
            trailing_active=bool(t_dict.get("trailing_active") or False),
            close_reason=t_dict.get("close_reason") or "",
            close_reason_raw=t_dict.get("close_reason_raw") or "",
            close_reason_detail=t_dict.get("close_reason_detail") or "",
            entry_tag=entry_tag,
            strategy=t_dict.get("strategy") or "",
        )

        bucket = buckets.get(entry_tag)
        if bucket is None:
            bucket = TagStats(entry_tag)
            buckets[entry_tag] = bucket

        bucket.add_trade(trade)

    results: list[dict] = []
    for tag, bucket in buckets.items():  # noqa: B007
        if bucket.n < min_trades:
            continue
        res = bucket.finalize()
        results.append(res)

    results.sort(key=lambda x: x["n"], reverse=True)
    return results


def format_report(results: list[dict], redis_client, source: str, symbol: str | None, trades: list[dict]) -> str:
    if not results:
        return "Нет данных по entry_tag (фильтр всё отфильтровал)."

    lines: list[str] = []
    for row in results:
        tag = row["tag"]
        if tag == NO_TAG:
            tag_disp = "(NO_TAG)"
        else:
            tag_disp = tag

        n = int(row["n"])
        n_fixed = int(row.get("n_fixed", 0))
        n_tr = int(row.get("trailing_trades", 0))

        wr = float(row.get("wr", 0.0)) * 100.0
        exp_r = float(row.get("expectancy_r", 0.0))
        payoff_r = float(row.get("payoff_r", 0.0))
        payoff_usd = float(row.get("payoff_usd", 0.0))

        wr_fixed = float(row.get("wr_fixed", 0.0)) * 100.0
        exp_fixed_r = float(row.get("expectancy_fixed_r", 0.0))
        payoff_fixed_r = float(row.get("payoff_fixed_r", 0.0))
        payoff_fixed_usd = float(row.get("payoff_fixed_usd", 0.0))

        delta_exp = float(row.get("delta_expectancy_r", 0.0))

        trailing_share = float(row.get("trailing_share", 0.0)) * 100.0
        trailing_close_share = float(row.get("trailing_close_share", 0.0)) * 100.0
        trailing_wr = float(row.get("trailing_wr", 0.0)) * 100.0
        trailing_exp = float(row.get("trailing_expectancy_r", 0.0))
        trailing_exp_fixed = float(row.get("trailing_expectancy_fixed_r", 0.0))
        trailing_delta = float(row.get("trailing_delta_expectancy_r", 0.0))

        gb_avg_usd = float(row.get("giveback_avg_usd", 0.0))
        gb_avg_r = float(row.get("giveback_avg_r", 0.0))
        gb_avg_ratio = float(row.get("giveback_avg_ratio", 0.0))
        gb_share = float(row.get("giveback_share", 0.0)) * 100.0

        mp_avg_usd = float(row.get("missed_avg_usd", 0.0))
        mp_avg_r = float(row.get("missed_avg_r", 0.0))
        mp_avg_ratio = float(row.get("missed_avg_ratio", 0.0))
        mp_share = float(row.get("missed_share", 0.0)) * 100.0

        mfe_avg_r = float(row.get("mfe_avg_r", 0.0))
        mae_avg_r = float(row.get("mae_avg_r", 0.0))

        lines.append(f"=== entry_tag: {tag_disp} (n={n}, n_fixed={n_fixed}, n_trailing={n_tr}) ===")
        lines.append(
            f"Managed:   WR={wr:.1f}% | Exp_R={exp_r:+.3f} | "
            f"Payoff(R)={payoff_r:.2f} | Payoff(USD)={payoff_usd:.2f}"
        )
        lines.append(
            f"Baseline:  WR={wr_fixed:.1f}% | Exp_R={exp_fixed_r:+.3f} | "
            f"Payoff(R)={payoff_fixed_r:.2f} | Payoff(USD)={payoff_fixed_usd:.2f}"
        )
        lines.append(f"ΔExp_R (managed - baseline): {delta_exp:+.3f}")

        # Add better/worse shares
        share_better = float(row.get("share_better", 0.0))
        share_worse = float(row.get("share_worse", 0.0))
        lines.append(f"ΔExp_R breakdown: better: {share_better:.1%}, worse: {share_worse:.1%}")

        lines.append(
            f"Trailing:  share={trailing_share:.1f}% | close_share={trailing_close_share:.1f}% | "
            f"WR_trail={trailing_wr:.1f}% | Exp_R_trail={trailing_exp:+.3f} | "
            f"Exp_R_trail_base={trailing_exp_fixed:+.3f} | Δ={trailing_delta:+.3f}"
        )

        lines.append(
            f"Giveback: avg={gb_avg_usd:.2f}$ ({gb_avg_r:.3f}R), "
            f"ratio(avg)={gb_avg_ratio:.2f}, share={gb_share:.1f}%"
        )
        lines.append(
            f"Missed:   avg={mp_avg_usd:.2f}$ ({mp_avg_r:.3f}R), "
            f"ratio(avg)={mp_avg_ratio:.2f}, share={mp_share:.1f}%"
        )
        lines.append(
            f"Excursions: MFE={mfe_avg_r:.2f}R, MAE={mae_avg_r:.2f}R"
        )

        # Добавляем trailing-рекомендации
        try:
            # Группируем trades по этому тегу
            tag_trades = [t for t in trades if str(t.get("entry_tag") or "").strip() == tag]
            if tag_trades:
                # Получаем stop_atr_mult для символа
                symbol_up = (symbol or "").upper().strip()
                if symbol_up:
                    try:
                        from services.pnl_math import get_symbol_info
                        info = get_symbol_info(symbol_up, redis_client) or {}
                        stop_atr_mult = float(info.get("stop_atr_mult", 1.0))
                    except Exception:
                        stop_atr_mult = 1.0
                else:
                    stop_atr_mult = 1.0

                snaps = _build_trailing_snapshots_for_group(tag_trades)
                trailing_md = _format_trailing_rec_for_tag(
                    source=source,
                    symbol=symbol_up or "UNKNOWN",
                    entry_tag=tag,
                    snaps=snaps,
                    stop_atr_mult=stop_atr_mult,
                    min_trades=30,
                    mfe_quantile=0.25,
                )
                if trailing_md:
                    lines.append(trailing_md)
        except Exception as e:
            lines.append(f"- Ошибка при расчёте trailing: {e}\n")

        lines.append("")  # пустая строка между блоками

    return "\n".join(lines)


def main(argv=None) -> int:
    parser = argparse.ArgumentParser(
        description="Анализ baseline vs managed по entry_tag (trades:closed)."
    )
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--source", default=None, help="Фильтр по source (например, CryptoOrderFlow)")
    parser.add_argument("--symbol", default=None, help="Фильтр по symbol (например, BTCUSDT)")
    parser.add_argument("--limit", type=int, default=1000, help="Сколько последних сделок брать из trades:closed")
    parser.add_argument("--min-trades", type=int, default=5, help="Минимум сделок на тег для вывода")
    parser.add_argument("--include-untagged", action="store_true", help="Включать сделки без entry_tag")

    args = parser.parse_args(argv)

    r = redis.from_url(args.redis_url, decode_responses=True)
    trades = load_trades_from_redis(r, limit=args.limit)

    results = analyze_by_entry_tag(
        trades,
        source=args.source,
        symbol=args.symbol,
        min_trades=args.min_trades,
        include_untagged=args.include_untagged,
    )

    print(format_report(results, r, args.source, args.symbol, trades))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
