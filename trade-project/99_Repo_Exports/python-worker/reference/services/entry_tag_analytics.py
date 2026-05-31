#!/usr/bin/env python
from __future__ import annotations

"""
Edge analytics by entry_tag with baseline pnl_if_fixed_exit.
Provides reusable functions for CLI and programmatic use.
"""


import argparse
import json
import os
from collections.abc import Iterable
from typing import Any

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

from domain.normalizers import canon_source, canon_symbol
from services import analytics_db

try:
    from analysis.trailing_recommender import (
        ClosedTradeSnapshot,
        recommend_trailing_size,
    )
except ImportError:
    ClosedTradeSnapshot = None
    recommend_trailing_size = None

EPS = 1e-9
NO_TAG = "__NO_TAG__"


def _to_float(v, default: float = 0.0) -> float:
    try:
        if isinstance(v, (int, float)):
            return float(v)
        if v is None:
            return default
        s = str(v)
        if not s:
            return default
        return float(s)
    except Exception:
        return default


def _to_str(v) -> str:
    if v is None:
        return ""
    if isinstance(v, (bytes, bytearray)):
        return v.decode("utf-8", errors="ignore")
    return str(v)


class TagStats:
    """Статистика по entry_tag с разделением baseline vs managed."""

    def __init__(self, tag: str):
        self.tag = tag

        # --- managed (фактический PnL) ---
        self.n = 0
        self.wins = 0
        self.losses = 0
        self.be = 0

        self.sum_r = 0.0
        self.sum_r2 = 0.0

        self.sum_win_r = 0.0
        self.sum_loss_r = 0.0

        self.sum_win_usd = 0.0
        self.sum_loss_usd = 0.0

        # --- baseline (fixed exit) ---
        self.n_fixed = 0
        self.fixed_wins = 0
        self.fixed_losses = 0
        self.fixed_be = 0

        self.sum_fixed_r = 0.0
        self.sum_fixed_r2 = 0.0

        self.sum_fixed_win_r = 0.0
        self.sum_fixed_loss_r = 0.0

        self.sum_fixed_win_usd = 0.0
        self.sum_fixed_loss_usd = 0.0

        # --- Счётчики сравнения managed vs baseline ---
        self.better_count = 0  # managed > baseline
        self.worse_count = 0   # managed < baseline
        self.equal_count = 0   # managed ≈ baseline

        # --- Giveback ---
        self.gb_sum = 0.0              # сумма giveback в USD
        self.gb_sum_r = 0.0            # сумма giveback в R
        self.gb_sum_ratio = 0.0        # сумма giveback / MFE
        self.gb_n = 0                  # число сделок с giveback > 0

        # --- Missed profit ---
        self.mp_sum = 0.0              # сумма missed_profit в USD
        self.mp_sum_r = 0.0            # сумма missed_profit в R
        self.mp_sum_ratio = 0.0        # сумма missed_profit / MFE
        self.mp_n = 0                  # число сделок с missed_profit > 0

        # --- Excursions ---
        self.mfe_sum_r = 0.0           # сумма MFE в R
        self.mae_sum_r = 0.0           # сумма MAE в R
        self.mfe_n = 0                 # сделок с валидным MFE
        self.mae_n = 0                 # сделок с валидным MAE

        # --- Trailing usage ---
        self.trades_trailing_started = 0          # трейлинг стартовал
        self.trades_trailing_active_close = 0     # трейлинг активен на момент закрытия
        self.trades_trailing_close = 0            # закрыто по трейлингу
        self.trades_trailing_win = 0              # выигрыши среди трейлинговых закрытий
        self.trades_trailing_loss = 0             # проигрыши среди трейлинговых закрытий

        # --- Baseline vs managed только по трейлинговым сделкам ---
        self.tr_n = 0
        self.tr_sum_r = 0.0
        self.tr_sum_fixed_r = 0.0

    def add_trade(self, t: dict) -> None:
        """
        t — дикт в формате TradeClosed.__dict__/asdict:
        ожидаем поля:
        - pnl_net
        - pnl_if_fixed_exit
        - one_r_money
        - notional_usd
        - r_multiple (опционально, можно пересчитать)
        - giveback, missed_profit, mfe_pnl, mae_pnl
        - trailing_started, trailing_active, close_reason_raw, close_reason_detail
        """
        try:
            pnl_net = _to_float(t.get("pnl_net") or t.get("pnl") or 0.0)
        except Exception:
            pnl_net = 0.0

        try:
            pnl_fixed = _to_float(t.get("pnl_if_fixed_exit") or 0.0)
        except Exception:
            pnl_fixed = 0.0

        try:
            one_r = _to_float(t.get("one_r_money") or 0.0)
        except Exception:
            one_r = 0.0

        try:
            notional = abs(_to_float(t.get("notional_usd") or 0.0))
        except Exception:
            notional = 0.0

        # --- managed / фактический R ---
        try:
            r = _to_float(t.get("r_multiple") or t.get("r") or 0.0)
        except Exception:
            r = 0.0

        if abs(r) < EPS and abs(one_r) > EPS:
            r = pnl_net / one_r

        self.n += 1
        self.sum_r += r
        self.sum_r2 += r * r

        if pnl_net > EPS:
            self.wins += 1
            self.sum_win_r += r
            self.sum_win_usd += pnl_net
        elif pnl_net < -EPS:
            self.losses += 1
            self.sum_loss_r += abs(r)
            self.sum_loss_usd += abs(pnl_net)
        else:
            self.be += 1

        # --- baseline / fixed-exit R ---
        if abs(one_r) > EPS:
            r_fixed = pnl_fixed / one_r
            self.n_fixed += 1
            self.sum_fixed_r += r_fixed
            self.sum_fixed_r2 += r_fixed * r_fixed

            if pnl_fixed > EPS:
                self.fixed_wins += 1
                self.sum_fixed_win_r += r_fixed
                self.sum_fixed_win_usd += pnl_fixed
            elif pnl_fixed < -EPS:
                self.fixed_losses += 1
                self.sum_fixed_loss_r += abs(r_fixed)
                self.sum_fixed_loss_usd += abs(pnl_fixed)
            else:
                self.fixed_be += 1

            # Считаем сравнение managed vs baseline
            delta = r - r_fixed
            eps = 1e-6
            if delta > eps:
                self.better_count += 1
            elif delta < -eps:
                self.worse_count += 1
            else:
                self.equal_count += 1

        # --- улучшенные поля: giveback / missed / MFE / MAE ---
        try:
            giveback = _to_float(t.get("giveback") or 0.0)
        except Exception:
            giveback = 0.0

        try:
            missed_profit = _to_float(t.get("missed_profit") or 0.0)
        except Exception:
            missed_profit = 0.0

        try:
            mfe_pnl = _to_float(t.get("mfe_pnl") or 0.0)
        except Exception:
            mfe_pnl = 0.0

        try:
            mae_pnl = _to_float(t.get("mae_pnl") or 0.0)
        except Exception:
            mae_pnl = 0.0

        # Giveback: считаем только, когда он реально > 0
        if giveback > EPS:
            self.gb_n += 1
            self.gb_sum += giveback
            if abs(one_r) > EPS:
                self.gb_sum_r += giveback / one_r
            if mfe_pnl > EPS:
                self.gb_sum_ratio += giveback / mfe_pnl

        # Missed profit (обычно в SL_AFTER_TP)
        if missed_profit > EPS:
            self.mp_n += 1
            self.mp_sum += missed_profit
            if abs(one_r) > EPS:
                self.mp_sum_r += missed_profit / one_r
            if mfe_pnl > EPS:
                self.mp_sum_ratio += missed_profit / mfe_pnl

        # Excursions в R
        if abs(one_r) > EPS and abs(mfe_pnl) > EPS:
            self.mfe_sum_r += mfe_pnl / one_r
            self.mfe_n += 1

        if abs(one_r) > EPS and abs(mae_pnl) > EPS:
            # mae_pnl обычно <= 0 для long, >=0 для short; берём модуль
            self.mae_sum_r += abs(mae_pnl) / abs(one_r)
            self.mae_n += 1

        # --- Trailing usage / качество ---
        tr_started = False
        tr_active = False

        try:
            v = t.get("trailing_started")
            if v is not None:
                tr_started = bool(int(v))
        except Exception:
            tr_started = bool(t.get("trailing_started"))

        try:
            v = t.get("trailing_active")
            if v is not None:
                tr_active = bool(int(v))
        except Exception:
            tr_active = tr_active or bool(t.get("trailing_active"))

        reason_raw = (t.get("close_reason_raw") or "")
        reason_det = (t.get("close_reason_detail") or "")

        is_trailing_close = (
            "TRAIL" in reason_raw.upper()
            or "TRAIL" in reason_det.upper()
        )

        if tr_started:
            self.trades_trailing_started += 1
        if tr_active:
            self.trades_trailing_active_close += 1

        if is_trailing_close:
            self.trades_trailing_close += 1
            if pnl_net > EPS:
                self.trades_trailing_win += 1
            elif pnl_net < -EPS:
                self.trades_trailing_loss += 1

        # baseline vs managed ТОЛЬКО по трейлинговым сделкам (там, где трейлинг хоть как-то участвовал)
        is_trailing_trade = tr_started or tr_active or is_trailing_close
        if is_trailing_trade:
            self.tr_n += 1
            self.tr_sum_r += r
            if abs(one_r) > EPS:
                self.tr_sum_fixed_r += pnl_fixed / one_r

    def finalize(self) -> dict:
        """Возвращает все метрики по тегу в виде dict."""
        res = {
            "tag": self.tag,
            "n": self.n,
            "wins": self.wins,
            "losses": self.losses,
            "be": self.be,
            "n_fixed": self.n_fixed,
            "fixed_wins": self.fixed_wins,
            "fixed_losses": self.fixed_losses,
            "fixed_be": self.fixed_be,
        }

        # --- managed ---
        if self.n > 0:
            exp_r = self.sum_r / self.n
        else:
            exp_r = 0.0

        total_wl = self.wins + self.losses
        if total_wl > 0:
            wr = self.wins / total_wl  # доля 0–1
        else:
            wr = 0.0

        if self.wins > 0:
            avg_win_r = self.sum_win_r / self.wins
            avg_win_usd = self.sum_win_usd / self.wins
        else:
            avg_win_r = 0.0
            avg_win_usd = 0.0

        if self.losses > 0:
            avg_loss_r = self.sum_loss_r / self.losses
            avg_loss_usd = self.sum_loss_usd / self.losses
        else:
            avg_loss_r = 0.0
            avg_loss_usd = 0.0

        if avg_loss_r > EPS:
            payoff_r = avg_win_r / avg_loss_r
        else:
            payoff_r = 0.0

        if avg_loss_usd > EPS:
            payoff_usd = avg_win_usd / avg_loss_usd
        else:
            payoff_usd = 0.0

        res.update(
            expectancy_r=exp_r,
            wr=wr,
            payoff_r=payoff_r,
            payoff_usd=payoff_usd,
        )

        # --- baseline ---
        if self.n_fixed > 0:
            exp_fixed_r = self.sum_fixed_r / self.n_fixed
        else:
            exp_fixed_r = 0.0

        total_fixed_wl = self.fixed_wins + self.fixed_losses
        if total_fixed_wl > 0:
            wr_fixed = self.fixed_wins / total_fixed_wl
        else:
            wr_fixed = 0.0

        if self.fixed_wins > 0:
            avg_fixed_win_r = self.sum_fixed_win_r / self.fixed_wins
            avg_fixed_win_usd = self.sum_fixed_win_usd / self.fixed_wins
        else:
            avg_fixed_win_r = 0.0
            avg_fixed_win_usd = 0.0

        if self.fixed_losses > 0:
            avg_fixed_loss_r = self.sum_fixed_loss_r / self.fixed_losses
            avg_fixed_loss_usd = self.sum_fixed_loss_usd / self.fixed_losses
        else:
            avg_fixed_loss_r = 0.0
            avg_fixed_loss_usd = 0.0

        if avg_fixed_loss_r > EPS:
            payoff_fixed_r = avg_fixed_win_r / avg_fixed_loss_r
        else:
            payoff_fixed_r = 0.0

        if avg_fixed_loss_usd > EPS:
            payoff_fixed_usd = avg_fixed_win_usd / avg_fixed_loss_usd
        else:
            payoff_fixed_usd = 0.0

        delta_exp_r = exp_r - exp_fixed_r

        res.update(
            wr_fixed=wr_fixed,
            expectancy_fixed_r=exp_fixed_r,
            payoff_fixed_r=payoff_fixed_r,
            payoff_fixed_usd=payoff_fixed_usd,
            delta_expectancy_r=delta_exp_r,
        )

        # --- Giveback aggregated ---
        if self.gb_n > 0:
            giveback_avg_usd = self.gb_sum / self.gb_n
            giveback_avg_r = self.gb_sum_r / self.gb_n
            giveback_avg_ratio = self.gb_sum_ratio / self.gb_n
        else:
            giveback_avg_usd = 0.0
            giveback_avg_r = 0.0
            giveback_avg_ratio = 0.0

        if self.n > 0:
            giveback_share = self.gb_n / self.n
        else:
            giveback_share = 0.0

        res.update(
            giveback_avg_usd=giveback_avg_usd,
            giveback_avg_r=giveback_avg_r,
            giveback_avg_ratio=giveback_avg_ratio,
            giveback_share=giveback_share,
        )

        # --- Missed profit aggregated ---
        if self.mp_n > 0:
            missed_avg_usd = self.mp_sum / self.mp_n
            missed_avg_r = self.mp_sum_r / self.mp_n
            missed_avg_ratio = self.mp_sum_ratio / self.mp_n
        else:
            missed_avg_usd = 0.0
            missed_avg_r = 0.0
            missed_avg_ratio = 0.0

        if self.n > 0:
            missed_share = self.mp_n / self.n
        else:
            missed_share = 0.0

        res.update(
            missed_avg_usd=missed_avg_usd,
            missed_avg_r=missed_avg_r,
            missed_avg_ratio=missed_avg_ratio,
            missed_share=missed_share,
        )

        # --- Excursions aggregated ---
        if self.mfe_n > 0:
            mfe_avg_r = self.mfe_sum_r / self.mfe_n
        else:
            mfe_avg_r = 0.0

        if self.mae_n > 0:
            mae_avg_r = self.mae_sum_r / self.mae_n
        else:
            mae_avg_r = 0.0

        res.update(
            mfe_avg_r=mfe_avg_r,
            mae_avg_r=mae_avg_r,
        )

        # --- Trailing aggregated ---
        if self.n > 0:
            trailing_share = self.trades_trailing_started / self.n
            trailing_close_share = self.trades_trailing_close / self.n
        else:
            trailing_share = 0.0
            trailing_close_share = 0.0

        tr_wl = self.trades_trailing_win + self.trades_trailing_loss
        if tr_wl > 0:
            trailing_wr = self.trades_trailing_win / tr_wl
        else:
            trailing_wr = 0.0

        if self.tr_n > 0:
            trailing_expectancy_r = self.tr_sum_r / self.tr_n
            trailing_expectancy_fixed_r = self.tr_sum_fixed_r / self.tr_n
            trailing_delta_expectancy_r = trailing_expectancy_r - trailing_expectancy_fixed_r
        else:
            trailing_expectancy_r = 0.0
            trailing_expectancy_fixed_r = 0.0
            trailing_delta_expectancy_r = 0.0

        res.update(
            trailing_share=trailing_share,
            trailing_close_share=trailing_close_share,
            trailing_wr=trailing_wr,
            trailing_expectancy_r=trailing_expectancy_r,
            trailing_expectancy_fixed_r=trailing_expectancy_fixed_r,
            trailing_delta_expectancy_r=trailing_delta_expectancy_r,
            trailing_trades=self.tr_n,
        )

        # Доли сравнения managed vs baseline
        n_total = float(self.n_fixed)
        if n_total > 0:
            res.update(
                share_better=self.better_count / n_total,
                share_worse=self.worse_count / n_total,
                share_equal=self.equal_count / n_total,
            )
        else:
            res.update(
                share_better=0.0,
                share_worse=0.0,
                share_equal=0.0,
            )

        return res


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


def _try_float_or_str(v):
    try:
        if isinstance(v, (int, float)):
            return v
        if v is None:
            return 0.0
        s = str(v)
        # пустые строки → 0
        if not s:
            return 0.0
        return float(s)
    except Exception:
        return v


def load_trades_from_redis(r: redis.Redis, limit: int) -> list[dict]:
    """
    Берёт последние limit записей из стрима trades:closed в обратном порядке (свежее → старое).
    """
    # xrevrange: [max, min], max='+' → хвост, count=limit
    entries = r.xrevrange("trades:closed", max="+", min="-", count=limit)
    trades: list[dict] = []
    for _id, fields in entries:
        if isinstance(fields, dict):
            t = _parse_trade(fields)
            t["_stream_id"] = _id
            trades.append(t)
    return trades


def load_trades_from_pg(limit: int) -> list[dict[str, Any]]:
    rows = analytics_db.fetch_trades_closed(limit=limit)
    trades: list[dict[str, Any]] = []
    for row in rows:
        trades.append({
            "source": row.get("source"),
            "symbol": row.get("symbol"),
            "entry_tag": row.get("entry_tag"),
            "pnl_net": row.get("pnl_net"),
            "pnl_if_fixed_exit": row.get("pnl_if_fixed_exit"),
            "one_r_money": row.get("one_r_money"),
            "giveback": row.get("giveback"),
            "missed_profit": row.get("missed_profit"),
            "mfe_pnl": row.get("mfe_pnl"),
            "mae_pnl": row.get("mae_pnl"),
            "trailing_started": row.get("trailing_started"),
            "trailing_active": row.get("trailing_active"),
            "close_reason": row.get("close_reason"),
            "close_reason_raw": row.get("close_reason_raw"),
            "close_reason_detail": row.get("close_reason_detail"),
            "notional_usd": row.get("notional_usd"),
            "exit_ts_ms": row.get("exit_ts_ms"),
        })
    return trades


def load_trades(r, source: str, symbol: str, limit: int = 5000) -> Iterable[dict[str, str]]:
    """
    Берёт последние `limit` записей из trades:closed и фильтрует по source/symbol.
    Совместимость со старым API.
    """
    src = canon_source(source)
    sym = canon_symbol(symbol)

    entries = r.xrevrange("trades:closed", max="+", count=limit) or []
    for _, fields in entries:
        if not fields:
            continue
        t = _parse_trade(fields)

        t_source = canon_source(t.get("source") or t.get("strategy") or "")
        t_symbol = canon_symbol(t.get("symbol") or "")

        if t_source != src or t_symbol != sym:
            continue

        yield t


def analyze_by_entry_tag(
    trades: Iterable[dict[str, Any]],
    source: str | None = None,
    symbol: str | None = None,
    min_trades: int = 5,
    include_untagged: bool = False,
    legacy_format: bool = False,
) -> list[dict] | dict[str, dict[str, Any]]:
    """
    Анализирует сделки по entry_tag с разделением baseline vs managed.
    
    Args:
        trades: Итератор или список сделок
        source: Опциональный фильтр по source
        symbol: Опциональный фильтр по symbol
        min_trades: Минимум сделок на тег для вывода
        include_untagged: Включать сделки без entry_tag
    
    Returns:
        Список словарей с метриками по каждому entry_tag
    """
    buckets: dict[str, TagStats] = {}

    source_norm = canon_source(source) if source else None
    symbol_norm = canon_symbol(symbol).upper() if symbol else None

    for t in trades:
        t_source = canon_source(t.get("source") or t.get("strategy") or "")
        t_symbol = canon_symbol(t.get("symbol") or "").upper()

        if source_norm is not None and t_source != source_norm:
            continue
        if symbol_norm is not None and t_symbol != symbol_norm:
            continue

        entry_tag = (t.get("entry_tag") or "").strip()
        if not entry_tag:
            entry_tag = NO_TAG

        if entry_tag == NO_TAG and not include_untagged:
            continue

        bucket = buckets.get(entry_tag)
        if bucket is None:
            bucket = TagStats(entry_tag)
            buckets[entry_tag] = bucket

        bucket.add_trade(t)

    results: list[dict] = []
    for tag, bucket in buckets.items():
        if bucket.n < min_trades:
            continue
        res = bucket.finalize()
        results.append(res)

    # сортировка по количеству сделок (убывание)
    results.sort(key=lambda x: x["n"], reverse=True)

    # Конвертация в старый формат для обратной совместимости
    if legacy_format:
        legacy_dict: dict[str, dict[str, Any]] = {}
        for res in results:
            tag = res["tag"]
            legacy_dict[tag] = {
                "n": res["n"],
                "wins": res["wins"],
                "losses": res["losses"],
                "sum_pnl": res.get("sum_win_usd", 0.0) - res.get("sum_loss_usd", 0.0),
                "sum_r_net": res.get("expectancy_r", 0.0) * res["n"],
                "sum_r_fixed": res.get("expectancy_fixed_r", 0.0) * res.get("n_fixed", 0),
                "sum_r_mgmt": (res.get("expectancy_r", 0.0) - res.get("expectancy_fixed_r", 0.0)) * res["n"],
                "sum_r_win": res.get("sum_win_r", 0.0),
                "sum_r_loss": res.get("sum_loss_r", 0.0),
                "sum_r_fixed_win": res.get("sum_fixed_win_r", 0.0),
                "sum_r_fixed_loss": res.get("sum_fixed_loss_r", 0.0),
                "n_r": res["n"],
                "wins_fixed": res.get("fixed_wins", 0),
                "losses_fixed": res.get("fixed_losses", 0),
                "n_fixed": res.get("n_fixed", 0),
            }
        return legacy_dict

    return results


def format_report(results: list[dict], trailing_reports: dict[str, str] | None = None) -> str:
    """Форматирует результаты анализа в читаемый текст."""
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

        # Добавляем trailing рекомендации если есть
        if trailing_reports and tag in trailing_reports:
            lines.append(trailing_reports[tag].rstrip())

        lines.append("")  # пустая строка между блоками

    return "\n".join(lines)


def print_tag_stats(per_tag: dict[str, dict[str, Any]], min_trades_per_tag: int = 5) -> None:
    """
    Старый API для совместимости.
    Конвертирует старый формат per_tag в новый и выводит.
    """
    # Конвертируем старый формат в новый
    results = []
    for tag, stats in per_tag.items():
        if tag == "_ALL_":
            continue
        n = stats.get("n", 0)
        if n < min_trades_per_tag:
            continue

        # Создаем TagStats для конвертации
        tag_stats = TagStats(tag)
        # Не можем восстановить полные данные, но можем показать что есть
        results.append({
            "tag": tag,
            "n": n,
            "wins": stats.get("wins", 0),
            "losses": stats.get("losses", 0),
            "be": n - stats.get("wins", 0) - stats.get("losses", 0),
            "n_fixed": stats.get("n_fixed", 0),
            "fixed_wins": stats.get("wins_fixed", 0),
            "fixed_losses": stats.get("losses_fixed", 0),
            "fixed_be": stats.get("n_fixed", 0) - stats.get("wins_fixed", 0) - stats.get("losses_fixed", 0),
            "expectancy_r": stats.get("sum_r_net", 0.0) / max(stats.get("n_r", 1), 1),
            "wr": stats.get("wins", 0) / max(n, 1),
            "payoff_r": 0.0,  # нужно больше данных
            "payoff_usd": 0.0,
            "wr_fixed": stats.get("wins_fixed", 0) / max(stats.get("n_fixed", 1), 1),
            "expectancy_fixed_r": stats.get("sum_r_fixed", 0.0) / max(stats.get("n_fixed", 1), 1),
            "payoff_fixed_r": 0.0,
            "payoff_fixed_usd": 0.0,
            "delta_expectancy_r": (stats.get("sum_r_net", 0.0) / max(stats.get("n_r", 1), 1)) -
                                  (stats.get("sum_r_fixed", 0.0) / max(stats.get("n_fixed", 1), 1)),
        })

    print(format_report(results))


def run_cli() -> None:
    """CLI entry point для анализа entry_tag."""
    parser = argparse.ArgumentParser(
        description="Анализ baseline vs managed по entry_tag (trades:closed)."
    )
    parser.add_argument(
        "--redis-url",
        default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
    )
    parser.add_argument("--source", default=None, help="Фильтр по source (например, CryptoOrderFlow)")
    parser.add_argument("--symbol", default=None, help="Фильтр по symbol (например, BTCUSDT)")
    parser.add_argument(
        "--limit", type=int, default=1000, help="Сколько последних сделок брать из trades:closed"
    )
    parser.add_argument(
        "--min-trades", type=int, default=5, help="Минимум сделок на тег для вывода"
    )
    parser.add_argument(
        "--include-untagged", action="store_true", help="Включать сделки без entry_tag"
    )
    parser.add_argument(
        "--use-pg",
        action="store_true",
        help="Брать сделки из Timescale/Postgres (TRADES_DB_DSN) вместо Redis.",
    )

    args = parser.parse_args()

    if args.use_pg:
        trades = load_trades_from_pg(limit=args.limit)
    else:
        if redis is None:
            raise RuntimeError("Модуль redis не установлен. Установите пакет 'redis'.")
        r = redis.from_url(args.redis_url, decode_responses=True)
        trades = load_trades_from_redis(r, limit=args.limit)

    results = analyze_by_entry_tag(
        trades,
        source=args.source,
        symbol=args.symbol,
        min_trades=args.min_trades,
        include_untagged=args.include_untagged,
    )

    # Анализируем trailing рекомендации
    trailing_reports = analyze_trailing_by_entry_tag(
        trades,
        source=args.source,
        symbol=args.symbol,
        stop_atr_mult=1.0,  # можно параметризовать
        min_trades=args.min_trades,
        mfe_quantile=0.25,
    )

    print(format_report(results, trailing_reports))


# ===============================
# Trailing analysis integration
# ===============================

def _build_trailing_snapshots_for_group(group_trades: list[dict]) -> list[ClosedTradeSnapshot]:
    """Создаёт список ClosedTradeSnapshot из списка dict-ов сделок."""
    if not ClosedTradeSnapshot:
        return []

    snaps = []
    for t in group_trades:
        try:
            snap = ClosedTradeSnapshot(
                source=str(t.get("source") or t.get("strategy_source") or "Unknown"),
                symbol=(t.get("symbol") or "UNKNOWN").upper(),
                pnl_net=float(t.get("pnl_net") or 0.0),
                one_r_money=float(t.get("one_r_money") or 0.0),
                mfe_pnl=float(t.get("mfe_pnl") or 0.0),
                giveback=float(t.get("giveback") or 0.0),
                trailing_started=(t.get("trailing_started") or "0") in ("1", "true", "True"),
                trailing_active=(t.get("trailing_active") or "0") in ("1", "true", "True"),
                exit_ts_ms=int(t.get("exit_ts_ms") or 0),
                entry_tag=(t.get("entry_tag") or ""),
            )
            snaps.append(snap)
        except Exception:
            continue
    return snaps


def analyze_trailing_by_entry_tag(
    trades: Iterable[dict[str, Any]],
    source: str | None = None,
    symbol: str | None = None,
    stop_atr_mult: float = 1.0,
    min_trades: int = 30,
    mfe_quantile: float = 0.25,
) -> dict[str, str]:
    """
    Анализирует trailing рекомендации для каждого entry_tag.

    Returns:
        Dict[tag, markdown_string] с рекомендациями по трейлингу
    """
    if not recommend_trailing_size:
        return {}

    # Группируем сделки по entry_tag
    trades_by_tag: dict[str, list[dict[str, Any]]] = {}
    source_norm = canon_source(source) if source else None
    symbol_norm = canon_symbol(symbol).upper() if symbol else None

    for t in trades:
        t_source = canon_source(t.get("source") or t.get("strategy") or "")
        t_symbol = canon_symbol(t.get("symbol") or "").upper()

        if source_norm is not None and t_source != source_norm:
            continue
        if symbol_norm is not None and t_symbol != symbol_norm:
            continue

        entry_tag = (t.get("entry_tag") or "").strip()
        if not entry_tag:
            entry_tag = NO_TAG

        if entry_tag not in trades_by_tag:
            trades_by_tag[entry_tag] = []
        trades_by_tag[entry_tag].append(t)

    # Анализируем trailing для каждого тега
    trailing_reports: dict[str, str] = {}

    for entry_tag, group_trades in trades_by_tag.items():
        if len(group_trades) < min_trades:
            continue

        # Создаём snapshots для анализа
        snaps = _build_trailing_snapshots_for_group(group_trades)

        if not snaps:
            continue

        # Получаем рекомендации
        md_trail = _format_trailing_rec_for_tag(
            source=source or "",
            symbol=symbol or "",
            entry_tag=entry_tag,
            snaps=snaps,
            stop_atr_mult=stop_atr_mult,
            min_trades=min_trades,
            mfe_quantile=mfe_quantile,
        )

        if md_trail.strip():
            trailing_reports[entry_tag] = md_trail

    return trailing_reports


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
    if not recommend_trailing_size or not snaps:
        return ""

    rec_all = recommend_trailing_size(
        snaps,
        source=source,
        symbol=symbol,
        stop_atr_mult=stop_atr_mult,
        min_trades=min_trades,
        mfe_quantile=mfe_quantile,
        trailing_only=False,
    )
    rec_tr = recommend_trailing_size(
        snaps,
        source=source,
        symbol=symbol,
        stop_atr_mult=stop_atr_mult,
        min_trades=max(10, min_trades // 2),
        mfe_quantile=mfe_quantile,
        trailing_only=True,
    )

    lines = []
    lines.append(f"- Trailing recommendation for tag `{entry_tag}`:")

    if not rec_all and not rec_tr:
        lines.append("  - недостаточно данных для оценки.\n")
        return "\n".join(lines)

    def fmt(rec, label: str) -> str:
        return (
            f"  - {label}: n_total={rec.sample_size}, n_wins={rec.wins_count}, "
            f"lock_r≈{rec.lock_r:.2f}R → TP1_OFFSET_ATR≈{rec.lock_offset_atr:.2f}; "
            f"MFE_R avg/median≈{rec.avg_mfe_r:.2f}/{rec.median_mfe_r:.2f}, "
            f"giveback_R≈{rec.avg_giveback_r:.2f}, ratio≈{rec.avg_giveback_ratio:.2f}, "
            f"confidence≈{rec.confidence:.2f}"
        )

    if rec_all:
        lines.append(fmt(rec_all, "все win-сделки"))
    if rec_tr:
        lines.append(fmt(rec_tr, "только трейлинговые win-сделки"))

    return "\n".join(lines) + "\n"


if __name__ == "__main__":
    run_cli()
