#!/usr/bin/env python
"""
Анализ baseline vs managed по entry_tag.

Берёт последние N закрытых сделок из Redis-стрима trades:closed,
фильтрует по source / symbol,
группирует по entry_tag,
для каждого entry_tag считает:

Managed — фактические результаты с трейлингом и всей логикой выхода.

Baseline — результаты, как если бы ты закрывал по baseline (SL/TP/time).

ΔExp_R — на сколько стратегия управления улучшает (или ухудшает) чистый edge входа по этому классу сигналов.
"""

import argparse
import json
import os

import redis

EPS = 1e-9
NO_TAG = "__NO_TAG__"


class TagStats:
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

    def add_trade(self, t: dict) -> None:
        """
        t — дикт в формате TradeClosed.__dict__/asdict:
        ожидаем поля:
        - pnl_net
        - pnl_if_fixed_exit
        - one_r_money
        - notional_usd
        - r_multiple (опционально, можно пересчитать)
        """
        try:
            pnl_net = float(t.get("pnl_net") or 0.0)
        except Exception:
            pnl_net = 0.0

        try:
            pnl_fixed = float(t.get("pnl_if_fixed_exit") or 0.0)
        except Exception:
            pnl_fixed = 0.0

        try:
            one_r = float(t.get("one_r_money") or 0.0)
        except Exception:
            one_r = 0.0

        try:
            notional = abs(float(t.get("notional_usd") or 0.0))
        except Exception:
            notional = 0.0

        # --- managed / фактический R ---
        try:
            r = float(t.get("r_multiple") or 0.0)
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
            payoff_fixed_usd = avg_win_usd / avg_loss_usd
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


def _try_float_or_str(v: str):
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
    Берём последние limit записей из стрима trades:closed в обратном порядке (свежее → старое).
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

    for t in trades:
        t_source = (t.get("source") or "").lower()
        t_symbol = (t.get("symbol") or "").upper()

        if source and t_source != source:
            continue
        if symbol_up and t_symbol != symbol_up:
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
    return results


def format_report(results: list[dict]) -> str:
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

        wr = float(row.get("wr", 0.0)) * 100.0
        exp_r = float(row.get("expectancy_r", 0.0))
        payoff_r = float(row.get("payoff_r", 0.0))
        payoff_usd = float(row.get("payoff_usd", 0.0))

        wr_fixed = float(row.get("wr_fixed", 0.0)) * 100.0
        exp_fixed_r = float(row.get("expectancy_fixed_r", 0.0))
        payoff_fixed_r = float(row.get("payoff_fixed_r", 0.0))
        payoff_fixed_usd = float(row.get("payoff_fixed_usd", 0.0))

        delta_exp = float(row.get("delta_expectancy_r", 0.0))

        lines.append(f"=== entry_tag: {tag_disp} (n={n}, n_fixed={n_fixed}) ===")
        lines.append(
            f"Managed:   WR={wr:.1f}% | Exp_R={exp_r:+.3f} | "
            f"Payoff(R)={payoff_r:.2f} | Payoff(USD)={payoff_usd:.2f}"
        )
        lines.append(
            f"Baseline:  WR={wr_fixed:.1f}% | Exp_R={exp_fixed_r:+.3f} | "
            f"Payoff(R)={payoff_fixed_r:.2f} | Payoff(USD)={payoff_fixed_usd:.2f}"
        )
        lines.append(f"ΔExp_R (managed - baseline): {delta_exp:+.3f}")
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

    print(format_report(results))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
