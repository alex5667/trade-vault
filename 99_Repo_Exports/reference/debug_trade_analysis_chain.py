#!/usr/bin/env python
# debug_trade_analysis_chain.py

from __future__ import annotations

import logging
import sys
import types
from typing import Any, Dict, List

# Provide a lightweight stub for common.log to satisfy imports without full app context.
if "common.log" not in sys.modules:
    log_module = types.ModuleType("common.log")

    def setup_logger(name: str = "app", level: int = logging.INFO):
        logging.basicConfig(level=level, format="%(asctime)s [%(levelname)s] %(message)s")
        return logging.getLogger(name)

    def get_logger(name: str = "app"):
        return logging.getLogger(name)

    log_module.setup_logger = setup_logger
    log_module.get_logger = get_logger
    sys.modules["common"] = types.ModuleType("common")
    sys.modules["common.log"] = log_module

from services.periodic_reporter import PeriodicReporter, EPS
from services.trade_metrics_service import TradeMetricsService
from domain.normalizers import bucket_close_reason


class TestReporter(PeriodicReporter):
    """
    Облегчённый репортёр:
    - НЕ вызывает PeriodicReporter.__init__ (нет Redis/Telegram)
    - даёт доступ к _accumulate_trade_metrics и tm (TradeMetricsService)
    """

    def __init__(self, eps: float = EPS):
        # Важно: не вызываем super().__init__()
        self.tm = TradeMetricsService(eps=eps)


def build_empty_metrics(tm: TradeMetricsService) -> Dict[str, Any]:
    """
    Инициализация m через TradeMetricsService.new_metrics(),
    который возвращает все необходимые поля.
    """
    return tm.new_metrics()


def make_test_trades() -> List[Dict[str, str]]:
    """
    Набор тестовых сделок, специально подобранный для проверки цепочки анализа.
    Все значения – строки (как после Redis/_norm_map).
    """

    # 1) Win через TRAILING_PROFIT -> должен уйти в bucket TP, win_strict
    t1 = {
        "order_id": "T1_TRAILING_PROFIT",
        "pnl": "50.0",          # net
        "pnl_gross": "60.0",
        "fees": "-10.0",
        "notional_usd": "1000.0",
        "pnl_pct": "5.0",       # 5%
        "close_reason_raw": "TRAILING_PROFIT",
        "duration_ms": "120000",
        "tp1_hit": "1",         # TP1 был
        "tp2_hit": "0",
        "tp3_hit": "0",
        "closed_time": "1730000000000",  # ms
        "r_multiple": "1.5",
        "mfe_pnl": "80.0",
        "missed_profit": "5.0",
        "trailing_started": "1",
    }

    # 2) SL после TP1: SL strict, tp1_then_sl++
    t2 = {
        "order_id": "T2_SL_AFTER_TP1",
        "pnl": "-30.0",
        "pnl_gross": "-25.0",
        "fees": "-5.0",
        "notional_usd": "1000.0",
        "pnl_pct": "-3.0",
        "close_reason_raw": "SL_AFTER_TP1",
        "duration_ms": "60000",
        "tp1_hit": "1",
        "tp2_hit": "0",
        "tp3_hit": "0",
        "closed_time": "1730000060000",
        "r_multiple": "-1.0",
        "mfe_pnl": "0.0",
        "missed_profit": "0.0",
        "trailing_started": "0",
    }

    # 3) Обычный SL без TP: чистый loss strict
    t3 = {
        "order_id": "T3_PURE_SL",
        "pnl": "-20.0",
        "pnl_gross": "-18.0",
        "fees": "-2.0",
        "notional_usd": "500.0",
        "pnl_pct": "-4.0",
        "close_reason_raw": "SL",
        "duration_ms": "30000",
        "tp1_hit": "0",
        "tp2_hit": "0",
        "tp3_hit": "0",
        "closed_time": "1730000120000",
        "r_multiple": "-0.8",
        "mfe_pnl": "0.0",
        "missed_profit": "0.0",
        "trailing_started": "0",
    }

    # 4) MANUAL_CLOSE с профитом:
    #    net: win, strict: BE (bucket не TP/SL/TRAILING_STOP)
    t4 = {
        "order_id": "T4_MANUAL_CLOSE",
        "pnl": "10.0",
        "pnl_gross": "11.0",
        "fees": "-1.0",
        "notional_usd": "500.0",
        "pnl_pct": "2.0",
        "close_reason_raw": "MANUAL_CLOSE",
        "duration_ms": "45000",
        "tp1_hit": "0",
        "tp2_hit": "0",
        "tp3_hit": "0",
        "closed_time": "1730000180000",
        "r_multiple": "0.5",
        "mfe_pnl": "15.0",
        "missed_profit": "0.0",
        "trailing_started": "0",
    }

    return [t1, t2, t3, t4]


def main() -> None:
    reporter = TestReporter()
    m = build_empty_metrics(reporter.tm)

    trades = make_test_trades()

    print("=== Пер-сделочный проход по цепочке анализа ===\n")

    for i, t in enumerate(trades, start=1):
        raw_reason = (
            t.get("close_reason_raw")
            or t.get("close_reason")
            or t.get("close_reason_norm")
            or ""
        )
        bucket_direct = bucket_close_reason(raw_reason)

        print(f"--- Сделка #{i} ---")
        print(f"order_id        : {t.get('order_id')}")
        print(f"pnl (net)       : {t.get('pnl')}")
        print(f"close_reason_raw: {raw_reason}")
        print(f"bucket_close_reason(direct): {bucket_direct}")

        # Прогоняем через ту же логику, что и в PeriodicReporter
        # Сохраняем состояние до накопления для показа bucket из _accumulate_trade_metrics
        wins_strict_before = m["wins_strict"]
        losses_strict_before = m["losses_strict"]
        breakeven_strict_before = m["breakeven_strict"]

        reporter._accumulate_trade_metrics(m, t)

        # Определяем какой bucket был использован по изменению счетчиков
        wins_strict_after = m["wins_strict"]
        losses_strict_after = m["losses_strict"]
        breakeven_strict_after = m["breakeven_strict"]

        if wins_strict_after > wins_strict_before:
            bucket_used = "TP"
        elif losses_strict_after > losses_strict_before:
            bucket_used = "SL/TRAILING_STOP"
        elif breakeven_strict_after > breakeven_strict_before:
            bucket_used = "BE"
        else:
            bucket_used = "unknown"

        print(f"bucket_used_in_accumulate: {bucket_used}")

        print(
            f"Net W/L/BE       : {m['wins']}/{m['losses']}/{m['breakeven']}"
        )
        print(
            f"Strict W/L/BE    : {m['wins_strict']}/{m['losses_strict']}/{m['breakeven_strict']}"
        )
        print(f"TP1/2/3 hits     : {m['tp1_hits']}/{m['tp2_hits']}/{m['tp3_hits']}")
        print(f"TP1/2/3 then SL  : {m['tp1_then_sl']}/{m['tp2_then_sl']}/{m['tp3_then_sl']}")
        print(f"Trailing started : {m['trailing_started']}")
        print(f"Trailing stops   : {m['trailing_stop_hits']}")
        print()

    print("=== Финализация TradeMetricsService ===\n")
    reporter.tm.finalize(m)

    total = m["total_trades"]
    print(f"Всего сделок     : {total}")
    print(
        f"Net W/L/BE       : {m['wins']}/{m['losses']}/{m['breakeven']}"
    )
    print(
        f"Strict W/L/BE    : {m['wins_strict']}/{m['losses_strict']}/{m['breakeven_strict']}"
    )
    print(f"Total PnL (net)  : {m['total_pnl']:+.2f}")
    print(f"Total PnL pct    : {m['total_pnl_pct']:+.3f}")
    print(f"Total fees       : {m['total_fees']:+.2f}")
    print()

    print("=== Edge / Risk ===")
    print(f"Expectancy R     : {m.get('expectancy_r', 0):+.3f}")
    print(f"Payoff(R)        : {m.get('payoff_r', 0):.3f}")
    print(f"Payoff(USD)      : {m.get('payoff_pnl', 0):.3f}")
    print(f"Kelly(R)         : {m.get('kelly_r', 0):.3f}")
    print(f"Std(R)           : {m.get('std_r', 0):.3f}")
    print(f"Std(ret)         : {m.get('std_ret', 0):.6f}")
    print(f"Sharpe*          : {m.get('sharpe_like', 0):.2f}")
    print(f"Sortino*         : {m.get('sortino_like', 0):.2f}")
    print(f"MDD (USD)        : {m.get('max_drawdown', 0):.2f}")
    print(f"Win/Loss streaks : {m.get('max_win_streak', 0)}/{m.get('max_loss_streak', 0)}")
    print()

    print("=== Execution / Exits ===")
    print(f"ExitEff(win) avg : {m.get('exit_eff_win_avg', 0):.2f} (n={m.get('exit_eff_win_n', 0)})")
    print(
        f"Giveback total   : {m.get('giveback_total', 0):.2f}, "
        f"avg ratio={m.get('giveback_ratio_avg', 0):.2f} (n={m.get('giveback_trades', 0)})"
    )
    print(
        f"Missed total     : {m.get('missed_profit_total', 0):.2f}, "
        f"avg ratio={m.get('missed_profit_ratio_avg', 0):.2f} (n={m.get('missed_profit_trades', 0)})"
    )
    print()

    print("=== Reasons buckets ===")
    for reason, cnt in sorted(m.get("reasons", {}).items(), key=lambda kv: kv[1], reverse=True):
        print(f"{reason or '<EMPTY>'}: {cnt}")


if __name__ == "__main__":
    main()

