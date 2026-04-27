from __future__ import annotations

import math
from dataclasses import dataclass
from typing import Dict


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
    strong_gate_ok: bool = False  # NEW: сильный/слабый сигнал


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
    eq_fixed: float = 0.0
    peak_fixed: float = 0.0
    mdd_fixed: float = 0.0

    # Sharpe/Sortino baseline & downside
    sum_r2_baseline: float = 0.0
    sum_r_downside_managed2: float = 0.0
    sum_r_downside_baseline2: float = 0.0

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
            if r_m < 0:
                self.sum_r_downside_managed2 += r_m * r_m

            self.n_r_fixed += 1
            self.sum_r_baseline += r_b
            self.sum_r2_baseline += r_b * r_b
            if r_b < 0:
                self.sum_r_downside_baseline2 += r_b * r_b

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
        # Прямое совпадение или наличие ключевого слова
        if any(x in cr for x in ("TRAIL", "TRAILING", "SL_AFTER", "MOVED_SL", "LOCK")):
            is_trailing_close = True
        elif any(x in crd for x in ("TRAIL", "TRAILING", "SL_AFTER", "MOVED_SL", "LOCK")):
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

        self.eq_fixed += t.pnl_if_fixed_exit
        if self.eq_fixed > self.peak_fixed:
            self.peak_fixed = self.eq_fixed
        dd_f = self.peak_fixed - self.eq_fixed
        if dd_f > self.mdd_fixed:
            self.mdd_fixed = dd_f

    def finalize(self) -> Dict[str, float]:
        if self.n == 0:
            return {"tag": self.tag, "n": 0.0}

        res: Dict[str, float] = {"tag": self.tag, "n": float(self.n)}

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

            # Sortino managed
            downside_var = self.sum_r_downside_managed2 / self.n_r
            downside_std_r = math.sqrt(max(downside_var, 0.0))
            res["sortino_r"] = (expectancy_r / downside_std_r) if downside_std_r > 1e-9 else 0.0
        else:
            std_r = 0.0
            res["sortino_r"] = 0.0

        res["std_r"] = std_r
        res["sharpe"] = (expectancy_r / std_r) if std_r > 1e-9 else 0.0

        if self.n_r_fixed > 1:
            mean_fixed_r = expectancy_fixed_r
            var_fixed = (self.sum_r2_baseline - self.n_r_fixed * mean_fixed_r * mean_fixed_r) / (self.n_r_fixed - 1)
            std_fixed_r = math.sqrt(max(var_fixed, 0.0))

            # Sortino baseline
            downside_fixed_var = self.sum_r_downside_baseline2 / self.n_r_fixed
            downside_fixed_std_r = math.sqrt(max(downside_fixed_var, 0.0))
            res["sortino_fixed_r"] = (expectancy_fixed_r / downside_fixed_std_r) if downside_fixed_std_r > 1e-9 else 0.0
            res["sharpe_fixed_r"] = (expectancy_fixed_r / std_fixed_r) if std_fixed_r > 1e-9 else 0.0
        else:
            res["sortino_fixed_r"] = 0.0
            res["sharpe_fixed_r"] = 0.0

        res["mdd_usd"] = self.mdd
        res["mdd_baseline_usd"] = self.mdd_fixed

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

        # Compatibility aliases for PeriodicReporter
        res["wr"] = res["wr_managed"]
        res["wr_fixed"] = res["wr_baseline"]
        res["expectancy_managed_r"] = res["expectancy_r"]
        res["expectancy_baseline_r"] = res["expectancy_fixed_r"]
        res["sharpe_r"] = res["sharpe"]
        # res["sortino_r"] already set above
        res["mdd_net_usd"] = res["mdd_usd"]
        # res["mdd_baseline_usd"] already set above

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

