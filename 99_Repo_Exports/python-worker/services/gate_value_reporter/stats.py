"""Cohort statistics and gate-lift math.

All functions are pure: list[GateOutcomeRecord] → dataclass. No I/O.
"""

from __future__ import annotations

import statistics

from services.gate_value_reporter.contracts import (
    CohortStats,
    GateLiftStats,
    GateOutcomeRecord,
)

_EMPTY_COHORT = CohortStats(
    n=0,
    win_rate=0.0,
    avg_r=0.0,
    median_r=0.0,
    p25_r=0.0,
    p75_r=0.0,
    profit_factor=0.0,
    tp_hit_rate=0.0,
    sl_hit_rate=0.0,
    timeout_rate=0.0,
    avg_ret_bps=0.0,
)


def _quantile(xs: list[float], q: float) -> float:
    if not xs:
        return 0.0
    ordered = sorted(xs)
    idx = int(round((len(ordered) - 1) * q))
    idx = max(0, min(idx, len(ordered) - 1))
    return float(ordered[idx])


def compute_cohort_stats(rows: list[GateOutcomeRecord]) -> CohortStats:
    n = len(rows)
    if n == 0:
        return _EMPTY_COHORT

    rs = [r.r_mult for r in rows]
    wins = [x for x in rs if x > 0]
    losses = [x for x in rs if x < 0]

    gross_win = sum(wins)
    gross_loss = abs(sum(losses))
    if gross_loss > 1e-9:
        profit_factor = gross_win / gross_loss
    elif gross_win > 0:
        profit_factor = 10.0  # cap: no losses recorded yet
    else:
        profit_factor = 0.0

    tp_hits = sum(1 for r in rows if r.tp_hit)
    sl_hits = sum(1 for r in rows if r.sl_hit)
    timeouts = sum(1 for r in rows if not r.tp_hit and not r.sl_hit)
    wins_count = sum(1 for r in rows if r.y == 1)

    return CohortStats(
        n=n,
        win_rate=wins_count / n,
        avg_r=sum(rs) / n,
        median_r=statistics.median(rs),
        p25_r=_quantile(rs, 0.25),
        p75_r=_quantile(rs, 0.75),
        profit_factor=profit_factor,
        tp_hit_rate=tp_hits / n,
        sl_hit_rate=sl_hits / n,
        timeout_rate=timeouts / n,
        avg_ret_bps=sum(r.ret_bps for r in rows) / n,
    )


def compute_gate_lift(
    passed: CohortStats,
    gated_out: CohortStats,
) -> GateLiftStats:
    """Differences between cohorts.

    avg_r_lift > 0  → gate is filtering losers (good)
    avg_r_lift < 0  → gate is filtering winners (bad)
    false_negative_rate = win_rate of the gated_out cohort
        (i.e. how often the gate rejected what would have been a winner)
    """
    return GateLiftStats(
        avg_r_lift=passed.avg_r - gated_out.avg_r,
        win_rate_lift=passed.win_rate - gated_out.win_rate,
        profit_factor_lift=passed.profit_factor - gated_out.profit_factor,
        sl_hit_rate_reduction=gated_out.sl_hit_rate - passed.sl_hit_rate,
        false_negative_rate=gated_out.win_rate,
    )
