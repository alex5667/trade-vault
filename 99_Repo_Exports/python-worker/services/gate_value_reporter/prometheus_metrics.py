"""Prometheus gauges for gate_value_reporter.

All metrics labelled by (kind, symbol, horizon_ms) so we can break down
gate effectiveness per cohort group. Decision label is a string enum
emitted as gauge=1 only for the current decision (others stay at 0 via
absence — Prometheus convention).
"""

from __future__ import annotations

from prometheus_client import Gauge

_LABELS = ("kind", "symbol", "horizon")

# Cohort sizes
gate_value_passed_n = Gauge(
    "gate_value_passed_n",
    "Number of passed-cohort outcomes in window",
    _LABELS,
)
gate_value_gated_out_n = Gauge(
    "gate_value_gated_out_n",
    "Number of gated_out-cohort outcomes in window",
    _LABELS,
)

# Cohort means
gate_value_passed_avg_r = Gauge(
    "gate_value_passed_avg_r",
    "Mean r_mult of passed cohort",
    _LABELS,
)
gate_value_gated_out_avg_r = Gauge(
    "gate_value_gated_out_avg_r",
    "Mean r_mult of gated_out cohort",
    _LABELS,
)
gate_value_passed_win_rate = Gauge(
    "gate_value_passed_win_rate",
    "Win rate (y==1) of passed cohort",
    _LABELS,
)
gate_value_gated_out_win_rate = Gauge(
    "gate_value_gated_out_win_rate",
    "Win rate (y==1) of gated_out cohort",
    _LABELS,
)
gate_value_passed_profit_factor = Gauge(
    "gate_value_passed_profit_factor",
    "Profit factor of passed cohort (sum_wins / |sum_losses|)",
    _LABELS,
)
gate_value_gated_out_profit_factor = Gauge(
    "gate_value_gated_out_profit_factor",
    "Profit factor of gated_out cohort",
    _LABELS,
)

# Lift
gate_value_avg_r_lift = Gauge(
    "gate_value_avg_r_lift",
    "passed.avg_r − gated_out.avg_r (>0 = gate helps)",
    _LABELS,
)
gate_value_win_rate_lift = Gauge(
    "gate_value_win_rate_lift",
    "passed.win_rate − gated_out.win_rate",
    _LABELS,
)
gate_value_profit_factor_lift = Gauge(
    "gate_value_profit_factor_lift",
    "passed.profit_factor − gated_out.profit_factor",
    _LABELS,
)
gate_value_false_negative_rate = Gauge(
    "gate_value_false_negative_rate",
    "Win rate of gated_out cohort (gate-rejected would-be winners)",
    _LABELS,
)

# Bootstrap CI
gate_value_avg_r_lift_ci_low = Gauge(
    "gate_value_avg_r_lift_ci_low",
    "Bootstrap p05 of avg_r_lift",
    _LABELS,
)
gate_value_avg_r_lift_ci_high = Gauge(
    "gate_value_avg_r_lift_ci_high",
    "Bootstrap p95 of avg_r_lift",
    _LABELS,
)

# Decision
gate_value_decision = Gauge(
    "gate_value_decision",
    "Current decision per group (1 for active enum value)",
    (*_LABELS, "decision"),
)

# Reporter liveness
gate_value_report_age_seconds = Gauge(
    "gate_value_report_age_seconds",
    "Seconds since last report cycle completed",
)
gate_value_reporter_up = Gauge(
    "gate_value_reporter_up",
    "1 if reporter loop produced a report on its last iteration",
)
gate_value_cycle_duration_seconds = Gauge(
    "gate_value_cycle_duration_seconds",
    "Wallclock duration of the last report cycle",
)
