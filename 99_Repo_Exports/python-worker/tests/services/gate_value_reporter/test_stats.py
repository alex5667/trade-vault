"""Tests for services/gate_value_reporter/stats.py."""

from __future__ import annotations

from services.gate_value_reporter.contracts import GateOutcomeRecord
from services.gate_value_reporter.stats import compute_cohort_stats, compute_gate_lift


def _r(
    *,
    r_mult: float,
    y: int = 0,
    tp_hit: bool = False,
    sl_hit: bool = False,
    ret_bps: float = 0.0,
) -> GateOutcomeRecord:
    return GateOutcomeRecord(
        sid="s",
        cohort="passed",
        symbol="BTCUSDT",
        kind="k",
        side="LONG",
        ts_ms=0,
        horizon_ms=60_000,
        entry_px=0.0,
        tp_bps=15.0,
        sl_bps=10.0,
        ret_bps=ret_bps,
        r_mult=r_mult,
        y=y,
        tp_hit=tp_hit,
        sl_hit=sl_hit,
        outcome_reason="tp" if tp_hit else ("sl" if sl_hit else "timeout"),
    )


def test_compute_cohort_stats_empty() -> None:
    s = compute_cohort_stats([])
    assert s.n == 0
    assert s.win_rate == 0.0
    assert s.profit_factor == 0.0


def test_compute_cohort_stats_basic() -> None:
    rows = [
        _r(r_mult=1.5, y=1, tp_hit=True, ret_bps=15.0),
        _r(r_mult=1.0, y=1, tp_hit=True, ret_bps=10.0),
        _r(r_mult=-1.0, y=0, sl_hit=True, ret_bps=-10.0),
        _r(r_mult=-1.0, y=0, sl_hit=True, ret_bps=-10.0),
        _r(r_mult=0.05, y=0, ret_bps=0.5),  # timeout
    ]
    s = compute_cohort_stats(rows)
    assert s.n == 5
    assert s.win_rate == 0.4
    assert abs(s.avg_r - (1.5 + 1.0 - 1.0 - 1.0 + 0.05) / 5) < 1e-9
    assert s.tp_hit_rate == 0.4
    assert s.sl_hit_rate == 0.4
    assert s.timeout_rate == 0.2
    # PF = (1.5+1.0+0.05) / (1.0+1.0) = 2.55 / 2.0 = 1.275
    assert abs(s.profit_factor - 1.275) < 1e-9
    assert abs(s.avg_ret_bps - 1.1) < 1e-9


def test_compute_cohort_stats_profit_factor_no_losses() -> None:
    rows = [_r(r_mult=1.0, y=1, tp_hit=True), _r(r_mult=2.0, y=1, tp_hit=True)]
    s = compute_cohort_stats(rows)
    assert s.profit_factor == 10.0


def test_compute_cohort_stats_profit_factor_only_losses() -> None:
    rows = [_r(r_mult=-1.0, sl_hit=True), _r(r_mult=-0.5, sl_hit=True)]
    s = compute_cohort_stats(rows)
    assert s.profit_factor == 0.0


def test_compute_gate_lift_directionality() -> None:
    passed = compute_cohort_stats(
        [
            _r(r_mult=1.0, y=1, tp_hit=True),
            _r(r_mult=1.0, y=1, tp_hit=True),
            _r(r_mult=-1.0, sl_hit=True),
        ]
    )
    gated = compute_cohort_stats(
        [
            _r(r_mult=-1.0, sl_hit=True),
            _r(r_mult=-1.0, sl_hit=True),
            _r(r_mult=0.2, y=1, tp_hit=True),
        ]
    )
    lift = compute_gate_lift(passed, gated)
    # passed avg_r=(2-1)/3=0.333; gated=(-2+0.2)/3=-0.6
    assert lift.avg_r_lift > 0
    assert lift.win_rate_lift > 0
    assert lift.sl_hit_rate_reduction > 0  # gated has more SLs
    assert lift.false_negative_rate == gated.win_rate
