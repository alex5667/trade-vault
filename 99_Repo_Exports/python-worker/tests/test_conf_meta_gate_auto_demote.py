"""Plan 1 Phase 7 — auto-demote watcher tests.

Exercise the pure stats functions and the sustained-negative state machine.
The DB and Redis paths are integration-tested separately; here we ensure
the thresholds fire on the right inputs.
"""
from __future__ import annotations

from orderflow_services.conf_meta_gate_auto_demote_v1 import (
    _SustainedNegativeMonitor,
    compute_canary_top_pct_expectancy,
    compute_disagreement_rate,
    compute_fallback_rate,
)


# ── fallback rate ───────────────────────────────────────────────────────────


def test_fallback_rate_zero_when_no_fallbacks() -> None:
    rows = [{"meta_decision": "ALLOW"}, {"meta_decision": "SHADOW_DENY"}]
    rate, n = compute_fallback_rate(rows)
    assert rate == 0.0
    assert n == 2


def test_fallback_rate_one_when_all_fallback() -> None:
    rows = [{"meta_decision": "FALLBACK_LEGACY"} for _ in range(10)]
    rate, n = compute_fallback_rate(rows)
    assert rate == 1.0
    assert n == 10


def test_fallback_rate_partial() -> None:
    rows = [
        {"meta_decision": "FALLBACK_LEGACY"},
        {"meta_decision": "ALLOW"},
        {"meta_decision": "ALLOW"},
        {"meta_decision": "ALLOW"},
    ]
    rate, n = compute_fallback_rate(rows)
    assert rate == 0.25
    assert n == 4


def test_fallback_rate_empty() -> None:
    assert compute_fallback_rate([]) == (0.0, 0)


# ── disagreement rate ──────────────────────────────────────────────────────


def test_disagreement_excludes_fallbacks() -> None:
    rows = [
        {"legacy_decision": "DENY", "meta_decision": "FALLBACK_LEGACY"},
        {"legacy_decision": "DENY", "meta_decision": "FALLBACK_LEGACY"},
        {"legacy_decision": "DENY", "meta_decision": "ALLOW"},
        {"legacy_decision": "ALLOW", "meta_decision": "ALLOW"},
    ]
    rate, n = compute_disagreement_rate(rows)
    # only 2 non-fallback rows; 1 disagreement.
    assert n == 2
    assert rate == 0.5


def test_disagreement_zero_when_perfectly_agree() -> None:
    rows = [
        {"legacy_decision": "ALLOW", "meta_decision": "ALLOW"},
        {"legacy_decision": "ALLOW", "meta_decision": "ALLOW"},
        {"legacy_decision": "DENY", "meta_decision": "DENY_SOFT"},
    ]
    rate, n = compute_disagreement_rate(rows)
    assert rate == 0.0
    assert n == 3


def test_disagreement_collapses_meta_variants() -> None:
    rows = [
        # All these meta decisions collapse to "ALLOW".
        {"legacy_decision": "ALLOW", "meta_decision": "ALLOW"},
        {"legacy_decision": "ALLOW", "meta_decision": "ALLOW_TIGHTENED"},
        {"legacy_decision": "ALLOW", "meta_decision": "SHADOW_ALLOW"},
    ]
    rate, _ = compute_disagreement_rate(rows)
    assert rate == 0.0


# ── canary expectancy ─────────────────────────────────────────────────────


def test_canary_expectancy_returns_none_under_min_n() -> None:
    rows = [{"p_win_calibrated": 0.6, "realized_r": 1.0} for _ in range(10)]
    val, n = compute_canary_top_pct_expectancy(rows, top_pct=0.1, min_n=50)
    assert val is None
    assert n == 10


def test_canary_expectancy_top_slice_picks_highest_p() -> None:
    rows = [
        {"p_win_calibrated": 0.9, "realized_r": 1.0},
        {"p_win_calibrated": 0.85, "realized_r": 2.0},
        {"p_win_calibrated": 0.1, "realized_r": -1.0},
        {"p_win_calibrated": 0.2, "realized_r": -1.0},
        {"p_win_calibrated": 0.3, "realized_r": -1.0},
    ]
    val, n = compute_canary_top_pct_expectancy(rows, top_pct=0.4, min_n=2)
    assert n == 5
    # top 40% of 5 = 2 rows → (0.9, 1.0) and (0.85, 2.0) → avg 1.5
    assert val == 1.5


def test_canary_expectancy_negative_when_top_loses() -> None:
    rows = [
        {"p_win_calibrated": 0.9, "realized_r": -2.0},
        {"p_win_calibrated": 0.85, "realized_r": -1.0},
        {"p_win_calibrated": 0.1, "realized_r": 1.0},
        {"p_win_calibrated": 0.2, "realized_r": 1.0},
    ]
    val, _ = compute_canary_top_pct_expectancy(rows, top_pct=0.5, min_n=2)
    assert val is not None
    assert val < 0


def test_canary_expectancy_drops_missing_fields() -> None:
    rows = [
        {"p_win_calibrated": 0.9, "realized_r": None},
        {"p_win_calibrated": None, "realized_r": 1.0},
        {"p_win_calibrated": 0.5, "realized_r": 0.5},
        {"p_win_calibrated": 0.6, "realized_r": 1.5},
    ]
    val, n = compute_canary_top_pct_expectancy(rows, top_pct=0.5, min_n=2)
    # Only 2 eligible rows survive the filter.
    assert n == 2
    assert val is not None


# ── sustained-negative monitor ──────────────────────────────────────────────


def test_sustained_negative_does_not_fire_under_streak() -> None:
    mon = _SustainedNegativeMonitor(threshold=0.5, sustain_scans=3)
    streak, fired = mon.evaluate(0.6)
    assert streak == 1 and not fired
    streak, fired = mon.evaluate(0.6)
    assert streak == 2 and not fired


def test_sustained_negative_fires_on_third_violation() -> None:
    mon = _SustainedNegativeMonitor(threshold=0.5, sustain_scans=3)
    mon.evaluate(0.6)
    mon.evaluate(0.6)
    streak, fired = mon.evaluate(0.6)
    assert streak == 3 and fired


def test_sustained_negative_resets_when_value_drops_below() -> None:
    mon = _SustainedNegativeMonitor(threshold=0.5, sustain_scans=3)
    mon.evaluate(0.6)
    mon.evaluate(0.6)
    streak, fired = mon.evaluate(0.4)
    assert streak == 0 and not fired


def test_sustained_negative_handles_none() -> None:
    mon = _SustainedNegativeMonitor(threshold=0.5, sustain_scans=2)
    mon.evaluate(0.6)
    streak, fired = mon.evaluate(None)
    assert streak == 0 and not fired
