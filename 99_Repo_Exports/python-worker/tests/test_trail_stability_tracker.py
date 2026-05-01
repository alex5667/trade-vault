from __future__ import annotations
"""
Tests for TrailStabilityTracker — callback CV + confidence trend.

These tests use mock data and do NOT require Redis.
"""

import pytest

from services.trail_stability_tracker import (
    _cv_pct,
    _linear_trend,
    compute_stability,
    RunSnapshot,
    StabilityReport,
    TrailStabilityTracker,
)


# ---------------------------------------------------------------------------
# _cv_pct (coefficient of variation)
# ---------------------------------------------------------------------------

class TestCvPct:
    def test_identical_values_zero_cv(self):
        assert _cv_pct([1.5, 1.5, 1.5, 1.5]) == pytest.approx(0.0)

    def test_some_variation(self):
        result = _cv_pct([1.0, 1.1, 0.9, 1.05, 0.95])
        assert 0 < result < 20  # expect ~7-8% CV

    def test_single_value(self):
        assert _cv_pct([5.0]) == 0.0

    def test_empty(self):
        assert _cv_pct([]) == 0.0

    def test_high_variation(self):
        result = _cv_pct([1.0, 3.0, 0.5, 2.5])
        assert result > 40


# ---------------------------------------------------------------------------
# _linear_trend
# ---------------------------------------------------------------------------

class TestLinearTrend:
    def test_rising_trend(self):
        assert _linear_trend([0.5, 0.6, 0.7, 0.8, 0.9]) == "rising"

    def test_falling_trend(self):
        assert _linear_trend([0.9, 0.8, 0.7, 0.6, 0.5]) == "falling"

    def test_flat_trend(self):
        assert _linear_trend([0.7, 0.71, 0.69, 0.70, 0.71]) == "flat"

    def test_too_few_values(self):
        assert _linear_trend([0.5, 0.9]) == "flat"

    def test_empty(self):
        assert _linear_trend([]) == "flat"


# ---------------------------------------------------------------------------
# compute_stability (pure function)
# ---------------------------------------------------------------------------

def _snap(ts: int, cb: float, conf: float, n: int = 100) -> RunSnapshot:
    return RunSnapshot(
        run_ts_ms=ts,
        callback_atr_mult=cb,
        activate_offset_bps=5.0,
        min_profit_lock_r=0.1,
        confidence=conf,
        n_total=n,
    )


class TestComputeStability:
    def test_insufficient_runs(self):
        """n_runs < min_runs → is_stable=False."""
        snaps = [_snap(1000 * i, 1.5, 0.6) for i in range(3)]
        report = compute_stability(snaps, min_runs=6, max_cv_pct=15, symbol="BTCUSDT", regime="na")
        assert report.is_stable is False
        assert report.n_runs == 3

    def test_high_cv_unstable(self):
        """High callback variation → is_stable=False."""
        # callbacks vary a lot: 1.0, 2.0, 0.5, 2.5, 1.5, 3.0
        snaps = [
            _snap(1000 * i, cb, 0.6)
            for i, cb in enumerate([1.0, 2.0, 0.5, 2.5, 1.5, 3.0])
        ]
        report = compute_stability(snaps, min_runs=6, max_cv_pct=15, symbol="BTCUSDT", regime="na")
        assert report.is_stable is False
        assert report.callback_cv_pct > 15

    def test_falling_confidence_unstable(self):
        """Falling confidence trend → is_stable=False."""
        snaps = [_snap(1000 * i, 1.5, conf) for i, conf in enumerate([0.9, 0.85, 0.75, 0.65, 0.55, 0.45])]
        report = compute_stability(snaps, min_runs=6, max_cv_pct=15, symbol="BTCUSDT", regime="na")
        assert report.is_stable is False
        assert report.conf_trend == "falling"

    def test_all_good_stable(self):
        """Stable callbacks + flat/rising confidence → is_stable=True."""
        snaps = [
            _snap(i * 21600_000, 1.50 + (i * 0.01), 0.60 + (i * 0.005))
            for i in range(8)
        ]
        report = compute_stability(snaps, min_runs=6, max_cv_pct=15, symbol="BTCUSDT", regime="na")
        assert report.is_stable is True
        assert report.n_runs == 8
        assert report.callback_cv_pct < 15
        assert report.conf_trend in ("rising", "flat")

    def test_empty_snapshots(self):
        report = compute_stability([], min_runs=6, max_cv_pct=15, symbol="BTCUSDT", regime="na")
        assert report.is_stable is False
        assert report.n_runs == 0

    def test_days_observed(self):
        """Verify days_observed calculation."""
        day_ms = 86400 * 1000
        snaps = [_snap(i * day_ms, 1.5, 0.6) for i in range(6)]
        report = compute_stability(snaps, min_runs=6, max_cv_pct=15, symbol="ETHUSDT", regime="na")
        assert report.days_observed == pytest.approx(5.0)


# ---------------------------------------------------------------------------
# Telegram report formatting
# ---------------------------------------------------------------------------

class TestStabilityTelegramReport:
    def test_empty_reports(self):
        assert TrailStabilityTracker.format_telegram_report([]) == ""

    def test_report_content(self):
        reports = [
            StabilityReport(
                symbol="BTCUSDT", regime="na", n_runs=8,
                callback_cv_pct=5.2, conf_trend="flat",
                is_stable=True, min_callback=1.4, max_callback=1.6,
                latest_confidence=0.65, first_run_ts_ms=1000,
                latest_run_ts_ms=100000, days_observed=3.5,
            ),
            StabilityReport(
                symbol="ARBUSDT", regime="na", n_runs=3,
                callback_cv_pct=22.0, conf_trend="falling",
                is_stable=False, min_callback=0.8, max_callback=1.5,
                latest_confidence=0.52, first_run_ts_ms=1000,
                latest_run_ts_ms=50000, days_observed=1.0,
            ),
        ]
        text = TrailStabilityTracker.format_telegram_report(reports)
        assert "Stability Assessment" in text
        assert "BTCUSDT" in text
        assert "stable" in text
        assert "collecting" in text  # ARBUSDT has only 3 runs
        assert "NOT READY" in text  # not all stable
