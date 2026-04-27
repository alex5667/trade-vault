# -*- coding: utf-8 -*-
"""Tests for core.research_guard_calibrator — pure computation module."""

import pytest
from core.research_guard_calibrator import (
    NightlyReport,
    ResearchGuardCalibResult,
    evaluate_research_guard,
    rg_mode_to_int,
    rg_is_promotion,
    rg_is_rollback,
)


# ---------------------------------------------------------------------------
# Helper factory
# ---------------------------------------------------------------------------

def _make_report(
    psr: float = 0.97,
    dsr: float = 0.93,
    pbo: float = 0.05,
    report_age_sec: float = 3600.0,
    has_data: bool = True,
) -> NightlyReport:
    return NightlyReport(
        psr=psr,
        dsr=dsr,
        pbo=pbo,
        blocker_active=False,
        report_age_sec=report_age_sec,
        report_ts=1700000000,
        has_data=has_data,
    )


# ---------------------------------------------------------------------------
# Test: no data → hold
# ---------------------------------------------------------------------------

class TestNoData:
    def test_no_data_returns_hold(self):
        report = _make_report(has_data=False)
        result = evaluate_research_guard(report)
        assert result.recommend == "hold"
        assert result.reason == "no_report_data"
        assert result.proof_streak == 0

    def test_no_data_preserves_rollback_streak(self):
        report = _make_report(has_data=False)
        result = evaluate_research_guard(report, rollback_streak=3)
        assert result.rollback_streak == 3


# ---------------------------------------------------------------------------
# Test: healthy report builds streak
# ---------------------------------------------------------------------------

class TestProofStreak:
    def test_first_healthy_report(self):
        report = _make_report()
        result = evaluate_research_guard(report, proof_streak=0)
        assert result.proof_streak == 1
        assert result.recommend == "hold"
        assert "building_proof" in result.reason

    def test_streak_builds_over_windows(self):
        report = _make_report()
        result = evaluate_research_guard(report, proof_streak=5)
        assert result.proof_streak == 6
        assert result.recommend == "hold"

    def test_streak_reaches_required_promotes(self):
        report = _make_report()
        result = evaluate_research_guard(
            report,
            proof_streak=6,
            proof_streak_required=7,
        )
        assert result.proof_streak == 7
        assert result.recommend == "promote"
        assert result.effective_mode == "enforce"
        assert "promote_to_enforce" in result.reason

    def test_failing_report_resets_streak(self):
        report = _make_report(psr=0.80)  # below threshold
        result = evaluate_research_guard(report, proof_streak=5)
        assert result.proof_streak == 0
        assert result.recommend == "hold"
        assert "not_qualifying" in result.reason


# ---------------------------------------------------------------------------
# Test: threshold checks
# ---------------------------------------------------------------------------

class TestThresholds:
    def test_psr_below_threshold(self):
        report = _make_report(psr=0.90)
        result = evaluate_research_guard(report, psr_min=0.95)
        assert len(result.failing_metrics) > 0
        assert any("PSR" in f for f in result.failing_metrics)
        assert result.proof_streak == 0

    def test_dsr_below_threshold(self):
        report = _make_report(dsr=0.80)
        result = evaluate_research_guard(report, dsr_min=0.90)
        assert any("DSR" in f for f in result.failing_metrics)
        assert result.proof_streak == 0

    def test_pbo_above_threshold(self):
        report = _make_report(pbo=0.15)
        result = evaluate_research_guard(report, pbo_max=0.10)
        assert any("PBO" in f for f in result.failing_metrics)
        assert result.proof_streak == 0

    def test_stale_report(self):
        report = _make_report(report_age_sec=200000)
        result = evaluate_research_guard(report, max_report_age_sec=129600)
        assert any("stale" in f for f in result.failing_metrics)
        assert result.proof_streak == 0

    def test_all_passing(self):
        report = _make_report(psr=0.98, dsr=0.95, pbo=0.03, report_age_sec=3600)
        result = evaluate_research_guard(report)
        assert len(result.failing_metrics) == 0
        assert result.proof_streak == 1

    def test_multiple_failures(self):
        report = _make_report(psr=0.80, dsr=0.70, pbo=0.20)
        result = evaluate_research_guard(report)
        assert len(result.failing_metrics) == 3


# ---------------------------------------------------------------------------
# Test: rollback logic (enforce mode)
# ---------------------------------------------------------------------------

class TestRollback:
    def test_failing_report_in_enforce_builds_rollback_streak(self):
        report = _make_report(psr=0.80)
        result = evaluate_research_guard(
            report,
            current_mode="enforce",
            rollback_streak=0,
        )
        assert result.rollback_streak == 1
        assert result.recommend == "hold"

    def test_rollback_streak_triggers_rollback(self):
        report = _make_report(psr=0.80)
        result = evaluate_research_guard(
            report,
            current_mode="enforce",
            rollback_streak=1,
            rollback_streak_required=2,
        )
        assert result.rollback_streak == 2
        assert result.recommend == "rollback"
        assert result.effective_mode == "report"
        assert "rollback" in result.reason

    def test_good_report_in_enforce_resets_rollback(self):
        report = _make_report()
        result = evaluate_research_guard(
            report,
            current_mode="enforce",
            rollback_streak=1,
        )
        assert result.rollback_streak == 0
        assert result.recommend == "hold"

    def test_no_rollback_in_report_mode(self):
        """Rollback logic only applies in enforce mode."""
        report = _make_report(psr=0.80)
        result = evaluate_research_guard(
            report,
            current_mode="report",
            rollback_streak=5,
        )
        # In report mode, rollback_streak isn't incremented
        assert result.recommend == "hold"


# ---------------------------------------------------------------------------
# Test: mode helpers
# ---------------------------------------------------------------------------

class TestModeHelpers:
    def test_mode_to_int(self):
        assert rg_mode_to_int("report") == 0
        assert rg_mode_to_int("enforce") == 1
        assert rg_mode_to_int("unknown") == 0

    def test_is_promotion(self):
        assert rg_is_promotion("report", "enforce") is True
        assert rg_is_promotion("enforce", "report") is False
        assert rg_is_promotion("report", "report") is False

    def test_is_rollback(self):
        assert rg_is_rollback("enforce", "report") is True
        assert rg_is_rollback("report", "enforce") is False
        assert rg_is_rollback("report", "report") is False


# ---------------------------------------------------------------------------
# Test: result properties
# ---------------------------------------------------------------------------

class TestResultProperties:
    def test_is_ready_for_promote(self):
        r = ResearchGuardCalibResult(recommend="promote")
        assert r.is_ready_for_promote is True
        r = ResearchGuardCalibResult(recommend="hold")
        assert r.is_ready_for_promote is False

    def test_is_rollback_property(self):
        r = ResearchGuardCalibResult(recommend="rollback")
        assert r.is_rollback is True
        r = ResearchGuardCalibResult(recommend="hold")
        assert r.is_rollback is False

    def test_as_dict(self):
        r = ResearchGuardCalibResult(latest_psr=0.97)
        d = r.as_dict()
        assert isinstance(d, dict)
        assert d["latest_psr"] == 0.97


# ---------------------------------------------------------------------------
# Test: full lifecycle — report → promote → enforce → rollback
# ---------------------------------------------------------------------------

class TestFullLifecycle:
    def test_full_cycle(self):
        # Phase 1: Build 7-day streak in report mode
        streak = 0
        for day in range(7):
            report = _make_report(psr=0.97, dsr=0.93, pbo=0.05)
            result = evaluate_research_guard(
                report,
                proof_streak=streak,
                proof_streak_required=7,
                current_mode="report",
            )
            streak = result.proof_streak

        # After 7 good windows → promote
        assert result.recommend == "promote"
        assert result.effective_mode == "enforce"

        # Phase 2: Enter enforce mode, metrics degrade
        roll_streak = 0
        for fail_day in range(2):
            report = _make_report(psr=0.80)  # bad PSR
            result = evaluate_research_guard(
                report,
                current_mode="enforce",
                rollback_streak=roll_streak,
                rollback_streak_required=2,
            )
            roll_streak = result.rollback_streak

        # After 2 failures → rollback
        assert result.recommend == "rollback"
        assert result.effective_mode == "report"
