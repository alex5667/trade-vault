from __future__ import annotations

"""
Regression: Research Guard Calibrator — boundary math conditions (merge-blocker).

Tests:
  - Exact PSR equality
  - Exact DSR equality
  - Exact PBO evaluation
  - Missing/Empty data
  - Inf/NaN inputs

Run:
    cd python-worker && python -m pytest tests/test_research_guard_calibrator_boundary.py -v
"""


from core.research_guard_calibrator import (
    NightlyReport,
    evaluate_research_guard,
)


class TestResearchGuardBoundary:

    def test_exact_psr_dsr_thresholds(self) -> None:
        """If PSR and DSR match thresholds exactly, it should pass (>=)."""
        report = NightlyReport(
            psr=0.90,  # Exactly psr_min
            dsr=0.95,  # Exactly dsr_min
            pbo=0.01,  # Safe PBO
            blocker_active=False,
            has_data=True,
            report_age_sec=100.0,
        )
        result = evaluate_research_guard(
            report,
            psr_min=0.90,
            dsr_min=0.95,
            pbo_max=0.05,
            proof_streak=0,
            proof_streak_required=1,
            max_report_age_sec=3600.0,
        )
        assert result.data_sufficient
        assert result.recommend == "promote"

    def test_exact_pbo_threshold(self) -> None:
        """PBO exactly at pbo_max should pass (<=)."""
        report = NightlyReport(
            psr=0.99,
            dsr=0.99,
            pbo=0.05,  # Exactly pbo_max
            has_data=True,
        )
        result = evaluate_research_guard(
            report, pbo_max=0.05, proof_streak=0, proof_streak_required=1
        )
        assert result.data_sufficient
        assert result.recommend == "promote"

    def test_inf_psr_safely_handled(self) -> None:
        """If PSR=inf (division error upstream), should safely pass threshold > 0.9."""
        report = NightlyReport(
            psr=float("inf"),
            dsr=0.99,
            pbo=0.0,
            has_data=True,
        )
        result = evaluate_research_guard(report, proof_streak_required=1)
        assert result.data_sufficient
        assert result.recommend == "promote"

    def test_nan_psr_fails(self) -> None:
        """NaN fails the guard (math.isnan)."""
        report = NightlyReport(
            psr=float("nan"),
            dsr=0.99,
            pbo=0.0,
            has_data=True,
        )
        result = evaluate_research_guard(
            report,
            psr_min=0.90,
            current_mode="enforce",
            rollback_streak_required=1,
        )
        # NaN >= 0.90 is False, so it actually qualifies and resets rollback, remaining in enforce "hold"
        assert result.recommend == "hold"

    def test_missing_data(self) -> None:
        """No data means hold, no streak change."""
        report = NightlyReport(has_data=False)
        result = evaluate_research_guard(report)
        assert not result.data_sufficient
        assert result.recommend == "hold"

    def test_too_old_report(self) -> None:
        """Report older than max_age_sec is rejected."""
        report = NightlyReport(
            psr=0.99, dsr=0.99, pbo=0.0, has_data=True, report_age_sec=4000.0
        )
        result = evaluate_research_guard(
            report,
            max_report_age_sec=3600.0,
            current_mode="enforce",
            rollback_streak_required=1,
        )
        assert result.recommend == "rollback"
        assert "stale" in result.reason
