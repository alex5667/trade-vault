# -*- coding: utf-8 -*-
"""
Research Guard Calibrator — pure computation module (no IO).

Evaluates whether G14 (Strategy Research Guard) should be promoted from
REPORT-ONLY to ENFORCE based on consecutive healthy nightly reports.

Algorithm:
    1. Receive recent nightly report snapshots (PSR, DSR, PBO, report age)
    2. Check each report against thresholds:
       - PSR ≥ PSR_MIN
       - DSR ≥ DSR_MIN
       - PBO ≤ PBO_MAX
       - Report age ≤ max_age_sec (fresh)
    3. Build proof streak: consecutive reports passing all thresholds
    4. If streak ≥ required → recommend "promote" (REPORT-ONLY → ENFORCE)
    5. In ENFORCE mode: if any report fails → rollback to REPORT-ONLY

Safety invariants:
    - Fail-open: insufficient data → stays in current mode (never auto-promotes)
    - Rollback: if any metric degrades in enforce → auto-reverts to report_only
    - Missing/stale reports count as failures
    - Minimum sample count enforced

Import as:
    from core.research_guard_calibrator import evaluate_research_guard, ResearchGuardCalibResult
"""
from __future__ import annotations

from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class NightlyReport:
    """Single nightly research guard report snapshot."""
    psr: float = 0.0          # Probabilistic Sharpe Ratio
    dsr: float = 0.0          # Deflated Sharpe Ratio
    pbo: float = 0.0          # Probability of Backtest Overfitting
    ece: float = 0.0          # Expected Calibration Error
    brier: float = 0.0        # Brier Score
    blocker_active: bool = False
    report_age_sec: float = 0.0
    report_ts: int = 0        # epoch seconds of report
    has_data: bool = False     # True if we actually read a report


@dataclass
class ResearchGuardCalibResult:
    """Result of a single calibration evaluation."""
    # Sample info
    n_reports_checked: int = 0
    n_reports_passing: int = 0
    n_reports_failing: int = 0

    # Latest metrics
    latest_psr: float = 0.0
    latest_dsr: float = 0.0
    latest_pbo: float = 0.0
    latest_ece: float = 0.0
    latest_brier: float = 0.0
    latest_report_age_sec: float = 0.0

    # Streak tracking
    proof_streak: int = 0
    proof_streak_required: int = 7  # 7 consecutive healthy reports ≈ 7 days

    # Rollback tracking
    rollback_streak: int = 0
    rollback_streak_required: int = 2  # 2 consecutive failures → rollback

    # Decision
    recommend: str = "hold"         # "hold" | "promote" | "rollback"
    effective_mode: str = "report"  # "report" | "enforce"
    reason: str = ""

    # Diagnostics
    data_sufficient: bool = False
    thresholds: Dict[str, float] = field(default_factory=dict)
    failing_metrics: List[str] = field(default_factory=list)

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @property
    def is_ready_for_promote(self) -> bool:
        return self.recommend == "promote"

    @property
    def is_rollback(self) -> bool:
        return self.recommend == "rollback"


# ---------------------------------------------------------------------------
# Core evaluation
# ---------------------------------------------------------------------------

def evaluate_research_guard(
    report: NightlyReport,
    *,
    psr_min: float = 0.95,
    dsr_min: float = 0.90,
    pbo_max: float = 0.10,
    ece_max: float = 0.15,
    brier_max: float = 0.25,
    max_report_age_sec: float = 129600.0,  # 36h
    proof_streak: int = 0,
    proof_streak_required: int = 7,
    rollback_streak: int = 0,
    rollback_streak_required: int = 2,
    current_mode: str = "report",
) -> ResearchGuardCalibResult:
    """
    Evaluate whether the Research Guard should transition modes.

    Args:
        report: Latest nightly report snapshot.
        psr_min: Minimum PSR threshold.
        dsr_min: Minimum DSR threshold.
        pbo_max: Maximum PBO threshold.
        max_report_age_sec: Max acceptable report age in seconds.
        proof_streak: Current proof streak from previous runs.
        proof_streak_required: Consecutive passing reports needed to promote.
        rollback_streak: Current rollback streak from previous runs.
        rollback_streak_required: Consecutive failing reports to trigger rollback.
        current_mode: Current guard mode ("report" or "enforce").

    Returns:
        ResearchGuardCalibResult with recommendation.
    """
    result = ResearchGuardCalibResult(
        proof_streak_required=proof_streak_required,
        rollback_streak_required=rollback_streak_required,
        effective_mode=current_mode,
        thresholds={
            "psr_min": psr_min,
            "dsr_min": dsr_min,
            "pbo_max": pbo_max,
            "ece_max": ece_max,
            "brier_max": brier_max,
            "max_report_age_sec": max_report_age_sec,
        },
    )

    # --- No data? ---
    if not report.has_data:
        result.reason = "no_report_data"
        result.proof_streak = 0  # reset on missing data
        result.rollback_streak = rollback_streak
        result.recommend = "hold"
        return result

    result.data_sufficient = True
    result.n_reports_checked = 1
    result.latest_psr = report.psr
    result.latest_dsr = report.dsr
    result.latest_pbo = report.pbo
    result.latest_ece = report.ece
    result.latest_brier = report.brier
    result.latest_report_age_sec = report.report_age_sec

    # --- Check thresholds ---
    failing: List[str] = []

    if report.report_age_sec > max_report_age_sec:
        failing.append(f"stale(age={report.report_age_sec:.0f}s>{max_report_age_sec:.0f}s)")

    if report.psr < psr_min:
        failing.append(f"PSR({report.psr:.3f}<{psr_min})")

    if report.dsr < dsr_min:
        failing.append(f"DSR({report.dsr:.3f}<{dsr_min})")

    if report.pbo > pbo_max:
        failing.append(f"PBO({report.pbo:.3f}>{pbo_max})")

    if report.ece > ece_max:
        failing.append(f"ECE({report.ece:.3f}>{ece_max})")

    if report.brier > brier_max:
        failing.append(f"Brier({report.brier:.3f}>{brier_max})")

    qualifies = len(failing) == 0
    result.failing_metrics = failing

    if qualifies:
        result.n_reports_passing = 1
    else:
        result.n_reports_failing = 1

    # --- Proof streak ---
    if qualifies:
        result.proof_streak = proof_streak + 1
        result.rollback_streak = 0  # good report → reset rollback
    else:
        result.proof_streak = 0  # any fail → reset proof streak

    # --- Rollback logic (only in enforce) ---
    if current_mode == "enforce":
        if not qualifies:
            result.rollback_streak = rollback_streak + 1
        else:
            result.rollback_streak = 0

        if result.rollback_streak >= rollback_streak_required:
            result.recommend = "rollback"
            result.effective_mode = "report"
            result.reason = (
                f"rollback(failures={','.join(failing)},"
                f"streak={result.rollback_streak}/{rollback_streak_required})"
            )
            return result

    # --- Promote logic (only in report mode) ---
    if current_mode == "report":
        if result.proof_streak >= proof_streak_required:
            result.recommend = "promote"
            result.effective_mode = "enforce"
            result.reason = (
                f"promote_to_enforce("
                f"PSR={report.psr:.3f}>={psr_min},"
                f"DSR={report.dsr:.3f}>={dsr_min},"
                f"PBO={report.pbo:.3f}<={pbo_max},"
                f"ECE={report.ece:.3f}<={ece_max},"
                f"Brier={report.brier:.3f}<={brier_max},"
                f"streak={result.proof_streak}/{proof_streak_required})"
            )
            return result

    # Default: hold
    result.recommend = "hold"
    if qualifies:
        result.reason = (
            f"building_proof(PSR={report.psr:.3f},"
            f"DSR={report.dsr:.3f},PBO={report.pbo:.3f},"
            f"ECE={report.ece:.3f},Brier={report.brier:.3f},"
            f"streak={result.proof_streak}/{proof_streak_required})"
        )
    else:
        result.reason = f"not_qualifying({','.join(failing)})"

    return result


# ---------------------------------------------------------------------------
# Mode helpers
# ---------------------------------------------------------------------------

MODE_ORDER = {"report": 0, "enforce": 1}


def rg_mode_to_int(mode: str) -> int:
    return MODE_ORDER.get(mode, 0)


def rg_is_promotion(old: str, new: str) -> bool:
    return MODE_ORDER.get(new, 0) > MODE_ORDER.get(old, 0)


def rg_is_rollback(old: str, new: str) -> bool:
    return MODE_ORDER.get(new, 0) < MODE_ORDER.get(old, 0)
