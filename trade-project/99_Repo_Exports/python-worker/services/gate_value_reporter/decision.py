"""Decision engine: map (cohort stats, lift, CI) → gate action.

Decisions are advisory only (Stage 5: manual decision only). Any auto-action
is deliberately gated behind a separate ENFORCE flag and is NOT implemented
in this service.
"""

from __future__ import annotations

from services.gate_value_reporter.contracts import (
    CohortStats,
    ConfidenceInterval,
    GateDecisionResult,
    GateLiftStats,
)


def decide_gate_action(
    *,
    passed: CohortStats,
    gated_out: CohortStats,
    lift: GateLiftStats,
    avg_r_ci: ConfidenceInterval,
    min_n_passed: int,
    min_n_gated_out: int,
    min_avg_r_lift: float,
    max_false_negative_rate: float,
) -> GateDecisionResult:
    """Return a GateDecisionResult.

    Decision ladder (in order):
      1. INSUFFICIENT_DATA — either cohort too small.
      2. KEEP_GATE — CI lower bound > min_avg_r_lift AND passed PF > gated_out PF.
      3. RELAX_GATE — CI upper bound < 0 (gated_out strictly better).
      4. RELAX_GATE — high FN rate but gated_out not catastrophic.
      5. DISABLE_GATE — passed cohort itself negative AND PF < 1.
      6. INCONCLUSIVE — overlapping CI, no clear signal.
    """
    reasons: list[str] = []

    if passed.n < min_n_passed:
        reasons.append("passed_n_too_low")
    if gated_out.n < min_n_gated_out:
        reasons.append("gated_out_n_too_low")

    if reasons:
        return GateDecisionResult(
            decision="INSUFFICIENT_DATA",
            reason_codes=reasons,
            severity="info",
            confidence=0.0,
        )

    if avg_r_ci.lo > min_avg_r_lift and passed.profit_factor > gated_out.profit_factor:
        return GateDecisionResult(
            decision="KEEP_GATE",
            reason_codes=["positive_avg_r_lift", "passed_pf_better"],
            severity="info",
            confidence=0.8,
        )

    if avg_r_ci.hi < 0.0:
        return GateDecisionResult(
            decision="RELAX_GATE",
            reason_codes=["gated_out_avg_r_better"],
            severity="warning",
            confidence=0.8,
        )

    if (
        lift.false_negative_rate > max_false_negative_rate
        and gated_out.avg_r >= -0.05
    ):
        return GateDecisionResult(
            decision="RELAX_GATE",
            reason_codes=["false_negative_rate_high", "gated_out_not_bad"],
            severity="warning",
            confidence=0.65,
        )

    if passed.avg_r < 0.0 and passed.profit_factor < 1.0:
        return GateDecisionResult(
            decision="DISABLE_GATE",
            reason_codes=["passed_cohort_negative", "gate_not_solving_problem"],
            severity="critical",
            confidence=0.75,
        )

    return GateDecisionResult(
        decision="INCONCLUSIVE",
        reason_codes=["overlapping_confidence_interval"],
        severity="info",
        confidence=0.3,
    )
