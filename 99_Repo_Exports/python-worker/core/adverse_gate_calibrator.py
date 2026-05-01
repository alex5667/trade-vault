# -*- coding: utf-8 -*-
from __future__ import annotations
"""
Adverse Gate Calibrator — pure computation module (no IO).

Evaluates whether G10 (Adverse Selection Gate) should be auto-enabled
per-symbol based on statistical evidence from trade outcomes.

G10 has two sub-gates:
  1. Reversal veto — blocks reversals lacking evidence (CVD reclaim, absorption, OBI, OFI)
  2. Continuation wait — buffers continuation signals, waits for bar-close confirmation

Algorithm:
    1. Load per-symbol trade outcomes annotated with adverse_veto / adverse_wait flags
    2. Compute reversal_veto_precision = P(loss | reversal vetoed)
    3. Compute baseline_loss_rate = P(loss | reversal passed)
    4. Compute veto_lift = reversal_veto_precision - baseline_loss_rate
    5. Optionally: continuation_confirm_lift = P(win | confirmed) - P(win | all)
    6. Build proof streak per-symbol: consecutive good windows → auto-enable
    7. Rollback: if precision degrades → auto-disable that symbol

Safety invariants:
    - Fail-open: insufficient data → stays disabled (never auto-enables)
    - Per-symbol: each symbol has its own streak & mode
    - Rollback: independent per symbol

Import as:
    from core.adverse_gate_calibrator import evaluate_adverse_gate, AdverseGateCalibResult
"""

import math
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class AdverseOutcome:
    """Single trade outcome with G10 gate decision metadata."""
    symbol: str
    pnl_pct: float
    is_loss: bool               # pnl_pct < 0
    scenario: str               # "reversal" | "continuation"
    direction: str              # "LONG" | "SHORT"
    # Reversal sub-gate
    reversal_vetoed: bool = False    # G10 vetoed this reversal (shadow mode)
    reversal_passed: bool = False    # G10 let reversal through
    has_evidence: bool = False       # had reclaim/absorb/obi/ofi
    # Continuation sub-gate
    continuation_confirmed: bool = False  # bar-close confirmed direction
    continuation_rejected: bool = False   # bar-close rejected direction
    continuation_timed_out: bool = False  # timed out without bar-close
    # Metadata
    adverse_wait_ms: int = 0
    ts_ms: int = 0


@dataclass
class AdverseGateCalibResult:
    """Result of a single per-symbol adverse gate calibration evaluation."""
    symbol=""
    window_h: int = 24

    # Sample counts
    n_total: int = 0
    n_reversals: int = 0
    n_continuations: int = 0

    # Reversal sub-gate metrics
    n_rev_vetoed: int = 0
    n_rev_vetoed_loss: int = 0     # vetoed AND loss → correct veto
    n_rev_vetoed_win: int = 0      # vetoed AND win → missed opportunity
    n_rev_passed: int = 0
    n_rev_passed_loss: int = 0     # passed AND loss → gate miss
    n_rev_passed_win: int = 0      # passed AND win → correct pass

    # Continuation sub-gate metrics
    n_cont_confirmed: int = 0
    n_cont_confirmed_win: int = 0
    n_cont_confirmed_loss: int = 0
    n_cont_rejected: int = 0
    n_cont_rejected_win: int = 0   # would have won → saved nothing
    n_cont_rejected_loss: int = 0  # would have lost → correct reject
    n_cont_timeout: int = 0

    # Core metrics
    reversal_veto_precision: float = 0.0   # P(loss | rev_vetoed)
    reversal_pass_loss_rate: float = 0.0   # P(loss | rev_passed)
    reversal_veto_lift: float = 0.0        # precision - pass_loss_rate
    continuation_confirm_wr: float = 0.0   # P(win | cont_confirmed)
    continuation_reject_loss: float = 0.0  # P(loss | cont_rejected)
    composite_score: float = 0.0           # Weighted composite

    # Streak tracking
    proof_streak: int = 0
    proof_streak_required: int = 3
    rollback_streak: int = 0
    rollback_streak_required: int = 2

    # Decision
    recommend: str = "hold"         # "hold" | "enable" | "disable"
    effective_mode: str = "disabled" # "disabled" | "shadow" | "enforce"
    reason: str = ""

    # Diagnostics
    data_sufficient: bool = False
    thresholds: Dict[str, float] = field(default_factory=dict)

    def as_dict(self) -> Dict[str, Any]:
        return asdict(self)

    @property
    def is_ready_for_enable(self) -> bool:
        return self.recommend == "enable"

    @property
    def is_disable(self) -> bool:
        return self.recommend == "disable"


# ---------------------------------------------------------------------------
# Core evaluation
# ---------------------------------------------------------------------------

def evaluate_adverse_gate(
    outcomes: List[AdverseOutcome],
    *,
    symbol="",
    window_h: int = 24,
    min_rev_veto_precision: float = 0.55,
    min_rev_veto_lift: float = 0.08,
    min_n_reversals: int = 5,
    min_n_total: int = 15,
    proof_streak: int = 0,
    proof_streak_required: int = 3,
    rollback_precision: float = 0.35,
    rollback_streak: int = 0,
    rollback_streak_required: int = 2,
    current_mode: str = "disabled",
) -> AdverseGateCalibResult:
    """
    Evaluate whether G10 should be auto-enabled for a specific symbol.

    Args:
        outcomes: Trade outcomes (already filtered by symbol).
        symbol: Symbol name.
        window_h: Window hours.
        min_rev_veto_precision: Min reversal veto precision for proof.
        min_rev_veto_lift: Min lift above baseline.
        min_n_reversals: Min reversal-scenario trades for valid window.
        min_n_total: Min total trades for valid window.
        proof_streak: Current proof streak from previous runs.
        proof_streak_required: Consecutive windows to auto-enable.
        rollback_precision: Precision below this → disable.
        rollback_streak: Current rollback streak from previous runs.
        rollback_streak_required: Consecutive degraded windows → disable.
        current_mode: Current gate mode for this symbol.

    Returns:
        AdverseGateCalibResult with per-symbol recommendation.
    """
    result = AdverseGateCalibResult(
        symbol=symbol,
        window_h=window_h,
        proof_streak_required=proof_streak_required,
        rollback_streak_required=rollback_streak_required,
        effective_mode=current_mode,
        thresholds={
            "min_rev_veto_precision": min_rev_veto_precision,
            "min_rev_veto_lift": min_rev_veto_lift,
            "min_n_reversals": float(min_n_reversals),
            "min_n_total": float(min_n_total),
            "rollback_precision": rollback_precision,
        },
    )

    # --- Count outcomes ---
    n_total = len(outcomes)
    result.n_total = n_total

    if n_total < min_n_total:
        result.reason = f"insufficient_data(n={n_total}<{min_n_total})"
        result.proof_streak = 0
        result.rollback_streak = rollback_streak
        result.recommend = "hold"
        return result

    result.data_sufficient = True

    # --- Partition by scenario ---
    reversals = [o for o in outcomes if "reversal" in o.scenario.lower()]
    continuations = [o for o in outcomes if "continuation" in o.scenario.lower()]

    result.n_reversals = len(reversals)
    result.n_continuations = len(continuations)

    # --- Reversal sub-gate metrics ---
    rev_vetoed = [o for o in reversals if o.reversal_vetoed]
    rev_passed = [o for o in reversals if o.reversal_passed]

    result.n_rev_vetoed = len(rev_vetoed)
    result.n_rev_vetoed_loss = sum(1 for o in rev_vetoed if o.is_loss)
    result.n_rev_vetoed_win = sum(1 for o in rev_vetoed if not o.is_loss)
    result.n_rev_passed = len(rev_passed)
    result.n_rev_passed_loss = sum(1 for o in rev_passed if o.is_loss)
    result.n_rev_passed_win = sum(1 for o in rev_passed if not o.is_loss)

    # --- Continuation sub-gate metrics ---
    cont_confirmed = [o for o in continuations if o.continuation_confirmed]
    cont_rejected = [o for o in continuations if o.continuation_rejected]
    cont_timeout = [o for o in continuations if o.continuation_timed_out]

    result.n_cont_confirmed = len(cont_confirmed)
    result.n_cont_confirmed_win = sum(1 for o in cont_confirmed if not o.is_loss)
    result.n_cont_confirmed_loss = sum(1 for o in cont_confirmed if o.is_loss)
    result.n_cont_rejected = len(cont_rejected)
    result.n_cont_rejected_win = sum(1 for o in cont_rejected if not o.is_loss)
    result.n_cont_rejected_loss = sum(1 for o in cont_rejected if o.is_loss)
    result.n_cont_timeout = len(cont_timeout)

    # --- Compute reversal metrics ---
    if result.n_rev_vetoed > 0:
        result.reversal_veto_precision = result.n_rev_vetoed_loss / result.n_rev_vetoed
    else:
        result.reversal_veto_precision = 0.0

    if result.n_rev_passed > 0:
        result.reversal_pass_loss_rate = result.n_rev_passed_loss / result.n_rev_passed
    else:
        result.reversal_pass_loss_rate = 0.0

    result.reversal_veto_lift = result.reversal_veto_precision - result.reversal_pass_loss_rate

    # --- Compute continuation metrics ---
    if result.n_cont_confirmed > 0:
        result.continuation_confirm_wr = result.n_cont_confirmed_win / result.n_cont_confirmed
    else:
        result.continuation_confirm_wr = 0.0

    if result.n_cont_rejected > 0:
        result.continuation_reject_loss = result.n_cont_rejected_loss / result.n_cont_rejected
    else:
        result.continuation_reject_loss = 0.0

    # --- Composite score ---
    # Weighted: reversal sub-gate is primary (70%), continuation is secondary (30%)
    rev_value = 0.0
    if result.reversal_veto_precision > 0 and result.reversal_veto_lift > 0:
        rev_value = result.reversal_veto_precision * min(1.0, result.reversal_veto_lift / 0.15)
    cont_value = 0.0
    if result.continuation_reject_loss > 0.5:
        cont_value = result.continuation_reject_loss
    result.composite_score = 0.7 * rev_value + 0.3 * cont_value

    # --- Check minimum reversal sample size ---
    if result.n_reversals < min_n_reversals:
        result.reason = f"insufficient_reversals(n_rev={result.n_reversals}<{min_n_reversals})"
        result.proof_streak = 0
        result.rollback_streak = rollback_streak
        result.recommend = "hold"
        return result

    # --- Proof streak logic ---
    qualifies = (
        result.reversal_veto_precision >= min_rev_veto_precision
        and result.reversal_veto_lift >= min_rev_veto_lift
    )

    if qualifies:
        result.proof_streak = proof_streak + 1
        result.rollback_streak = 0
    else:
        result.proof_streak = 0

    # --- Rollback logic (only relevant in enabled modes) ---
    if current_mode in ("shadow", "enforce"):
        if result.n_rev_vetoed >= min_n_reversals and result.reversal_veto_precision < rollback_precision:
            result.rollback_streak = rollback_streak + 1
        elif qualifies:
            result.rollback_streak = 0
        else:
            result.rollback_streak = max(0, rollback_streak)

        if result.rollback_streak >= rollback_streak_required:
            result.recommend = "disable"
            result.effective_mode = "disabled"
            result.reason = (
                f"disable(precision={result.reversal_veto_precision:.3f}<{rollback_precision},"
                f"streak={result.rollback_streak}/{rollback_streak_required})"
            )
            return result

    # --- Enable logic ---
    if current_mode == "disabled":
        if result.proof_streak >= proof_streak_required:
            result.recommend = "enable"
            result.effective_mode = "shadow"
            result.reason = (
                f"enable_shadow("
                f"precision={result.reversal_veto_precision:.3f}>={min_rev_veto_precision},"
                f"lift={result.reversal_veto_lift:.3f}>={min_rev_veto_lift},"
                f"streak={result.proof_streak}/{proof_streak_required})"
            )
            return result

    elif current_mode == "shadow":
        # shadow → enforce is manual (Telegram Approve only)
        pass

    # Default: hold
    result.recommend = "hold"
    if qualifies:
        result.reason = (
            f"building_proof(precision={result.reversal_veto_precision:.3f},"
            f"lift={result.reversal_veto_lift:.3f},"
            f"streak={result.proof_streak}/{proof_streak_required})"
        )
    else:
        reasons = []
        if result.reversal_veto_precision < min_rev_veto_precision:
            reasons.append(f"precision={result.reversal_veto_precision:.3f}<{min_rev_veto_precision}")
        if result.reversal_veto_lift < min_rev_veto_lift:
            reasons.append(f"lift={result.reversal_veto_lift:.3f}<{min_rev_veto_lift}")
        result.reason = f"not_qualifying({','.join(reasons)})"

    return result


# ---------------------------------------------------------------------------
# Mode transition helpers
# ---------------------------------------------------------------------------

ADV_MODE_ORDER = {"disabled": 0, "shadow": 1, "enforce": 2}


def adv_mode_to_int(mode: str) -> int:
    """Convert mode string to numeric gauge value."""
    return ADV_MODE_ORDER.get(mode, 0)


def is_adv_enable(old_mode: str, new_mode: str) -> bool:
    """Check if new mode is an upgrade from old mode."""
    return ADV_MODE_ORDER.get(new_mode, 0) > ADV_MODE_ORDER.get(old_mode, 0)


def is_adv_disable(old_mode: str, new_mode: str) -> bool:
    """Check if new mode is a downgrade from old mode."""
    return ADV_MODE_ORDER.get(new_mode, 0) < ADV_MODE_ORDER.get(old_mode, 0)
