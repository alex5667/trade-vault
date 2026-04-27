# -*- coding: utf-8 -*-
"""
Strong Gate Calibrator — pure computation module (no IO).

Evaluates whether G5 (Strong Gate) should be promoted from SHADOW to ENFORCE
based on statistical evidence from trade outcomes.

Algorithm:
    1. Receive trade outcomes labeled with shadow_vetoed / passed flags
    2. Compute veto_precision = P(loss | vetoed)  — how often vetoes were correct
    3. Compute pass_loss_rate = P(loss | passed)   — baseline loss rate
    4. Compute veto_lift = veto_precision - pass_loss_rate — added value of gate
    5. Build proof streak: consecutive windows where metrics exceed thresholds
    6. Recommend mode transition based on streak length and current mode

Safety invariants:
    - Fail-open: insufficient data → stays in current mode (never auto-promotes)
    - Rollback: if precision degrades → auto-reverts to shadow
    - Minimum sample sizes enforced at every checkpoint

Import as:
    from core.strong_gate_calibrator import evaluate_strong_gate, StrongGateCalibResult
"""
from __future__ import annotations

import math
from dataclasses import dataclass, field, asdict
from typing import Any, Dict, List, Optional


# ---------------------------------------------------------------------------
# Data structures
# ---------------------------------------------------------------------------

@dataclass
class TradeOutcome:
    """Single trade outcome with gate decision metadata."""
    symbol: str
    pnl_pct: float
    is_loss: bool  # pnl_pct < 0
    shadow_vetoed: bool  # strong_gate_shadow_veto=1 in indicators
    ok: bool  # of_confirm_ok=1 (gate passed)
    scenario: str = ""
    direction: str = ""
    ts_ms: int = 0


@dataclass
class StrongGateCalibResult:
    """Result of a single calibration evaluation."""
    # Window parameters
    window_h: int = 24

    # Sample counts
    n_total: int = 0
    n_vetoed: int = 0
    n_vetoed_loss: int = 0  # vetoed AND pnl < 0 (correct vetoes)
    n_vetoed_win: int = 0   # vetoed AND pnl >= 0 (missed opportunities)
    n_passed: int = 0
    n_passed_loss: int = 0  # passed AND pnl < 0 (gate misses)
    n_passed_win: int = 0   # passed AND pnl >= 0 (correct passes)

    # Core metrics
    veto_precision: float = 0.0   # P(loss|vetoed): how often vetoes were right
    pass_loss_rate: float = 0.0   # P(loss|passed): baseline loss rate
    veto_lift: float = 0.0        # veto_precision - pass_loss_rate: added value
    veto_value: float = 0.0       # Composite metric (precision * lift_normalized)

    # Streak tracking
    proof_streak: int = 0         # Current consecutive qualifying windows
    proof_streak_required: int = 3

    # Rollback tracking
    rollback_streak: int = 0      # Consecutive degraded windows
    rollback_streak_required: int = 2

    # Decision
    recommend: str = "hold"       # "hold" | "promote" | "rollback"
    effective_mode: str = "shadow"  # "shadow" | "shadow_enforce" | "full_enforce"
    reason: str = ""

    # Diagnostics
    data_sufficient: bool = False
    thresholds: Dict[str, float] = field(default_factory=dict)

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

def evaluate_strong_gate(
    outcomes: List[TradeOutcome],
    *,
    window_h: int = 24,
    min_precision: float = 0.55,
    min_lift: float = 0.10,
    min_n_vetoed: int = 10,
    min_n_total: int = 30,
    proof_streak: int = 0,
    proof_streak_required: int = 3,
    rollback_precision: float = 0.40,
    rollback_streak: int = 0,
    rollback_streak_required: int = 2,
    current_mode: str = "shadow",
) -> StrongGateCalibResult:
    """
    Evaluate whether the Strong Gate should transition modes.

    Args:
        outcomes: Trade outcomes with shadow/pass annotations.
        window_h: Hours of data in this window.
        min_precision: Minimum veto precision for proof.
        min_lift: Minimum lift above baseline for proof.
        min_n_vetoed: Minimum vetoed trades for valid window.
        min_n_total: Minimum total trades for valid window.
        proof_streak: Current proof streak from previous runs.
        proof_streak_required: Consecutive windows needed to promote.
        rollback_precision: Precision below this → rollback signal.
        rollback_streak: Current rollback streak from previous runs.
        rollback_streak_required: Consecutive degraded windows → rollback.
        current_mode: Current gate mode.

    Returns:
        StrongGateCalibResult with recommendation.
    """
    result = StrongGateCalibResult(
        window_h=window_h,
        proof_streak_required=proof_streak_required,
        rollback_streak_required=rollback_streak_required,
        effective_mode=current_mode,
        thresholds={
            "min_precision": min_precision,
            "min_lift": min_lift,
            "min_n_vetoed": float(min_n_vetoed),
            "min_n_total": float(min_n_total),
            "rollback_precision": rollback_precision,
        },
    )

    # --- Count outcomes ---
    n_total = len(outcomes)
    result.n_total = n_total

    if n_total < min_n_total:
        result.reason = f"insufficient_data(n={n_total}<{min_n_total})"
        result.proof_streak = 0  # Reset streak on insufficient data
        result.rollback_streak = rollback_streak  # Preserve (don't reset on missing data)
        result.recommend = "hold"
        return result

    result.data_sufficient = True

    # Partition outcomes
    vetoed = [o for o in outcomes if o.shadow_vetoed]
    passed = [o for o in outcomes if o.ok and not o.shadow_vetoed]

    result.n_vetoed = len(vetoed)
    result.n_passed = len(passed)

    result.n_vetoed_loss = sum(1 for o in vetoed if o.is_loss)
    result.n_vetoed_win = sum(1 for o in vetoed if not o.is_loss)
    result.n_passed_loss = sum(1 for o in passed if o.is_loss)
    result.n_passed_win = sum(1 for o in passed if not o.is_loss)

    # --- Compute metrics ---

    # Veto precision: how often vetoed signals were actually losers
    if result.n_vetoed > 0:
        result.veto_precision = result.n_vetoed_loss / result.n_vetoed
    else:
        result.veto_precision = 0.0

    # Pass loss rate: baseline loss rate for signals that passed
    if result.n_passed > 0:
        result.pass_loss_rate = result.n_passed_loss / result.n_passed
    else:
        result.pass_loss_rate = 0.0

    # Lift: how much better the gate is than random
    result.veto_lift = result.veto_precision - result.pass_loss_rate

    # Composite value metric (precision weighted by lift)
    if result.veto_precision > 0 and result.veto_lift > 0:
        result.veto_value = result.veto_precision * min(1.0, result.veto_lift / 0.20)
    else:
        result.veto_value = 0.0

    # --- Check minimum vetoed sample size ---
    if result.n_vetoed < min_n_vetoed:
        result.reason = f"insufficient_vetoes(n_vetoed={result.n_vetoed}<{min_n_vetoed})"
        result.proof_streak = 0
        result.rollback_streak = rollback_streak
        result.recommend = "hold"
        return result

    # --- Proof streak logic ---
    qualifies = (
        result.veto_precision >= min_precision
        and result.veto_lift >= min_lift
    )

    if qualifies:
        result.proof_streak = proof_streak + 1
        result.rollback_streak = 0  # Reset rollback on good window
    else:
        result.proof_streak = 0  # Reset proof on any bad window

    # --- Rollback logic (only relevant in enforce modes) ---
    if current_mode in ("shadow_enforce", "full_enforce"):
        if result.n_vetoed >= min_n_vetoed and result.veto_precision < rollback_precision:
            result.rollback_streak = rollback_streak + 1
        elif qualifies:
            result.rollback_streak = 0  # Good window → reset rollback streak
        else:
            result.rollback_streak = max(0, rollback_streak)  # Preserve if not degraded

        if result.rollback_streak >= rollback_streak_required:
            result.recommend = "rollback"
            result.effective_mode = "shadow"
            result.reason = (
                f"rollback(precision={result.veto_precision:.3f}<{rollback_precision},"
                f"streak={result.rollback_streak}/{rollback_streak_required})"
            )
            return result

    # --- Promotion logic ---
    if current_mode == "shadow":
        if result.proof_streak >= proof_streak_required:
            result.recommend = "promote"
            result.effective_mode = "shadow_enforce"
            result.reason = (
                f"promote_to_shadow_enforce("
                f"precision={result.veto_precision:.3f}>={min_precision},"
                f"lift={result.veto_lift:.3f}>={min_lift},"
                f"streak={result.proof_streak}/{proof_streak_required})"
            )
            return result

    elif current_mode == "shadow_enforce":
        # shadow_enforce → full_enforce is manual (Telegram Approve only)
        # But we keep building the streak for reporting
        pass

    # Default: hold current mode
    result.recommend = "hold"
    if qualifies:
        result.reason = (
            f"building_proof(precision={result.veto_precision:.3f},"
            f"lift={result.veto_lift:.3f},"
            f"streak={result.proof_streak}/{proof_streak_required})"
        )
    else:
        reasons = []
        if result.veto_precision < min_precision:
            reasons.append(f"precision={result.veto_precision:.3f}<{min_precision}")
        if result.veto_lift < min_lift:
            reasons.append(f"lift={result.veto_lift:.3f}<{min_lift}")
        result.reason = f"not_qualifying({','.join(reasons)})"

    return result


# ---------------------------------------------------------------------------
# Mode transition helpers
# ---------------------------------------------------------------------------

MODE_ORDER = {"shadow": 0, "shadow_enforce": 1, "full_enforce": 2}


def mode_to_int(mode: str) -> int:
    """Convert mode string to numeric gauge value."""
    return MODE_ORDER.get(mode, 0)


def is_promotion(old_mode: str, new_mode: str) -> bool:
    """Check if new mode is a promotion from old mode."""
    return MODE_ORDER.get(new_mode, 0) > MODE_ORDER.get(old_mode, 0)


def is_rollback(old_mode: str, new_mode: str) -> bool:
    """Check if new mode is a rollback from old mode."""
    return MODE_ORDER.get(new_mode, 0) < MODE_ORDER.get(old_mode, 0)
