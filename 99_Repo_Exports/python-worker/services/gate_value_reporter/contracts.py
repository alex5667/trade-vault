"""Unified DTOs for the gate_value_reporter.

The three input streams (labels:tb, metrics:ml_confirm, gated_out_outcomes)
expose different shapes; we collapse them to a single GateOutcomeRecord so
that cohort math doesn't care where the row came from.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

Cohort = Literal["passed", "gated_out"]
Side = Literal["LONG", "SHORT"]
OutcomeReason = Literal["tp", "sl", "timeout", "no_barrier", "unknown"]

GateDecision = Literal[
    "KEEP_GATE",
    "TIGHTEN_GATE",
    "RELAX_GATE",
    "DISABLE_GATE",
    "INSUFFICIENT_DATA",
    "INCONCLUSIVE",
]


@dataclass(frozen=True)
class GateOutcomeRecord:
    sid: str
    cohort: Cohort

    symbol: str
    kind: str
    side: Side

    ts_ms: int
    horizon_ms: int

    entry_px: float
    tp_bps: float
    sl_bps: float

    ret_bps: float
    r_mult: float
    y: int

    tp_hit: bool
    sl_hit: bool
    outcome_reason: OutcomeReason

    p_edge: float | None = None
    confidence: float | None = None

    source_stream: str = ""


@dataclass(frozen=True)
class CohortStats:
    n: int
    win_rate: float
    avg_r: float
    median_r: float
    p25_r: float
    p75_r: float
    profit_factor: float
    tp_hit_rate: float
    sl_hit_rate: float
    timeout_rate: float
    avg_ret_bps: float


@dataclass(frozen=True)
class GateLiftStats:
    avg_r_lift: float
    win_rate_lift: float
    profit_factor_lift: float
    sl_hit_rate_reduction: float
    false_negative_rate: float


@dataclass(frozen=True)
class ConfidenceInterval:
    lo: float
    mid: float
    hi: float


@dataclass(frozen=True)
class GateDecisionResult:
    decision: GateDecision
    reason_codes: list[str] = field(default_factory=list)
    severity: str = "info"
    confidence: float = 0.0
