"""Immutable input/output dataclasses for the confidence meta-gate.

The gate is pure — same input ⇒ same output. Inputs are gathered once at the
decision site in signal_pipeline; downstream consumers must not mutate them.
"""
from __future__ import annotations

from dataclasses import dataclass, field
from typing import Literal

MetaGateDecisionT = Literal[
    "ALLOW",
    "ALLOW_TIGHTENED",
    "DENY_SOFT",
    "SHADOW_ALLOW",
    "SHADOW_DENY",
    "FALLBACK_LEGACY",
]


@dataclass(frozen=True)
class ConfidenceMetaGateInput:
    """All fields the meta-gate consumes at decision time.

    No nested mutable structures except `features` which is captured before
    being handed to the model.
    """

    sid: str
    symbol: str
    kind: str
    side: str

    ts_ms: int
    now_ms: int

    # Legacy confidence gate context (kept side-by-side for SHADOW comparison).
    legacy_confidence: float
    legacy_min_confidence: float
    legacy_decision: str  # "ALLOW" | "DENY"

    # Probability + edge inputs already computed upstream.
    p_edge_raw: float
    p_edge_cal: float | None

    rule_score: float
    have: int
    need: int

    # Execution cost surface.
    spread_bps: float
    expected_slippage_bps: float
    fee_bps: float
    expected_edge_bps: float

    # Risk / DQ context.
    exec_risk_norm: float
    dq_score: float
    dq_flag_count: int

    # Regime / session.
    regime: str
    session: str

    # Schema fingerprints (for fail-fast schema check).
    schema_hash: str
    feature_cols_hash: str

    # Model feature vector (already in the schema the artifact was trained on).
    features: dict[str, float] = field(default_factory=dict)


@dataclass(frozen=True)
class ConfidenceMetaGateOutput:
    """What the meta-gate produces. Always populated, even on fallback."""

    sid: str
    model_ver: str
    mode: str

    decision: MetaGateDecisionT
    # `active` = True when this decision overrides the legacy result (CANARY
    # selected OR ENFORCE); False in SHADOW/CANARY-not-selected/FALLBACK.
    active: bool

    p_win_raw: float
    p_win_calibrated: float
    p_win_floor: float

    expected_r: float
    expected_edge_bps: float
    risk_multiplier: float

    canary_bucket: int
    canary_selected: bool

    reason_codes: list[str]

    latency_ms: float
