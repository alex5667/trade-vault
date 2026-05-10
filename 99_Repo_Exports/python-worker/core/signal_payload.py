from __future__ import annotations

"""Structured evidence payload for signals.

This is an operational feature:
- Makes debugging and post-mortem deterministic.
- Lets Telegram show only a short summary while Redis keeps full evidence.

Design goals
------------
- Backward compatible: you can add `evidence` field without breaking old consumers.
- JSON-serializable: dataclasses expose `to_dict()`.
"""

from dataclasses import dataclass
from typing import Any


@dataclass
class StrongGateDecision:
    """Decision returned by strong gate evaluation."""
    ok: bool
    scenario: str
    need: int
    have: int
    a: int = 0
    b: int = 0
    c: int = 0
    reason: str = ""
    gate_bits: int = 0
    # Optional detailed legs for debug (A/B/C breakdown is often enough)
    # Kept for backward compatibility with signal_pipeline
    legs: dict[str, int] | None = None

    def to_dict(self) -> dict[str, Any]:
        """Convert to dictionary for JSON serialization."""
        return {
            "ok": 1 if self.ok else 0,
            "scenario": str(self.scenario),
            "need": int(self.need),
            "have": int(self.have),
            "a": int(self.a),
            "b": int(self.b),
            "c": int(self.c),
            "reason": str(self.reason),
            "gate_bits": int(self.gate_bits),
            "legs": {str(k): int(v) for k, v in (self.legs or {}).items()} if self.legs else None,
        }


@dataclass
class SignalPayload:
    confirmations: dict[str, Any]
    indicators: dict[str, Any]
    gate: StrongGateDecision | None = None
    confidence_parts: dict[str, Any] | None = None
    rejection_reason: str | None = None

    # Optional extras to capture context
    ts_ms: int = 0
    symbol: str = ""
    signal_id: str = ""

    def to_dict(self) -> dict[str, Any]:
        d: dict[str, Any] = {
            "confirmations": dict(self.confirmations or {}),
            "indicators": dict(self.indicators or {}),
            "rejection_reason": self.rejection_reason,
            "ts_ms": self.ts_ms,
            "symbol": self.symbol,
            "signal_id": self.signal_id,
        }
        if self.gate is not None:
            d["gate"] = self.gate.to_dict()
        if self.confidence_parts is not None:
            d["confidence_parts"] = dict(self.confidence_parts)
        return d


@dataclass(frozen=True)
class GateDecisionV1:
    stage: str
    gate: str
    decision: str  # Literal["ALLOW", "DENY", "ABSTAIN", "TIGHTEN", "SHADOW_DENY"]
    reason_code: str
    severity: str  # Literal["INFO", "WARN", "RISK", "CRITICAL"]
    profile: str
    fail_policy: str  # Literal["OPEN", "CLOSED", "VIRTUAL_ONLY"]
    ts_event_ms: int
    ts_decision_ms: int
    latency_us: int
    inputs_hash: str
    notes: dict[str, Any]

    def to_dict(self) -> dict[str, Any]:
        return {
            "stage": self.stage,
            "gate": self.gate,
            "decision": self.decision,
            "reason_code": self.reason_code,
            "severity": self.severity,
            "profile": self.profile,
            "fail_policy": self.fail_policy,
            "ts_event_ms": self.ts_event_ms,
            "ts_decision_ms": self.ts_decision_ms,
            "latency_us": self.latency_us,
            "inputs_hash": self.inputs_hash,
            "notes": self.notes,
        }
