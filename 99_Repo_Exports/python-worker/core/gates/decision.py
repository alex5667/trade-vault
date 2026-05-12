from dataclasses import dataclass, field
from typing import Literal, Any

@dataclass(frozen=True)
class GateDecisionV1:
    stage: str
    gate: str
    decision: Literal["ALLOW", "DENY", "ABSTAIN", "TIGHTEN", "SHADOW_DENY"]
    reason_code: str
    severity: Literal["INFO", "WARN", "RISK", "CRITICAL"]
    profile: str
    fail_policy: Literal["OPEN", "CLOSED", "VIRTUAL_ONLY"]
    ts_event_ms: int
    ts_decision_ms: int
    latency_us: int
    inputs_hash: str
    notes: dict[str, Any] = field(default_factory=dict)

    def to_dict(self) -> dict[str, Any]:
        """Convert GateDecisionV1 to dictionary for JSON serialization."""
        return {
            "stage": str(self.stage),
            "gate": str(self.gate),
            "decision": str(self.decision),
            "reason_code": str(self.reason_code),
            "severity": str(self.severity),
            "profile": str(self.profile),
            "fail_policy": str(self.fail_policy),
            "ts_event_ms": int(self.ts_event_ms),
            "ts_decision_ms": int(self.ts_decision_ms),
            "latency_us": int(self.latency_us),
            "inputs_hash": str(self.inputs_hash),
            "notes": dict(self.notes) if self.notes else {},
        }
