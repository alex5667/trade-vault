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
