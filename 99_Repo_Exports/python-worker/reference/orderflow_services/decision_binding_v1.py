"""Decision Binding V1 (P48).

Explicit binding matrix between Rule and ML signals.
Determines the 'recommended' action and reason code based on input states.
"""

import os
from typing import Any, Dict

def _env_bool(name: str, default: str = "0") -> bool:
    v = os.getenv(name, default)
    return str(v).strip().lower() in {"1", "true", "yes", "on"}

def bind_rule_ml_v1(
    *
    rule_ok: int
    rule_ok_soft: int
    ml_state: str
    dq_state: str
    drift_state: str
) -> Dict[str, Any]:
    """Explicit binding matrix between Rule and ML.

    Returns a dict:
      {action, source, reason_code, soft}

    action: allow | deny
    source: rule | ml | both
    soft: 1 if allowed-but-soft

    Policy (default, conservative):
    - DQ bad -> deny (unless rule_strong override enabled)
    - Drift bad -> deny (unless rule_strong override enabled)
    - Rule-Strong + ML-Allow -> allow (both)
    - Rule-Strong + ML-Abstain -> allow (rule)
    - Rule-Strong + ML-Deny -> deny (ml veto) [override optional]
    - Rule-Soft + ML-Allow -> allow_soft (both)
    - Rule-Soft + ML-Abstain -> deny
    - Rule-Soft + ML-Deny -> deny
    - Rule-Deny -> deny always (even if ML-Allow)

    Overrides via env:
    - BIND_RULE_STRONG_OVERRIDES_ML_DENY (default 0)
    - BIND_ALLOW_RULE_STRONG_ON_BAD_DQ (default 0)
    - BIND_ALLOW_RULE_STRONG_ON_BAD_DRIFT (default 0)
    """

    rule_ok = int(rule_ok or 0)
    rule_ok_soft = int(rule_ok_soft or 0)
    ml_state = str(ml_state or "off").lower()
    dq_state = str(dq_state or "na").lower()
    drift_state = str(drift_state or "na").lower()

    rule_strength = "deny"
    if rule_ok == 1:
        rule_strength = "strong"
    elif rule_ok_soft == 1:
        rule_strength = "soft"

    # System knobs
    strong_overrides_ml_deny = _env_bool("BIND_RULE_STRONG_OVERRIDES_ML_DENY", "0")
    allow_rule_strong_on_bad_dq = _env_bool("BIND_ALLOW_RULE_STRONG_ON_BAD_DQ", "0")
    allow_rule_strong_on_bad_drift = _env_bool("BIND_ALLOW_RULE_STRONG_ON_BAD_DRIFT", "0")

    # DQ fail-closed
    if dq_state in {"bad", "fail", "veto"}:
        if rule_strength == "strong" and allow_rule_strong_on_bad_dq:
            return {
                "action": "allow"
                "source": "rule"
                "reason_code": "DQ_BAD__RULE_STRONG_OVERRIDE"
                "soft": 0
            }
        return {
            "action": "deny"
            "source": "dq"
            "reason_code": "DQ_BAD__DENY"
            "soft": 0
        }

    # Drift fail-closed (P51)
    # Norm drift_state to: ok | warn | block | na
    if drift_state in {"block", "fail", "veto", "2"}:
        # Check policy: DENY vs RULE_STRONG_ONLY
        # To check RULE_STRONG_ONLY, we need rule_strength="strong".
        # But wait, binding function doesn't see rule_score, only rule_ok (int).
        # We assume caller handles specific score thresholds if they want dynamic overrides.
        # But here we implement static policy from ENV or just binding logic.
        # The user req says: "если DRIFT_GATE_BLOCK_POLICY=RULE_STRONG_ONLY и rule_score >= OF_RULE_STRONG_MIN"
        # The binding function receiving `drift_state` doesn't know rule_score value, only rule_ok bool.
        # However, `drift_state` passed here might already be the *result* of the gate?
        # NO, the binding is "what should we do based on states".
        # The prompt says: "drift_state=BLOCK -> recommended_action = veto, reason_code = DRIFT_BLOCK"
        # AND "if DRIFT_GATE_BLOCK_POLICY=RULE_STRONG_ONLY ... recommended_action=emit"
        # We'll rely on the fact that if policy=RULE_STRONG_ONLY and it passed, handling upsteam might have set drift_state="block_but_allowed"?
        # Actually, let's look at `signal_pipeline.py`: _eval_drift_gate returns `reason_code="DRIFT_BLOCK_RULE_STRONG_ONLY_PASS"`
        # So if we receive drift_state="block", it means it WAS blocked.
        
        # Implementation:
        # If drift_state is explicitly "block" (2), we deny.
        # If it was "block_but_allowed", providing `drift_state` as "block" would be ambiguous.
        # Let's check `signal_pipeline.py` again. `_eval_drift_gate` returns `state="block_but_allowed"`.
        # So we should treat "block" as hard block.
        
        check_override = _env_bool("BIND_ALLOW_RULE_STRONG_ON_BAD_DRIFT", "0")
        if rule_strength == "strong" and check_override:
             return {
                "action": "allow"
                "source": "rule"
                "reason_code": "DRIFT_BLOCK__RULE_STRONG_OVERRIDE"
                "soft": 0
            }
        return {
            "action": "deny"
            "source": "drift"
            "reason_code": "DRIFT_BLOCK"
            "soft": 0
        }

    # Main matrix
    reason_suffix = ""
    if drift_state in {"warn", "1"}:
        reason_suffix = "_DRIFT_WARN"

    if rule_strength == "strong":
        if ml_state == "allow":
            return {"action": "allow", "source": "both", "reason_code": "RULE_STRONG__ML_ALLOW" + reason_suffix, "soft": 0}
        if ml_state == "abstain" or ml_state == "off" or ml_state == "error":
            return {"action": "allow", "source": "rule", "reason_code": f"RULE_STRONG__ML_{ml_state.upper()}{reason_suffix}", "soft": 0}
        if ml_state == "deny":
            if strong_overrides_ml_deny:
                return {"action": "allow", "source": "rule", "reason_code": "RULE_STRONG__ML_DENY_OVERRIDE" + reason_suffix, "soft": 0}
            return {"action": "deny", "source": "ml", "reason_code": "RULE_STRONG__ML_DENY_VETO" + reason_suffix, "soft": 0}
        return {"action": "allow", "source": "rule", "reason_code": "RULE_STRONG__ML_UNKNOWN" + reason_suffix, "soft": 0}

    if rule_strength == "soft":
        if ml_state == "allow":
            return {"action": "allow", "source": "both", "reason_code": "RULE_SOFT__ML_ALLOW" + reason_suffix, "soft": 1}
        if ml_state in {"abstain", "off", "error"}:
            return {"action": "deny", "source": "ml", "reason_code": f"RULE_SOFT__ML_{ml_state.upper()}_DENY{reason_suffix}", "soft": 0}
        if ml_state == "deny":
            return {"action": "deny", "source": "ml", "reason_code": "RULE_SOFT__ML_DENY" + reason_suffix, "soft": 0}
        return {"action": "deny", "source": "ml", "reason_code": "RULE_SOFT__ML_UNKNOWN_DENY", "soft": 0}

    # Rule deny
    return {"action": "deny", "source": "rule", "reason_code": "RULE_DENY" + reason_suffix, "soft": 0}

# P62 Adapter
from dataclasses import dataclass
from typing import Optional

@dataclass
class BindingInput:
    rule_score: float
    rule_ok: bool
    rule_ok_soft: bool = False  # Adapt alias
    rule_soft: bool = False
    ml_state: str = "na"
    ml_p_cal: Optional[float] = None
    dq_state: str = "unknown"
    drift_state: str = "unknown"

    def __post_init__(self):
        # normalize rule_soft vs rule_ok_soft
        if self.rule_soft: self.rule_ok_soft = True

def recommend_binding(i: BindingInput) -> Dict[str, Any]:
    """Adapter for P62 to use bind_rule_ml_v1."""
    res = bind_rule_ml_v1(
        rule_ok=int(i.rule_ok)
        rule_ok_soft=int(i.rule_ok_soft)
        ml_state=i.ml_state
        dq_state=i.dq_state
        drift_state=i.drift_state
    )
    # Map back to P62 expected keys if needed
    # P62 expects: "recommended_action", "recommended_reason_code"
    # bind_rule_ml_v1 returns: "action", "reason_code"
    return {
        "recommended_action": res["action"]
        "recommended_reason_code": res["reason_code"]
        "soft": res["soft"]
        "source": res["source"]
    }
