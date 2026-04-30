import time
from typing import Dict, Any, List, Optional
import hashlib
from datetime import datetime, timezone, timedelta

FREEZE_PRECEDENCE = {
    "clip": 10
    "no_new_risk": 20
    "scope_frozen": 30
    "venue_frozen": 40
    "promotions_frozen": 50
    "release_frozen": 60
    "hard_freeze": 100
}

class ATRFreezeMatrixService:
    """
    Service responsible for translating triggers (exhaustions, critical incidents, cert failures)
    into formal active freezes and escalating them properly.
    """

    def __init__(self, advisory_only: bool = True):
        self.advisory_only = advisory_only

    def _get_precedence(self, freeze_state: str) -> int:
        return FREEZE_PRECEDENCE.get(freeze_state, 0)

    def resolve_freeze_state(self, trigger_kind: str, scope_kind: str, severity: str
                             available_policies: List[Dict[str, Any]]) -> Optional[Dict[str, Any]]:
        """
        Find the matching policy from the catalog.
        """
        for p in available_policies:
            if not p.get("is_enabled", True):
                continue
            if p["trigger_kind"] == trigger_kind and p["severity"] == severity:
                # We could have a global policy applying to symbol scopes, but for now exact match:
                if p["scope_kind"] == scope_kind or p["scope_kind"] == "global":
                    return p
        return None

    def evaluate_trigger(self, trigger: Dict[str, Any]
                         active_freezes: List[Dict[str, Any]]
                         available_policies: List[Dict[str, Any]]) -> Dict[str, Any]:
        """
        Takes a trigger and the current list of active freezes, evaluates precedence
        and either creates a new freeze or escalates an existing one.
        """
        trigger_kind = trigger.get("trigger_kind", "unknown")
        scope_kind = trigger.get("scope_kind", "global")
        scope_value = trigger.get("scope_value", "all")
        severity = trigger.get("severity", "critical")
        reason_code = trigger.get("reason_code", "unknown_trigger")

        policy = self.resolve_freeze_state(trigger_kind, scope_kind, severity, available_policies)
        if not policy:
            return {"status": "skipped", "reason": "no_matching_policy"}

        target_freeze_state = policy["freeze_state"]
        ttl_sec = policy.get("ttl_sec", 3600)
        
        target_precedence = self._get_precedence(target_freeze_state)

        # Check existing freezes for the same scope
        matching_active = [f for f in active_freezes if f["scope_kind"] == scope_kind and f["scope_value"] == scope_value and f["status"] != "released"]

        highest_active_precedence = 0
        existing_freeze_id = None
        current_state = None

        if matching_active:
            highest_freeze = max(matching_active, key=lambda f: self._get_precedence(f["freeze_state"]))
            highest_active_precedence = self._get_precedence(highest_freeze["freeze_state"])
            existing_freeze_id = highest_freeze["freeze_id"]
            current_state = highest_freeze["freeze_state"]

        now_ts = int(time.time() * 1000)
        # We assign recovery dwell time: standard hysteresis delay is half of TTL, but capped at 1 hour
        hysteresis_seconds = min(ttl_sec // 2, 3600)

        now_utc = datetime.now(timezone.utc)
        expires_at_dt = now_utc + timedelta(seconds=ttl_sec)
        recovery_dt = now_utc + timedelta(seconds=hysteresis_seconds)

        if highest_active_precedence >= target_precedence:
            # Already in a stronger or equal freeze state
            # Extend TTL if necessary
            return {
                "status": "extended" if highest_active_precedence == target_precedence else "ignored_lower_priority"
                "freeze_id": existing_freeze_id
                "freeze_state": current_state
                "advisory_only": self.advisory_only
                "update_payload": {
                    "expires_at": expires_at_dt.isoformat()
                    "recovery_not_before": recovery_dt.isoformat()
                    "status": "active" # Refreeze if it was recovering
                }
            }
        else:
            # Ascend to harder freeze state
            new_id = existing_freeze_id or hashlib.sha1(f"{trigger_kind}|{scope_kind}|{scope_value}|{now_ts}".encode()).hexdigest()[:20]
            
            return {
                "status": "escalated" if existing_freeze_id else "created"
                "freeze_id": new_id
                "freeze_state": target_freeze_state
                "advisory_only": self.advisory_only
                "payload": {
                    "trigger_kind": trigger_kind
                    "scope_kind": scope_kind
                    "scope_value": scope_value
                    "freeze_state": target_freeze_state
                    "source_reason_code": reason_code
                    "status": "active"
                    "started_at": now_utc.isoformat()
                    "expires_at": expires_at_dt.isoformat()
                    "recovery_not_before": recovery_dt.isoformat()
                    "freeze_json": {
                        "escalated_from": current_state
                        "escalation_policy": policy.get("policy_id")
                    }
                }
            }

    def generate_redis_keys(self, active_freezes: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
        """
        Translate active freezes into Redis keys for runtime consumption
        """
        redis_updates = []
        for freeze in active_freezes:
            if freeze["status"] == "released":
                continue
            
            scope = f"{freeze['scope_kind']}:{freeze['scope_value']}"
            state = freeze["freeze_state"]

            # Runtime uses these generic keys with state payload
            if state in ["clip", "no_new_risk", "scope_frozen", "venue_frozen", "hard_freeze"]:
                redis_updates.append({
                    "key": f"cfg:atr_degrade:{scope}"
                    "value": {"state": state, "freeze_id": freeze["freeze_id"], "advisory": self.advisory_only}
                })
            
            if state in ["promotions_frozen", "release_frozen"]:
                redis_updates.append({
                    "key": f"cfg:atr_promotion_freeze:{scope}"
                    "value": {"state": state, "freeze_id": freeze["freeze_id"], "advisory": self.advisory_only}
                })

            if state in ["release_frozen", "hard_freeze"]:
                redis_updates.append({
                    "key": f"cfg:atr_release_freeze:{scope}"
                    "value": {"state": state, "freeze_id": freeze["freeze_id"], "advisory": self.advisory_only}
                })

        return redis_updates
