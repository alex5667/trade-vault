import time
from typing import Dict, Any, List, Optional
from datetime import datetime, timezone

class ATRUnfreezeHysteresisService:
    """
    Service responsible for the gradual unfreeze of scope boundaries.
    Enforces dwell times and strict health checks before reverting freeze states.
    Transitions: active -> recovering -> clip -> released
    """

    def __init__(self, require_cert: bool = True):
        self.require_cert = require_cert
    
    def evaluate_unfreeze_candidates(self, active_freezes: List[Dict[str, Any]], 
                                     health_context: Dict[str, Any]) -> List[Dict[str, Any]]:
        """
        Evaluate active and recovering freezes to see if they can graduate to a lighter state.
        
        health_context expected keys:
        - burn_rate_healthy (bool)
        - allocator_fresh (bool)
        - open_critical_incidents (int)
        - recent_violations (int)
        """
        transitions = []
        now_utc = datetime.now(timezone.utc)
        
        # Unpack health context
        burn_rate_healthy = health_context.get("burn_rate_healthy", False)
        allocator_fresh = health_context.get("allocator_fresh", False)
        open_critical_incidents = health_context.get("open_critical_incidents", 1)  # safe default
        recent_violations = health_context.get("recent_violations", 1)              # safe default

        health_check_passed = (burn_rate_healthy and allocator_fresh and 
                               open_critical_incidents == 0 and recent_violations == 0)

        for freeze in active_freezes:
            status = freeze.get("status")
            if status not in ["active", "recovering"]:
                continue
            
            recovery_str = freeze.get("recovery_not_before")
            if not recovery_str:
                continue

            try:
                recovery_dt = datetime.fromisoformat(recovery_str)
            except ValueError:
                continue

            if now_utc < recovery_dt:
                # Dwell time not yet completed
                continue

            current_state = freeze.get("freeze_state")
            
            if status == "active":
                # Graduating from active -> recovering
                if health_check_passed:
                    transitions.append({
                        "freeze_id": freeze["freeze_id"],
                        "old_status": "active",
                        "new_status": "recovering",
                        "reason_code": "HYSTERESIS_DWELL_COMPLETE_HEALTHY",
                        "update_payload": {
                            "status": "recovering",
                            # Reset dwell time for next phase
                            "recovery_not_before": (now_utc + (recovery_dt - datetime.fromisoformat(freeze["started_at"]))).isoformat()
                        }
                    })
            elif status == "recovering":
                # Graduating from recovering -> clip or released
                if health_check_passed:
                    
                    if self.require_cert and current_state in ["hard_freeze", "venue_frozen", "scope_frozen"]:
                        cert_passed = health_context.get(f"cert_passed_{freeze['freeze_id']}", False)
                        if not cert_passed:
                            transitions.append({
                                "freeze_id": freeze["freeze_id"],
                                "old_status": "recovering",
                                "new_status": "recovering",
                                "reason_code": "HYSTERESIS_PENDING_CERT",
                                "update_payload": {}
                            })
                            continue
                            
                    # Stage down depending on the freeze strength
                    next_state = "released"
                    if current_state in ["hard_freeze", "scope_frozen"]:
                        # Enforce a final 'clip' safety buffer before full release
                        next_state = "clip"
                        # We change the freeze state logically, but status remains recovering until released
                        transitions.append({
                            "freeze_id": freeze["freeze_id"],
                            "old_status": "recovering",
                            "new_status": "recovering",
                            "reason_code": "HYSTERESIS_STAGED_TO_CLIP",
                            "update_payload": {
                                "freeze_state": "clip",
                                "recovery_not_before": (now_utc + (now_utc - recovery_dt)).isoformat()
                            }
                        })
                    else:
                        # Fully release things like no_new_risk, clip, promotions_frozen
                        transitions.append({
                            "freeze_id": freeze["freeze_id"],
                            "old_status": "recovering",
                            "new_status": "released",
                            "reason_code": "HYSTERESIS_FULLY_RELEASED",
                            "update_payload": {
                                "status": "released",
                                "released_at": now_utc.isoformat()
                            }
                        })
                else:
                    # Health check failed during recovering: refreeze
                    transitions.append({
                        "freeze_id": freeze["freeze_id"],
                        "old_status": "recovering",
                        "new_status": "active",
                        "reason_code": "HYSTERESIS_HEALTH_FAILED_REFREEZE",
                        "update_payload": {
                            "status": "active",
                            "recovery_not_before": (now_utc + (now_utc - recovery_dt)).isoformat()
                        }
                    })
                    
        return transitions
