from __future__ import annotations

import hashlib
import time
from typing import Any


class InvariantRemediationExecutor:
    """
    Executes only bounded, idempotent, reversible remediations.
    No payload auto-fixes. No silent control-plane mutation.
    """

    def execute(self, violation: dict[str, Any], policy: dict[str, Any]) -> dict[str, Any]:
        kind = (policy.get("remediation_kind", "unknown"))
        inv = (violation.get("invariant_id", "unknown"))
        scope_kind = (violation.get("scope_kind", "global"))
        scope_value = (violation.get("scope_value", "none"))
        action_id = hashlib.sha1(f"{inv}|{scope_kind}|{scope_value}|{int(time.time()*1000)}".encode()).hexdigest()[:20]

        if kind == "deny_only":
            return {
                "action_id": action_id,
                "status": "skipped",
                "reason_code": "REMEDIATION_DENY_ONLY",
                "action_json": {"invariant_id": inv}
            }

        if kind == "runtime_clip":
            return {
                "action_id": action_id,
                "status": "executed",
                "reason_code": "REMEDIATION_RUNTIME_CLIP",
                "action_json": {
                    "scope_kind": scope_kind,
                    "scope_value": scope_value,
                    "clip_mult": policy.get("policy_json", {}).get("clip_mult", 0.5)
                }
            }

        if kind == "scope_freeze":
            return {
                "action_id": action_id,
                "status": "executed",
                "reason_code": "REMEDIATION_SCOPE_FREEZE",
                "action_json": {
                    "scope_kind": scope_kind,
                    "scope_value": scope_value,
                    "target_state": policy.get("policy_json", {}).get("target_state", "no_new_risk")
                }
            }

        if kind == "rollout_pause":
            return {
                "action_id": action_id,
                "status": "executed",
                "reason_code": "REMEDIATION_ROLLOUT_PAUSE",
                "action_json": {"scope_kind": scope_kind, "scope_value": scope_value}
            }

        if kind == "rollback_request":
            return {
                "action_id": action_id,
                "status": "requested",
                "reason_code": "REMEDIATION_ROLLBACK_REQUEST",
                "action_json": policy.get("policy_json", {})
            }

        if kind == "last_good_restore":
            return {
                "action_id": action_id,
                "status": "requested",
                "reason_code": "REMEDIATION_LAST_GOOD_RESTORE_REQUEST",
                "action_json": policy.get("policy_json", {})
            }

        if kind == "serving_rebuild":
            return {
                "action_id": action_id,
                "status": "requested",
                "reason_code": "REMEDIATION_SERVING_REBUILD_REQUEST",
                "action_json": policy.get("policy_json", {})
            }

        return {
            "action_id": action_id,
            "status": "failed",
            "reason_code": "REMEDIATION_UNKNOWN_KIND",
            "action_json": {}
        }
