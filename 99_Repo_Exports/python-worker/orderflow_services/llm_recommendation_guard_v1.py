from __future__ import annotations

from typing import Any, Dict, List, Tuple


ALLOWED_ACTIONS = {
    "require_shadow_retrain",
    "freeze_candidate",
    "unfreeze_candidate",
    "request_calibration_refresh",
    "propose_threshold_canary",
    "open_incident",
    "draft_postmortem",
}

BLOCKED_ACTIONS = {
    "enable_enforce",
    "raise_risk_limit",
    "lower_risk_limit",
    "change_execution_caps",
    "change_exit_policy",
    "change_position_size",
}


def validate_analysis_output(payload: Dict[str, Any]) -> Tuple[bool, List[str]]:
    errs: List[str] = []
    if not isinstance(payload, dict):
        return False, ["payload_not_dict"]
    for field in ("schema_version", "analysis_run_id", "status", "summary", "findings", "recommendations"):
        if field not in payload:
            errs.append(f"missing_{field}")
    if not isinstance(payload.get("findings", []), list):
        errs.append("findings_not_list")
    if not isinstance(payload.get("recommendations", []), list):
        errs.append("recommendations_not_list")
    return len(errs) == 0, errs


def guard_recommendations(payload: Dict[str, Any]) -> Dict[str, Any]:
    ok, errs = validate_analysis_output(payload)
    recommendations = payload.get("recommendations", []) if isinstance(payload, dict) else []
    guarded = []
    blocked: List[Dict[str, Any]] = []
    if not ok:
        return {
            "valid": False,
            "errors": errs,
            "guarded_recommendations": [],
            "blocked_recommendations": [],
        }
    for item in recommendations:
        if not isinstance(item, dict):
            blocked.append({"reason": "recommendation_not_dict", "item": item})
            continue
        action = str(item.get("action", "")).strip()
        risk = str(item.get("risk", "medium")).strip() or "medium"
        if action in BLOCKED_ACTIONS:
            blocked.append({"reason": "blocked_action", "action": action, "item": item})
            continue
        if action not in ALLOWED_ACTIONS:
            blocked.append({"reason": "action_not_allowed", "action": action, "item": item})
            continue
        item2 = dict(item)
        item2["apply_mode"] = "REVIEW_ONLY"
        item2["risk"] = risk
        guarded.append(item2)
    return {
        "valid": True,
        "errors": [],
        "guarded_recommendations": guarded,
        "blocked_recommendations": blocked,
    }
