from __future__ import annotations

import copy
import json
from dataclasses import dataclass
from typing import Any

from utils.time_utils import get_ny_time_millis

ALLOWED_ACTIONS = {
    "propose_threshold_canary",
    "request_calibration_refresh",
    "freeze_candidate",
    "unfreeze_candidate",
}

LOW_RISK_ACTIONS = {
    "propose_threshold_canary",
    "request_calibration_refresh",
    "freeze_candidate",
    "unfreeze_candidate",
}

ALLOWED_TARGET_KINDS = {
    "ml_confirm_cfg",
    "model_registry_flag",
    "calibration_job",
}

REPLAY_REQUIRED_ACTIONS = {
    "propose_threshold_canary",
}


@dataclass(frozen=True)
class AdapterResult:
    ok: bool
    dry_run: bool
    action: str
    target_kind: str
    target_ref: str
    before_json: str
    after_json: str
    patch_json: str
    rollback_json: str
    reason_code: str = "OK"


def stable_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def _to_float(v: Any) -> float | None:
    try:
        x = float(v)
        if x != x:
            return None
        return x
    except Exception:
        return None


def _bounded_threshold_change(current: float, proposed: float, max_delta: float, floor: float, ceil: float) -> tuple[bool, float, str]:
    if proposed < floor or proposed > ceil:
        return False, current, "THRESHOLD_OUT_OF_BOUNDS"
    if abs(proposed - current) > max_delta:
        return False, current, "THRESHOLD_DELTA_TOO_LARGE"
    return True, proposed, "OK"


def apply_recommendation_adapter(
    *,
    action_type: str,
    target_kind: str,
    target_ref: str,
    recommendation_json: dict[str, Any],
    current_state: dict[str, Any],
    dry_run: bool,
    max_threshold_delta: float = 0.03,
    threshold_floor: float = 0.0,
    threshold_ceil: float = 1.0,
) -> AdapterResult:
    action_type = (action_type or "")
    target_kind = (target_kind or "")
    target_ref = (target_ref or "")
    rec = copy.deepcopy(recommendation_json or {})
    before = copy.deepcopy(current_state or {})
    after = copy.deepcopy(before)

    if action_type not in ALLOWED_ACTIONS:
        return AdapterResult(False, dry_run, action_type, target_kind, target_ref, stable_json(before), stable_json(after), stable_json({}), stable_json({}), "ACTION_NOT_ALLOWED")
    if action_type not in LOW_RISK_ACTIONS:
        return AdapterResult(False, dry_run, action_type, target_kind, target_ref, stable_json(before), stable_json(after), stable_json({}), stable_json({}), "ACTION_NOT_LOW_RISK")
    if target_kind not in ALLOWED_TARGET_KINDS:
        return AdapterResult(False, dry_run, action_type, target_kind, target_ref, stable_json(before), stable_json(after), stable_json({}), stable_json({}), "TARGET_KIND_NOT_ALLOWED")

    patch: dict[str, Any] = {}

    if action_type == "freeze_candidate":
        after["promotion_state"] = "FROZEN"
        patch = {"promotion_state": "FROZEN"}
    elif action_type == "unfreeze_candidate":
        after["promotion_state"] = "SHADOW"
        patch = {"promotion_state": "SHADOW"}
    elif action_type == "request_calibration_refresh":
        after["calibration_refresh_requested"] = 1
        after["calibration_refresh_requested_at_ms"] = get_ny_time_millis()
        patch = {
            "calibration_refresh_requested": 1,
            "calibration_refresh_requested_at_ms": after["calibration_refresh_requested_at_ms"],
        }
    elif action_type == "propose_threshold_canary":
        current = _to_float(before.get("p_min"))
        proposed = _to_float(rec.get("to"))
        if current is None or proposed is None:
            return AdapterResult(False, dry_run, action_type, target_kind, target_ref, stable_json(before), stable_json(after), stable_json({}), stable_json({}), "THRESHOLD_FIELDS_MISSING")
        ok, bounded, reason = _bounded_threshold_change(current, proposed, max_threshold_delta, threshold_floor, threshold_ceil)
        if not ok:
            return AdapterResult(False, dry_run, action_type, target_kind, target_ref, stable_json(before), stable_json(after), stable_json({}), stable_json({}), reason)
        after["p_min_canary"] = bounded
        after["canary_enabled"] = 1
        patch = {"p_min_canary": bounded, "canary_enabled": 1}
    else:
        return AdapterResult(False, dry_run, action_type, target_kind, target_ref, stable_json(before), stable_json(after), stable_json({}), stable_json({}), "ACTION_NOT_IMPLEMENTED")

    rollback = {
        "action_type": action_type,
        "target_kind": target_kind,
        "target_ref": target_ref,
        "before": before,
    }
    return AdapterResult(True, dry_run, action_type, target_kind, target_ref, stable_json(before), stable_json(after), stable_json(patch), stable_json(rollback), "OK")
