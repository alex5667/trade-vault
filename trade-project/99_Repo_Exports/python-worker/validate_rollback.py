import sys
import logging
sys.path.append("/app")

import time
import json
from services.atr_rollback_control_service import request_rollback, get_rollback, get_conn, approve_rollback, execute_rollback, certify_rollback, finalize_rollback
from services.atr_rollback_telegram_surface import publish_rollback_to_telegram

logging.basicConfig(level=logging.INFO)

def main():
    change_id = f"chg_mock_{int(time.time())}"
    rollback_id = f"rbk_mock_{int(time.time())}"
    
    manifest = {
        "rollback_id": rollback_id,
        "change_id": change_id,
        "action_plan": {"rollout_stage_target": "shadow", "policy_ver_target": 1},
        "open_position_policy": {"new_entries": "deny", "trailing_behavior": "freeze_current"}
    }
    
    print("1. Requesting Rollback...")
    ok = request_rollback(
        rollback_id=rollback_id,
        change_id=change_id,
        rollback_class="LAYER_ROLLBACK",
        scope_kind="layer",
        manifest=manifest,
        author="auto_test",
        owner="auto_test",
        reason_code="smoke_test",
        layer="trailing"
    )
    print(f"Request OK: {ok}")
    
    rb = get_rollback(rollback_id)
    print(f"Status after request: {rb['status']}")
    
    # 2. Approve
    ok = approve_rollback(rollback_id, "admin")
    print(f"Approve OK: {ok}")
    rb = get_rollback(rollback_id)
    print(f"Status after approve: {rb['status']}")
    
    ok = execute_rollback(rollback_id)
    print(f"Execute OK: {ok}")
    rb = get_rollback(rollback_id)
    print(f"Status after execute: {rb['status']}")
    
    ok = certify_rollback(rollback_id)
    print(f"Certify OK: {ok}")
    rb = get_rollback(rollback_id)
    print(f"Status after certify: {rb['status']}")
    
    ok = finalize_rollback(rollback_id)
    print(f"Finalize OK: {ok}")
    rb = get_rollback(rollback_id)
    print(f"Status after finalize: {rb['status']}")

    try:
        ok = publish_rollback_to_telegram(rb)
        print(f"Publish to TG OK: {ok}")
    except Exception as e:
        print(f"Publish error: {e}")
        
if __name__ == "__main__":
    main()
