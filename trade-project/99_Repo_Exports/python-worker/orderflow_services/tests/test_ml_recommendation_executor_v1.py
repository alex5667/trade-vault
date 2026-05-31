from orderflow_services.ml_recommendation_executor_v1 import evaluate_apply_request, evaluate_rollback_request


def test_apply_requires_replay_for_threshold_canary():
    payload = {
        "action_type": "propose_threshold_canary",
        "target_kind": "ml_confirm_cfg",
        "target_ref": "edge_stack_v1",
        "recommendation_json": '{"to":0.58}',
        "approval_status": "APPROVED",
        "replay_status": "FAIL",
    }
    dec = evaluate_apply_request(payload, {"p_min": 0.60}, mode="DRY_RUN")
    assert dec.ok is False
    assert dec.reason_code == "REPLAY_REQUIRED"


def test_apply_dry_run_success():
    payload = {
        "action_type": "freeze_candidate",
        "target_kind": "model_registry_flag",
        "target_ref": "edge_stack_v1_candidate",
        "recommendation_json": '{}',
        "approval_status": "APPROVED",
        "replay_status": "PASS",
    }
    dec = evaluate_apply_request(payload, {"promotion_state": "SHADOW"}, mode="DRY_RUN")
    assert dec.ok is True
    assert dec.status == "DRY_RUN"


def test_rollback_uses_before_state():
    payload = {
        "rollback_json": '{"action_type":"freeze_candidate","target_kind":"model_registry_flag","target_ref":"x","before":{"promotion_state":"SHADOW"}}',
        "target_kind": "model_registry_flag",
        "target_ref": "x",
    }
    dec = evaluate_rollback_request(payload, {"promotion_state": "FROZEN"}, mode="DRY_RUN")
    assert dec.ok is True
    assert dec.status == "DRY_RUN"
    assert '"promotion_state":"SHADOW"' in dec.result_payload["after_json"]
