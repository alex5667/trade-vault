from orderflow_services.recommendation_action_adapters_v1 import apply_recommendation_adapter



def test_threshold_canary_within_bounds():
    res = apply_recommendation_adapter(
        action_type="propose_threshold_canary",
        target_kind="ml_confirm_cfg",
        target_ref="edge_stack_v1",
        recommendation_json={"to": 0.58},
        current_state={"p_min": 0.60},
        dry_run=True,
        max_threshold_delta=0.03,
    )
    assert res.ok is True
    assert '"p_min_canary":0.58' in res.after_json
    assert '"canary_enabled":1' in res.after_json


def test_threshold_canary_rejects_large_delta():
    res = apply_recommendation_adapter(
        action_type="propose_threshold_canary",
        target_kind="ml_confirm_cfg",
        target_ref="edge_stack_v1",
        recommendation_json={"to": 0.50},
        current_state={"p_min": 0.60},
        dry_run=True,
        max_threshold_delta=0.03,
    )
    assert res.ok is False
    assert res.reason_code == "THRESHOLD_DELTA_TOO_LARGE"
