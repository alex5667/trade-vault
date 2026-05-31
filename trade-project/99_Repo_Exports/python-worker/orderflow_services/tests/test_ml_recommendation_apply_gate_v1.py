
import json

from orderflow_services.ml_recommendation_apply_gate_v1 import evaluate_apply_request


def test_apply_gate_blocks_when_replay_missing(monkeypatch):
    monkeypatch.setenv("ML_RECOMMENDATION_APPLY_MODE", "REVIEW_ONLY")
    monkeypatch.setenv("ML_RECOMMENDATION_APPLY_DRY_RUN", "1")
    monkeypatch.setenv("ML_RECOMMENDATION_MIN_APPROVALS", "1")

    dec = evaluate_apply_request({
        "recommendation_id": "r1",
        "action_type": "propose_threshold_canary",
        "target_kind": "edge_stack_v1",
        "target_ref": "cfg:ml_confirm:edge_stack_v1:champion",
        "review_status": "APPROVED",
        "approved_count": 1,
        "rejected_count": 0,
        "replay_status": "UNKNOWN",
        "risk_level": "low",
    })
    reasons = set(json.loads(dec.reason_codes_json))
    assert dec.allow == 0
    assert "REPLAY_REQUIRED_NOT_PASS" in reasons


def test_apply_gate_allows_review_only_when_replay_pass(monkeypatch):
    monkeypatch.setenv("ML_RECOMMENDATION_APPLY_MODE", "REVIEW_ONLY")
    monkeypatch.setenv("ML_RECOMMENDATION_APPLY_DRY_RUN", "1")
    monkeypatch.setenv("ML_RECOMMENDATION_MIN_APPROVALS", "1")

    dec = evaluate_apply_request({
        "recommendation_id": "r2",
        "action_type": "request_calibration_refresh",
        "target_kind": "confidence_cal",
        "target_ref": "conf_cal:latest",
        "review_status": "APPROVED",
        "approved_count": 1,
        "rejected_count": 0,
        "replay_status": "PASS",
        "risk_level": "low",
    })
    assert dec.allow == 1
    assert dec.status == "REVIEW_ONLY"
