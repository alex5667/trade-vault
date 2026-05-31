
from orderflow_services.ml_recommendation_review_bus_v1 import (
    proposal_to_review_request,
    should_emit_apply_request,
)


def test_proposal_to_review_request_marks_replay_required():
    req = proposal_to_review_request({
        "analysis_run_id": "run1",
        "ts_ms": 1,
        "action_type": "propose_threshold_canary",
        "target_kind": "edge_stack_v1",
        "target_ref": "cfg:ml_confirm:edge_stack_v1:champion",
        "risk_level": "low",
    })
    assert req["review_status"] == "PENDING"
    assert req["replay_required"] == 1


def test_should_emit_apply_request_requires_approval_and_replay(monkeypatch):
    monkeypatch.setenv("ML_RECOMMENDATION_MIN_APPROVALS", "1")
    ok, reasons = should_emit_apply_request({
        "action_type": "propose_threshold_canary",
        "approved_count": 1,
        "rejected_count": 0,
        "risk_level": "low",
        "replay_required": 1,
        "replay_status": "PASS",
    }, min_approvals=1)
    assert ok is True
    assert reasons == []

    ok2, reasons2 = should_emit_apply_request({
        "action_type": "propose_threshold_canary",
        "approved_count": 1,
        "rejected_count": 0,
        "risk_level": "low",
        "replay_required": 1,
        "replay_status": "UNKNOWN",
    }, min_approvals=1)
    assert ok2 is False
    assert "REPLAY_REQUIRED_NOT_PASS" in reasons2
