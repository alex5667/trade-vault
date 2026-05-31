from orderflow_services.route_incident_rca_shadow_handoff_adapter_v3_5 import (
    build_handoff_shadow_row,
    build_legacy_shadow_row,
    evaluate_row,
    policy_from_hash,
)


def _policy(mode: str = "AUDIT_ONLY"):
    return {
        "enabled": 1,
        "mode": mode,
        "max_payload_bytes": 131072,
        "max_prompt_chars": 16000,
    }


def test_evaluate_row_rejects_missing_identifiers():
    row = {"summary": "x"}
    out = evaluate_row(row, _policy())
    assert out["decision"] == "REJECT"
    assert out["reason_code"] == "IDENTIFIER_MISSING"


def test_evaluate_row_accepts_bounded_payload():
    row = {"request_id": "rr-1", "incident_id": "route-inc-1", "summary": "x"}
    out = evaluate_row(row, _policy())
    assert out["decision"] == "ROUTE_SHADOW"
    assert out["reason_code"] == "OK"


def test_evaluate_row_rejects_if_disabled():
    row = {"request_id": "rr-1"}
    p = _policy()
    p["enabled"] = 0
    out = evaluate_row(row, p)
    assert out["decision"] == "REJECT"
    assert out["reason_code"] == "DISABLED"


def test_evaluate_row_rejects_if_mode_disabled():
    row = {"request_id": "rr-1"}
    p = _policy(mode="DISABLED")
    out = evaluate_row(row, p)
    assert out["decision"] == "REJECT"
    assert out["reason_code"] == "MODE_DISABLED"


def test_evaluate_row_rejects_oversized_prompt():
    row = {"request_id": "rr-1", "prompt": "x" * 20000}
    out = evaluate_row(row, _policy())
    assert out["decision"] == "REJECT"
    assert out["reason_code"] == "PROMPT_TOO_LARGE"


def test_build_shadow_rows_have_expected_family_and_shape():
    row = {
        "request_id": "rr-1",
        "incident_id": "route-inc-1",
        "severity": "warning",
        "task_type": "route_incident_rca",
        "summary": "Shadow this payload",
    }
    handoff = build_handoff_shadow_row(row)
    legacy = build_legacy_shadow_row(row)

    assert handoff["task_family"] == "route_incident_rca"
    assert handoff["shadow_mode"] == "1"
    assert "payload_json" in handoff
    assert handoff["force_local"] == "0"
    
    assert legacy["shadow_mode"] == "1"
    assert legacy["incident_id"] == "route-inc-1"
    assert "payload_json" in legacy


def test_policy_from_hash_invalid_mode_fallback():
    raw = {"enabled": "1", "mode": "UNKNOWN"}
    pol = policy_from_hash(raw)
    assert pol["mode"] == "AUDIT_ONLY"  # Default fallback


def test_build_handoff_shadow_row_handles_missing_request_id_fallback_to_incident_id():
    row = {"incident_id": "inc-2"}
    handoff = build_handoff_shadow_row(row)
    assert handoff["request_id"] == "inc-2"
