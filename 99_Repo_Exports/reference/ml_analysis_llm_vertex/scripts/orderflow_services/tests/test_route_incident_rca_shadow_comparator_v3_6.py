from orderflow_services.route_incident_rca_shadow_comparator_v3_6 import (
    compare_rows,
    correlation_key,
)


def test_correlation_key_prefers_incident_id():
    row = {"incident_id": "i1", "request_id": "r1", "compact_hash": "h1"}
    assert correlation_key(row) == "i1"


def test_compare_rows_match():
    handoff = {
        "incident_id": "i1",
        "task_type": "route_incident_rca",
        "severity": "warning",
        "compact_hash": "abc",
        "payload_json": '{"summary":"x","primary_reason_codes":["ROUTE_MISMATCH"]}',
    }
    legacy = {
        "incident_id": "i1",
        "task_type": "route_incident_rca",
        "severity": "warning",
        "compact_hash": "abc",
        "payload_json": '{"summary":"x","primary_reason_codes":["ROUTE_MISMATCH"]}',
    }
    out = compare_rows(handoff, legacy)
    assert out["status"] == "MATCH"
    assert out["score"] >= 0.90


def test_compare_rows_detects_drift():
    handoff = {
        "incident_id": "i1",
        "task_type": "route_incident_rca",
        "severity": "warning",
        "payload_json": '{"summary":"x","primary_reason_codes":["A"],"extra_h":"1"}',
    }
    legacy = {
        "incident_id": "i1",
        "task_type": "route_incident_rca",
        "severity": "warning",
        "payload_json": '{"summary":"x","primary_reason_codes":["B"],"extra_l":"1"}',
    }
    out = compare_rows(handoff, legacy)
    assert out["status"] in {"DRIFT", "MISMATCH"}
    assert "PAYLOAD_KEY_DRIFT" in out["reason_codes"]
