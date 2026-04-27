from orderflow_services.offline_debug_handoff_rewire_adapter_v3_3 import (
    build_handoff_row,
    build_payload_json,
    evaluate_row,
)


def _policy():
    return {
        "enabled": 1,
        "mode": "ENABLED",
        "max_payload_bytes": 131072,
        "max_prompt_chars": 16000,
        "force_local": 1,
    }


def test_evaluate_row_rejects_missing_request_id():
    row = {"prompt": "hello"}
    out = evaluate_row(row, _policy())
    assert out["decision"] == "REJECT"
    assert out["reason_code"] == "REQUEST_ID_MISSING"


def test_evaluate_row_accepts_bounded_offline_debug():
    row = {"request_id": "od-1", "prompt": "hello"}
    out = evaluate_row(row, _policy())
    assert out["decision"] == "ROUTE_HANDOFF"
    assert out["reason_code"] == "OK"


def test_evaluate_row_rejects_disabled_policy():
    row = {"request_id": "od-1", "prompt": "hello"}
    p = _policy()
    p["enabled"] = 0
    out = evaluate_row(row, p)
    assert out["decision"] == "REJECT"
    assert out["reason_code"] == "DISABLED"


def test_evaluate_row_rejects_disabled_mode():
    row = {"request_id": "od-1", "prompt": "hello"}
    p = _policy()
    p["mode"] = "DISABLED"
    out = evaluate_row(row, p)
    assert out["decision"] == "REJECT"
    assert out["reason_code"] == "MODE_DISABLED"


def test_evaluate_row_rejects_oversized_prompt():
    row = {"request_id": "od-1", "prompt": "x" * 17000}
    out = evaluate_row(row, _policy())
    assert out["decision"] == "REJECT"
    assert out["reason_code"] == "PROMPT_TOO_LARGE"


def test_build_handoff_row_sets_offline_debug_family():
    row = {
        "request_id": "od-1",
        "severity": "warning",
        "prompt": "Analyze replay mismatch",
        "snapshot_json": {"expected": "A", "got": "B"},
    }
    out = build_handoff_row(row, _policy())
    assert out["task_family"] == "offline_debug"
    assert out["task_type"] == "offline_debug"
    assert out["request_id"] == "od-1"
    assert out["source"] == "offline_debug_handoff_rewire_v3_3"
    assert out["severity"] == "warning"
    assert out["force_local"] == "1"
    assert int(out["ts_ms"]) > 0


def test_build_handoff_row_default_severity_is_warning():
    row = {"request_id": "od-2", "prompt": "test"}
    out = build_handoff_row(row, _policy())
    assert out["severity"] == "warning"


def test_build_payload_json_uses_snapshot():
    row = {
        "prompt": "debug this",
        "snapshot_json": '{"stream":"events:foo","expected":"A","got":"B"}',
    }
    payload = build_payload_json(row)
    assert '"snapshot"' in payload
    assert '"prompt":"debug this"' in payload


def test_build_handoff_row_force_local_from_row_overrides():
    """Row force_local=0 should still be 1 because policy force_local=1 (max)."""
    row = {"request_id": "od-3", "prompt": "test", "force_local": "0"}
    out = build_handoff_row(row, _policy())
    assert out["force_local"] == "1"
