from orderflow_services.local_report_handoff_rewire_adapter_v3_2 import (
    build_handoff_row,
    evaluate_row,
)


def _policy():
    return {
        "enabled": 1,
        "mode": "ENABLED",
        "max_payload_bytes": 65536,
        "max_prompt_chars": 12000,
        "force_local": 0,
    }


def test_evaluate_row_rejects_missing_request_id():
    row = {"prompt": "hello"}
    out = evaluate_row(row, _policy())
    assert out["decision"] == "REJECT"
    assert out["reason_code"] == "REQUEST_ID_MISSING"


def test_evaluate_row_accepts_bounded_local_report():
    row = {"request_id": "lr-1", "prompt": "hello"}
    out = evaluate_row(row, _policy())
    assert out["decision"] == "ROUTE_HANDOFF"
    assert out["reason_code"] == "OK"


def test_evaluate_row_rejects_disabled_policy():
    row = {"request_id": "lr-1", "prompt": "hello"}
    p = _policy()
    p["enabled"] = 0
    out = evaluate_row(row, p)
    assert out["decision"] == "REJECT"
    assert out["reason_code"] == "DISABLED"


def test_evaluate_row_rejects_disabled_mode():
    row = {"request_id": "lr-1", "prompt": "hello"}
    p = _policy()
    p["mode"] = "DISABLED"
    out = evaluate_row(row, p)
    assert out["decision"] == "REJECT"
    assert out["reason_code"] == "MODE_DISABLED"


def test_evaluate_row_rejects_oversized_prompt():
    row = {"request_id": "lr-1", "prompt": "x" * 13000}
    out = evaluate_row(row, _policy())
    assert out["decision"] == "REJECT"
    assert out["reason_code"] == "PROMPT_TOO_LARGE"


def test_build_handoff_row_sets_local_report_family():
    row = {
        "request_id": "lr-1",
        "severity": "info",
        "title": "Daily local report",
        "prompt": "Summarize system state",
    }
    out = build_handoff_row(row, _policy())
    assert out["task_family"] == "local_report"
    assert out["task_type"] == "local_report"
    assert out["request_id"] == "lr-1"
    assert out["source"] == "local_report_handoff_rewire_v3_2"
    assert out["severity"] == "info"
    assert int(out["ts_ms"]) > 0


def test_build_handoff_row_force_local_from_policy():
    row = {
        "request_id": "lr-2",
        "prompt": "test",
    }
    p = _policy()
    p["force_local"] = 1
    out = build_handoff_row(row, p)
    assert out["force_local"] == "1"
