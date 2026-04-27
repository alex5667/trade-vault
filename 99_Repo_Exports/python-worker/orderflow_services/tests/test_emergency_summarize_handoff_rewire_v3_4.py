from orderflow_services.emergency_summarize_handoff_rewire_adapter_v3_4 import (
    build_handoff_row,
    build_payload_json,
    evaluate_row,
    severity_allowed,
)


def _policy():
    return {
        "enabled": 1,
        "mode": "ENABLED",
        "max_payload_bytes": 65536,
        "max_prompt_chars": 8000,
        "force_local": 0,
        "allow_warning": 1,
        "allow_critical": 1,
        "allow_info": 0,
    }


# ── evaluate_row tests ──────────────────────────────────────────────────

def test_evaluate_row_rejects_missing_request_id():
    row = {"prompt": "hello", "severity": "critical"}
    out = evaluate_row(row, _policy())
    assert out["decision"] == "REJECT"
    assert out["reason_code"] == "REQUEST_ID_MISSING"


def test_evaluate_row_rejects_info_severity_by_default():
    row = {"request_id": "es-1", "prompt": "hello", "severity": "info"}
    out = evaluate_row(row, _policy())
    assert out["decision"] == "REJECT"
    assert out["reason_code"] == "SEVERITY_NOT_ALLOWED"


def test_evaluate_row_accepts_critical():
    row = {"request_id": "es-1", "prompt": "hello", "severity": "critical"}
    out = evaluate_row(row, _policy())
    assert out["decision"] == "ROUTE_HANDOFF"
    assert out["reason_code"] == "OK"


def test_evaluate_row_accepts_warning():
    row = {"request_id": "es-1", "prompt": "hello", "severity": "warning"}
    out = evaluate_row(row, _policy())
    assert out["decision"] == "ROUTE_HANDOFF"
    assert out["reason_code"] == "OK"


def test_evaluate_row_rejects_disabled_policy():
    row = {"request_id": "es-1", "prompt": "hello", "severity": "critical"}
    p = _policy()
    p["enabled"] = 0
    out = evaluate_row(row, p)
    assert out["decision"] == "REJECT"
    assert out["reason_code"] == "DISABLED"


def test_evaluate_row_rejects_disabled_mode():
    row = {"request_id": "es-1", "prompt": "hello", "severity": "critical"}
    p = _policy()
    p["mode"] = "DISABLED"
    out = evaluate_row(row, p)
    assert out["decision"] == "REJECT"
    assert out["reason_code"] == "MODE_DISABLED"


def test_evaluate_row_rejects_oversized_prompt():
    row = {"request_id": "es-1", "prompt": "x" * 9000, "severity": "critical"}
    out = evaluate_row(row, _policy())
    assert out["decision"] == "REJECT"
    assert out["reason_code"] == "PROMPT_TOO_LARGE"


def test_evaluate_row_no_severity_defaults_to_info_rejected():
    """Missing severity defaults to 'info' which is rejected by default policy."""
    row = {"request_id": "es-1", "prompt": "hello"}
    out = evaluate_row(row, _policy())
    assert out["decision"] == "REJECT"
    assert out["reason_code"] == "SEVERITY_NOT_ALLOWED"


# ── severity_allowed tests ──────────────────────────────────────────────

def test_severity_allowed_critical():
    assert severity_allowed("critical", _policy()) is True


def test_severity_allowed_warning():
    assert severity_allowed("warning", _policy()) is True


def test_severity_allowed_info_blocked():
    assert severity_allowed("info", _policy()) is False


def test_severity_allowed_unknown_blocked():
    assert severity_allowed("debug", _policy()) is False


# ── build_handoff_row tests ─────────────────────────────────────────────

def test_build_handoff_row_sets_emergency_summarize_family():
    row = {
        "request_id": "es-1",
        "severity": "critical",
        "title": "Emergency summary",
        "prompt": "Summarize degradation",
    }
    out = build_handoff_row(row, _policy())
    assert out["task_family"] == "emergency_summarize"
    assert out["task_type"] == "emergency_summarize"
    assert out["request_id"] == "es-1"
    assert out["source"] == "emergency_summarize_handoff_rewire_v3_4"
    assert out["severity"] == "critical"
    assert int(out["ts_ms"]) > 0


def test_build_handoff_row_force_local_from_policy():
    row = {"request_id": "es-2", "prompt": "test", "severity": "warning"}
    p = _policy()
    p["force_local"] = 1
    out = build_handoff_row(row, p)
    assert out["force_local"] == "1"


def test_build_payload_json_includes_summary():
    row = {
        "title": "Emergency",
        "prompt": "summarize",
        "summary": "service degraded",
    }
    payload = build_payload_json(row)
    assert '"summary":"service degraded"' in payload
    assert '"title":"Emergency"' in payload
