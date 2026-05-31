from orderflow_services.operator_rca_routing_incident_bundle_builder_v2_8 import (
    build_bundle,
    primary_reason_codes,
    severity_from_reason_codes,
    summarize_route_diff,
)


def test_summarize_route_diff_detects_provider_model_prompt_changes():
    baseline = {
        "provider": "vertex",
        "model_name": "gemini-2.5-flash-lite",
        "prompt_version": "ml_triage_v1",
        "policy_version": "policy_v1",
    }
    current = {
        "provider": "vertex",
        "model_name": "gemini-2.5-flash",
        "prompt_version": "ml_triage_v2",
        "policy_version": "policy_v1",
    }
    diff = summarize_route_diff(baseline, current)
    assert diff["model_name"]["before"] == "gemini-2.5-flash-lite"
    assert diff["model_name"]["after"] == "gemini-2.5-flash"
    assert diff["prompt_version"]["after"] == "ml_triage_v2"


def test_primary_reason_codes_and_severity():
    sections = {
        "verify_results": [{"reason_code": "USEFULNESS_DROP"}],
        "rollback_results": [{"reason_codes": '["ROLLBACK_FAILED","ERROR_RATE_SPIKE"]'}],
    }
    reasons = primary_reason_codes(sections)
    assert "ROLLBACK_FAILED" in reasons
    assert severity_from_reason_codes(reasons) == "critical"


def test_build_bundle_contains_timeline_and_hash():
    request_row = {"route_change_id": "rc-1", "ts_ms": "1000"}
    sections = {
        "apply_results": [
            {
                "route_change_id": "rc-1",
                "ts_ms": "1010",
                "baseline_route_json": '{"provider":"vertex","model_name":"gemini-2.5-flash-lite","prompt_version":"v1"}',
            }
        ],
        "verify_results": [
            {
                "route_change_id": "rc-1",
                "ts_ms": "1020",
                "current_route_json": '{"provider":"vertex","model_name":"gemini-2.5-flash","prompt_version":"v2"}',
                "reason_code": "USEFULNESS_DROP",
            }
        ],
        "rollback_results": [],
        "rollback_requests": [],
        "rollback_journal": [],
        "retry_requests": [],
        "escalations": [],
        "slo_rollups": [],
        "audit": [],
    }
    bundle = build_bundle("rc-1", request_row, sections)
    assert bundle["route_change_id"] == "rc-1"
    assert bundle["severity"] in {"warning", "critical", "info"}
    assert bundle["timeline_json"]
    assert bundle["bundle_hash"]
