from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_bundle_builder_v3_52 import (
    build_summary,
    choose_severity,
    evaluate_bundle,
)


def _policy():
    return {
        "enabled": 1,
        "kill_switch": 0,
        "mode": "ENABLED",
        "allow_severities": {"warning", "critical"},
        "min_verification_events": 1,
        "verify_keep_rate_crit": 0.60,
        "rollback_mttr_p95_crit_sec": 900.0,
        "escalation_rate_crit": 0.20,
        "max_bundle_bytes": 196608,
    }


def test_build_summary_collects_reason_codes():
    summary = build_summary(
        verification_rows=[{"reason_code": "TARGET_SHARE_TOO_LOW_AFTER_APPLY", "ts_ms": "9999999999999"}],
        rollback_rows=[{"reason_code": "TARGET_SHARE_TOO_LOW_AFTER_APPLY", "ts_ms": "9999999999998"}],
        retry_rows=[{"reason_code": "TARGET_SHARE_TOO_LOW_AFTER_APPLY", "ts_ms": "9999999999997"}],
        escalation_rows=[{"reason_code": "WEIGHTS_MISMATCH_AFTER_APPLY", "severity": "critical", "ts_ms": "9999999999996"}],
        slo_payload={
            "verify_keep_rate": 0.5,
            "rollback_plan_rate": 0.5,
            "rollback_applied_rate": 0.2,
            "rollback_mttr_p95_sec": 1200,
            "escalation_rate": 0.3,
        },
    )
    assert summary["verification_events_n"] == 1
    assert "TARGET_SHARE_TOO_LOW_AFTER_APPLY" in summary["verification_reason_codes"]
    assert "critical" in summary["escalation_severities"]


def test_choose_severity_escalation_preserved():
    severity = choose_severity(
        trigger_stream="stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_escalations",
        trigger_row={"severity": "critical"},
        summary={
            "verify_keep_rate": 0.8,
            "rollback_mttr_p95_sec": 10,
            "escalation_rate": 0.0,
        },
        policy=_policy(),
    )
    assert severity == "critical"


def test_evaluate_bundle_accepts_valid_summary():
    out = evaluate_bundle(
        summary={
            "verification_events_n": 2,
        },
        severity="warning",
        policy=_policy(),
    )
    assert out["decision"] == "BUILD_BUNDLE"
    assert out["reason_code"] == "OK"
