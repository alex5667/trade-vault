from orderflow_services.route_incident_rca_mirror_verification_loop_v3_8 import (
    evaluate_verification,
)


def _policy():
    return {
        "advisory_only": 1,
        "executor_mode": "DRY_RUN",
        "min_sample": 20,
        "max_mismatch_rate": 0.00,
        "max_drift_rate": 0.25,
        "min_match_rate": 0.65,
        "max_pending_total": 10,
        "max_comparator_age_ms": 1800000,
        "rollback_cooldown_sec": 21600,
    }


def test_hold_when_not_in_mirror():
    out = evaluate_verification(
        current_mode="AUDIT_ONLY",
        comparator_age_ms=1000,
        pending_total=0,
        window_stats={
            "total": 30,
            "match_rate": 0.80,
            "drift_rate": 0.20,
            "mismatch_rate": 0.0,
        },
        policy=_policy(),
        last_switch_ts_ms=0,
        now_ts_ms=100000,
    )
    assert out["decision"] == "HOLD"
    assert out["reason_code"] == "NOT_IN_MIRROR"


def test_keep_mirror_when_stable():
    out = evaluate_verification(
        current_mode="MIRROR",
        comparator_age_ms=1000,
        pending_total=0,
        window_stats={
            "total": 30,
            "match_rate": 0.80,
            "drift_rate": 0.20,
            "mismatch_rate": 0.0,
        },
        policy=_policy(),
        last_switch_ts_ms=0,
        now_ts_ms=100000,
    )
    assert out["decision"] == "KEEP_MIRROR"
    assert out["reason_code"] == "POST_SWITCH_STABLE"


def test_rollback_to_audit_when_mismatch_rate_is_high():
    out = evaluate_verification(
        current_mode="MIRROR",
        comparator_age_ms=1000,
        pending_total=0,
        window_stats={
            "total": 30,
            "match_rate": 0.70,
            "drift_rate": 0.20,
            "mismatch_rate": 0.10,
        },
        policy=_policy(),
        last_switch_ts_ms=0,
        now_ts_ms=100000,
    )
    assert out["decision"] == "ROLLBACK_TO_AUDIT"
    assert out["target_mode"] == "AUDIT_ONLY"
    assert out["reason_code"] == "MISMATCH_RATE_TOO_HIGH"
