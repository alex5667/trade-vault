from orderflow_services.route_incident_rca_mirror_governor_v3_7 import (
    evaluate_decision,
)


def _policy():
    return {
        "advisory_only": 1,
        "executor_mode": "DRY_RUN",
        "min_sample": 20,
        "max_mismatch_rate": 0.00,
        "max_drift_rate": 0.20,
        "min_match_rate": 0.70,
        "max_pending_total": 10,
        "max_comparator_age_ms": 1800000,
        "cooldown_sec": 21600,
        "allow_demotion": 1,
    },


def test_promote_to_mirror_when_stable():
    out = evaluate_decision(
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
    assert out["decision"] == "PROMOTE_TO_MIRROR"
    assert out["target_mode"] == "MIRROR"


def test_keep_audit_only_when_not_enough_sample():
    out = evaluate_decision(
        current_mode="AUDIT_ONLY",
        comparator_age_ms=1000,
        pending_total=0,
        window_stats={
            "total": 5,
            "match_rate": 1.0,
            "drift_rate": 0.0,
            "mismatch_rate": 0.0,
        },
        policy=_policy(),
        last_switch_ts_ms=0,
        now_ts_ms=100000,
    )
    assert out["decision"] == "KEEP_AUDIT_ONLY"
    assert out["reason_code"] == "STABILITY_NOT_REACHED"


def test_demote_to_audit_when_mirror_loses_stability():
    out = evaluate_decision(
        current_mode="MIRROR",
        comparator_age_ms=1000,
        pending_total=0,
        window_stats={
            "total": 30,
            "match_rate": 0.60,
            "drift_rate": 0.20,
            "mismatch_rate": 0.20,
        },
        policy=_policy(),
        last_switch_ts_ms=0,
        now_ts_ms=100000,
    )
    assert out["decision"] == "DEMOTE_TO_AUDIT"
    assert out["target_mode"] == "AUDIT_ONLY"
