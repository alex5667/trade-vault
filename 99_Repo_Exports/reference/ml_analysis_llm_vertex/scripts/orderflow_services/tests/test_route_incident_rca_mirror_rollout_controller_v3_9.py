from orderflow_services.route_incident_rca_mirror_rollout_controller_v3_9 import (
    evaluate_event,
)


def _policy():
    return {
        "advisory_only": 1,
        "executor_mode": "DRY_RUN",
        "promotion_cooldown_sec": 21600,
        "allow_governor_promotion": 1,
        "allow_verification_rollback": 1,
    }


def test_governor_promotes_only_from_audit():
    event = {
        "source": "governor",
        "decision": "PROMOTE_TO_MIRROR",
        "reason_code": "STABLE_COMPARATOR_METRICS",
    }
    out = evaluate_event(
        event=event,
        current_mode="AUDIT_ONLY",
        rollout_state="AUDIT_ONLY_STABLE",
        last_transition_ts_ms=0,
        policy=_policy(),
        now_ts_ms=100000,
    )
    assert out["controller_decision"] == "PROMOTE"
    assert out["target_mode"] == "MIRROR"


def test_governor_promotion_blocked_by_cooldown():
    event = {
        "source": "governor",
        "decision": "PROMOTE_TO_MIRROR",
        "reason_code": "STABLE_COMPARATOR_METRICS",
    }
    out = evaluate_event(
        event=event,
        current_mode="AUDIT_ONLY",
        rollout_state="AUDIT_ONLY_STABLE",
        last_transition_ts_ms=95000,
        policy=_policy(),
        now_ts_ms=100000,
    )
    assert out["controller_decision"] == "HOLD"
    assert out["controller_reason_code"] == "PROMOTION_COOLDOWN_ACTIVE"


def test_verification_rolls_back_only_from_mirror():
    event = {
        "source": "verification",
        "decision": "ROLLBACK_TO_AUDIT",
        "reason_code": "MISMATCH_RATE_TOO_HIGH",
    }
    out = evaluate_event(
        event=event,
        current_mode="MIRROR",
        rollout_state="MIRROR_ACTIVE",
        last_transition_ts_ms=0,
        policy=_policy(),
        now_ts_ms=100000,
    )
    assert out["controller_decision"] == "ROLLBACK"
    assert out["target_mode"] == "AUDIT_ONLY"
