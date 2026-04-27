from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_experiment_harness_v3_30 import (
    build_arm_request,
    choose_arm,
    evaluate_bundle,
)


def _bundle():
    return {
        "bundle_id": "winner-apply-apply-bundle:rollback:1",
        "trigger_type": "rollback",
        "trigger_severity": "critical",
        "summary": {"verification_events_n": 2},
    }


def _policy(mode: str = "SHADOW"):
    return {
        "enabled": 1,
        "kill_switch": 0,
        "mode": mode,
        "hash_salt": "salt",
        "allow_severities": {"warning", "critical"},
        "max_bundle_bytes": 131072,
        "arm_weights": {"deterministic": 70, "vertex_candidate": 20, "local_fallback_candidate": 10},
        "primary_arm": "deterministic",
        "shadow_arms": ["vertex_candidate", "local_fallback_candidate"],
    }


def test_choose_arm_is_deterministic():
    a1 = choose_arm("bundle-1", "salt", {"deterministic": 70, "vertex_candidate": 20, "local_fallback_candidate": 10})
    a2 = choose_arm("bundle-1", "salt", {"deterministic": 70, "vertex_candidate": 20, "local_fallback_candidate": 10})
    assert a1 == a2


def test_shadow_mode_uses_primary_and_shadow_arms():
    out = evaluate_bundle(_bundle(), _policy("SHADOW"))
    assert out["decision"] == "EXPOSE"
    assert out["primary_arm"] == "deterministic"
    assert "vertex_candidate" in out["shadow_arms"]


def test_build_arm_request_contains_arm_metadata():
    req = build_arm_request(_bundle(), "vertex_candidate", True)
    assert req["arm"] == "vertex_candidate"
    assert req["is_primary"] == "1"
    assert req["provider_mode"] == "VERTEX_CANDIDATE"
