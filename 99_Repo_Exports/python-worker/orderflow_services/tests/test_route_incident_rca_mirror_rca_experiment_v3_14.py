from orderflow_services.route_incident_rca_mirror_rca_experiment_harness_v3_14 import (
    decide_arms,
    deterministic_assignment,
)


def test_deterministic_assignment():
    weights = {"deterministic": 70.0, "vertex_candidate": 20.0, "local_fallback_candidate": 10.0}

    # Deterministic behavior check
    arm1 = deterministic_assignment("test-b1", "salt", weights)
    arm2 = deterministic_assignment("test-b1", "salt", weights)
    assert arm1 == arm2

def test_decide_arms_shadow():
    mode = "SHADOW"
    primary_arm = "deterministic"
    shadow_arms = ["vertex_candidate", "local_fallback_candidate"]
    weights = {}

    arms = decide_arms("test-b1", mode, primary_arm, shadow_arms, weights)
    assert primary_arm in arms
    assert "vertex_candidate" in arms
    assert "local_fallback_candidate" in arms
    assert len(arms) == 3

def test_decide_arms_multi_arm():
    mode = "MULTI_ARM"
    primary_arm = "deterministic"
    shadow_arms = []
    weights = {"a": 50.0, "b": 50.0}

    arms = decide_arms("test-b1", mode, primary_arm, shadow_arms, weights)
    assert len(arms) == 1
    assert arms[0] in ["a", "b"]

def test_decide_arms_disabled():
    mode = "DISABLED"
    arms = decide_arms("test-b1", mode, "deterministic", [], {})
    assert len(arms) == 0
