from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_harness_v3_47 import (
    build_request,
    choose_arm,
    deterministic_bucket,
    evaluate_bundle,
)


def _policy():
    return {
        "enabled": 1,
        "kill_switch": 0,
        "mode": "SHADOW",
        "allow_severities": {"warning", "critical"},
        "vertex_primary_weight": 50,
        "vertex_compact_weight": 30,
        "local_candidate_weight": 20,
        "total_weight": 100,
        "max_bundle_bytes": 196608,
    }


def _bundle():
    return {
        "bundle_id": "winner-apply-apply-governance-apply-flow-bundle:123",
        "trigger_type": "verification",
        "trigger_severity": "critical",
    }


def test_deterministic_bucket_is_stable():
    b1 = deterministic_bucket("bundle-1")
    b2 = deterministic_bucket("bundle-1")
    assert b1 == b2


def test_choose_arm_returns_known_arm():
    arm = choose_arm("bundle-1", _policy())
    assert arm in {"vertex_primary", "vertex_compact_candidate", "local_candidate"}


def test_evaluate_bundle_accepts_valid_bundle():
    out = evaluate_bundle(_bundle(), _policy())
    assert out["decision"] == "EXPOSE_AND_ROUTE"
    assert out["arm"] in {"vertex_primary", "vertex_compact_candidate", "local_candidate"}


def test_build_request_routes_by_arm():
    stream, row = build_request(_bundle(), "local_candidate")
    assert "local" in stream
    assert row["force_local"] == "1"
    stream2, row2 = build_request(_bundle(), "vertex_compact_candidate")
    assert "vertex" in stream2
    assert row2["experiment_arm"] == "vertex_compact_candidate"
