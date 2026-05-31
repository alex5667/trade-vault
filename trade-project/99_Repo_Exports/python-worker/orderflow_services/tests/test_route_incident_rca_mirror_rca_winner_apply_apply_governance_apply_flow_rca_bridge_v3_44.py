from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_bundle_rca_bridge_v3_44 import (
    build_local_request,
    build_vertex_request,
    evaluate_route,
    vertex_degraded_from_hash,
)


def _policy(mode: str = "AUTO"):
    return {
        "enabled": 1,
        "kill_switch": 0,
        "mode": mode,
        "require_vertex_degraded_for_local": 1,
        "allow_severities": {"warning", "critical"},
        "max_bundle_bytes": 196608,
        "max_prompt_chars": 12000,
    }


def _bundle():
    return {
        "bundle_id": "winner-apply-apply-governance-apply-flow-bundle:rollback:1",
        "trigger_type": "verification",
        "trigger_severity": "critical",
        "summary": {"verification_events_n": 3},
    }


def test_vertex_degraded_from_hash():
    assert vertex_degraded_from_hash({"degraded": "1"}) is True
    assert vertex_degraded_from_hash({"status": "down"}) is True
    assert vertex_degraded_from_hash({"status": "ok", "err_rate": "0.1"}) is False


def test_auto_routes_vertex_when_healthy():
    out = evaluate_route(_bundle(), _policy("AUTO"), vertex_degraded=False)
    assert out["decision"] == "ROUTE_VERTEX"
    assert out["route"] == "vertex"


def test_auto_routes_local_when_vertex_degraded():
    out = evaluate_route(_bundle(), _policy("AUTO"), vertex_degraded=True)
    assert out["decision"] == "ROUTE_LOCAL"
    assert out["route"] == "local"


def test_build_requests_have_expected_shape():
    vertex_req = build_vertex_request(_bundle())
    local_req = build_local_request(_bundle())
    assert vertex_req["task_family"] == "route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_rca"
    assert local_req["task_type"] == "vertex_unavailable_fallback"
    assert local_req["force_local"] == "1"
