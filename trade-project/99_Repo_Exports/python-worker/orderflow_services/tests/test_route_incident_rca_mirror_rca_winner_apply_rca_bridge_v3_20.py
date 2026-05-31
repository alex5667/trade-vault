from orderflow_services.route_incident_rca_mirror_rca_winner_apply_bundle_rca_bridge_v3_20 import (
    build_local_fallback_payload,
    build_vertex_payload,
    decide_route,
    vertex_degraded_from_hash,
)


def test_vertex_degraded_from_hash():
    # Deterministic test
    assert isinstance(vertex_degraded_from_hash("foo123"), bool)

def test_decide_route_auto_healthy():
    assert decide_route("AUTO", vertex_is_healthy=True, require_degraded=1, bundle_size=1000, max_size=2000) == "ROUTE_VERTEX"

def test_decide_route_auto_degraded():
    assert decide_route("AUTO", vertex_is_healthy=False, require_degraded=1, bundle_size=1000, max_size=2000) == "ROUTE_LOCAL"

def test_decide_route_oversized():
    assert decide_route("AUTO", vertex_is_healthy=True, require_degraded=1, bundle_size=3000, max_size=2000) == "REJECT"

def test_decide_route_disabled():
    assert decide_route("DISABLED", vertex_is_healthy=True, require_degraded=1, bundle_size=1000, max_size=2000) == "REJECT"

def test_build_vertex_payload():
    pl = build_vertex_payload('{"foo":"bar"}')
    assert pl["task_family"] == "route_incident_rca_mirror_rca_winner_apply_rca"
    assert pl["bundle_json"] == '{"foo":"bar"}'

def test_build_local_fallback_payload():
    pl = build_local_fallback_payload('{"foo":"bar"}')
    assert pl["task_type"] == "vertex_unavailable_fallback"
    assert pl["source"] == "route_incident_rca_mirror_rca_winner_apply_bundle_rca_bridge_v3_20"
    assert pl["input_json"] == '{"foo":"bar"}'
