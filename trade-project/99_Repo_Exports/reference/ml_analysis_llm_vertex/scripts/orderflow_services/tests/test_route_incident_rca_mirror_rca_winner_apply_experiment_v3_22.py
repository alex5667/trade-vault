import json
import pytest
from orderflow_services.route_incident_rca_mirror_rca_winner_apply_experiment_harness_v3_22 import resolve_multi_arm, compute_exposures, build_payload

def test_resolve_multi_arm():
    weights = {"deterministic": 70, "vertex_candidate": 20, "local_fallback_candidate": 10}
    
    # Needs to be purely deterministic, bundle_id "1" vs "2" etc.
    a1 = resolve_multi_arm("bndl_1", weights)
    a2 = resolve_multi_arm("bndl_1", weights)
    assert a1 in weights
    assert a1 == a2 # Same id same resolution

def test_compute_exposures_disabled():
    ex = compute_exposures("DISABLED", "b1", {})
    assert len(ex) == 0

def test_compute_exposures_shadow():
    # Will read the SHADOW_ARMS_JSON from os.environ but we mocked it in script
    # It defaults to vertex and local
    ex = compute_exposures("SHADOW", "b1", {})
    assert len(ex) == 3 # Primary + 2 shadows
    
    primary = [e for e in ex if e["type"] == "primary"]
    assert len(primary) == 1
    assert primary[0]["arm"] == "deterministic"

def test_compute_exposures_single_arm():
    ex = compute_exposures("SINGLE_ARM", "b1", {})
    assert len(ex) == 1
    assert ex[0]["arm"] == "deterministic"
    assert ex[0]["type"] == "primary"

def test_build_payload():
    pl = build_payload("local_fallback_candidate", '{"a":"b"}')
    assert pl["task_type"] == "vertex_unavailable_fallback"
    
    pl = build_payload("vertex_candidate", '{"a":"b"}')
    assert pl["task_type"] == "route_incident_rca_mirror_rca_winner_apply_rca"
