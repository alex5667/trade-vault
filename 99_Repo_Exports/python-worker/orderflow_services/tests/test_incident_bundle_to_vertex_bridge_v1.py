from orderflow_services.incident_bundle_to_vertex_bridge_v1 import IncidentBundle, build_rca_input_pack, should_bridge


def _bundle(**overrides):
    base = IncidentBundle(
        recommendation_id="rec-1",
        ts_ms=1710000000000,
        model_id="edge_stack_v1:champion",
        family="edge_stack_v1",
        severity="critical",
        primary_reason_codes=["LATENCY_P95_REGRESSION", "ERROR_RATE_SPIKE"],
        summary="regression after commit",
        snapshot_before={"latency_p95_max_ms": 2.1},
        snapshot_after={"latency_p95_max_ms": 6.8},
        snapshot_diff={
            "latency_p95_max_ms": {"before": 2.1, "after": 6.8, "delta": 4.7},
            "error_rate_max": {"before": 0.01, "after": 0.08, "delta": 0.07},
        },
        timeline=[{"ts_ms": 1, "event": "APPLY", "state": "EXECUTED"}],
    )
    for k, v in overrides.items():
        setattr(base, k, v)
    return base


def test_should_bridge_for_critical_bundle():
    ok, reason = should_bridge(_bundle())
    assert ok is True
    assert reason == "ok"


def test_should_skip_low_severity():
    ok, reason = should_bridge(_bundle(severity="info"))
    assert ok is False
    assert reason == "severity_below_threshold"


def test_build_rca_input_pack_is_deterministic_and_compact():
    bundle = _bundle()
    pack = build_rca_input_pack(bundle, prompt_version="pv1", policy_version="pol1", diff_limit=1)
    assert pack["task_type"] == "incident_rca"
    assert pack["prompt_version"] == "pv1"
    assert pack["policy_version"] == "pol1"
    assert len(pack["top_snapshot_diff"]) == 1
    assert pack["timeline"][0]["event"] == "APPLY"
    assert pack["instructions"]["advisory_only"] is True
PATCH
