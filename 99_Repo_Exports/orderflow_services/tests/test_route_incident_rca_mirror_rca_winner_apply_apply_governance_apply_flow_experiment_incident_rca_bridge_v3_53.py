from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_bundle_rca_bridge_v3_53 import (
    build_request,
    choose_route,
    policy_from_hash,
)


def _bundle(severity: str = "critical"):
    return {
        "bundle_id": "apply-flow-experiment-bundle:1",
        "trigger_type": "rollback",
        "trigger_reason_code": "TARGET_SHARE_TOO_LOW_AFTER_APPLY",
        "trigger_severity": severity,
    }


def test_auto_routes_critical_to_vertex():
    policy = policy_from_hash(
        {
            "enabled": "1",
            "kill_switch": "0",
            "mode": "AUTO",
            "allow_severities_json": '["warning","critical"]',
            "vertex_for_severities_json": '["critical"]',
            "local_for_severities_json": '["warning"]',
            "max_bundle_bytes": "262144",
            "task_timeout_sec": "900",
        }
    )
    out = choose_route(_bundle("critical"), policy)
    assert out["decision"] == "ROUTE"
    assert out["route"] == "VERTEX"


def test_auto_routes_warning_to_local():
    policy = policy_from_hash(
        {
            "enabled": "1",
            "kill_switch": "0",
            "mode": "AUTO",
            "allow_severities_json": '["warning","critical"]',
            "vertex_for_severities_json": '["critical"]',
            "local_for_severities_json": '["warning"]',
            "max_bundle_bytes": "262144",
            "task_timeout_sec": "900",
        }
    )
    out = choose_route(_bundle("warning"), policy)
    assert out["decision"] == "ROUTE"
    assert out["route"] == "LOCAL"


def test_build_request_vertex_contract():
    stream_key, row = build_request(_bundle("critical"), "VERTEX", 900)
    assert "vertex" in stream_key
    assert row["task_timeout_sec"] == "900"
    assert row["task_type"].endswith("incident_vertex_rca")


def test_build_request_local_contract():
    stream_key, row = build_request(_bundle("warning"), "LOCAL", 900)
    assert "local" in stream_key
    assert row["force_local"] == "1"
    assert row["task_type"].endswith("incident_local_rca")
