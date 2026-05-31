import json

from orderflow_services.ml_analysis_input_pack_builder_v1 import (
    build_analysis_request,
    normalize_snapshot,
    severity_of_snapshot,
)


def test_severity_detects_high_error():
    snap = normalize_snapshot({
        "model_id": "m1",
        "family": "edge_stack_v1",
        "kind": "edge_stack_v1",
        "status": "ok",
        "promotion_state": "champion",
        "champion_flag": "1",
        "artifact_uri": "/tmp/m.joblib",
        "schema_ver": "v12_of",
        "schema_hash": "abc",
        "reason_codes_json": "[]",
        "latest_runtime_ts_ms": "100",
        "runtime_age_sec": "12",
        "latency_p95_max_ms": "2.0",
        "latency_p99_max_ms": "3.0",
        "allow_rate_avg": "0.3",
        "block_rate_avg": "0.1",
        "abstain_rate_avg": "0.1",
        "shadow_rate_avg": "0.0",
        "error_rate_max": "0.05",
        "missing_critical_rate_max": "0.0",
        "hot_symbols_json": '["BTCUSDT"]',
        "psi_top_json": "[]",
        "ks_top_json": "[]",
    })
    sev, reasons = severity_of_snapshot(snap)
    assert sev == "high"
    assert "ERR_RATE_HIGH" in reasons


def test_build_analysis_request_contains_constraints():
    snap = normalize_snapshot({
        "model_id": "m1",
        "family": "meta_lr",
        "kind": "meta_lr",
        "status": "critical",
        "promotion_state": "champion",
        "champion_flag": "1",
        "artifact_uri": "/tmp/m.json",
        "schema_ver": "meta_feat_v9",
        "schema_hash": "abc",
        "reason_codes_json": '["ARTIFACT_MISSING"]',
        "latest_runtime_ts_ms": "100",
        "runtime_age_sec": "999",
        "latency_p95_max_ms": "2.0",
        "latency_p99_max_ms": "3.0",
        "allow_rate_avg": "0.3",
        "block_rate_avg": "0.1",
        "abstain_rate_avg": "0.1",
        "shadow_rate_avg": "0.0",
        "error_rate_max": "0.0",
        "missing_critical_rate_max": "0.0",
        "hot_symbols_json": '["BTCUSDT","ETHUSDT"]',
        "psi_top_json": '[{"feature":"obi"}]',
        "ks_top_json": "[]",
    })
    req = build_analysis_request(snap, {"run_id": "train1"})
    payload = json.loads(req["input_pack_json"])
    assert req["task_type"] == "root_cause_degradation"
    assert payload["constraints"]["advisory_only"] is True
    assert "require_shadow_retrain" in payload["constraints"]["allowed_actions"]
