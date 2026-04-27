from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_gate_decisions_persister_v3_57_4_1 import normalize
from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_dashboard_snapshot_v3_57_4_1 import build_snapshot

def test_normalize_gate_decision_payload():
    row = {
        "payload": '{"ts_ms":1712246400000,"window_start_ts_ms":1712242800000,"window_end_ts_ms":1712246400000,"aliases_ok":3,"aliases_required":3,"decision":"PASS","gate_reasons":[]}'
    }
    out = normalize(row)
    assert out["ts_ms"] == 1712246400000
    assert out["decision"] == "PASS"
    assert out["aliases_ok"] == 3

def test_build_snapshot_ok():
    gate = {
        "window_start_ts_ms": 1000,
        "window_end_ts_ms": 2000,
        "decision": "PASS",
        "gate_reasons": [],
        "aliases_ok": 3,
        "aliases_required": 3,
    }
    reports = {
        "slo": {"status": "PASS", "key_coverage_ratio": 1.0, "hash_match": 1, "stream_row_count": 10, "pg_row_count": 10, "missing_in_pg_n": 0, "extra_in_pg_n": 0, "ts_ms": 2000},
        "retry": {"status": "PASS", "key_coverage_ratio": 1.0, "hash_match": 1, "stream_row_count": 5, "pg_row_count": 5, "missing_in_pg_n": 0, "extra_in_pg_n": 0, "ts_ms": 2000},
        "escalation": {"status": "PASS", "key_coverage_ratio": 1.0, "hash_match": 1, "stream_row_count": 1, "pg_row_count": 1, "missing_in_pg_n": 0, "extra_in_pg_n": 0, "ts_ms": 2000},
    }
    out = build_snapshot(gate, reports)
    assert out["snapshot_status"] == "OK"
    assert out["gate_decision"] == "PASS"

def test_build_snapshot_attention_when_alias_missing():
    gate = {
        "window_start_ts_ms": 1000,
        "window_end_ts_ms": 2000,
        "decision": "BLOCK",
        "gate_reasons": ["retry:HASH_MISMATCH"],
        "aliases_ok": 2,
        "aliases_required": 3,
    }
    reports = {
        "slo": {"status": "PASS", "key_coverage_ratio": 1.0, "hash_match": 1, "stream_row_count": 10, "pg_row_count": 10, "missing_in_pg_n": 0, "extra_in_pg_n": 0, "ts_ms": 2000},
        "retry": {"status": "HASH_MISMATCH", "key_coverage_ratio": 1.0, "hash_match": 0, "stream_row_count": 5, "pg_row_count": 5, "missing_in_pg_n": 0, "extra_in_pg_n": 0, "ts_ms": 2000},
    }
    out = build_snapshot(gate, reports)
    assert out["snapshot_status"] == "ATTENTION"
    assert out["alias_views"]["escalation"]["status"] == "MISSING"

import pytest
from unittest.mock import patch, MagicMock

@pytest.mark.asyncio
async def test_snapshot_worker_survives_missing_report():
    gate = {
        "window_start_ts_ms": 1000,
        "window_end_ts_ms": 2000,
        "decision": "PASS",
        "gate_reasons": [],
        "aliases_ok": 3,
        "aliases_required": 3,
    }
    # missing entirely
    reports = {}
    out = build_snapshot(gate, reports)
    assert out["snapshot_status"] == "ATTENTION"
    assert out["alias_views"]["slo"]["status"] == "MISSING"
