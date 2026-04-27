import asyncio
import os
import sys
import pytest
from unittest.mock import patch, MagicMock, AsyncMock

from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_gate_v3_57_4 import (
    evaluate_report,
    run_gate,
)
from orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_gate_runner_v3_57_4 import (
    floor_minute_ms,
)


def test_evaluate_report_pass():
    report = {
        "ts_ms": 1712246400000,
        "status": "PASS",
        "key_coverage_ratio": 1.0,
        "hash_match": 1,
        "missing_in_pg_n": 0,
        "extra_in_pg_n": 0,
    }
    reasons = evaluate_report(report, 1712246405000)
    assert reasons == []


def test_evaluate_report_stale():
    report = {
        "ts_ms": 1712240000000,
        "status": "PASS",
        "key_coverage_ratio": 1.0,
        "hash_match": 1,
        "missing_in_pg_n": 0,
        "extra_in_pg_n": 0,
    }
    reasons = evaluate_report(report, 1712250000000)
    assert "STALE_REPORT" in reasons


def test_evaluate_report_hash_mismatch():
    report = {
        "ts_ms": 1712246400000,
        "status": "HASH_MISMATCH",
        "key_coverage_ratio": 1.0,
        "hash_match": 0,
        "missing_in_pg_n": 0,
        "extra_in_pg_n": 0,
    }
    reasons = evaluate_report(report, 1712246405000)
    assert "HASH_MISMATCH" in reasons


def test_evaluate_report_key_gap():
    report = {
        "ts_ms": 1712246400000,
        "status": "KEY_GAP",
        "key_coverage_ratio": 0.99,
        "hash_match": 1,
        "missing_in_pg_n": 1,
        "extra_in_pg_n": 0,
    }
    reasons = evaluate_report(report, 1712246405000)
    assert "KEY_COVERAGE_LT_1" in reasons
    assert "MISSING_IN_PG" in reasons


def test_runner_closed_window():
    # runner выбирает именно закрытое окно now - lag
    now_ts_ms = 1712246500000 # 1712246500000 -> 2024-04-04T16:01:40.000Z
    lag_min = 15
    window_min = 60
    
    end_ts_ms = floor_minute_ms(now_ts_ms - lag_min * 60 * 1000, window_min)
    start_ts_ms = end_ts_ms - window_min * 60 * 1000
    
    # 1712246500000 - 15m = 1712245600000. 
    # floor to 60m: 1712242800000 (15:00:00)
    # start is 14:00:00
    assert end_ts_ms == 1712242800000
    assert start_ts_ms == 1712239200000
    assert (end_ts_ms - start_ts_ms) == window_min * 60 * 1000


@pytest.mark.asyncio
async def test_gate_missing_alias(monkeypatch):
    # один alias missing → gate BLOCK
    monkeypatch.setattr("orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_gate_v3_57_4.WINDOW_START_TS_MS", 1712239200000)
    monkeypatch.setattr("orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_gate_v3_57_4.WINDOW_END_TS_MS", 1712242800000)
    
    reports = {
        "slo": {
            "ts_ms": 1712246400000,
            "status": "PASS",
            "key_coverage_ratio": 1.0,
            "hash_match": 1,
            "missing_in_pg_n": 0,
            "extra_in_pg_n": 0,
        },
        # "retry" is missing
        "escalation": {
            "ts_ms": 1712246400000,
            "status": "PASS",
            "key_coverage_ratio": 1.0,
            "hash_match": 1,
            "missing_in_pg_n": 0,
            "extra_in_pg_n": 0,
        }
    }
    
    async def mock_latest(*args, **kwargs):
        return reports
        
    monkeypatch.setattr("orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_gate_v3_57_4.latest_reports_for_window", mock_latest)
    monkeypatch.setattr("orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_gate_v3_57_4.now_ms", lambda: 1712246405000)
    
    monkeypatch.setattr("orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_gate_v3_57_4.start_http_server", MagicMock())
    mock_redis_module = MagicMock()
    mock_r = AsyncMock()
    mock_redis_module.from_url.return_value = mock_r
    monkeypatch.setattr("orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_gate_v3_57_4.redis", mock_redis_module)
    
    code = await run_gate()
    assert code == 2 # BLOCK


@pytest.mark.asyncio
async def test_gate_all_aliases_pass(monkeypatch):
    # все aliases PASS → exit code 0
    monkeypatch.setattr("orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_gate_v3_57_4.WINDOW_START_TS_MS", 1712239200000)
    monkeypatch.setattr("orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_gate_v3_57_4.WINDOW_END_TS_MS", 1712242800000)
    
    reports = {
        "slo": {
            "ts_ms": 1712246400000,
            "status": "PASS",
            "key_coverage_ratio": 1.0,
            "hash_match": 1,
            "missing_in_pg_n": 0,
            "extra_in_pg_n": 0,
        },
        "retry": {
            "ts_ms": 1712246400000,
            "status": "PASS",
            "key_coverage_ratio": 1.0,
            "hash_match": 1,
            "missing_in_pg_n": 0,
            "extra_in_pg_n": 0,
        },
        "escalation": {
            "ts_ms": 1712246400000,
            "status": "PASS",
            "key_coverage_ratio": 1.0,
            "hash_match": 1,
            "missing_in_pg_n": 0,
            "extra_in_pg_n": 0,
        }
    }
    
    async def mock_latest(*args, **kwargs):
        return reports
        
    monkeypatch.setattr("orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_gate_v3_57_4.latest_reports_for_window", mock_latest)
    monkeypatch.setattr("orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_gate_v3_57_4.now_ms", lambda: 1712246405000)
    
    monkeypatch.setattr("orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_gate_v3_57_4.start_http_server", MagicMock())
    mock_redis_module = MagicMock()
    mock_r = AsyncMock()
    mock_redis_module.from_url.return_value = mock_r
    monkeypatch.setattr("orderflow_services.route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_gate_v3_57_4.redis", mock_redis_module)
    
    code = await run_gate()
    assert code == 0 # PASS


def test_evaluate_report_count_mismatch_blocks_even_with_hash_match():
    # STATUS_COUNT_MISMATCH имеет block effect даже при hash_match=1
    report = {
        "ts_ms": 1712246400000,
        "status": "COUNT_MISMATCH",
        "key_coverage_ratio": 1.0,
        "hash_match": 1,
        "missing_in_pg_n": 0,
        "extra_in_pg_n": 0,
    }
    reasons = evaluate_report(report, 1712246405000)
    assert "STATUS_COUNT_MISMATCH" in reasons
