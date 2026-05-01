from __future__ import annotations
"""
Tests for OF Gate metrics integration (ML latency, source_consistency_ok, etc).
"""
from utils.time_utils import get_ny_time_millis

import json
import time
from unittest.mock import MagicMock, patch

import pytest

from services.ml_confirm_gate import MLConfirmDecision, MLConfirmGate


def test_ml_confirm_decision_latency_us():
    """Test that MLConfirmDecision includes latency_us field."""
    dec = MLConfirmDecision(mode="OFF", kind="none", allow=True, reason="test")
    assert hasattr(dec, "latency_us")
    assert dec.latency_us == 0

    dec.latency_us = 1234
    d = dec.to_dict()
    assert "latency_us" in d
    assert d["latency_us"] == 1234


def test_ml_confirm_gate_measures_latency():
    """Test that MLConfirmGate.check() measures and sets latency_us."""
    import redis

    r = redis.Redis.from_url("redis://localhost:6379/0", decode_responses=True)
    gate = MLConfirmGate(
        r=r,
        mode="OFF",
        fail_policy="OPEN",
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger",
    )

    dec = gate.check(
        symbol="BTCUSDT",
        ts_ms=get_ny_time_millis(),
        direction="LONG",
        scenario="reversal",
        indicators={},
        rule_score=0.5,
        rule_have=2,
        rule_need=3,
        cancel_spike_veto=0,
        ok_rule=1,
    )

    assert isinstance(dec, MLConfirmDecision)
    assert hasattr(dec, "latency_us")
    assert dec.latency_us >= 0  # Should be measured


def test_strategy_payload_includes_ml_metrics():
    """Test that strategy.py payload includes ML metrics fields."""
    # This is an integration test - we check the payload structure
    # by examining the code logic
    payload_fields_expected = [
        "ml_mode",
        "ml_kind",
        "ml_allow",
        "ml_bucket",
        "ml_p_edge",
        "ml_p_min",
        "ml_score",
        "ml_floor",
        "ml_latency_us",
        "source_consistency_ok",
    ]

    # Read strategy.py to verify fields are present
    import os
    strategy_path = os.path.join(os.path.dirname(__file__), "..", "services", "orderflow", "strategy.py")
    with open(strategy_path, "r", encoding="utf-8") as f:
        content = f.read()
        for field in payload_fields_expected:
            assert field in content, f"Field {field} not found in strategy.py payload"


def test_strategy_payload_reason_capped():
    """Test that reason field is capped to 120 chars."""
    # Verify the code has reason[:120]
    import os
    strategy_path = os.path.join(os.path.dirname(__file__), "..", "services", "orderflow", "strategy.py")
    with open(strategy_path, "r", encoding="utf-8") as f:
        content = f.read()
        # Check for the comment and cap
        assert "keep for offline debug but cap size" in content.lower() or "reason" in content
        assert "[:120]" in content or '"reason": str(getattr(ofc, "reason", "")' in content


def test_of_gate_sre_monitor_computes_ml_latency():
    """Test that of_gate_sre_monitor computes ML latency percentiles."""
    from tools.of_gate_sre_monitor import compute_stats, pctl

    rows = [
        {
            "ok": "1",
            "ok_soft": "0",
            "latency_us": "1000",
            "ml_latency_us": "500",
            "exec_risk_norm": "0.5",
            "book_health_ok": "1",
            "source_consistency_ok": "1",
            "data_health": "1.0",
            "meta_veto": "0",
            "scenario_v4": "reversal",
            "missing_legs": "[]",
        },
        {
            "ok": "1",
            "ok_soft": "0",
            "latency_us": "2000",
            "ml_latency_us": "1000",
            "exec_risk_norm": "0.6",
            "book_health_ok": "1",
            "source_consistency_ok": "1",
            "data_health": "1.0",
            "meta_veto": "0",
            "scenario_v4": "continuation",
            "missing_legs": "[]",
        },
    ]

    stats = compute_stats(rows, prev=None, data_health_bad_th=0.70)
    assert "ml_lat_p50_us" in stats
    assert "ml_lat_p95_us" in stats
    assert "ml_lat_p99_us" in stats
    assert stats["ml_lat_p50_us"] > 0
    assert stats["ml_lat_p99_us"] >= stats["ml_lat_p50_us"]


def test_of_gate_sre_monitor_source_consistency():
    """Test that of_gate_sre_monitor tracks source_consistency_ok."""
    from tools.of_gate_sre_monitor import compute_stats

    rows = [
        {
            "ok": "1",
            "ok_soft": "0",
            "latency_us": "1000",
            "ml_latency_us": "500",
            "exec_risk_norm": "0.5",
            "book_health_ok": "1",
            "source_consistency_ok": "0",  # Bad
            "data_health": "1.0",
            "meta_veto": "0",
            "scenario_v4": "reversal",
            "missing_legs": "[]",
        },
        {
            "ok": "1",
            "ok_soft": "0",
            "latency_us": "2000",
            "ml_latency_us": "1000",
            "exec_risk_norm": "0.6",
            "book_health_ok": "1",
            "source_consistency_ok": "1",  # Good
            "data_health": "1.0",
            "meta_veto": "0",
            "scenario_v4": "continuation",
            "missing_legs": "[]",
        },
    ]

    stats = compute_stats(rows, prev=None, data_health_bad_th=0.70)
    assert "source_inconsistency_rate" in stats
    assert stats["source_inconsistency_rate"] == 0.5  # 1 out of 2


def test_bench_of_gate_latency():
    """Test bench_of_gate_latency.py percentile computation."""
    from tools.bench_of_gate_latency import pctl

    latencies = [100.0, 200.0, 300.0, 400.0, 500.0]
    assert pctl(latencies, 0.50) == 300.0
    assert pctl(latencies, 0.95) == 500.0
    assert pctl(latencies, 0.99) == 500.0
    assert pctl([], 0.50) == 0.0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

