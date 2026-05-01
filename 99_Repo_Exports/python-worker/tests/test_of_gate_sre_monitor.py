from __future__ import annotations
"""Tests for of_gate_sre_monitor.py (NoData vs 0% semantics)"""

import pytest
from tools.of_gate_sre_monitor import compute_stats, build_alerts, pctl, _f, _i


def test_pctl_empty():
    assert pctl([], 0.5) == 0.0


def test_pctl_single():
    assert pctl([1.0], 0.5) == 1.0


def test_pctl_multiple():
    xs = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert pctl(xs, 0.5) == 3.0
    assert pctl(xs, 0.95) == 5.0
    assert pctl(xs, 0.0) == 1.0


def test_compute_stats_empty():
    result = compute_stats([], None, dh_bad_th=0.70)
    assert result["n"] == 0
    assert result["ok_rate"] is None
    assert result["no_data"] == 1
    assert result["no_data_total"] == 1


def test_compute_stats_basic():
    rows = [
        {
            "ts_ms": "1670000000000",
            "symbol": "BTCUSDT",
            "scenario_v4": "range_breakout",
            "ok": "1",
            "ok_soft": "0",
            "latency_us": "100",
            "ml_latency_us": "50",
            "exec_risk_norm": "0.5",
            "missing_legs": '[]',
            "book_health_ok": "1",
            "source_consistency_ok": "1",
            "data_health": "0.9",
            "meta_veto": "0",
        },
        {
            "ts_ms": "1670000000000",
            "symbol": "BTCUSDT",
            "scenario_v4": "range_breakout",
            "ok": "0",
            "ok_soft": "1",
            "latency_us": "200",
            "ml_latency_us": "100",
            "exec_risk_norm": "0.7",
            "missing_legs": '["obi"]',
            "book_health_ok": "1",
            "source_consistency_ok": "1",
            "data_health": "0.8",
            "meta_veto": "0",
        },
        {
            "ts_ms": "1670000000000",
            "symbol": "ETHUSDT",
            "scenario_v4": "vol_shock",
            "ok": "1",
            "ok_soft": "0",
            "latency_us": "150",
            "ml_latency_us": "75",
            "exec_risk_norm": "0.6",
            "missing_legs": '[]',
            "book_health_ok": "1",
            "source_consistency_ok": "1",
            "data_health": "0.95",
            "meta_veto": "0",
        },
    ]
    result = compute_stats(rows, None, dh_bad_th=0.70)
    
    assert result["n"] == 3
    assert result["ok_rate"] == pytest.approx(2.0 / 3.0, abs=0.01)
    assert result["soft_rate"] == pytest.approx(1.0 / 3.0, abs=0.01)
    assert result["lat_p50_us"] == 150.0
    assert result["lat_p99_us"] >= 200.0
    assert result["exec_p90"] >= 0.6
    assert "range_breakout" in result["scenario_dist"]
    assert "vol_shock" in result["scenario_dist"]
    assert result["book_bad_rate"] == 0.0
    assert result["source_inconsistency_rate"] == 0.0
    assert result["data_health_bad_rate"] == 0.0


def test_compute_stats_missing_legs():
    rows = [
        {
            "ts_ms": "1670000000000",
            "symbol": "BTCUSDT",
            "scenario_v4": "range",
            "ok": "0",
            "ok_soft": "0",
            "latency_us": "100",
            "ml_latency_us": "50",
            "exec_risk_norm": "0.5",
            "missing_legs": '["obi", "sweep"]',
            "book_health_ok": "1",
            "source_consistency_ok": "1",
            "data_health": "0.9",
            "meta_veto": "0",
        },
        {
            "ts_ms": "1670000000000",
            "symbol": "BTCUSDT",
            "scenario_v4": "range",
            "ok": "0",
            "ok_soft": "0",
            "latency_us": "100",
            "ml_latency_us": "50",
            "exec_risk_norm": "0.5",
            "missing_legs": '["obi"]',
            "book_health_ok": "1",
            "source_consistency_ok": "1",
            "data_health": "0.9",
            "meta_veto": "0",
        },
    ]
    result = compute_stats(rows, None, dh_bad_th=0.70)
    
    assert result["n"] == 2
    top_missing = result["top_missing_legs"]
    assert len(top_missing) > 0
    # "obi" should appear in top missing legs
    obi_items = [item for item in top_missing if item.get("k") == "obi"]
    assert len(obi_items) > 0


def test_compute_stats_scenario_drift():
    prev = {
        "scenario_dist": {"range": 0.5, "vol_shock": 0.5},
    }
    cur_rows = [
        {
            "ts_ms": "1670000000000",
            "symbol": "BTCUSDT",
            "scenario_v4": "range",
            "ok": "1",
            "ok_soft": "0",
            "latency_us": "100",
            "ml_latency_us": "50",
            "exec_risk_norm": "0.5",
            "missing_legs": '[]',
            "book_health_ok": "1",
            "source_consistency_ok": "1",
            "data_health": "0.9",
            "meta_veto": "0",
        },
        {
            "ts_ms": "1670000000000",
            "symbol": "ETHUSDT",
            "scenario_v4": "vol_shock",
            "ok": "1",
            "ok_soft": "0",
            "latency_us": "100",
            "ml_latency_us": "50",
            "exec_risk_norm": "0.5",
            "missing_legs": '[]',
            "book_health_ok": "1",
            "source_consistency_ok": "1",
            "data_health": "0.9",
            "meta_veto": "0",
        },
    ]
    result = compute_stats(cur_rows, prev, dh_bad_th=0.70)
    
    assert result["scenario_l1"] >= 0.0
    assert "scenario_dist" in result
    assert "scenario_max_share" in result


def test_build_alerts_low_n():
    stats = {"n": 50, "n_total": 250}
    cfg = {"min_n": 200}
    alerts = build_alerts(stats, cfg=cfg)
    assert len(alerts) == 1
    assert alerts[0]["code"] == "low_n"


def test_build_alerts_low_n_total():
    stats = {"n": 50, "n_total": 50}
    cfg = {"min_n": 200}
    alerts = build_alerts(stats, cfg=cfg)
    assert len(alerts) == 1
    assert alerts[0]["code"] == "low_n_total"


def test_build_alerts_ok_rate_low():
    stats = {
        "n": 300,
        "n_total": 350,
        "ok_rate": 0.05,
        "soft_rate": 0.3,
        "lat_p99_us": 1000.0,
        "ml_lat_p99_us": 500.0,
        "exec_p90": 0.5,
        "scenario_l1": 0.1,
        "scenario_max_share": 0.5,
        "source_inconsistency_rate": 0.01,
        "book_bad_rate": 0.01,
        "data_health_bad_rate": 0.05,
    }
    cfg = {
        "min_n": 200,
        "ok_min": 0.10,
        "soft_max": 0.70,
        "lat_p99_us_max": 25000.0,
        "ml_lat_p99_us_max": 25000.0,
        "exec_p90_max": 0.90,
        "scenario_l1_max": 0.35,
        "scenario_max_share_max": 0.75,
        "src_bad_max": 0.02,
        "book_bad_max": 0.02,
        "dh_bad_max": 0.10,
    }
    alerts = build_alerts(stats, cfg=cfg)
    assert len(alerts) > 0
    assert any(a["code"] == "ok_rate_low" for a in alerts)


def test_f_helpers():
    assert _f("1.5", 0.0) == 1.5
    assert _f("invalid", 0.0) == 0.0
    assert _f(None, 1.0) == 1.0
    assert _i("123", 0) == 123
    assert _i("invalid", 0) == 0
    assert _i(None, 42) == 42
