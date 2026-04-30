import json
import pytest
from ml_analysis.tools.confidence_parts_drift_report_v1 import build_report

def test_drift_report_detects_shift():
    rows = []
    # baseline day: 2024-02-17
    # 1708128000000 -> 2024-02-17
    for i in range(120):
        rows.append({
            "ts_ms": 1708128000000 + i*1000
            "symbol": "BTCUSDT"
            "indicators": {"confidence_parts": {"base": 0.50, "bonuses": 0.05}, "regime_class": "trend"}
        })
    # target day: 2024-02-18
    # 1708214400000 -> 2024-02-18
    for i in range(200):
        val = 0.80 if i < 150 else 0.82 # Shifted mean
        rows.append({
            "ts_ms": 1708214400000 + i*1000
            "symbol": "BTCUSDT"
            "indicators": {"confidence_parts": {"base": val, "bonuses": 0.05}, "regime_class": "trend"}
        })

    rep = build_report(rows, group_by="symbol_regime", baseline_days=1, target_day=None, top_n=10)
    assert rep["groups"], "expected at least one group"
    g = rep["groups"][0]
    parts = {p["key"]: p for p in g["parts"]}
    
    # Check shift detection
    assert "base" in parts
    p = parts["base"]
    assert p["n_base"] >= 50
    assert p["n_target"] >= 50
    # Median should be ~0.80 vs 0.50, so very high Z
    assert p["drift_z"] > 5.0 
    assert abs(p["baseline_median"] - 0.50) < 0.01

    # Bonuses unchanged
    assert "bonuses" in parts
    pb = parts["bonuses"]
    # If variability is 0, MAD is 0 -> Z might be 0 or NaN depending on impl, 
    # but here medians are identical so Z should be 0 (numerator 0).
    # wait, if mad is 0, we use eps epsilon.
    # 0.05 - 0.05 = 0
    assert abs(pb["drift_z"]) < 0.1

def test_drift_report_grouping():
    rows = [
        {"ts_ms": 1708128000000, "symbol": "BTC", "indicators": {"confidence_parts": {"x": 1}, "regime_class": "trend"}}
        {"ts_ms": 1708128000000, "symbol": "ETH", "indicators": {"confidence_parts": {"x": 1}, "regime_class": "range"}}
    ]
    rep = build_report(rows, group_by="symbol_regime")
    # Should have 2 groups
    assert len(rep["groups"]) == 2
    groups = sorted([g["group"] for g in rep["groups"]])
    assert groups == [['BTC', 'trend'], ['ETH', 'range']]
