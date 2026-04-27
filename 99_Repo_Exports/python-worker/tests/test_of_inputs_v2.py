"""
Unit tests for OFInputsV2 contract, serialization, replay, and validation.
"""
import json
import tempfile
import os
from unittest.mock import Mock

import pytest

from core.of_inputs_contract import OFInputsV1, OFInputsV2
from tools.of_engine_replay_from_inputs import (
    build_runtime_from_inputs,
    build_cfg_indicators,
)
from tools.check_of_inputs_indicators import check_inputs_file


def test_of_inputs_v2_inherits_from_v1():
    """Test that OFInputsV2 inherits all fields from OFInputsV1."""
    v2 = OFInputsV2(
        v=2,
        symbol="BTCUSDT",
        ts_ms=1000000,
        regime="trend",
        direction="LONG",
        scenario="reversal",
        delta_z=2.5,
        weak_progress=1,
        sweep_recent=1,
        reclaim_recent=1,
        obi_stable=1,
        iceberg_strict=1,
        abs_lvl_ok=1,
        trend_dir="LONG",
        hidden_ctx_recent=1,
        cont_ctx_recent=1,
        cfg={},
        fp_eff_quote=50000.0,
        fp_quote_delta=10.0,
    )
    
    # Check V1 fields are accessible
    assert v2.symbol == "BTCUSDT"
    assert v2.ts_ms == 1000000
    assert v2.delta_z == 2.5
    
    # Check V2 fields have defaults
    assert v2.ofi == 0.0
    assert v2.ofi_z == 0.0
    assert v2.ofi_stable == 0
    assert v2.ofi_dir_ok == 0
    assert v2.ofi_age_ms == -1
    assert v2.fp_edge_absorb == 0
    assert v2.fp_edge_absorb_strength == 0.0
    assert v2.fp_edge_age_ms == -1


def test_of_inputs_v2_serialization():
    """Test that OFInputsV2 serializes correctly with all fields."""
    v2 = OFInputsV2(
        v=2,
        symbol="BTCUSDT",
        ts_ms=1000000,
        regime="trend",
        direction="LONG",
        scenario="reversal",
        delta_z=2.5,
        weak_progress=1,
        sweep_recent=1,
        reclaim_recent=1,
        obi_stable=1,
        iceberg_strict=1,
        abs_lvl_ok=1,
        trend_dir="LONG",
        hidden_ctx_recent=1,
        cont_ctx_recent=1,
        cfg={"test": 1},
        fp_eff_quote=50000.0,
        fp_quote_delta=10.0,
        # V2 fields
        ofi=1.5,
        ofi_z=2.0,
        ofi_stable=1,
        ofi_dir_ok=1,
        ofi_stable_secs=3.0,
        ofi_stability_score=0.8,
        ofi_age_ms=500,
        fp_edge_absorb=1,
        fp_edge_absorb_strength=1.5,
        fp_edge_age_ms=1000,
    )
    
    d = v2.to_dict()
    
    # Check V1 fields
    assert d["v"] == 2
    assert d["symbol"] == "BTCUSDT"
    assert d["delta_z"] == 2.5
    
    # Check V2 fields
    assert d["ofi"] == 1.5
    assert d["ofi_z"] == 2.0
    assert d["ofi_stable"] == 1
    assert d["ofi_dir_ok"] == 1
    assert d["ofi_stable_secs"] == 3.0
    assert d["ofi_stability_score"] == 0.8
    assert d["ofi_age_ms"] == 500
    assert d["fp_edge_absorb"] == 1
    assert d["fp_edge_absorb_strength"] == 1.5
    assert d["fp_edge_age_ms"] == 1000
    
    # JSON serialization
    blob = json.dumps(d, ensure_ascii=False)
    parsed = json.loads(blob)
    assert parsed["v"] == 2
    assert parsed["ofi"] == 1.5
    assert parsed["fp_edge_absorb"] == 1


def test_of_inputs_v1_backward_compatibility():
    """Test that V1 inputs still work correctly."""
    v1 = OFInputsV1(
        v=1,
        symbol="BTCUSDT",
        ts_ms=1000000,
        regime="trend",
        direction="LONG",
        scenario="reversal",
        delta_z=2.5,
        weak_progress=1,
        sweep_recent=1,
        reclaim_recent=1,
        obi_stable=1,
        iceberg_strict=1,
        abs_lvl_ok=1,
        trend_dir="LONG",
        hidden_ctx_recent=1,
        cont_ctx_recent=1,
        cfg={},
        fp_eff_quote=50000.0,
        fp_quote_delta=10.0,
    )
    
    d = v1.to_dict()
    assert d["v"] == 1
    assert "ofi" not in d
    assert "fp_edge_absorb" not in d


def test_build_runtime_from_inputs_v2():
    """Test runtime reconstruction from V2 inputs."""
    inp = {
        "v": 2,
        "symbol": "BTCUSDT",
        "ts_ms": 1000000,
        "direction": "LONG",
        "regime": "trend",
        "obi_stable": 1,
        "iceberg_strict": 1,
        "ofi_stable": 1,
        "ofi_dir_ok": 1,
        "ofi": 1.5,
        "ofi_z": 2.0,
        "ofi_stable_secs": 3.0,
        "ofi_stability_score": 0.8,
        "ofi_age_ms": 500,
        "fp_edge_absorb": 1,
        "fp_edge_absorb_strength": 1.5,
        "fp_edge_age_ms": 1000,
        "weak_progress": 1,
    }
    
    rt = build_runtime_from_inputs(inp)
    assert rt.symbol == "BTCUSDT"
    assert rt.last_regime == "trend"
    assert rt.last_ofi_event is not None
    assert rt.last_ofi_event["ofi"] == 1.5
    assert rt.last_ofi_event["ofi_z"] == 2.0
    assert rt.last_ofi_event["ts_ms"] == 1000000 - 500  # Reconstructed from age
    assert rt.last_fp_edge is not None
    assert rt.last_fp_edge["strength"] == 1.5
    assert rt.last_fp_edge["ts_ms"] == 1000000 - 1000  # Reconstructed from age


def test_build_runtime_from_inputs_v1():
    """Test runtime reconstruction from V1 inputs (backward compatibility)."""
    inp = {
        "v": 1,
        "symbol": "BTCUSDT",
        "ts_ms": 1000000,
        "direction": "LONG",
        "regime": "trend",
        "obi_stable": 1,
        "iceberg_strict": 1,
        "weak_progress": 1,
    }
    
    rt = build_runtime_from_inputs(inp)
    assert rt.symbol == "BTCUSDT"
    # V1 doesn't have OFI/FP edge, so they should be None or not set
    # (depends on implementation, but should not fail)


def test_build_cfg_indicators_v2():
    """Test cfg/indicators extraction from V2 inputs."""
    inp = {
        "v": 2,
        "cfg": {"test_param": 1.0},
        "indicators": {"book_health_ok": 1},
        "ofi": 1.5,
        "ofi_z": 2.0,
        "ofi_stable": 1,
        "ofi_dir_ok": 1,
        "ofi_stable_secs": 3.0,
        "ofi_stability_score": 0.8,
        "ofi_age_ms": 500,
        "fp_edge_absorb": 1,
        "fp_edge_absorb_strength": 1.5,
        "fp_edge_age_ms": 1000,
    }
    
    cfg, indicators, absorption = build_cfg_indicators(inp)
    assert cfg["test_param"] == 1.0
    assert indicators["book_health_ok"] == 1
    
    # V2 fields should be propagated to indicators
    assert indicators["ofi"] == 1.5
    assert indicators["ofi_z"] == 2.0
    assert indicators["ofi_stable"] == 1
    assert indicators["ofi_dir_ok"] == 1
    assert indicators["ofi_stable_secs"] == 3.0
    assert indicators["ofi_stability_score"] == 0.8
    assert indicators["ofi_age_ms"] == 500
    assert indicators["fp_edge_absorb"] == 1
    assert indicators["fp_edge_absorb_strength"] == 1.5
    assert indicators["fp_edge_age_ms"] == 1000


def test_check_inputs_file_v2():
    """Test validation of V2 inputs file."""
    v2_input = {
        "v": 2,
        "symbol": "BTCUSDT",
        "ts_ms": 1000000,
        "direction": "LONG",
        "scenario": "reversal",
        "regime": "trend",
        "delta_z": 2.5,
        "weak_progress": 1,
        "sweep_recent": 1,
        "reclaim_recent": 1,
        "obi_stable": 1,
        "iceberg_strict": 1,
        "abs_lvl_ok": 1,
        "trend_dir": "LONG",
        "hidden_ctx_recent": 1,
        "cont_ctx_recent": 1,
        "cfg": {"of_score_min": 0.6},
        "fp_eff_quote": 50000.0,
        "fp_quote_delta": 10.0,
        # V2 fields
        "ofi": 1.5,
        "ofi_z": 2.0,
        "ofi_stable": 1,
        "ofi_dir_ok": 1,
        "ofi_stable_secs": 3.0,
        "ofi_stability_score": 0.8,
        "ofi_age_ms": 500,
        "fp_edge_absorb": 1,
        "fp_edge_absorb_strength": 1.5,
        "fp_edge_age_ms": 1000,
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
        f.write(json.dumps(v2_input) + "\n")
        temp_path = f.name
    
    try:
        results = check_inputs_file(temp_path)
        assert "error" not in results
        assert results["version"] == "v2"
        assert results["ofi_present"] is True
        assert results["fp_edge_present"] is True
        assert results["missing_inputs_ofi"] == 0
        assert results["missing_inputs_fp"] == 0
    finally:
        os.unlink(temp_path)


def test_check_inputs_file_v1():
    """Test validation of V1 inputs file (backward compatibility)."""
    v1_input = {
        "v": 1,
        "symbol": "BTCUSDT",
        "ts_ms": 1000000,
        "direction": "LONG",
        "scenario": "reversal",
        "regime": "trend",
        "delta_z": 2.5,
        "weak_progress": 1,
        "sweep_recent": 1,
        "reclaim_recent": 1,
        "obi_stable": 1,
        "iceberg_strict": 1,
        "abs_lvl_ok": 1,
        "trend_dir": "LONG",
        "hidden_ctx_recent": 1,
        "cont_ctx_recent": 1,
        "cfg": {"of_score_min": 0.6},
        "fp_eff_quote": 50000.0,
        "fp_quote_delta": 10.0,
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
        f.write(json.dumps(v1_input) + "\n")
        temp_path = f.name
    
    try:
        results = check_inputs_file(temp_path)
        assert "error" not in results
        assert results["version"] == "v1"
        # V1 doesn't require OFI/FP edge fields
        assert "missing_inputs_ofi" not in results or results.get("missing_inputs_ofi") == 0
    finally:
        os.unlink(temp_path)


def test_check_inputs_file_v2_missing_fields():
    """Test validation detects missing V2 fields."""
    v2_incomplete = {
        "v": 2,
        "symbol": "BTCUSDT",
        "ts_ms": 1000000,
        "direction": "LONG",
        "scenario": "reversal",
        "regime": "trend",
        "delta_z": 2.5,
        "weak_progress": 1,
        "sweep_recent": 1,
        "reclaim_recent": 1,
        "obi_stable": 1,
        "iceberg_strict": 1,
        "abs_lvl_ok": 1,
        "trend_dir": "LONG",
        "hidden_ctx_recent": 1,
        "cont_ctx_recent": 1,
        "cfg": {},
        "fp_eff_quote": 50000.0,
        "fp_quote_delta": 10.0,
        # Missing V2 fields
    }
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
        f.write(json.dumps(v2_incomplete) + "\n")
        temp_path = f.name
    
    try:
        results = check_inputs_file(temp_path)
        assert "error" not in results
        assert results["version"] == "v2"
        # Should detect missing OFI/FP fields
        assert results["missing_inputs_ofi"] == 1
        assert results["missing_inputs_fp"] == 1
    finally:
        os.unlink(temp_path)

