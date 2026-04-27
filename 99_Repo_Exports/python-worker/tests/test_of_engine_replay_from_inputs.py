# python-worker/tests/test_of_engine_replay_from_inputs.py
"""
Unit tests for of_engine_replay_from_inputs.py
"""
import json
import tempfile
import os
from unittest.mock import Mock

import pytest

from tools.of_engine_replay_from_inputs import (
    iter_ndjson,
    load_inputs,
    build_runtime_from_inputs,
    build_cfg_indicators,
    main,
)


def test_iter_ndjson():
    """Test NDJSON iterator."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
        f.write('{"a": 1}\n')
        f.write('{"b": 2}\n')
        f.write('\n')  # empty line
        f.write('{"c": 3}\n')
        temp_path = f.name
    
    try:
        rows = list(iter_ndjson(temp_path))
        assert len(rows) == 3
        assert rows[0]["a"] == 1
        assert rows[1]["b"] == 2
        assert rows[2]["c"] == 3
    finally:
        os.unlink(temp_path)


def test_load_inputs_direct():
    """Test loading direct OFInputsV1."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
        f.write('{"symbol": "BTCUSDT", "direction": "LONG", "ts_ms": 1000}\n')
        temp_path = f.name
    
    try:
        rows = list(load_inputs(temp_path))
        assert len(rows) == 1
        assert rows[0]["symbol"] == "BTCUSDT"
    finally:
        os.unlink(temp_path)


def test_load_inputs_wrapped():
    """Test loading wrapped payload."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
        f.write('{"payload": {"symbol": "BTCUSDT", "direction": "LONG"}}\n')
        temp_path = f.name
    
    try:
        rows = list(load_inputs(temp_path))
        assert len(rows) == 1
        assert rows[0]["symbol"] == "BTCUSDT"
    finally:
        os.unlink(temp_path)


def test_build_runtime_from_inputs():
    """Test runtime reconstruction from inputs."""
    inp = {
        "symbol": "BTCUSDT",
        "ts_ms": 1000000,
        "direction": "LONG",
        "regime": "trend",
        "book_churn_hi": 1,
        "sweep_recent": 1,
        "reclaim_recent": 1,
        "obi_stable": 1,
        "iceberg_strict": 1,
        "ofi_stable": 1,
        "fp_edge_absorb": 1,
        "weak_progress": 1,
    }
    
    rt = build_runtime_from_inputs(inp)
    assert rt.symbol == "BTCUSDT"
    assert rt.last_regime == "trend"
    assert rt.book_churn_hi == 1
    assert rt.last_sweep is not None
    assert rt.last_reclaim is not None
    assert rt.last_obi_event is not None
    assert rt.last_iceberg_event is not None
    assert rt.last_ofi_event is not None
    assert rt.last_fp_edge is not None
    assert rt.last_wp.weak_any is True


def test_build_cfg_indicators():
    """Test cfg/indicators extraction."""
    inp = {
        "cfg": {"test_param": 1.0},
        "indicators": {"book_health_ok": 1},
        "spread_bps": 10.0,
        "expected_slippage_bps": 2.0,
        "fp_edge_absorb": 1,
        "cancel_bid_rate_ema": 0.5,
    }
    
    cfg, indicators, absorption = build_cfg_indicators(inp)
    assert cfg["test_param"] == 1.0
    assert indicators["book_health_ok"] == 1
    assert indicators["spread_bps"] == 10.0
    assert indicators["expected_slippage_bps"] == 2.0
    assert indicators["fp_edge_absorb"] == 1
    assert indicators["cancel_bid_rate_ema"] == 0.5
    assert absorption is None


def test_main_end_to_end():
    """Test end-to-end replay."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
        inp = {
            "symbol": "BTCUSDT",
            "direction": "LONG",
            "ts_ms": 1000000,
            "delta_z": 2.5,
            "price": 50000.0,
            "sid": "test-sid-1",
            "cfg": {},
            "indicators": {
                "spread_bps": 10.0,
                "expected_slippage_bps": 2.0,
                "book_health_ok": 1,
            },
        }
        f.write(json.dumps(inp) + "\n")
        inputs_path = f.name
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
        out_path = f.name
    
    try:
        import sys
        old_argv = sys.argv
        sys.argv = ["test", "--inputs", inputs_path, "--out", out_path, "--tf", "1s", "--sort", "1"]
        
        try:
            main()
        finally:
            sys.argv = old_argv
        
        # Check output exists and has sid
        with open(out_path, "r") as f:
            lines = f.readlines()
            assert len(lines) > 0
            row = json.loads(lines[0])
            assert "sid" in row
            assert row["sid"] == "test-sid-1"
    finally:
        os.unlink(inputs_path)
        if os.path.exists(out_path):
            os.unlink(out_path)

