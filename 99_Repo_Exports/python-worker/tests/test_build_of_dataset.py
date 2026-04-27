# python-worker/tests/test_build_of_dataset.py
"""
Unit tests for build_of_dataset.py
"""
import json
import tempfile
import os

import pytest

from tools.build_of_dataset import (
    iter_ndjson,
    build_trade_index,
    extract_features,
    extract_trade_labels,
    make_label_binary,
    main,
)


def test_build_trade_index():
    """Test trade index building."""
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
        f.write('{"sid": "s1", "r_mult": 1.5, "pnl": 100.0}\n')
        f.write('{"sid": "s2", "r_mult": -0.8, "pnl": -50.0}\n')
        f.write('{"sid": "s3", "r_mult": 0.3, "pnl": 20.0}\n')
        temp_path = f.name
    
    try:
        idx = build_trade_index(temp_path)
        assert len(idx) == 3
        assert idx["s1"]["r_mult"] == 1.5
        assert idx["s2"]["r_mult"] == -0.8
    finally:
        os.unlink(temp_path)


def test_extract_features():
    """Test feature extraction from replay row."""
    replay_row = {
        "sid": "test-sid",
        "symbol": "BTCUSDT",
        "ts_ms": 1000000,
        "direction": "LONG",
        "scenario": "reversal",
        "ok": 1,
        "score": 0.75,
        "have": 2,
        "need": 2,
        "evidence": {
            "scenario_v4": "reversal",
            "exec_risk_bps": 12.0,
            "exec_risk_norm": 0.6,
            "ok_soft": 0,
            "need_reason": "test_reason",
            "legs": {
                "ofi_leg": 1,
                "fp_edge_absorb": 1,
                "obi_stable": 0,
            },
            "score_breakdown": {
                "base_score": 0.80,
            },
            "meta_p": 0.65,
            "meta_veto": 0,
        },
    }
    
    feat = extract_features(replay_row)
    assert feat["sid"] == "test-sid"
    assert feat["symbol"] == "BTCUSDT"
    assert feat["score"] == 0.75
    assert feat["base_score"] == 0.80
    assert feat["exec_risk_bps"] == 12.0
    assert feat["leg_ofi_leg"] == 1
    assert feat["leg_fp_edge_absorb"] == 1
    assert feat["meta_p"] == 0.65


def test_extract_trade_labels():
    """Test trade label extraction."""
    tr = {
        "r_mult": 1.2,
        "pnl": 120.0,
        "risk_usd": 100.0,
        "meta": {
            "close_reason": "TP1",
        },
    }
    
    lab = extract_trade_labels(tr)
    assert lab["r_mult"] == 1.2
    assert lab["pnl"] == 120.0
    assert lab["risk_usd"] == 100.0
    assert lab["close_reason"] == "TP1"


def test_make_label_binary():
    """Test binary label creation."""
    assert make_label_binary(1.5, pos_th=0.5, neg_th=-0.5) == 1
    assert make_label_binary(-0.8, pos_th=0.5, neg_th=-0.5) == 0
    assert make_label_binary(0.2, pos_th=0.5, neg_th=-0.5) is None


def test_main_end_to_end():
    """Test end-to-end dataset building."""
    # Create replay file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
        replay_row = {
            "sid": "s1",
            "symbol": "BTCUSDT",
            "ts_ms": 1000000,
            "direction": "LONG",
            "scenario": "reversal",
            "ok": 1,
            "score": 0.75,
            "have": 2,
            "need": 2,
            "evidence": {
                "scenario_v4": "reversal",
                "exec_risk_bps": 12.0,
                "exec_risk_norm": 0.6,
                "ok_soft": 0,
                "legs": {
                    "ofi_leg": 1,
                    "fp_edge_absorb": 1,
                },
                "score_breakdown": {
                    "base_score": 0.80,
                },
            },
        }
        f.write(json.dumps(replay_row) + "\n")
        replay_path = f.name
    
    # Create trades file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
        f.write('{"sid": "s1", "r_mult": 1.5, "pnl": 100.0, "risk_usd": 100.0}\n')
        trades_path = f.name
    
    # Create output file
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
        out_path = f.name
    
    try:
        import sys
        old_argv = sys.argv
        sys.argv = ["test", "--replay", replay_path, "--trades", trades_path, "--out", out_path, "--pos-th", "0.5", "--neg-th", "-0.5", "--min-n", "1"]
        
        try:
            main()
        finally:
            sys.argv = old_argv
        
        # Check output
        with open(out_path, "r") as f:
            lines = f.readlines()
            assert len(lines) == 1
            row = json.loads(lines[0])
            assert row["sid"] == "s1"
            assert row["r_mult"] == 1.5
            assert row["y"] == 1
            assert "base_score" in row
    finally:
        os.unlink(replay_path)
        os.unlink(trades_path)
        if os.path.exists(out_path):
            os.unlink(out_path)

