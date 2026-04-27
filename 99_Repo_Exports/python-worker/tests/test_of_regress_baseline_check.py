"""Unit tests for of_regress_baseline_check.py"""
import json
import os
import tempfile

import pytest

from tools.of_regress_baseline_check import (
    iter_ndjson,
    _get,
    row_key,
    diff,
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


def test_get():
    """Test _get helper."""
    r1 = {"ok": 1, "score": 0.5}
    assert _get(r1, "ok") == 1
    assert _get(r1, "score") == 0.5
    assert _get(r1, "missing") is None
    
    r2 = {"evidence": {"ok": 0, "score": 0.8}}
    assert _get(r2, "ok") == 0
    assert _get(r2, "score") == 0.8


def test_row_key():
    """Test row key generation."""
    r1 = {"sid": "test-123"}
    assert row_key(r1) == "test-123"
    
    r2 = {"symbol": "BTCUSDT", "ts_ms": 1000, "direction": "LONG"}
    assert row_key(r2) == "BTCUSDT|1000|LONG"


def test_diff_no_mismatches():
    """Test diff with identical baseline and candidate."""
    baseline_data = [
        {"sid": "1", "ok": 1, "score": 0.8, "have": 2, "need": 2, "scenario": "reversal", "reason": "ok"},
        {"sid": "2", "ok": 0, "score": 0.3, "have": 1, "need": 3, "scenario": "continuation", "reason": "low_score"},
    ]
    candidate_data = baseline_data.copy()
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
        for r in baseline_data:
            f.write(json.dumps(r) + "\n")
        baseline_path = f.name
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
        for r in candidate_data:
            f.write(json.dumps(r) + "\n")
        candidate_path = f.name
    
    try:
        result = diff(baseline_path, candidate_path)
        assert result["n"] == 2
        assert result["mismatches"] == 0
    finally:
        os.unlink(baseline_path)
        os.unlink(candidate_path)


def test_diff_with_mismatches():
    """Test diff with mismatches (ok field changed)."""
    baseline_data = [
        {"sid": "1", "ok": 1, "score": 0.8, "have": 2, "need": 2, "scenario": "reversal", "reason": "ok"},
    ]
    candidate_data = [
        {"sid": "1", "ok": 0, "score": 0.8, "have": 2, "need": 2, "scenario": "reversal", "reason": "ok"},
    ]
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
        for r in baseline_data:
            f.write(json.dumps(r) + "\n")
        baseline_path = f.name
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
        for r in candidate_data:
            f.write(json.dumps(r) + "\n")
        candidate_path = f.name
    
    try:
        result = diff(baseline_path, candidate_path)
        assert result["n"] == 1
        assert result["mismatches"] > 0
        assert "ok" in result["mismatch_by_field"]
        assert result["mismatch_by_field"]["ok"] >= 1
    finally:
        os.unlink(baseline_path)
        os.unlink(candidate_path)


def test_diff_score_epsilon():
    """Test that score differences < 1e-9 are ignored."""
    baseline_data = [
        {"sid": "1", "ok": 1, "score": 0.8, "have": 2, "need": 2},
    ]
    candidate_data = [
        {"sid": "1", "ok": 1, "score": 0.8 + 1e-10, "have": 2, "need": 2},  # tiny diff
    ]
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
        for r in baseline_data:
            f.write(json.dumps(r) + "\n")
        baseline_path = f.name
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
        for r in candidate_data:
            f.write(json.dumps(r) + "\n")
        candidate_path = f.name
    
    try:
        result = diff(baseline_path, candidate_path)
        # Score diff < 1e-9 should not count as mismatch
        assert "score" not in result.get("mismatch_by_field", {}) or result["mismatch_by_field"].get("score", 0) == 0
    finally:
        os.unlink(baseline_path)
        os.unlink(candidate_path)


def test_main_fail_on_mismatch():
    """Test main() exits with code 2 when mismatches exceed threshold."""
    baseline_data = [
        {"sid": "1", "ok": 1, "score": 0.8},
    ]
    candidate_data = [
        {"sid": "1", "ok": 0, "score": 0.8},  # mismatch
    ]
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
        for r in baseline_data:
            f.write(json.dumps(r) + "\n")
        baseline_path = f.name
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
        for r in candidate_data:
            f.write(json.dumps(r) + "\n")
        candidate_path = f.name
    
    with tempfile.NamedTemporaryFile(mode='w', suffix='.json', delete=False) as f:
        out_path = f.name
    
    try:
        import sys
        old_argv = sys.argv
        sys.argv = [
            "test",
            "--baseline", baseline_path,
            "--candidate", candidate_path,
            "--out", out_path,
            "--fail-on-mismatch", "1",
            "--max-mismatches", "0",
        ]
        
        try:
            with pytest.raises(SystemExit) as exc_info:
                main()
            assert exc_info.value.code == 2
        finally:
            sys.argv = old_argv
        
        # Check output file exists
        assert os.path.exists(out_path)
        with open(out_path, "r") as f:
            rep = json.load(f)
            assert rep["mismatches"] > 0
    finally:
        os.unlink(baseline_path)
        os.unlink(candidate_path)
        if os.path.exists(out_path):
            os.unlink(out_path)

