#!/usr/bin/env python3
"""Tests for tools/ofc_golden_replay.py"""

import json
import os
import tempfile
from unittest.mock import patch

import pytest

from tools.ofc_golden_replay import (
    _bucket_key,
    _decision_fingerprint,
    _pctl,
    _read_ndjson,
    _runtime_from_snapshot,
    main,
)


def test_pctl():
    """Test percentile calculation"""
    values = [1.0, 2.0, 3.0, 4.0, 5.0]
    assert _pctl(values, 50.0) == 3.0
    assert _pctl(values, 0.0) == 1.0
    assert _pctl(values, 100.0) == 5.0
    assert _pctl([], 50.0) == 0.0


def test_read_ndjson():
    """Test reading NDJSON file"""
    with tempfile.NamedTemporaryFile(mode="w", suffix=".ndjson", delete=False) as f:
        f.write('{"a": 1}\n')
        f.write('{"b": 2}\n')
        f.write('\n')  # empty line
        f.write('{"c": 3}\n')
        fpath = f.name

    try:
        rows = _read_ndjson(fpath)
        assert len(rows) == 3
        assert rows[0]["a"] == 1
        assert rows[1]["b"] == 2
        assert rows[2]["c"] == 3

        # Test limit
        rows_limited = _read_ndjson(fpath, limit=2)
        assert len(rows_limited) == 2
    finally:
        os.unlink(fpath)


def test_bucket_key():
    """Test bucket key extraction"""
    row1 = {"symbol": "BTCUSDT", "bucket_id": 100, "tick_ts_ms": 1000}
    row2 = {"symbol": "ETHUSDT", "bucket_id": None, "tick_ts_ms": 2000}
    row3 = {"symbol": "SOLUSDT", "indicators": {"bucket_id": 50}, "tick_ts_ms": 3000}

    key1 = _bucket_key(row1, 0)
    key2 = _bucket_key(row2, 1)
    key3 = _bucket_key(row3, 2)

    assert key1[0] == "BTCUSDT"
    assert key1[1] == 100
    assert key2[1] == -1  # None bucket_id
    assert key3[1] == 50  # from indicators


def test_runtime_from_snapshot():
    """Test runtime reconstruction from snapshot"""
    snap = {
        "last_obi_event": {"ts_ms": 1000, "direction": "LONG"},
        "last_regime": "trend",
        "pressure": {"per_min": 5.0},
    }
    rt = _runtime_from_snapshot(snap)
    assert hasattr(rt, "last_obi_event")
    assert hasattr(rt, "last_regime")
    assert rt.last_regime == "trend"


def test_decision_fingerprint():
    """Test decision fingerprint generation"""
    from types import SimpleNamespace

    symbol = "BTCUSDT"
    row = {"tick_ts_ms": 1000, "direction": "LONG"}
    ofc = SimpleNamespace(ok=1, have=3, need=3, score=0.85, scenario="trend", gate_bits=7)

    fp = _decision_fingerprint(symbol, row, ofc)
    assert "BTCUSDT" in fp
    assert "ok=1" in fp
    assert "have=3" in fp
    assert "need=3" in fp
    assert "scn=trend" in fp


def test_main_basic(tmp_path):
    """Test main function with minimal valid input"""
    # Create a minimal capture file
    capture_file = tmp_path / "test_capture.ndjson"
    row = {
        "symbol": "BTCUSDT",
        "tf": "1s",
        "direction": "LONG",
        "tick_ts_ms": 1000,
        "price": 50000.0,
        "delta_z": 3.5,
        "indicators": {},
        "runtime_snapshot": {},
        "cfg": {},
    }
    with open(capture_file, "w") as f:
        f.write(json.dumps(row) + "\n")

    # Mock OFConfirmEngine to avoid full initialization
    with patch("tools.ofc_golden_replay.OFConfirmEngine") as mock_engine_class:
        mock_engine = mock_engine_class.return_value
        from types import SimpleNamespace

        mock_ofc = SimpleNamespace(ok=1, have=3, need=3, score=0.85, scenario="trend", gate_bits=7)
        mock_engine.build.return_value = (mock_ofc, None)

        # Test with --path argument
        import sys

        old_argv = sys.argv
        try:
            sys.argv = ["ofc_golden_replay.py", "--path", str(capture_file), "--limit", "1"]
            main()
        except SystemExit:
            pass  # Expected when baseline digest mismatch
        finally:
            sys.argv = old_argv


def test_main_no_rows(tmp_path):
    """Test main function with empty file"""
    capture_file = tmp_path / "empty.ndjson"
    capture_file.write_text("")

    import sys

    old_argv = sys.argv
    try:
        sys.argv = ["ofc_golden_replay.py", "--path", str(capture_file)]
        with pytest.raises(SystemExit) as exc_info:
            main()
        assert exc_info.value.code == "no_rows_in_capture"
    finally:
        sys.argv = old_argv


if __name__ == "__main__":
    pytest.main([__file__, "-v"])

