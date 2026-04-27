"""
Тесты для tools/_ml_common.py - общие утилиты для ML инструментов.
"""
from __future__ import annotations

import json
import sys
import tempfile
from pathlib import Path

import pytest

# Add tools to path
tools_path = Path(__file__).parent.parent.parent / "tools"
if str(tools_path) not in sys.path:
    sys.path.insert(0, str(tools_path))

from _ml_common import (
    now_ms,
    pctl,
    clamp01,
    safe_float,
    safe_int,
    read_ndjson,
    ece,
    brier,
)


class TestClamp01:
    """Тесты для clamp01."""
    
    def test_clamp01_negative(self):
        assert clamp01(-1.0) == 0.0
        assert clamp01(-100.0) == 0.0
    
    def test_clamp01_normal(self):
        assert clamp01(0.25) == 0.25
        assert clamp01(0.5) == 0.5
        assert clamp01(0.75) == 0.75
    
    def test_clamp01_positive(self):
        assert clamp01(1.0) == 1.0
        assert clamp01(2.0) == 1.0
        assert clamp01(100.0) == 1.0


class TestPctl:
    """Тесты для pctl (percentile)."""
    
    def test_pctl_empty(self):
        assert pctl([], 0.5) == 0.0
    
    def test_pctl_single(self):
        assert pctl([1.0], 0.5) == 1.0
        assert pctl([1.0], 0.0) == 1.0
        assert pctl([1.0], 1.0) == 1.0
    
    def test_pctl_multiple(self):
        xs = [1.0, 2.0, 3.0, 4.0, 5.0]
        assert pctl(xs, 0.0) == 1.0
        assert pctl(xs, 0.5) == 3.0
        assert pctl(xs, 1.0) == 5.0
        assert pctl(xs, 0.25) == 2.0
        assert pctl(xs, 0.75) == 4.0


class TestSafeFloat:
    """Тесты для safe_float."""
    
    def test_safe_float_valid(self):
        assert safe_float("1.5", 0.0) == 1.5
        assert safe_float(2.5, 0.0) == 2.5
        assert safe_float(10, 0.0) == 10.0
    
    def test_safe_float_invalid(self):
        assert safe_float("invalid", 0.0) == 0.0
        assert safe_float(None, 5.0) == 5.0
        assert safe_float("", 3.0) == 3.0


class TestSafeInt:
    """Тесты для safe_int."""
    
    def test_safe_int_valid(self):
        assert safe_int("10", 0) == 10
        assert safe_int(20, 0) == 20
        assert safe_int(30.5, 0) == 30
    
    def test_safe_int_invalid(self):
        assert safe_int("invalid", 0) == 0
        assert safe_int(None, 5) == 5
        assert safe_int("", 3) == 3


class TestReadNdjson:
    """Тесты для read_ndjson."""
    
    def test_read_ndjson_empty(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
            f.write("")
            f.flush()
            path = f.name
        
        try:
            rows = list(read_ndjson(path))
            assert len(rows) == 0
        finally:
            Path(path).unlink()
    
    def test_read_ndjson_single(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
            json.dump({"a": 1, "b": 2}, f)
            f.write("\n")
            f.flush()
            path = f.name
        
        try:
            rows = list(read_ndjson(path))
            assert len(rows) == 1
            assert rows[0] == {"a": 1, "b": 2}
        finally:
            Path(path).unlink()
    
    def test_read_ndjson_multiple(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
            json.dump({"a": 1}, f)
            f.write("\n")
            json.dump({"b": 2}, f)
            f.write("\n")
            json.dump({"c": 3}, f)
            f.write("\n")
            f.flush()
            path = f.name
        
        try:
            rows = list(read_ndjson(path))
            assert len(rows) == 3
            assert rows[0] == {"a": 1}
            assert rows[1] == {"b": 2}
            assert rows[2] == {"c": 3}
        finally:
            Path(path).unlink()
    
    def test_read_ndjson_skip_empty(self):
        with tempfile.NamedTemporaryFile(mode='w', suffix='.ndjson', delete=False) as f:
            json.dump({"a": 1}, f)
            f.write("\n")
            f.write("\n")  # empty line
            f.write("   \n")  # whitespace only
            json.dump({"b": 2}, f)
            f.write("\n")
            f.flush()
            path = f.name
        
        try:
            rows = list(read_ndjson(path))
            assert len(rows) == 2
            assert rows[0] == {"a": 1}
            assert rows[1] == {"b": 2}
        finally:
            Path(path).unlink()


class TestECE:
    """Тесты для ECE (Expected Calibration Error)."""
    
    def test_ece_empty(self):
        assert ece([], []) == 0.0
    
    def test_ece_perfect(self):
        # Perfect calibration: p matches y
        probs = [0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9]
        y = [0, 0, 0, 0, 1, 1, 1, 1, 1]
        # ECE should be low (not exactly 0 due to binning)
        ece_val = ece(probs, y)
        assert ece_val >= 0.0
        assert ece_val < 0.5  # reasonable upper bound
    
    def test_ece_miscalibrated(self):
        # Miscalibrated: high probs but low y
        probs = [0.9, 0.9, 0.9, 0.9, 0.9]
        y = [0, 0, 0, 0, 0]
        ece_val = ece(probs, y)
        assert ece_val > 0.0  # should detect miscalibration


class TestBrier:
    """Тесты для Brier score."""
    
    def test_brier_empty(self):
        assert brier([], []) == 0.0
    
    def test_brier_perfect(self):
        # Perfect predictions
        probs = [0.0, 0.0, 1.0, 1.0]
        y = [0, 0, 1, 1]
        score = brier(probs, y)
        assert score == 0.0
    
    def test_brier_worst(self):
        # Worst predictions (always wrong)
        probs = [1.0, 1.0, 0.0, 0.0]
        y = [0, 0, 1, 1]
        score = brier(probs, y)
        assert score == 1.0
    
    def test_brier_partial(self):
        # Partial accuracy
        probs = [0.5, 0.5, 0.5, 0.5]
        y = [0, 1, 0, 1]
        score = brier(probs, y)
        assert score > 0.0
        assert score < 1.0
        # Expected: 0.25 (each prediction is 0.5, error is 0.5^2 = 0.25)
        assert abs(score - 0.25) < 0.01


class TestNowMs:
    """Тесты для now_ms."""
    
    def test_now_ms_type(self):
        ts = now_ms()
        assert isinstance(ts, int)
        assert ts > 0
    
    def test_now_ms_increasing(self):
        ts1 = now_ms()
        ts2 = now_ms()
        assert ts2 >= ts1  # should be monotonic (or equal if very fast)

