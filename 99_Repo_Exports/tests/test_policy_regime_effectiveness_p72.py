#!/usr/bin/env python3
"""tests/test_policy_regime_effectiveness_p72.py

Unit tests for P72 policy regime effectiveness worker.
"""

import pytest
from typing import Dict, List, Any


# Import functions from the worker
import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'tools'))

from policy_regime_effectiveness_report_worker_p72 import (
    _norm_policy_mode,
    _norm_state,
    _to_float,
    _to_str,
    _score_from_fields,
    _r_mult_from_fields,
    _pick_first,
    _ece,
    _precision_top_p,
    _metrics,
    Acc,
    _acc_add,
    _make_csv,
)


class TestNormalization:
    """Test state normalization functions."""
    
    def test_norm_policy_mode_ok(self):
        assert _norm_policy_mode("ok") == "ok"
        assert _norm_policy_mode("OK") == "ok"
        assert _norm_policy_mode("normal") == "ok"
        assert _norm_policy_mode("green") == "ok"
    
    def test_norm_policy_mode_warn(self):
        assert _norm_policy_mode("warn") == "warn"
        assert _norm_policy_mode("warning") == "warn"
        assert _norm_policy_mode("yellow") == "warn"
        assert _norm_policy_mode("degraded") == "warn"
    
    def test_norm_policy_mode_block(self):
        assert _norm_policy_mode("block") == "block"
        assert _norm_policy_mode("blocked") == "block"
        assert _norm_policy_mode("red") == "block"
    
    def test_norm_policy_mode_unknown(self):
        assert _norm_policy_mode("") == "unknown"
        assert _norm_policy_mode("invalid") == "unknown"
        assert _norm_policy_mode(None) == "unknown"
    
    def test_norm_state_ok(self):
        assert _norm_state("ok") == "ok"
        assert _norm_state("good") == "ok"
        assert _norm_state("pass") == "ok"
        assert _norm_state("green") == "ok"
        assert _norm_state("healthy") == "ok"
        assert _norm_state("none") == "ok"
    
    def test_norm_state_warn(self):
        assert _norm_state("warn") == "warn"
        assert _norm_state("warning") == "warn"
        assert _norm_state("yellow") == "warn"
        assert _norm_state("soft") == "warn"
        assert _norm_state("stale") == "warn"
        assert _norm_state("degraded") == "warn"
    
    def test_norm_state_block(self):
        assert _norm_state("block") == "block"
        assert _norm_state("blocked") == "block"
        assert _norm_state("red") == "block"
        assert _norm_state("hard") == "block"
        assert _norm_state("fail") == "block"
        assert _norm_state("bad") == "block"
        assert _norm_state("quarantine") == "block"
    
    def test_norm_state_unknown(self):
        assert _norm_state("") == "unknown"
        assert _norm_state("invalid") == "unknown"
        assert _norm_state(None) == "unknown"


class TestFieldExtraction:
    """Test field extraction and conversion functions."""
    
    def test_to_float_valid(self):
        assert _to_float(1.5) == 1.5
        assert _to_float("2.5") == 2.5
        assert _to_float(3) == 3.0
        assert _to_float(b"4.5") == 4.5
    
    def test_to_float_invalid(self):
        assert _to_float(None) is None
        assert _to_float("") is None
        assert _to_float("invalid") is None
    
    def test_to_str(self):
        assert _to_str("test") == "test"
        assert _to_str(b"test") == "test"
        assert _to_str(123) == "123"
        assert _to_str(None) == ""
    
    def test_pick_first(self):
        fields = {"a": 1, "b": 2, "c": 3}
        assert _pick_first(fields, ["x", "b", "c"]) == 2
        assert _pick_first(fields, ["x", "y"]) is None
        assert _pick_first(fields, ["a"]) == 1
    
    def test_score_from_fields(self):
        fields = {"score": 0.75, "other": 0.5}
        assert _score_from_fields(fields, ["score", "p"]) == 0.75
        
        # Test percent conversion (values between 1 and 100 are treated as percentages)
        fields = {"score": 75.0}
        assert _score_from_fields(fields, ["score"]) == 0.75
        
        # Test values > 100 are clamped to 1.0
        fields = {"score": 150.0}
        assert _score_from_fields(fields, ["score"]) == 1.0
        
        # Test negative values are clamped to 0.0
        fields = {"score": -0.5}
        assert _score_from_fields(fields, ["score"]) == 0.0
    
    def test_r_mult_from_fields(self):
        fields = {"r_mult": 1.5, "other": 0.5}
        assert _r_mult_from_fields(fields, ["r_mult", "r"]) == 1.5
        
        fields = {"r": -0.5}
        assert _r_mult_from_fields(fields, ["r_mult", "r"]) == -0.5


class TestMetrics:
    """Test metrics computation functions."""
    
    def test_acc_add(self):
        acc = Acc()
        _acc_add(acc, 0.8, 1, 1.5)
        _acc_add(acc, 0.6, 0, -0.5)
        
        assert len(acc.scores) == 2
        assert len(acc.ys) == 2
        assert len(acc.rs) == 2
        assert acc.scores == [0.8, 0.6]
        assert acc.ys == [1, 0]
        assert acc.rs == [1.5, -0.5]
    
    def test_precision_top_p(self):
        scores = [0.9, 0.8, 0.7, 0.6, 0.5, 0.4, 0.3, 0.2, 0.1, 0.0]
        ys = [1, 1, 1, 0, 0, 0, 0, 0, 0, 0]
        
        # Top 20% (2 samples): both wins
        pr = _precision_top_p(scores, ys, 0.2)
        assert pr == 1.0
        
        # Top 50% (5 samples): 3 wins
        pr = _precision_top_p(scores, ys, 0.5)
        assert pr == 0.6
    
    def test_ece(self):
        # Perfect calibration
        scores = [0.9] * 10 + [0.1] * 10
        ys = [1] * 9 + [0] + [0] * 9 + [1]
        ece = _ece(scores, ys, bins=2)
        assert ece < 0.1  # Should be low
        
        # Poor calibration
        scores = [0.9] * 10 + [0.1] * 10
        ys = [0] * 10 + [1] * 10
        ece = _ece(scores, ys, bins=2)
        assert ece > 0.5  # Should be high
    
    def test_metrics(self):
        acc = Acc()
        _acc_add(acc, 0.9, 1, 2.0)
        _acc_add(acc, 0.8, 1, 1.5)
        _acc_add(acc, 0.7, 0, -1.0)
        _acc_add(acc, 0.6, 0, -0.5)
        
        m = _metrics(acc, top_p=0.5, ece_bins=10)
        
        assert m["n"] == 4.0
        assert m["expectancy_r"] == (2.0 + 1.5 - 1.0 - 0.5) / 4.0
        assert 0.0 <= m["precision_top5p"] <= 1.0
        assert 0.0 <= m["ece"] <= 1.0
    
    def test_metrics_empty(self):
        acc = Acc()
        m = _metrics(acc, top_p=0.05, ece_bins=10)
        
        assert m["n"] == 0.0
        assert m["expectancy_r"] == 0.0
        assert m["precision_top5p"] == 0.0
        assert m["ece"] == 0.0


class TestCSVGeneration:
    """Test CSV generation."""
    
    def test_make_csv(self):
        rows = [
            {"dq_state": "ok", "drift_state": "ok", "n_24h": 100},
            {"dq_state": "warn", "drift_state": "ok", "n_24h": 50},
        ]
        
        csv_str = _make_csv(rows)
        
        assert "dq_state" in csv_str
        assert "drift_state" in csv_str
        assert "n_24h" in csv_str
        assert "ok" in csv_str
        assert "warn" in csv_str
        assert "100" in csv_str
        assert "50" in csv_str
    
    def test_make_csv_empty(self):
        csv_str = _make_csv([])
        assert csv_str == ""


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
