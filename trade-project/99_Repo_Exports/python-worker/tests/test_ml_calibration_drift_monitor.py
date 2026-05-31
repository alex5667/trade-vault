"""Tests for Task 2.1 rolling ECE/Brier monitor helpers."""
from __future__ import annotations

from collections import deque

import pytest

from orderflow_services.ml_calibration_drift_monitor_v1 import (
    _brier,
    _ece,
    _field_get,
    _parse_outcome,
    _safe_float,
)


def test_brier_empty_window():
    assert _brier(deque()) == 0.0


def test_brier_perfect_prediction():
    # p=1, y=1 → Brier 0; p=0, y=0 → Brier 0
    win = deque([(1.0, 1), (0.0, 0), (1.0, 1)])
    assert _brier(win) == 0.0


def test_brier_worst_prediction():
    # p=1, y=0 → 1; p=0, y=1 → 1
    win = deque([(1.0, 0), (0.0, 1)])
    assert _brier(win) == 1.0


def test_brier_mixed():
    win = deque([(0.5, 1), (0.5, 0)])
    # each contributes 0.25 → mean = 0.25
    assert _brier(win) == pytest.approx(0.25)


def test_ece_empty():
    assert _ece(deque()) == 0.0


def test_ece_perfectly_calibrated():
    # All predictions p=0.5, exactly half wins → ECE near 0
    win = deque([(0.5, 1)] * 50 + [(0.5, 0)] * 50)
    assert _ece(win) == pytest.approx(0.0, abs=0.01)


def test_ece_miscalibrated_high():
    # p=0.9 but no wins → bucket mismatch big
    win = deque([(0.9, 0)] * 100)
    # only bucket 9 populated: mean_p=0.9, mean_y=0 → contribution 1.0*0.9 = 0.9
    assert _ece(win) == pytest.approx(0.9)


def test_field_get_str_keys():
    fields = {"ml_prob": "0.65", "result": "WIN"}
    assert _field_get(fields, "ml_prob") == "0.65"
    assert _field_get(fields, "result") == "WIN"


def test_field_get_bytes_keys():
    fields = {b"ml_prob": b"0.65", b"result": b"WIN"}
    assert _field_get(fields, "ml_prob") == b"0.65"
    assert _field_get(fields, "result") == b"WIN"


def test_field_get_fallback_chain():
    fields = {"p_edge": "0.5"}
    assert _field_get(fields, "ml_prob", "p_edge") == "0.5"


def test_field_get_missing_returns_none():
    assert _field_get({}, "ml_prob") is None
    assert _field_get(None, "ml_prob") is None


def test_safe_float_handles_nan_inf():
    assert _safe_float(float("nan"), default=1.5) == 1.5
    assert _safe_float(float("inf"), default=1.5) == 1.5
    assert _safe_float("not a number", default=2.0) == 2.0
    assert _safe_float(None, default=3.0) == 3.0


def test_parse_outcome_win_str_keys():
    fields = {"ml_prob": "0.7", "result": "WIN", "model_ver": "v14_of", "regime": "trend"}
    res = _parse_outcome(fields)
    assert res is not None
    p, y, schema, regime = res
    assert p == pytest.approx(0.7)
    assert y == 1
    assert schema == "v14_of"
    assert regime == "trend"


def test_parse_outcome_loss_bytes_keys():
    fields = {b"p_edge": b"0.3", b"result": b"LOSS", b"model_ver": b"v14_of"}
    res = _parse_outcome(fields)
    assert res is not None
    p, y, schema, regime = res
    assert p == pytest.approx(0.3)
    assert y == 0
    assert schema == "v14_of"
    assert regime == "na"


def test_parse_outcome_skips_breakeven():
    fields = {"ml_prob": "0.5", "result": "BE"}
    assert _parse_outcome(fields) is None


def test_parse_outcome_skips_missing_p():
    assert _parse_outcome({"result": "WIN"}) is None


def test_parse_outcome_skips_out_of_range_p():
    assert _parse_outcome({"ml_prob": "1.5", "result": "WIN"}) is None
    assert _parse_outcome({"ml_prob": "-0.1", "result": "LOSS"}) is None


def test_parse_outcome_tp_variants_count_as_win():
    for r in ("TP", "TP1", "TP2", "TP3", "W"):
        fields = {"ml_prob": "0.6", "result": r}
        res = _parse_outcome(fields)
        assert res is not None and res[1] == 1


def test_parse_outcome_sl_variants_count_as_loss():
    for r in ("SL", "STOP", "L"):
        fields = {"ml_prob": "0.6", "result": r}
        res = _parse_outcome(fields)
        assert res is not None and res[1] == 0


if __name__ == "__main__":
    pytest.main([__file__, "-v"])
