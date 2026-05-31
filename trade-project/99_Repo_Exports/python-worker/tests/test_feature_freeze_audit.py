"""Phase 2.2 — point-in-time / leakage guard tests."""
from __future__ import annotations

from calibration.feature_freeze_audit import audit_record


DECISION_TS = 1_780_000_000_000  # 2026-05-29 ~


def test_clean_features_no_violations():
    feats = {"obi": 0.5, "atr_bps": 8.0, "regime": "ranging"}
    assert audit_record(DECISION_TS, feats) == []


def test_past_timestamp_no_violation():
    feats = {"obi_snapshot": {"value": 0.5, "_ts_ms": DECISION_TS - 1000}}
    assert audit_record(DECISION_TS, feats) == []


def test_future_timestamp_flagged():
    feats = {"obi_snapshot": {"value": 0.5, "_ts_ms": DECISION_TS + 500}}
    viols = audit_record(DECISION_TS, feats)
    assert len(viols) == 1
    assert viols[0].path == "obi_snapshot._ts_ms"
    assert viols[0].delta_ms == 500


def test_tolerance_suppresses_minor_skew():
    feats = {"x": {"ts_ms": DECISION_TS + 50}}
    assert audit_record(DECISION_TS, feats, tolerance_ms=100) == []
    assert len(audit_record(DECISION_TS, feats, tolerance_ms=10)) == 1


def test_nested_list_walk():
    feats = {"levels": [{"px": 1.0, "_ts_ms": DECISION_TS + 1000}]}
    viols = audit_record(DECISION_TS, feats)
    assert len(viols) == 1
    assert "levels[0]._ts_ms" in viols[0].path


def test_multiple_violations_returned():
    feats = {
        "a": {"_ts_ms": DECISION_TS + 1},
        "b": {"ts_ms": DECISION_TS + 2},
        "c": {"timestamp_ms": DECISION_TS - 1},  # ok
    }
    viols = audit_record(DECISION_TS, feats)
    assert len(viols) == 2


def test_invalid_ts_ignored():
    feats = {"x": {"_ts_ms": "garbage"}, "y": {"_ts_ms": None}}
    assert audit_record(DECISION_TS, feats) == []


def test_implausibly_small_ts_ignored():
    # Some sources put `_ts_ms` = 0 as a sentinel.
    feats = {"x": {"_ts_ms": 0}, "y": {"_ts_ms": 12345}}
    assert audit_record(DECISION_TS, feats) == []


def test_empty_features():
    assert audit_record(DECISION_TS, {}) == []
    assert audit_record(DECISION_TS, None) == []  # type: ignore[arg-type]


def test_non_ts_keys_with_large_numbers_ignored():
    feats = {"depth": {"qty": DECISION_TS + 99999}}  # qty just happens to be large
    assert audit_record(DECISION_TS, feats) == []
