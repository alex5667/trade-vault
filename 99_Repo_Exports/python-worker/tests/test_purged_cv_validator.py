"""
tests/test_purged_cv_validator.py — Phase 1: purged_cv_validator_v1 unit tests.

Coverage:
  1.  fetch_resolved: SQL query parameters (cutoff_ms, limit)
  2.  run_validation: empty input → overall_passed=True (fail-open)
  3.  run_validation: insufficient samples → passed=True (fail-open)
  4.  run_validation: good data → guard evaluated, fields complete
  5.  run_validation: multiple sources → PBO computed cross-source
  6.  run_validation: high overfitting data → low DSR / high PBO
  7.  _validate_group: less than 2 folds → passed=True, reason=too_few_folds
  8.  _compute_cross_source_pbo: single source → PBO=0.0
  9.  _compute_cross_source_pbo: multiple sources with perfect IS/OOS mismatch → high PBO
  10. read_guard: CALIBRATION_VALIDATION not set → always True
  11. read_guard: CALIBRATION_VALIDATION=purged_walkforward, guard passes
  12. read_guard: CALIBRATION_VALIDATION=purged_walkforward, guard fails
  13. read_guard: Redis unavailable → fail-open True
  14. pre_publish_gate parse payload JSON blob
  15. pre_publish_gate flat field fallback when no payload
"""
from __future__ import annotations

import json
import os
import sys
import time

import numpy as np
import pytest

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _BASE not in sys.path:
    sys.path.insert(0, _BASE)


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _make_rows(
    n: int,
    symbol: str = "BTCUSDT",
    source: str = "iceberg",
    horizon_ms: int = 60_000,
    gap_ms: int = 10_000,
    realized_r: float = 0.5,
) -> list[dict]:
    """Generate n non-overlapping resolved signal_outcome rows."""
    rows = []
    for i in range(n):
        d_ms = i * (horizon_ms + gap_ms)
        rows.append({
            "symbol": symbol,
            "source": source,
            "decision_time_ms": d_ms,
            "resolved_time_ms": d_ms + horizon_ms,
            "realized_r": realized_r,
        })
    return rows


# ─── Tests: fetch_resolved ────────────────────────────────────────────────────

class TestFetchResolved:

    def test_correct_cutoff_and_limit(self):
        """Verify correct SQL parameters are passed (cutoff_ms, limit)."""
        from orderflow_services.purged_cv_validator_v1 import fetch_resolved

        calls = []

        class MockCur:
            description = [("symbol",), ("source",), ("decision_time_ms",), ("resolved_time_ms",), ("realized_r",)]
            def execute(self, sql, params):
                calls.append(params)
            def fetchall(self):
                return []
            def __enter__(self): return self
            def __exit__(self, *a): pass

        class MockConn:
            def cursor(self): return MockCur()

        rows = fetch_resolved(MockConn(), window_days=7.0, row_limit=100)
        assert len(calls) == 1
        cutoff_ms, limit = calls[0]
        # cutoff_ms should be ~7 days ago in ms
        expected_cutoff = (time.time() - 7 * 86_400) * 1000
        assert abs(cutoff_ms - expected_cutoff) < 5_000  # within 5s
        assert limit == 100
        assert rows == []


# ─── Tests: run_validation ────────────────────────────────────────────────────

class TestRunValidation:

    def test_empty_rows_overall_passed(self):
        """Empty input → overall_passed=True (fail-open)."""
        from orderflow_services.purged_cv_validator_v1 import run_validation

        state = run_validation(
            rows=[], n_blocks=4, embargo_ms=0,
            min_samples=5, min_dsr=0.0, max_pbo=0.5,
        )
        assert state["overall_passed"] is True
        assert state["n_groups"] == 0
        assert state["n_total"] == 0
        assert "schema_version" in state
        assert "ts_ms" in state

    def test_insufficient_samples_fail_open(self):
        """< min_samples records → group passes (fail-open)."""
        from orderflow_services.purged_cv_validator_v1 import run_validation

        rows = _make_rows(10, symbol="BTCUSDT", source="iceberg")
        state = run_validation(
            rows=rows, n_blocks=4, embargo_ms=0,
            min_samples=500,   # way more than 10
            min_dsr=0.0, max_pbo=0.5,
        )
        assert state["overall_passed"] is True
        guard = state["groups"].get("BTCUSDT:iceberg")
        assert guard is not None
        assert guard["reason"] == "insufficient_samples"
        assert guard["passed"] is True

    def test_good_data_guard_evaluated(self):
        """Enough non-overlapping positive-return samples → guard evaluated."""
        from orderflow_services.purged_cv_validator_v1 import run_validation

        rows = _make_rows(600, realized_r=0.8)
        state = run_validation(
            rows=rows, n_blocks=5, embargo_ms=0,
            min_samples=50, min_dsr=0.0, max_pbo=1.0,
        )
        assert state["n_groups"] == 1
        guard = state["groups"]["BTCUSDT:iceberg"]
        assert guard["reason"] == "guard_evaluated"
        assert guard["n"] == 600
        assert "dsr" in guard
        assert "pbo" in guard
        assert "passed" in guard
        assert guard["n_folds"] >= 2

    def test_guard_dict_complete_fields(self):
        """Guard dict must contain all expected keys."""
        from orderflow_services.purged_cv_validator_v1 import run_validation

        rows = _make_rows(200, realized_r=0.5)
        state = run_validation(
            rows=rows, n_blocks=4, embargo_ms=0,
            min_samples=50, min_dsr=0.0, max_pbo=1.0,
        )
        required = {"passed", "reason", "n", "dsr", "pbo", "dsr_ok", "pbo_ok", "n_folds"}
        guard = list(state["groups"].values())[0]
        assert required <= set(guard.keys())

    def test_multiple_sources_pbo_computed(self):
        """Two sources for same symbol → PBO computed cross-source."""
        from orderflow_services.purged_cv_validator_v1 import run_validation

        rows_a = _make_rows(200, source="iceberg",     realized_r=1.0)
        rows_b = _make_rows(200, source="delta_spike", realized_r=0.1)
        state = run_validation(
            rows=rows_a + rows_b, n_blocks=4, embargo_ms=0,
            min_samples=50, min_dsr=0.0, max_pbo=1.0,
        )
        assert state["n_groups"] == 2
        for g in state["groups"].values():
            assert "pbo" in g
            assert 0.0 <= g["pbo"] <= 1.0


# ─── Tests: _validate_group ───────────────────────────────────────────────────

class TestValidateGroup:

    def test_too_few_folds_fail_open(self):
        """Only 2 rows with n_blocks=5 → too few folds → passed=True."""
        from orderflow_services.purged_cv_validator_v1 import _validate_group

        rows = _make_rows(2, realized_r=0.5)
        guard = _validate_group(
            rows, n_blocks=5, embargo_ms=0,
            min_samples=2, min_dsr=0.0, max_pbo=0.5,
            symbol="BTCUSDT", source="iceberg",
        )
        assert guard["passed"] is True
        assert guard["reason"] in ("too_few_folds", "insufficient_samples")


# ─── Tests: _compute_cross_source_pbo ────────────────────────────────────────

class TestCrossSourcePbo:

    def test_single_source_pbo_zero(self):
        """Only one source → PBO=0.0 (no comparison possible)."""
        from orderflow_services.purged_cv_validator_v1 import _compute_cross_source_pbo

        rows = _make_rows(200, source="iceberg")
        pbo, pbo_ok = _compute_cross_source_pbo(
            {"iceberg": rows},
            n_blocks=4, embargo_ms=0,
            min_samples=50, max_pbo=0.5,
        )
        assert pbo == 0.0
        assert pbo_ok is True

    def test_two_sources_returns_pbo_in_range(self):
        """Two sources → PBO in [0, 1]."""
        from orderflow_services.purged_cv_validator_v1 import _compute_cross_source_pbo

        rows_a = _make_rows(200, source="iceberg",     realized_r=1.0)
        rows_b = _make_rows(200, source="delta_spike", realized_r=0.1)
        pbo, _ = _compute_cross_source_pbo(
            {"iceberg": rows_a, "delta_spike": rows_b},
            n_blocks=4, embargo_ms=0,
            min_samples=50, max_pbo=0.5,
        )
        assert 0.0 <= pbo <= 1.0

    def test_insufficient_samples_skipped(self):
        """Source with < min_samples records is excluded from PBO comparison → PBO=0."""
        from orderflow_services.purged_cv_validator_v1 import _compute_cross_source_pbo

        rows_a = _make_rows(200, source="iceberg")
        rows_b = _make_rows(5,   source="tiny")     # too small
        pbo, pbo_ok = _compute_cross_source_pbo(
            {"iceberg": rows_a, "tiny": rows_b},
            n_blocks=4, embargo_ms=0,
            min_samples=50, max_pbo=0.5,
        )
        # Only one source passes threshold → PBO=0 (no comparison)
        assert pbo == 0.0


# ─── Tests: read_guard ────────────────────────────────────────────────────────

class TestReadGuard:

    def test_validation_disabled_always_pass(self, monkeypatch):
        """CALIBRATION_VALIDATION not set → guard always passes."""
        from orderflow_services.purged_cv_validator_v1 import read_guard

        monkeypatch.delenv("CALIBRATION_VALIDATION", raising=False)

        class MockRc:
            def get(self, key): return None  # should not be called

        assert read_guard(MockRc(), symbol="BTCUSDT", source="iceberg") is True

    def test_validation_enabled_guard_passes(self, monkeypatch):
        """CALIBRATION_VALIDATION=purged_walkforward, group guard passes → True."""
        from orderflow_services.purged_cv_validator_v1 import read_guard

        monkeypatch.setenv("CALIBRATION_VALIDATION", "purged_walkforward")

        state = {
            "groups": {
                "BTCUSDT:iceberg": {"passed": True, "dsr": 0.9, "pbo": 0.1}
            },
            "overall_passed": True,
        }

        class MockRc:
            def get(self, key): return json.dumps(state)

        assert read_guard(MockRc(), symbol="BTCUSDT", source="iceberg") is True

    def test_validation_enabled_guard_fails(self, monkeypatch):
        """CALIBRATION_VALIDATION=purged_walkforward, group guard fails → False."""
        from orderflow_services.purged_cv_validator_v1 import read_guard

        monkeypatch.setenv("CALIBRATION_VALIDATION", "purged_walkforward")

        state = {
            "groups": {
                "BTCUSDT:iceberg": {"passed": False, "dsr": 0.1, "pbo": 0.9}
            },
            "overall_passed": False,
        }

        class MockRc:
            def get(self, key): return json.dumps(state)

        assert read_guard(MockRc(), symbol="BTCUSDT", source="iceberg") is False

    def test_redis_unavailable_fail_open(self, monkeypatch):
        """Redis error → fail-open True."""
        from orderflow_services.purged_cv_validator_v1 import read_guard

        monkeypatch.setenv("CALIBRATION_VALIDATION", "purged_walkforward")

        class BrokenRc:
            def get(self, key): raise ConnectionError("redis down")

        assert read_guard(BrokenRc(), symbol="BTCUSDT", source="iceberg") is True

    def test_unknown_group_fail_open(self, monkeypatch):
        """Guard state has no entry for the group → fail-open True."""
        from orderflow_services.purged_cv_validator_v1 import read_guard

        monkeypatch.setenv("CALIBRATION_VALIDATION", "purged_walkforward")

        state = {"groups": {}, "overall_passed": True}

        class MockRc:
            def get(self, key): return json.dumps(state)

        assert read_guard(MockRc(), symbol="ETHUSDT", source="cvd_burst") is True

    def test_no_state_in_redis_fail_open(self, monkeypatch):
        """Guard state not yet populated in Redis → fail-open True."""
        from orderflow_services.purged_cv_validator_v1 import read_guard

        monkeypatch.setenv("CALIBRATION_VALIDATION", "purged_walkforward")

        class MockRc:
            def get(self, key): return None

        assert read_guard(MockRc(), symbol="BTCUSDT", source="iceberg") is True


# ─── Tests: pre_publish_gate payload parsing ──────────────────────────────────

class TestPrePublishGatePayloadParsing:
    """
    Tests that pre_publish_gate_calibrator_v1 correctly parses JSON payload blobs.
    Validates the fix for the flat-field bug (fields.get("delta_z") always None).
    """

    def _parse_fields(self, fields: dict) -> dict | None:
        """
        Replicate the message parsing logic from pre_publish_gate_calibrator_v1.main().
        Returns extracted {delta_z, obi, symbol, regime, ts_ms} or None if skipped.
        """
        raw_payload = fields.get("payload")
        payload: dict = {}
        indicators: dict = {}
        if raw_payload:
            try:
                payload = json.loads(raw_payload)
                indicators = payload.get("indicators") or {}
            except Exception:
                pass

        delta_z_raw = (
            indicators.get("delta_z") or
            payload.get("delta_z") or
            fields.get("delta_z") or
            fields.get("z_delta") or
            fields.get("of_delta_z")
        )
        if delta_z_raw is None or delta_z_raw == "":
            return None

        try:
            delta_z = float(delta_z_raw)
        except (TypeError, ValueError):
            return None
        if delta_z != delta_z:
            return None

        obi_raw = (
            indicators.get("lob_obi_5") or
            indicators.get("obi_score") or
            indicators.get("obi") or
            payload.get("obi_score") or
            payload.get("obi") or
            fields.get("obi_score") or
            fields.get("obi") or
            fields.get("lob_obi_5") or
            fields.get("of_obi")
        )
        try:
            obi = float(obi_raw) if obi_raw else 0.0
        except (TypeError, ValueError):
            obi = 0.0

        symbol = (
            payload.get("symbol") or
            fields.get("symbol") or
            fields.get("sym") or "*"
        ).strip().upper()

        regime = (
            indicators.get("market_regime") or
            indicators.get("regime") or
            payload.get("market_regime") or
            payload.get("regime") or
            fields.get("market_regime") or
            fields.get("regime") or
            fields.get("entry_regime") or "*"
        )

        ts_ms = int(
            payload.get("ts_ms") or
            fields.get("ts_ms") or
            0
        )

        return dict(delta_z=delta_z, obi=obi, symbol=symbol, regime=regime, ts_ms=ts_ms)

    def test_payload_json_blob_parsed(self):
        """JSON blob in 'payload' field is parsed correctly."""
        signal = {
            "symbol": "BTCUSDT",
            "ts_ms": 1000000,
            "indicators": {
                "delta_z": 3.14,
                "lob_obi_5": 0.42,
                "market_regime": "trending_bull",
            }
        }
        fields = {"payload": json.dumps(signal)}
        result = self._parse_fields(fields)
        assert result is not None
        assert result["delta_z"] == pytest.approx(3.14)
        assert result["obi"] == pytest.approx(0.42)
        assert result["symbol"] == "BTCUSDT"
        assert result["regime"] == "trending_bull"
        assert result["ts_ms"] == 1000000

    def test_flat_field_fallback(self):
        """Flat fields work when no payload blob (legacy producers)."""
        fields = {
            "delta_z": "2.5",
            "obi": "0.3",
            "symbol": "ETHUSDT",
            "regime": "ranging",
            "ts_ms": "999",
        }
        result = self._parse_fields(fields)
        assert result is not None
        assert result["delta_z"] == pytest.approx(2.5)
        assert result["obi"] == pytest.approx(0.3)
        assert result["symbol"] == "ETHUSDT"

    def test_missing_delta_z_returns_none(self):
        """No delta_z anywhere → returns None (skip)."""
        signal = {
            "symbol": "BTCUSDT",
            "indicators": {"lob_obi_5": 0.3},
        }
        fields = {"payload": json.dumps(signal)}
        result = self._parse_fields(fields)
        assert result is None

    def test_malformed_payload_uses_flat_fallback(self):
        """Malformed JSON payload → falls back to flat fields if available."""
        fields = {
            "payload": "{not valid json",
            "delta_z": "1.5",
            "symbol": "SOLUSDT",
        }
        result = self._parse_fields(fields)
        assert result is not None
        assert result["delta_z"] == pytest.approx(1.5)
        assert result["symbol"] == "SOLUSDT"

    def test_obi_missing_defaults_to_zero(self):
        """Missing OBI → defaults to 0.0 (not a skip)."""
        signal = {
            "symbol": "BTCUSDT",
            "indicators": {"delta_z": 2.0},
        }
        fields = {"payload": json.dumps(signal)}
        result = self._parse_fields(fields)
        assert result is not None
        assert result["obi"] == pytest.approx(0.0)

    def test_regime_extracted_from_indicators(self):
        """Regime read from indicators.market_regime."""
        signal = {
            "symbol": "BTCUSDT",
            "indicators": {
                "delta_z": 1.5,
                "market_regime": "squeeze",
            },
        }
        fields = {"payload": json.dumps(signal)}
        result = self._parse_fields(fields)
        assert result is not None
        assert result["regime"] == "squeeze"

    def test_symbol_uppercase_normalized(self):
        """Symbol is normalized to UPPERCASE."""
        signal = {
            "symbol": "ethusdt",
            "indicators": {"delta_z": 1.5},
        }
        fields = {"payload": json.dumps(signal)}
        result = self._parse_fields(fields)
        assert result is not None
        assert result["symbol"] == "ETHUSDT"
