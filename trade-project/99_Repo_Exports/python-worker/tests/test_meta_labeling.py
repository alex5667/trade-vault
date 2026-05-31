"""
tests/test_meta_labeling.py — Phase 2.1: meta-labeling model + gate tests.

Coverage:
  Model:
    1.  extract_features: known indicators → correct numeric mapping
    2.  extract_features: missing fields → 0.0 default
    3.  extract_features: regime / session / direction encoding
    4.  extract_features: non-finite values → 0.0
    5.  features_to_array: correct column order
    6.  train_meta_labeling_model: insufficient data → None
    7.  train_meta_labeling_model: imbalanced (no positives) → None
    8.  train_meta_labeling_model: valid data → state dict with required fields
    9.  train_meta_labeling_model: roc_auc_oos in [0, 1]
   10.  predict_prob: valid state → float in [0, 1]
   11.  predict_prob: corrupt state → 0.5 (indeterminate)
   12.  get_threshold: regime-specific threshold returned
   13.  get_threshold: unknown regime → default threshold

  Gate:
   14.  MetaLabelGate: no model → PASS (fail-open)
   15.  MetaLabelGate: prob >= threshold → PASS
   16.  MetaLabelGate: prob < threshold, enabled=False → SHADOW_VETO (not VETO)
   17.  MetaLabelGate: prob < threshold, enabled=True → VETO
   18.  MetaLabelGate: should_veto=False in shadow mode
   19.  MetaLabelGate: model TTL — reload after expiry
   20.  MetaLabelGate: Redis error on reload → fail-open PASS
   21.  MetaLabelGate: metrics injected → counter incremented
   22.  get_gate: singleton returns same instance
   23.  get_gate: reset_gate creates fresh instance
"""
from __future__ import annotations

import json
import os
import sys
import time

import pytest

_BASE = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
if _BASE not in sys.path:
    sys.path.insert(0, _BASE)


# ─── Helpers ─────────────────────────────────────────────────────────────────

def _make_rows(n: int, label_cycle: list[int] | None = None, horizon_ms: int = 60_000):
    """Generate synthetic signal_outcome rows with rich indicators."""
    if label_cycle is None:
        label_cycle = [1, -1, 0, 1]
    rows = []
    for i in range(n):
        d_ms = i * (horizon_ms + 10_000)
        rows.append({
            "decision_time_ms": d_ms,
            "resolved_time_ms": d_ms + horizon_ms,
            "label": label_cycle[i % len(label_cycle)],
            "features": {
                "delta_z": float(i % 5),
                "lob_obi_5": 0.3 + (i % 3) * 0.1,
                "spread_bps": 2.0 + (i % 4),
                "atr_bps": 10.0 + (i % 10),
                "ml_prob": 0.4 + (i % 5) * 0.05,
                "of_score": 0.5,
                "market_regime": ["ranging", "trending_bull", "squeeze"][i % 3],
                "session": "london",
            },
        })
    return rows


class MockRc:
    """Fake Redis client for testing."""
    def __init__(self, state_json: str | None = None):
        self._data = state_json
        self.get_calls = 0

    def get(self, _key):
        self.get_calls += 1
        return self._data

    def raise_on_get(self):
        def _get(_key):
            raise ConnectionError("redis down")
        self.get = _get


# ─── Tests: extract_features ─────────────────────────────────────────────────

class TestExtractFeatures:

    def test_known_fields_extracted(self):
        from calibration.meta_labeling_model import extract_features

        ind = {"delta_z": 2.5, "lob_obi_5": 0.4, "spread_bps": 3.0, "atr_bps": 12.0}
        feats = extract_features(ind)
        assert feats["delta_z"] == pytest.approx(2.5)
        assert feats["lob_obi_5"] == pytest.approx(0.4)
        assert feats["spread_bps"] == pytest.approx(3.0)

    def test_missing_fields_default_zero(self):
        from calibration.meta_labeling_model import extract_features

        feats = extract_features({})
        for col in ["delta_z", "lob_obi_5", "spread_bps", "atr_bps"]:
            assert feats[col] == 0.0

    def test_regime_encoding(self):
        from calibration.meta_labeling_model import extract_features, _REGIME_CODES

        for regime, code in _REGIME_CODES.items():
            feats = extract_features({"delta_z": 1.0, "market_regime": regime})
            assert feats["regime_code"] == float(code)

    def test_unknown_regime_encodes_as_4(self):
        from calibration.meta_labeling_model import extract_features

        feats = extract_features({"delta_z": 1.0, "market_regime": "nonexistent"})
        assert feats["regime_code"] == 4.0

    def test_session_encoding(self):
        from calibration.meta_labeling_model import extract_features, _SESSION_CODES

        for session, code in _SESSION_CODES.items():
            feats = extract_features({"delta_z": 1.0, "session": session})
            assert feats["session_code"] == float(code)

    def test_direction_long(self):
        from calibration.meta_labeling_model import extract_features

        feats = extract_features({"delta_z": 1.0, "direction": "LONG"})
        assert feats["is_long"] == 1.0

    def test_direction_short(self):
        from calibration.meta_labeling_model import extract_features

        feats = extract_features({"delta_z": 1.0, "direction": "SHORT"})
        assert feats["is_long"] == 0.0

    def test_non_finite_values_default_zero(self):
        from calibration.meta_labeling_model import extract_features

        feats = extract_features({"delta_z": float("nan"), "lob_obi_5": float("inf")})
        assert feats["delta_z"] == 0.0
        assert feats["lob_obi_5"] == 0.0

    def test_non_numeric_values_default_zero(self):
        from calibration.meta_labeling_model import extract_features

        feats = extract_features({"delta_z": "not_a_number"})
        assert feats["delta_z"] == 0.0


class TestFeaturesToArray:

    def test_correct_column_order(self):
        from calibration.meta_labeling_model import features_to_array

        feats = {"b": 2.0, "a": 1.0, "c": 3.0}
        cols = ["a", "b", "c"]
        arr = features_to_array(feats, cols)
        assert arr.shape == (1, 3)
        assert arr[0, 0] == pytest.approx(1.0)
        assert arr[0, 1] == pytest.approx(2.0)
        assert arr[0, 2] == pytest.approx(3.0)

    def test_missing_col_defaults_zero(self):
        from calibration.meta_labeling_model import features_to_array

        feats = {"a": 5.0}
        arr = features_to_array(feats, ["a", "missing_col"])
        assert arr[0, 1] == 0.0


# ─── Tests: train_meta_labeling_model ────────────────────────────────────────

class TestTrainMetaLabelingModel:

    def test_insufficient_data_returns_none(self):
        from calibration.meta_labeling_model import train_meta_labeling_model

        rows = _make_rows(10)
        state = train_meta_labeling_model(rows, min_samples=200)
        assert state is None

    def test_no_positives_returns_none(self):
        from calibration.meta_labeling_model import train_meta_labeling_model

        rows = _make_rows(300, label_cycle=[-1, 0])  # no positive labels
        state = train_meta_labeling_model(rows, min_samples=100)
        assert state is None

    def test_valid_data_returns_state(self):
        from calibration.meta_labeling_model import train_meta_labeling_model

        rows = _make_rows(500)
        state = train_meta_labeling_model(rows, n_blocks=4, embargo_ms=0, min_samples=100)
        assert state is not None

    def test_state_has_required_fields(self):
        from calibration.meta_labeling_model import train_meta_labeling_model

        rows = _make_rows(500)
        state = train_meta_labeling_model(rows, n_blocks=4, embargo_ms=0, min_samples=100)
        assert state is not None
        required = {
            "schema_version", "ts_ms", "n_samples", "n_folds", "roc_auc_oos",
            "feature_cols", "default_threshold", "thresholds_by_regime",
            "model_bytes_b64", "calibrator_bytes_b64", "dsr",
        }
        assert required <= set(state.keys())

    def test_roc_auc_in_valid_range(self):
        from calibration.meta_labeling_model import train_meta_labeling_model

        rows = _make_rows(500)
        state = train_meta_labeling_model(rows, n_blocks=4, embargo_ms=0, min_samples=100)
        assert state is not None
        assert 0.0 <= state["roc_auc_oos"] <= 1.0

    def test_feature_cols_list_nonempty(self):
        from calibration.meta_labeling_model import train_meta_labeling_model

        rows = _make_rows(500)
        state = train_meta_labeling_model(rows, n_blocks=4, embargo_ms=0, min_samples=100)
        assert state is not None
        assert isinstance(state["feature_cols"], list)
        assert len(state["feature_cols"]) > 0

    def test_thresholds_by_regime_populated(self):
        from calibration.meta_labeling_model import train_meta_labeling_model, _REGIME_CODES

        rows = _make_rows(500)
        state = train_meta_labeling_model(rows, n_blocks=4, embargo_ms=0, min_samples=100)
        assert state is not None
        for regime in _REGIME_CODES:
            assert regime in state["thresholds_by_regime"]

    def test_n_folds_correct(self):
        from calibration.meta_labeling_model import train_meta_labeling_model

        rows = _make_rows(500)
        state = train_meta_labeling_model(rows, n_blocks=4, embargo_ms=0, min_samples=100)
        assert state is not None
        assert state["n_folds"] == 3  # n_blocks - 1


# ─── Tests: predict_prob ─────────────────────────────────────────────────────

class TestPredictProb:

    def test_predict_returns_float_in_01(self):
        from calibration.meta_labeling_model import train_meta_labeling_model, predict_prob

        rows = _make_rows(500)
        state = train_meta_labeling_model(rows, n_blocks=4, embargo_ms=0, min_samples=100)
        assert state is not None

        prob = predict_prob({"delta_z": 2.0, "lob_obi_5": 0.3}, state)
        assert isinstance(prob, float)
        assert 0.0 <= prob <= 1.0

    def test_corrupt_state_returns_half(self):
        from calibration.meta_labeling_model import predict_prob

        bad_state = {"model_bytes_b64": "NOTBASE64=", "calibrator_bytes_b64": "NOTBASE64=", "feature_cols": ["x"]}
        prob = predict_prob({"x": 1.0}, bad_state)
        assert prob == pytest.approx(0.5)

    def test_missing_model_bytes_returns_half(self):
        from calibration.meta_labeling_model import predict_prob

        prob = predict_prob({}, {})
        assert prob == pytest.approx(0.5)


class TestGetThreshold:

    def test_known_regime_returns_specific(self):
        from calibration.meta_labeling_model import get_threshold

        state = {
            "thresholds_by_regime": {"trending_bull": 0.6, "ranging": 0.4},
            "default_threshold": 0.45,
        }
        assert get_threshold(state, "trending_bull") == pytest.approx(0.6)
        assert get_threshold(state, "ranging") == pytest.approx(0.4)

    def test_unknown_regime_uses_default(self):
        from calibration.meta_labeling_model import get_threshold

        state = {"thresholds_by_regime": {}, "default_threshold": 0.45}
        assert get_threshold(state, "nonexistent") == pytest.approx(0.45)

    def test_empty_state_uses_builtin_default(self):
        from calibration.meta_labeling_model import get_threshold

        assert get_threshold({}, "trending_bull") == pytest.approx(0.45)


# ─── Tests: MetaLabelGate ─────────────────────────────────────────────────────

class TestMetaLabelGate:

    def test_no_model_returns_pass(self):
        """No model in Redis → fail-open PASS."""
        from services.meta_labeling_gate import MetaLabelGate

        gate = MetaLabelGate(MockRc(None), enabled=True)
        decision, prob, reason = gate.evaluate({}, regime="ranging")
        assert decision == "PASS"
        assert prob == pytest.approx(0.5)
        assert reason is None

    def test_prob_above_threshold_pass(self):
        """prob >= threshold → PASS."""
        from calibration.meta_labeling_model import train_meta_labeling_model
        from services.meta_labeling_gate import MetaLabelGate

        rows = _make_rows(500)
        state = train_meta_labeling_model(rows, n_blocks=4, embargo_ms=0, min_samples=100)
        assert state is not None

        # Patch threshold very low so signal passes
        state["default_threshold"] = 0.0
        for k in state["thresholds_by_regime"]:
            state["thresholds_by_regime"][k] = 0.0

        rc = MockRc(json.dumps(state))
        gate = MetaLabelGate(rc, enabled=True, model_ttl_sec=0)

        decision, prob, reason = gate.evaluate({"delta_z": 3.0, "lob_obi_5": 0.5}, regime="ranging")
        assert decision == "PASS"
        assert reason is None

    def test_prob_below_threshold_shadow_veto_when_disabled(self):
        """prob < threshold, enabled=False → SHADOW_VETO."""
        from calibration.meta_labeling_model import train_meta_labeling_model
        from services.meta_labeling_gate import MetaLabelGate

        rows = _make_rows(500)
        state = train_meta_labeling_model(rows, n_blocks=4, embargo_ms=0, min_samples=100)
        assert state is not None

        # Set threshold impossibly high → always below
        state["default_threshold"] = 2.0
        for k in state["thresholds_by_regime"]:
            state["thresholds_by_regime"][k] = 2.0

        rc = MockRc(json.dumps(state))
        gate = MetaLabelGate(rc, enabled=False, model_ttl_sec=0)

        decision, prob, reason = gate.evaluate({}, regime="ranging")
        assert decision == "SHADOW_VETO"
        assert reason == "META_LOW_PROB"

    def test_prob_below_threshold_veto_when_enabled(self):
        """prob < threshold, enabled=True → VETO."""
        from calibration.meta_labeling_model import train_meta_labeling_model
        from services.meta_labeling_gate import MetaLabelGate

        rows = _make_rows(500)
        state = train_meta_labeling_model(rows, n_blocks=4, embargo_ms=0, min_samples=100)
        assert state is not None

        state["default_threshold"] = 2.0
        for k in state["thresholds_by_regime"]:
            state["thresholds_by_regime"][k] = 2.0

        rc = MockRc(json.dumps(state))
        gate = MetaLabelGate(rc, enabled=True, model_ttl_sec=0)

        decision, prob, reason = gate.evaluate({}, regime="ranging")
        assert decision == "VETO"
        assert reason == "META_LOW_PROB"

    def test_should_veto_false_in_shadow_mode(self):
        """should_veto always False when enabled=False."""
        from calibration.meta_labeling_model import train_meta_labeling_model
        from services.meta_labeling_gate import MetaLabelGate

        rows = _make_rows(500)
        state = train_meta_labeling_model(rows, n_blocks=4, embargo_ms=0, min_samples=100)
        assert state is not None
        state["default_threshold"] = 2.0  # force low prob

        rc = MockRc(json.dumps(state))
        gate = MetaLabelGate(rc, enabled=False, model_ttl_sec=0)
        should_veto, _, _ = gate.should_veto({}, regime="ranging")
        assert should_veto is False

    def test_redis_error_fail_open(self):
        """Redis error on model reload → fail-open PASS."""
        from services.meta_labeling_gate import MetaLabelGate

        rc = MockRc()
        rc.raise_on_get()
        gate = MetaLabelGate(rc, enabled=True, model_ttl_sec=0)
        decision, prob, reason = gate.evaluate({}, regime="ranging")
        assert decision == "PASS"

    def test_model_ttl_reload(self):
        """Model reloads after TTL expires."""
        from services.meta_labeling_gate import MetaLabelGate

        rc = MockRc(None)
        gate = MetaLabelGate(rc, enabled=False, model_ttl_sec=0)

        # First call loads (miss)
        gate.evaluate({}, regime="ranging")
        calls_after_1 = rc.get_calls

        # Second call before TTL should use cache
        gate._state_loaded_ms = time.time() * 1000 + 1_000_000  # far future
        gate.evaluate({}, regime="ranging")
        assert rc.get_calls == calls_after_1  # no reload

        # After TTL expires
        gate._state_loaded_ms = 0.0
        gate.evaluate({}, regime="ranging")
        assert rc.get_calls > calls_after_1  # reloaded

    def test_metrics_counter_incremented(self):
        """Metrics counter is called when model is loaded and metrics injected."""
        from calibration.meta_labeling_model import train_meta_labeling_model
        from services.meta_labeling_gate import MetaLabelGate

        rows = _make_rows(500)
        state = train_meta_labeling_model(rows, n_blocks=4, embargo_ms=0, min_samples=100)
        assert state is not None
        state["default_threshold"] = 0.0  # always pass

        called = []

        class FakeCounter:
            def labels(self, **kw):
                called.append(kw)
                return self
            def inc(self): pass

        rc = MockRc(json.dumps(state))
        gate = MetaLabelGate(rc, enabled=False, log_sample=1.0, model_ttl_sec=0)
        gate.register_metrics({"score_total": FakeCounter()})
        gate.evaluate({}, regime="ranging")

        assert len(called) > 0
        assert called[0]["regime"] == "ranging"


class TestGetGateSingleton:

    def test_singleton_returns_same_instance(self):
        from services.meta_labeling_gate import get_gate, reset_gate

        reset_gate()
        g1 = get_gate(MockRc())
        g2 = get_gate()
        assert g1 is g2

    def test_reset_creates_fresh(self):
        from services.meta_labeling_gate import get_gate, reset_gate

        reset_gate()
        g1 = get_gate(MockRc())
        reset_gate()
        g2 = get_gate(MockRc())
        assert g1 is not g2

    def test_no_rc_without_init_raises(self):
        from services.meta_labeling_gate import get_gate, reset_gate

        reset_gate()
        with pytest.raises(RuntimeError):
            get_gate()  # no rc provided on first call
