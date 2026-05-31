"""Tests for the v14_of route in MLScoringGate.

These tests verify the train↔serve parity contract for the v14_of pack
(`kind="edge_stack_v1"`):

- `_load_model_pack` accepts the new pack shape (gbdt + lr + feature_cols).
- `_extract_features` routes v14_of through a pack-aligned vectorizer.
- `_predict_r` uses `predict_proba()` for the binary classifier path.
- `_calibrate_to_conf01` clamps directly (no sigmoid double-application).
- Vector length equals pack's feature_cols length (catches schema drift).
"""
from __future__ import annotations

from types import SimpleNamespace

import joblib
import pytest

# Avoid module-level Redis/Prom dependencies during import
import services.ml_scoring_gate as msg


class _StubGBDT:
    """Mimics sklearn-API binary classifier for predict_proba."""

    def __init__(self, p1: float = 0.73):
        self.p1 = p1

    def predict_proba(self, x):
        # n_rows × 2  (column 1 = positive class)
        import numpy as np
        n = len(x)
        return np.array([[1 - self.p1, self.p1] for _ in range(n)], dtype=float)


class _StubRegressor:
    """Mimics sklearn-API regression model for legacy v2/v3 pack tests."""

    def predict(self, x):
        import numpy as np
        return np.zeros(len(x), dtype=float)


def _make_v14_pack(feature_cols: list[str], p1: float = 0.73) -> dict:
    return {
        "kind": "edge_stack_v1",
        "gbdt": _StubGBDT(p1=p1),
        "lr": None,
        "meta": None,
        "feature_cols": feature_cols,
        "feature_cols_hash": "abc123",
        "n_features_expected": len(feature_cols),
        "feature_schema_version": "v14_of",
        "feature_schema_ver": "v14_of",
        "schema_name": "v14_of",
        "created_ms": 0,
        "run_id": "edge_stack_v14_of_challenger_test",
        "metrics": {"roc_auc_oof": 0.87},
    }


def _make_gate_with_pack(tmp_path, pack) -> msg.MLScoringGate:
    """Construct an MLScoringGate that has the given pack already loaded."""
    path = str(tmp_path / "scorer_v14_of.joblib")
    joblib.dump(pack, path)
    gate = msg.MLScoringGate(model_path=path)
    # Force-load (bypass mtime throttle)
    loaded = gate._try_load()
    assert loaded, "load should succeed"
    return gate


class TestPackLoading:
    def test_accepts_edge_stack_v1_kind(self, tmp_path):
        pack = _make_v14_pack(["delta_z", "obi_z", "spread_bps"], p1=0.77)
        gate = _make_gate_with_pack(tmp_path, pack)
        assert gate._is_classifier is True
        # joblib roundtrip creates a new object — verify by type + behaviour
        assert isinstance(gate._model, _StubGBDT)
        assert gate._model.p1 == pytest.approx(0.77)
        assert gate._feature_names == ["delta_z", "obi_z", "spread_bps"]
        assert gate._scaler_params == {}  # raw features for v14_of
        assert gate._calibrator is None

    def test_rejects_unknown_kind(self, tmp_path):
        pack = _make_v14_pack(["delta_z"])
        pack["kind"] = "bogus_model"
        path = str(tmp_path / "bad.joblib")
        joblib.dump(pack, path)
        gate = msg.MLScoringGate(model_path=path)
        assert gate._try_load() is False
        assert gate._model is None

    def test_legacy_v3_kind_still_loads(self, tmp_path):
        # Make sure v14_of route doesn't break v2/v3 packs
        legacy_pack = {
            "kind": "ml_scorer_v3",
            "model": _StubRegressor(),
            "feature_names": ["f_atr_14"],
            "robust_scaler_params": {"f_atr_14": {"center": 0.0, "scale": 1.0}},
            "calibrator": None,
            "feature_schema_ver": "v12_of",
        }
        path = str(tmp_path / "legacy.joblib")
        joblib.dump(legacy_pack, path)
        gate = msg.MLScoringGate(model_path=path)
        loaded = gate._try_load()
        assert loaded
        assert gate._is_classifier is False  # legacy uses regression path
        assert gate._feature_names == ["f_atr_14"]
        assert gate._scaler_params == {"f_atr_14": {"center": 0.0, "scale": 1.0}}


class TestClassifierInference:
    def test_predict_uses_predict_proba(self, tmp_path):
        pack = _make_v14_pack(["a", "b"], p1=0.81)
        gate = _make_gate_with_pack(tmp_path, pack)
        out = gate._predict_r([1.0, 2.0])
        assert out == pytest.approx(0.81)

    def test_calibrate_passes_through_classifier_score(self, tmp_path):
        pack = _make_v14_pack(["a"], p1=0.30)
        gate = _make_gate_with_pack(tmp_path, pack)
        # Classifier path: 0.30 → clamp [0.05, 0.98] → 0.30
        assert gate._calibrate_to_conf01(0.30) == pytest.approx(0.30)

    def test_classifier_score_clamped_to_api_range(self, tmp_path):
        pack = _make_v14_pack(["a"])
        gate = _make_gate_with_pack(tmp_path, pack)
        assert gate._calibrate_to_conf01(0.99) == 0.98  # upper clamp
        assert gate._calibrate_to_conf01(0.01) == 0.05  # lower clamp


class TestVectorizationParity:
    def test_vector_length_matches_pack_feature_cols(self, tmp_path):
        cols = ["delta_z", "obi_z", "spread_bps", "atr_bps", "depth_imbalance_5"]
        pack = _make_v14_pack(cols)
        gate = _make_gate_with_pack(tmp_path, pack)

        ctx = SimpleNamespace(symbol="BTCUSDT", indicators={
            "delta_z": 1.5, "obi_z": -0.3, "spread_bps": 0.4,
            "atr_bps": 12.0, "depth_imbalance_5": 0.2,
        })
        vec = gate._extract_features(ctx, side="LONG")
        assert vec is not None
        assert len(vec) == len(cols), "vector len must equal pack feature_cols len"
        assert vec == [1.5, -0.3, 0.4, 12.0, 0.2]

    def test_missing_keys_default_to_zero(self, tmp_path):
        cols = ["present_key", "missing_key", "another_missing"]
        pack = _make_v14_pack(cols)
        gate = _make_gate_with_pack(tmp_path, pack)
        ctx = SimpleNamespace(symbol="BTC", indicators={"present_key": 42.0})
        vec = gate._extract_features(ctx, side="LONG")
        assert vec == [42.0, 0.0, 0.0]

    def test_indicators_via_dict_fallback(self, tmp_path):
        """Ctx without .indicators attribute should fall back to raw dict."""
        pack = _make_v14_pack(["delta_z"])
        gate = _make_gate_with_pack(tmp_path, pack)
        ctx_as_dict = {"symbol": "ETH", "delta_z": 0.7}
        vec = gate._extract_features(ctx_as_dict, side="SHORT")
        assert vec == [0.7]


class TestSchemaRouting:
    def test_v14_of_tag_routes_to_v14_extractor(self, tmp_path):
        cols = ["delta_z", "obi_z"]
        pack = _make_v14_pack(cols)
        # Sanity: feature_schema_ver is read from pack — and routing uses it.
        gate = _make_gate_with_pack(tmp_path, pack)
        assert gate._feature_schema_ver == "v14_of"

        # _extract_features should NOT raise "unsupported schema"
        ctx = SimpleNamespace(symbol="BTC", indicators={"delta_z": 1.0, "obi_z": 2.0})
        vec = gate._extract_features(ctx, side="LONG")
        assert vec == [1.0, 2.0]
