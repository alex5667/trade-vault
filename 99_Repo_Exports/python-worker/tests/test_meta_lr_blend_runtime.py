"""Runtime tests for meta_lr_blend: loader, decision policy, arbiter guard.

Covers:
  - model_loader._load_model_cached parses valid JSON, rejects bad payloads
  - DecisionPolicy._decide_meta_lr_blend computes sigmoid, ALLOW/BLOCK,
    bad_model_type, ABSTAIN_MISSING_CRITICAL
  - schema_arbiter_v1._cfg_is_runtime_supported gates promotion
"""
from __future__ import annotations

import json
import math
import os
from pathlib import Path
from types import SimpleNamespace

import pytest


# ─────────────────────────────────────────────────────────────────────────────
# model_loader
# ─────────────────────────────────────────────────────────────────────────────

def _write_blend_artifact(tmp_path: Path, *, intercept=-1.0, c14=0.2, c5=7.5,
                          kind="meta_lr_blend", feature_names=None,
                          drop_key: str | None = None) -> Path:
    pack = {
        "kind": kind,
        "schema_version": 1,
        "intercept": intercept,
        "coef_v14": c14,
        "coef_v5": c5,
        "feature_names": feature_names if feature_names is not None else ["p_v14", "p_v5"],
        "metrics": {"auc_meta": 0.82},
        "run_id": "test_blend",
    }
    if drop_key is not None and drop_key in pack:
        del pack[drop_key]
    p = tmp_path / "meta_lr_blend.json"
    p.write_text(json.dumps(pack))
    return p


def test_loader_accepts_valid_meta_lr_blend(tmp_path):
    from services.ml_confirm_gate.model_loader import _load_model_cached
    path = _write_blend_artifact(tmp_path)
    model = _load_model_cached(str(path), "meta_lr_blend", logger=None)
    assert isinstance(model, dict)
    assert model["kind"] == "meta_lr_blend"
    assert model["feature_names"] == ["p_v14", "p_v5"]
    assert model["intercept"] == -1.0


def test_loader_rejects_wrong_kind_in_artifact(tmp_path):
    from services.ml_confirm_gate.model_loader import _load_model_cached
    path = _write_blend_artifact(tmp_path, kind="not_blend")
    assert _load_model_cached(str(path), "meta_lr_blend", logger=None) is None


def test_loader_rejects_missing_intercept(tmp_path):
    from services.ml_confirm_gate.model_loader import _load_model_cached
    path = _write_blend_artifact(tmp_path, drop_key="intercept")
    assert _load_model_cached(str(path), "meta_lr_blend", logger=None) is None


def test_loader_normalizes_invalid_feature_names(tmp_path):
    from services.ml_confirm_gate.model_loader import _load_model_cached
    path = _write_blend_artifact(tmp_path, feature_names=["only_one"])
    model = _load_model_cached(str(path), "meta_lr_blend", logger=None)
    assert model is not None
    assert model["feature_names"] == ["p_v14", "p_v5"]


def test_loader_handles_bad_json(tmp_path):
    from services.ml_confirm_gate.model_loader import _load_model_cached
    p = tmp_path / "bad.json"
    p.write_text("not a json {{{")
    assert _load_model_cached(str(p), "meta_lr_blend", logger=None) is None


# ─────────────────────────────────────────────────────────────────────────────
# DecisionPolicy._decide_meta_lr_blend
# ─────────────────────────────────────────────────────────────────────────────

def _make_gate(**overrides):
    """Stub gate with the minimum surface DecisionPolicy._decide_meta_lr_blend reads."""
    base = dict(
        mode="SHADOW",
        fail_policy="OPEN",
        _cfg={"p_min": 0.5},
        _model=None,
        _model_load_error=None,
        _abstain_on_missing=True,
        _p_min_hard_floor=0.0,
        _calibrator=None,
        _calib_type=None,
        _forbid_scenario_v4_onehot=False,
    )
    base.update(overrides)
    return SimpleNamespace(**base)


def _blend_pack(intercept=-1.0, c14=0.2, c5=7.5,
                feature_names=("p_v14", "p_v5")) -> dict:
    return {
        "kind": "meta_lr_blend",
        "intercept": intercept,
        "coef_v14": c14,
        "coef_v5": c5,
        "feature_names": list(feature_names),
    }


def _new_dec():
    from services.ml_confirm_gate.dto import MLConfirmDecision
    return MLConfirmDecision()


def test_decide_blend_allows_when_p_above_floor():
    from services.ml_confirm_gate.decision_policy import DecisionPolicy
    pack = _blend_pack(intercept=0.0, c14=0.0, c5=10.0)
    # z = 0 + 0 + 10*0.9 = 9 → sigmoid ≈ 0.9999
    gate = _make_gate(_cfg={"p_min": 0.5}, _model=pack)
    pol = DecisionPolicy(gate)
    dec = pol._decide_meta_lr_blend(
        _new_dec(),
        symbol="BTCUSDT", ts_ms=0, direction="LONG", scenario="trend_up",
        indicators={"p_v14": 0.1, "p_v5": 0.9},
        cfg=gate._cfg, model=pack,
    )
    assert dec.allow is True
    assert dec.status == "ALLOW"
    assert dec.kind == "meta_lr_blend"
    assert dec.p_edge == pytest.approx(1.0 / (1.0 + math.exp(-9.0)), rel=1e-6)
    assert dec.p_min == 0.5
    assert dec.error == ""


def test_decide_blend_blocks_when_p_below_floor():
    from services.ml_confirm_gate.decision_policy import DecisionPolicy
    # z = -5 + 0 + 0 → sigmoid ≈ 0.0067
    pack = _blend_pack(intercept=-5.0, c14=0.0, c5=0.0)
    gate = _make_gate(_cfg={"p_min": 0.5}, _model=pack)
    pol = DecisionPolicy(gate)
    dec = pol._decide_meta_lr_blend(
        _new_dec(),
        symbol="BTCUSDT", ts_ms=0, direction="LONG", scenario="trend_up",
        indicators={"p_v14": 0.5, "p_v5": 0.5},
        cfg=gate._cfg, model=pack,
    )
    assert dec.allow is False
    assert dec.status == "BLOCK"
    assert dec.p_edge < 0.5


def test_decide_blend_rejects_bad_model_type():
    from services.ml_confirm_gate.decision_policy import DecisionPolicy
    # MetaModelLR-shaped object would NOT be a dict with kind=meta_lr_blend
    gate = _make_gate(_cfg={"p_min": 0.5}, _model={"kind": "meta_lr"})
    pol = DecisionPolicy(gate)
    dec = pol._decide_meta_lr_blend(
        _new_dec(),
        symbol="X", ts_ms=0, direction="LONG", scenario="trend_up",
        indicators={"p_v14": 0.5, "p_v5": 0.5},
        cfg=gate._cfg, model={"kind": "meta_lr"},
    )
    assert dec.error == "bad_model_type"
    assert dec.status == "ERR_BAD_MODEL"


def test_decide_blend_no_model_loaded():
    from services.ml_confirm_gate.decision_policy import DecisionPolicy
    gate = _make_gate(_cfg={"p_min": 0.5}, _model=None,
                      _model_load_error="missing_artifact_file")
    pol = DecisionPolicy(gate)
    dec = pol._decide_meta_lr_blend(
        _new_dec(),
        symbol="X", ts_ms=0, direction="LONG", scenario="trend_up",
        indicators={"p_v14": 0.5, "p_v5": 0.5},
        cfg=gate._cfg, model=None,
    )
    assert dec.error == "missing_artifact_file"
    assert dec.status == "ERR_NO_MODEL"


def test_decide_blend_abstain_on_missing_in_enforce():
    from services.ml_confirm_gate.decision_policy import DecisionPolicy
    pack = _blend_pack()
    gate = _make_gate(mode="ENFORCE", _cfg={"p_min": 0.5}, _model=pack,
                      _abstain_on_missing=True)
    pol = DecisionPolicy(gate)
    dec = pol._decide_meta_lr_blend(
        _new_dec(),
        symbol="X", ts_ms=0, direction="LONG", scenario="trend_up",
        indicators={},
        effective_mode="ENFORCE",
        cfg=gate._cfg, model=pack,
    )
    assert dec.abstain is True
    assert dec.allow is True
    assert dec.status == "ABSTAIN_MISSING_CRITICAL"
    assert set(dec.missing) == {"p_v14", "p_v5"}


def test_decide_blend_shadow_mode_computes_with_missing_inputs():
    """In SHADOW, missing inputs are recorded but inference still runs (zeros)."""
    from services.ml_confirm_gate.decision_policy import DecisionPolicy
    pack = _blend_pack(intercept=-1.0, c14=0.2, c5=7.5)
    gate = _make_gate(mode="SHADOW", _cfg={"p_min": 0.5}, _model=pack)
    pol = DecisionPolicy(gate)
    dec = pol._decide_meta_lr_blend(
        _new_dec(),
        symbol="X", ts_ms=0, direction="LONG", scenario="trend_up",
        indicators={},
        effective_mode="SHADOW",
        cfg=gate._cfg, model=pack,
    )
    # z = -1 → sigmoid ≈ 0.269 < 0.5 → BLOCK but no error
    assert dec.error == ""
    assert dec.status == "BLOCK"
    assert dec.allow is False
    assert set(dec.missing) == {"p_v14", "p_v5"}


# ─────────────────────────────────────────────────────────────────────────────
# schema_arbiter_v1 guard
# ─────────────────────────────────────────────────────────────────────────────

def test_arbiter_guard_accepts_known_kind_with_existing_path(tmp_path):
    from tools.schema_arbiter_v1 import _cfg_is_runtime_supported
    art = tmp_path / "model.json"
    art.write_text("{}")
    cfg = {"kind": "meta_lr_blend", "model_path": str(art)}
    ok, reason = _cfg_is_runtime_supported(cfg)
    assert ok is True, reason
    assert reason == ""


def test_arbiter_guard_blocks_unknown_kind(tmp_path):
    from tools.schema_arbiter_v1 import _cfg_is_runtime_supported
    art = tmp_path / "model.bin"
    art.write_text("x")
    cfg = {"kind": "future_blend_v9", "model_path": str(art)}
    ok, reason = _cfg_is_runtime_supported(cfg)
    assert ok is False
    assert reason.startswith("unsupported_kind:")


def test_arbiter_guard_blocks_missing_kind(tmp_path):
    from tools.schema_arbiter_v1 import _cfg_is_runtime_supported
    art = tmp_path / "model.bin"
    art.write_text("x")
    ok, reason = _cfg_is_runtime_supported({"model_path": str(art)})
    assert ok is False
    assert reason == "missing_kind"


def test_arbiter_guard_blocks_missing_model_path():
    from tools.schema_arbiter_v1 import _cfg_is_runtime_supported
    ok, reason = _cfg_is_runtime_supported({"kind": "meta_lr_blend"})
    assert ok is False
    assert reason == "missing_model_path"


def test_arbiter_guard_blocks_non_existent_model_path():
    from tools.schema_arbiter_v1 import _cfg_is_runtime_supported
    ok, reason = _cfg_is_runtime_supported(
        {"kind": "meta_lr_blend", "model_path": "/no/such/file.json"}
    )
    assert ok is False
    assert reason.startswith("model_path_missing:")


def test_arbiter_guard_rejects_non_dict():
    from tools.schema_arbiter_v1 import _cfg_is_runtime_supported
    ok, reason = _cfg_is_runtime_supported("not a dict")  # type: ignore[arg-type]
    assert ok is False
    assert reason == "cfg_not_dict"


# ─────────────────────────────────────────────────────────────────────────────
# Self-contained blend: trainer artifact + loader child inference
# ─────────────────────────────────────────────────────────────────────────────

def _make_blend_artifact_with_children(tmp_path: Path):
    """Train a tiny logistic regression + GBDT pair, save as child models,
    return artifact dict + path mirroring nightly_v_meta_train_bundle.py output.
    """
    import numpy as np
    import joblib
    from sklearn.ensemble import GradientBoostingClassifier
    from sklearn.linear_model import LogisticRegression
    from sklearn.preprocessing import StandardScaler

    rng = np.random.default_rng(42)
    n = 200
    X14 = rng.normal(size=(n, 4))
    X5 = rng.normal(size=(n, 3))
    # Label: linear combination of v14 features → v14 will learn signal,
    # v5 mostly noise.
    logits = 0.8 * X14[:, 0] + 0.5 * X14[:, 1] - 0.3 * X14[:, 2]
    p = 1.0 / (1.0 + np.exp(-logits))
    y = (rng.uniform(size=n) < p).astype(np.int64)

    sc14 = StandardScaler().fit(X14)
    lr14 = LogisticRegression(C=1.0, max_iter=500, random_state=42, solver="liblinear")
    lr14.fit(sc14.transform(X14), y)
    gb5 = GradientBoostingClassifier(max_depth=2, n_estimators=20, random_state=42)
    gb5.fit(X5, y)

    out = tmp_path / "blend_v2"
    out.mkdir()
    lr_path = out / "lr_v14.joblib"
    sc_path = out / "scaler_v14.joblib"
    gb_path = out / "gb_v5.joblib"
    joblib.dump(lr14, lr_path)
    joblib.dump(sc14, sc_path)
    joblib.dump(gb5, gb_path)

    v14_features = ["f14_a", "f14_b", "f14_c", "f14_d"]
    v5_features = ["f5_a", "f5_b", "f5_c"]

    art = {
        "kind": "meta_lr_blend",
        "schema_version": 2,
        "intercept": -0.5,
        "coef_v14": 4.0,
        "coef_v5": 1.0,
        "feature_names": ["p_v14", "p_v5"],
        "metrics": {"auc_meta": 0.8},
        "child_models": {
            "v14": {
                "model_path": str(lr_path),
                "scaler_path": str(sc_path),
                "features": v14_features,
            },
            "v5": {
                "model_path": str(gb_path),
                "scaler_path": None,
                "features": v5_features,
            },
        },
    }
    art_path = out / "meta_lr_blend.json"
    art_path.write_text(json.dumps(art))
    return art, art_path, v14_features, v5_features


def test_loader_loads_child_models_from_pack(tmp_path):
    from services.ml_confirm_gate.model_loader import _load_model_cached
    _, art_path, _, _ = _make_blend_artifact_with_children(tmp_path)
    model = _load_model_cached(str(art_path), "meta_lr_blend", logger=None)
    assert model is not None
    assert "_v14_model" in model
    assert "_v5_model" in model
    assert hasattr(model["_v14_model"], "predict_proba")
    assert hasattr(model["_v5_model"], "predict_proba")
    assert model["_v14_scaler"] is not None
    assert len(model["_v14_features"]) == 4
    assert len(model["_v5_features"]) == 3


def test_loader_legacy_artifact_without_child_models(tmp_path):
    """schema_version=1 artifact (current production) loads without child models."""
    from services.ml_confirm_gate.model_loader import _load_model_cached
    p = tmp_path / "legacy.json"
    p.write_text(json.dumps({
        "kind": "meta_lr_blend",
        "schema_version": 1,
        "intercept": -1.0,
        "coef_v14": 0.2,
        "coef_v5": 7.5,
    }))
    model = _load_model_cached(str(p), "meta_lr_blend", logger=None)
    assert model is not None
    assert "_v14_model" not in model
    assert "_v5_model" not in model


def test_loader_handles_missing_child_paths(tmp_path):
    """child_models present but paths point nowhere — load proceeds, no _v14_model set."""
    from services.ml_confirm_gate.model_loader import _load_model_cached
    p = tmp_path / "broken_children.json"
    p.write_text(json.dumps({
        "kind": "meta_lr_blend",
        "schema_version": 2,
        "intercept": -1.0,
        "coef_v14": 0.2,
        "coef_v5": 7.5,
        "child_models": {
            "v14": {"model_path": "/nope/lr.joblib", "scaler_path": "/nope/sc.joblib", "features": ["a"]},
            "v5":  {"model_path": "/nope/gb.joblib", "features": ["b"]},
        },
    }))
    model = _load_model_cached(str(p), "meta_lr_blend", logger=None)
    assert model is not None
    assert "_v14_model" not in model
    assert "_v5_model" not in model


def test_decide_uses_child_models_when_present(tmp_path):
    """When pack carries child models, p_v14/p_v5 are computed inline (src=child)."""
    from services.ml_confirm_gate.model_loader import _load_model_cached
    from services.ml_confirm_gate.decision_policy import DecisionPolicy
    _, art_path, v14_features, v5_features = _make_blend_artifact_with_children(tmp_path)
    pack = _load_model_cached(str(art_path), "meta_lr_blend", logger=None)
    assert pack is not None and "_v14_model" in pack

    gate = _make_gate(_cfg={"p_min": 0.5}, _model=pack)
    pol = DecisionPolicy(gate)
    # Provide strong positive signal on v14_features → p_v14 should be high
    indicators = {k: 2.0 for k in v14_features}
    indicators.update({k: 0.0 for k in v5_features})
    dec = pol._decide_meta_lr_blend(
        _new_dec(),
        symbol="BTCUSDT", ts_ms=0, direction="LONG", scenario="trend_up",
        indicators=indicators,
        cfg=gate._cfg, model=pack,
    )
    assert dec.error == ""
    assert "src=child" in dec.reason
    # No abstain — children produced the inputs, so feat_names are no longer missing
    assert dec.missing == []


def test_decide_falls_back_to_indicators_without_children(tmp_path):
    """Legacy artifact without child models still reads p_v14/p_v5 from indicators (src=ind)."""
    from services.ml_confirm_gate.decision_policy import DecisionPolicy
    pack = _blend_pack(intercept=0.0, c14=0.0, c5=10.0)  # no _v14_model / _v5_model
    gate = _make_gate(_cfg={"p_min": 0.5}, _model=pack)
    pol = DecisionPolicy(gate)
    dec = pol._decide_meta_lr_blend(
        _new_dec(),
        symbol="BTCUSDT", ts_ms=0, direction="LONG", scenario="trend_up",
        indicators={"p_v14": 0.1, "p_v5": 0.9},
        cfg=gate._cfg, model=pack,
    )
    assert dec.error == ""
    assert "src=ind" in dec.reason


def test_decide_child_prediction_failure_returns_err():
    """If child predict_proba blows up, surface ERR_PRED."""
    from services.ml_confirm_gate.decision_policy import DecisionPolicy

    class _BoomModel:
        def predict_proba(self, X):
            raise RuntimeError("kaboom")

    pack = _blend_pack(intercept=0.0, c14=0.0, c5=10.0)
    pack["_v14_model"] = _BoomModel()
    pack["_v14_features"] = ["a"]
    pack["_v14_scaler"] = None
    pack["_v5_model"] = _BoomModel()
    pack["_v5_features"] = ["b"]

    gate = _make_gate(_cfg={"p_min": 0.5}, _model=pack)
    pol = DecisionPolicy(gate)
    dec = pol._decide_meta_lr_blend(
        _new_dec(),
        symbol="X", ts_ms=0, direction="LONG", scenario="trend_up",
        indicators={"a": 1.0, "b": 1.0},
        cfg=gate._cfg, model=pack,
    )
    assert dec.error == "child_prediction_failed"
    assert dec.status == "ERR_PRED"


def test_predict_blend_children_helper_returns_floats(tmp_path):
    """Direct exercise of the helper used by the decide branch."""
    from services.ml_confirm_gate.model_loader import _load_model_cached
    from services.ml_confirm_gate.decision_policy import _predict_blend_children

    _, art_path, v14_features, v5_features = _make_blend_artifact_with_children(tmp_path)
    pack = _load_model_cached(str(art_path), "meta_lr_blend", logger=None)
    assert pack is not None

    indicators = {k: 0.5 for k in v14_features + v5_features}
    p14, p5 = _predict_blend_children(pack, indicators)
    assert 0.0 <= p14 <= 1.0
    assert 0.0 <= p5 <= 1.0


def test_trainer_artifact_has_child_models(tmp_path):
    """nightly_v_meta_train_bundle.train_final_children produces fitted child models."""
    import numpy as np
    from tools.nightly_v_meta_train_bundle import train_final_children

    rng = np.random.default_rng(0)
    n = 60
    X14 = rng.normal(size=(n, len([
        "delta_z", "ofi_z", "ofi_stability_score", "spread_bps", "expected_slippage_bps",
    ])))
    X5 = rng.normal(size=(n, 4))
    y = (rng.uniform(size=n) < 0.5).astype(np.int64)
    children = train_final_children(X14, X5, y)
    assert "lr_v14" in children and "scaler_v14" in children and "gb_v5" in children
    assert hasattr(children["lr_v14"], "predict_proba")
    assert hasattr(children["gb_v5"], "predict_proba")
    # smoke predict
    x14_one = children["scaler_v14"].transform(X14[:1])
    assert 0.0 <= float(children["lr_v14"].predict_proba(x14_one)[0, 1]) <= 1.0
    assert 0.0 <= float(children["gb_v5"].predict_proba(X5[:1])[0, 1]) <= 1.0
