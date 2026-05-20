import json
import math
import tempfile

from core.meta_model_lr import MetaModelLR


def test_meta_model_lr_transforms_and_scaler():
    d = {
        "features": ["x"],
        "intercept": 0.0,
        "coef": [1.0],
        "threshold": 0.5,
        "transforms": {"x": {"type": "clip", "lo": 0.0, "hi": 10.0}},
        "robust_scaler": {"x": {"center": 5.0, "scale": 5.0}},
    }

    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(d, f)
        path = f.name

    m = MetaModelLR.load(path)
    # x=100 -> clip to 10 -> scale => (10-5)/5 = 1
    p = m.predict_proba({"x": 100.0})
    # sigmoid(1) ~ 0.731
    assert math.isclose(p, 1.0 / (1.0 + math.exp(-1.0)), rel_tol=1e-6)


def _make_model_with_intercept(intercept: float, feature: str, coef: float) -> MetaModelLR:
    d = {
        "features": [feature],
        "intercept": intercept,
        "coef": [coef],
        "threshold": 0.5,
    }
    with tempfile.NamedTemporaryFile(mode="w", suffix=".json", delete=False) as f:
        json.dump(d, f)
        path = f.name
    return MetaModelLR.load(path)


def test_meta_model_lr_raw_feature_name_passes_through():
    """Regression: MetaModelLR gate must use indicators directly (not _build_feature_row),
    otherwise raw feature names like 'confidence' fall through to 0.0 in _build_feature_row's
    else-branch, producing sigmoid(intercept) ≈ 0.000 for all signals.
    Verify that a model with a known feature gets a non-trivial prediction when that
    feature is present in the indicators dict."""
    # intercept = -3.0, coef for "confidence" = 5.0
    # with confidence=0.7: sigmoid(-3.0 + 5.0*0.7) = sigmoid(-3.0 + 3.5) = sigmoid(0.5) ≈ 0.622
    m = _make_model_with_intercept(-3.0, "confidence", 5.0)
    p_with = m.predict_proba({"confidence": 0.7})
    p_zero = m.predict_proba({})
    assert p_with > 0.5, f"Expected >0.5 with confidence=0.7, got {p_with}"
    # sigmoid(-3.0) ≈ 0.047 when feature absent
    assert p_zero < 0.1, f"Expected <0.1 with no features, got {p_zero}"
    assert p_with > p_zero * 5, "Model should respond to non-zero feature value"


def test_meta_model_lr_gate_uses_indicators_not_feat_dict():
    """Regression: _decide_meta_lr path must call predict_proba(indicators) directly.
    Simulates the gate bug where feat_dict was built from x_row (all-zero for raw feature
    names in _build_feature_row) instead of indicators."""
    from unittest.mock import MagicMock
    import signal_quality_gating.services.ml_confirm_gate as gate_module

    m = _make_model_with_intercept(-10.0, "confidence", 15.0)

    gate = MagicMock()
    gate._cfg = {"model_path": "/fake/path.json", "kind": "meta_lr", "p_min": 0.55}
    gate._model = m
    gate.mode = "SHADOW"
    gate._p_min_hard_floor = 0.0
    gate._abstain_on_missing = False
    gate._abstain_band = 0.0
    gate._conf_min = 0.0
    gate._calib_type = "none"
    gate._calibrator = None
    gate._fail_allow = lambda: True
    # _build_feature_row returns (x_row, missing); x_row is all-zero for MetaModelLR raw names
    gate._build_feature_row = MagicMock(return_value=([0.0], []))

    captured_args: list = []

    original_predict = m.predict_proba

    def spy_predict(feat):
        captured_args.append(feat)
        return original_predict(feat)

    m.predict_proba = spy_predict  # type: ignore[method-assign]

    indicators = {
        "confidence": 0.75,
        "spread_bps": 2.0,
        "exec_risk_norm": 0.1,
    }

    # Call the gate's _decide_meta_lr (bound method via unbound call)
    dec = gate_module.MLConfirmGate._decide_meta_lr(
        gate,
        symbol="BTCUSDT",
        indicators=indicators,
        direction="LONG",
        scenario="continuation",
        ts_ms=1_700_000_000_000,
    )

    assert len(captured_args) == 1, "predict_proba should be called exactly once"
    passed_feat = captured_args[0]
    # Key assertion: 'confidence' must be in the dict with the real value, not 0.0
    assert "confidence" in passed_feat, "indicators must be passed directly to predict_proba"
    assert passed_feat["confidence"] == 0.75, (
        f"Expected confidence=0.75, got {passed_feat.get('confidence')}. "
        "Likely feat_dict was built from x_row (all-zero for raw feature names)."
    )
    # p_edge should be non-trivial: sigmoid(-10 + 15*0.75) = sigmoid(1.25) ≈ 0.778
    assert dec.p_edge > 0.5, f"Expected p_edge > 0.5, got {dec.p_edge}"
