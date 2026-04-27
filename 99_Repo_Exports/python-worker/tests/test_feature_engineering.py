import math

from core.feature_engineering import (
    RobustScalerPack,
    apply_transform,
    bucketize,
    derive_regime_label,
    derive_session_label,
)


def test_apply_transform_log1p_abs():
    assert math.isclose(apply_transform(0.0, {"type": "log1p"}), 0.0)
    assert apply_transform(9.0, {"type": "log1p"}) > 0
    assert apply_transform(-9.0, {"type": "log1p"}) < 0


def test_apply_transform_clip():
    assert apply_transform(10.0, {"type": "clip", "lo": 0.0, "hi": 5.0}) == 5.0
    assert apply_transform(-1.0, {"type": "clip", "lo": 0.0, "hi": 5.0}) == 0.0


def test_robust_scaler_pack():
    rs = RobustScalerPack(params={"x": {"center": 10.0, "scale": 2.0}})
    assert rs.scale("x", 10.0) == 0.0
    assert rs.scale("x", 12.0) == 1.0


def test_bucketize_edges():
    edges = [2.0, 5.0, 10.0]
    assert bucketize(0.0, edges) == 0
    assert bucketize(2.0, edges) == 0
    assert bucketize(2.1, edges) == 1
    assert bucketize(5.0, edges) == 1
    assert bucketize(9.9, edges) == 2
    assert bucketize(10.0, edges) == 2
    assert bucketize(10.1, edges) == 3


def test_derive_regime_label():
    assert derive_regime_label("low") == "low"
    assert derive_regime_label(None, fallback_score=None) == "unknown"
    assert derive_regime_label(None, fallback_score=0.1, cfg={"regime_thresholds": [0.3, 0.7]}) == "low"
    assert derive_regime_label(None, fallback_score=0.5, cfg={"regime_thresholds": [0.3, 0.7]}) == "mid"
    assert derive_regime_label(None, fallback_score=0.9, cfg={"regime_thresholds": [0.3, 0.7]}) == "high"


def test_derive_session_label_default():
    # 00:00 UTC -> asia by default
    ts = 0
    assert derive_session_label(ts) in {"asia", "eu", "us", "off"}










