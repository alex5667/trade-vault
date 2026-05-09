"""Unit tests for meta_feat_v8 inventory (P1)."""

from core.meta_features_v8 import (
    META_FEAT_V8_COLS,
    META_FEAT_V8_NAME,
    META_FEAT_V8_NEW_COLS,
    META_FEAT_V8_VERSION,
    build_meta_features_v8,
)
from core.meta_schema_registry import get_schema_info


def test_meta_schema_registry_has_v8():
    ver, cols, h = get_schema_info(META_FEAT_V8_NAME)
    assert ver == META_FEAT_V8_VERSION
    assert cols == list(META_FEAT_V8_COLS)
    assert isinstance(h, str) and len(h) > 0

def test_build_meta_features_v8_missing_filled():
    feat, missing = build_meta_features_v8(evidence={}, indicators={})
    for k in META_FEAT_V8_COLS:
        assert k in feat
        assert isinstance(feat[k], float)
    for k in META_FEAT_V8_NEW_COLS:
        assert k in missing
        assert feat[k] == 0.0

def test_build_meta_features_v8_type_coercion_and_priority():
    evidence = {
        "obi_z": "2.5",
        "absorption_volume": 1234,
        "cvd_quarantine_active": True,
        "indicators": {"ofi_z": "3.0", "iceberg_dist_bp": "10.0"},
    }
    indicators = {"obi_z": 999, "book_churn_hi": 1}
    feat, missing = build_meta_features_v8(evidence=evidence, indicators=indicators)
    assert feat["obi_z"] == 2.5
    assert feat["absorption_volume"] == 1234.0
    assert feat["cvd_quarantine_active"] == 1.0
    assert feat["ofi_z"] == 3.0
    assert feat["iceberg_dist_bp"] == 10.0
    assert feat["book_churn_hi"] == 1.0
    for k in ("obi_z", "absorption_volume", "cvd_quarantine_active", "ofi_z", "iceberg_dist_bp", "book_churn_hi"):
        assert k not in missing
