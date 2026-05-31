"""P0 fixes: central registry now exposes v10 + builder lookup."""
from core.meta_features_v10 import (
    META_FEAT_V10_COLS,
    META_FEAT_V10_HASH,
    META_FEAT_V10_NAME,
    META_FEAT_V10_VERSION,
    build_meta_features_v10,
)
from core.meta_schema_registry import (
    META_SCHEMA_BUILDERS,
    META_SCHEMA_REGISTRY,
    get_schema_builder,
    get_schema_cols,
    get_schema_info,
)


def test_central_registry_contains_v10():
    ver, cols, h = get_schema_info(META_FEAT_V10_NAME)
    assert ver == META_FEAT_V10_VERSION
    assert cols == list(META_FEAT_V10_COLS)
    assert h == META_FEAT_V10_HASH


def test_get_schema_cols_matches_registry_for_all_versions():
    for name, spec in META_SCHEMA_REGISTRY.items():
        assert get_schema_cols(name) == list(spec.cols), f"cols mismatch for {name}"


def test_get_schema_cols_unknown_returns_empty():
    assert get_schema_cols("meta_feat_does_not_exist") == []


def test_every_registered_schema_has_a_builder():
    for name in META_SCHEMA_REGISTRY:
        b = get_schema_builder(name)
        assert b is not None, f"no builder for {name}"
        assert callable(b)
    # And the builder map mirrors the registry exactly.
    assert set(META_SCHEMA_BUILDERS.keys()) == set(META_SCHEMA_REGISTRY.keys())


def test_v10_builder_callable_returns_v10_cols():
    feat, _missing = build_meta_features_v10(evidence={}, indicators={})
    for k in META_FEAT_V10_COLS:
        assert k in feat
