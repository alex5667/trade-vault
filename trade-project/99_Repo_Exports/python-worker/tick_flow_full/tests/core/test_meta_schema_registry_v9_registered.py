"""Tests: meta_feat_v9 is registered in META_SCHEMA_REGISTRY.

Ensures that after C1 wiring patch, get_schema_info() returns the canonical
(version, cols, hash) triple and the entry is present in the registry dict.
Guards against accidental removal of v9 registration.
"""
from core.meta_features_v9 import (
    META_FEAT_V9_COLS,
    META_FEAT_V9_HASH,
    META_FEAT_V9_NAME,
    META_FEAT_V9_VERSION,
)
from core.meta_schema_registry import META_SCHEMA_REGISTRY, get_schema_info


def test_meta_schema_registry_registers_v9():
    """v9 must be present in the registry dict."""
    assert META_FEAT_V9_NAME in META_SCHEMA_REGISTRY, (
        f"meta_feat_v9 not found in META_SCHEMA_REGISTRY: {list(META_SCHEMA_REGISTRY.keys())}"
    )


def test_meta_schema_registry_v9_version_cols_hash():
    """get_schema_info() must return the canonical (version, cols, hash) for v9."""
    vers, cols, h = get_schema_info(META_FEAT_V9_NAME)
    assert int(vers) == int(META_FEAT_V9_VERSION), (
        f"Version mismatch: registry={vers}, expected={META_FEAT_V9_VERSION}"
    )
    assert str(h) == str(META_FEAT_V9_HASH), (
        f"Hash mismatch: registry={h}, expected={META_FEAT_V9_HASH}"
    )
    assert list(cols) == list(META_FEAT_V9_COLS), (
        f"Cols mismatch: registry ncols={len(cols)}, expected ncols={len(META_FEAT_V9_COLS)}"
    )
