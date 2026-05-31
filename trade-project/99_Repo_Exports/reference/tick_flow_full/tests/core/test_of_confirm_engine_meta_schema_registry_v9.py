"""Tests: meta_feat_v9 is wired into OFConfirmEngine runtime.

C1 wiring guard: ensures both the code-side META_SCHEMA_REGISTRY and the
builder map (SCHEMAS) inside OFConfirmEngine.build() have v9 entries.
Static source inspection prevents silent removal of the wiring.
"""
import inspect

import core.of_confirm_engine as ofe
from core.meta_features_v9 import (
    META_FEAT_V9_NAME,
    META_FEAT_V9_VERSION,
    META_FEAT_V9_HASH,
)


def test_of_confirm_engine_meta_schema_registry_has_v9():
    """Code-side META_SCHEMA_REGISTRY in of_confirm_engine must contain v9."""
    assert META_FEAT_V9_NAME in ofe.META_SCHEMA_REGISTRY, (
        f"v9 not in ofe.META_SCHEMA_REGISTRY: {list(ofe.META_SCHEMA_REGISTRY.keys())}"
    )
    vers, h = ofe.META_SCHEMA_REGISTRY[META_FEAT_V9_NAME]
    assert int(vers) == int(META_FEAT_V9_VERSION), (
        f"Version mismatch: ofe registry={vers}, expected={META_FEAT_V9_VERSION}"
    )
    assert str(h) == str(META_FEAT_V9_HASH), (
        f"Hash mismatch: ofe registry={h}, expected={META_FEAT_V9_HASH}"
    )


def test_of_confirm_engine_meta_schema_v2p_contains_v9():
    """META_SCHEMA_V2P must include v9 to keep the 'modern schemas' list consistent."""
    assert META_FEAT_V9_NAME in ofe.META_SCHEMA_V2P, (
        f"v9 not in META_SCHEMA_V2P: {ofe.META_SCHEMA_V2P}"
    )


def test_of_confirm_engine_source_has_v9_builder():
    """Static source guard: build_meta_features_v9 and META_FEAT_V9_NAME must
    appear in of_confirm_engine source (prevents accidental removal of wiring)."""
    src = inspect.getsource(ofe)
    assert "build_meta_features_v9" in src, (
        "build_meta_features_v9 not found in of_confirm_engine source — v9 builder wiring removed!"
    )
    assert "META_FEAT_V9_NAME" in src, (
        "META_FEAT_V9_NAME not found in of_confirm_engine source — v9 wiring removed!"
    )
