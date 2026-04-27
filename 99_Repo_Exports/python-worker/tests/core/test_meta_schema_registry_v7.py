"""Registry contract tests for meta_feat_v7.

Ensures that:
  - META_SCHEMA_REGISTRY in of_confirm_engine contains meta_feat_v7
  - The registered (version, hash) tuple exactly matches the code-side values
  - META_SCHEMA_V2P includes meta_feat_v7 (so it is treated as a valid schema for meta-model gating)
"""
import pytest

from core.meta_features_v7 import (
    META_FEAT_V7_HASH,
    META_FEAT_V7_NAME,
    META_FEAT_V7_VERSION,
)
from core.of_confirm_engine import META_SCHEMA_REGISTRY, META_SCHEMA_V2P


def test_meta_schema_registry_includes_v7():
    """v7 must be registered with correct version and hash."""
    assert META_FEAT_V7_NAME in META_SCHEMA_REGISTRY, (
        f"{META_FEAT_V7_NAME!r} not found in META_SCHEMA_REGISTRY"
    )
    registered_version, registered_hash = META_SCHEMA_REGISTRY[META_FEAT_V7_NAME]
    assert registered_version == META_FEAT_V7_VERSION, (
        f"version mismatch: registry={registered_version}, code={META_FEAT_V7_VERSION}"
    )
    assert registered_hash == META_FEAT_V7_HASH, (
        f"hash mismatch: registry={registered_hash!r}, code={META_FEAT_V7_HASH!r}"
    )


def test_meta_schema_v2p_includes_v7():
    """v7 must be in META_SCHEMA_V2P so the engine treats it as a valid schema."""
    assert META_FEAT_V7_NAME in META_SCHEMA_V2P, (
        f"{META_FEAT_V7_NAME!r} not in META_SCHEMA_V2P"
    )
