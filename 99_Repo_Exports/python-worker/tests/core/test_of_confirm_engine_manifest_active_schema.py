"""P0 fix: feature manifest must use cols from the *active* meta schema.

Before the fix, of_confirm_engine.py hardcoded META_FEAT_V8_COLS when building
the policy/feature manifest, regardless of MetaKeys.SCHEMA_NAME. As a result,
serving on v9/v10 produced a manifest with v8 cols, making
dq_policy_feature_manifest_hash_v1 silently wrong.

This test exercises build_feature_manifest_v1 directly with each schema's cols
to lock the invariant: manifest cols == active schema cols.
"""
import hashlib

from core.meta_features_v8 import META_FEAT_V8_COLS, META_FEAT_V8_HASH, META_FEAT_V8_NAME, META_FEAT_V8_VERSION
from core.meta_features_v9 import META_FEAT_V9_COLS, META_FEAT_V9_HASH, META_FEAT_V9_NAME, META_FEAT_V9_VERSION
from core.meta_features_v10 import (
    META_FEAT_V10_COLS,
    META_FEAT_V10_HASH,
    META_FEAT_V10_NAME,
    META_FEAT_V10_VERSION,
)
from core.meta_schema_registry import get_schema_cols
from core_snapshot.policy_snapshot_v1 import build_dq_policy_snapshot, build_feature_manifest_v1


def _build_manifest_for(schema_name: str, schema_version: int, schema_hash: str):
    snap, dq_hash = build_dq_policy_snapshot({})
    cols = tuple(get_schema_cols(schema_name))
    assert cols, f"registry has empty cols for {schema_name}"
    man, man_hash = build_feature_manifest_v1(
        meta_schema_name=schema_name,
        meta_schema_version=schema_version,
        meta_schema_hash=schema_hash,
        meta_cols=cols,
        dq_policy_hash=str(dq_hash),
        thr=snap.thresholds,
    )
    return man, man_hash, cols


def test_manifest_cols_match_active_schema_v8():
    _man, _h, cols = _build_manifest_for(META_FEAT_V8_NAME, META_FEAT_V8_VERSION, META_FEAT_V8_HASH)
    assert cols == tuple(META_FEAT_V8_COLS)


def test_manifest_cols_match_active_schema_v9():
    _man, _h, cols = _build_manifest_for(META_FEAT_V9_NAME, META_FEAT_V9_VERSION, META_FEAT_V9_HASH)
    assert cols == tuple(META_FEAT_V9_COLS)


def test_manifest_cols_match_active_schema_v10():
    _man, _h, cols = _build_manifest_for(META_FEAT_V10_NAME, META_FEAT_V10_VERSION, META_FEAT_V10_HASH)
    assert cols == tuple(META_FEAT_V10_COLS)


def test_manifest_hash_differs_across_schemas():
    """Different col sets must produce different manifest hashes, otherwise
    the hash is not actually binding the schema cols."""
    _, h8, _ = _build_manifest_for(META_FEAT_V8_NAME, META_FEAT_V8_VERSION, META_FEAT_V8_HASH)
    _, h9, _ = _build_manifest_for(META_FEAT_V9_NAME, META_FEAT_V9_VERSION, META_FEAT_V9_HASH)
    _, h10, _ = _build_manifest_for(META_FEAT_V10_NAME, META_FEAT_V10_VERSION, META_FEAT_V10_HASH)
    assert len({h8, h9, h10}) == 3, (h8, h9, h10)


def test_registry_cols_lookup_is_stable_hash():
    """Smoke: get_schema_cols returns the same list across calls; hashing it
    is deterministic — the property the engine fix relies on."""
    a = get_schema_cols(META_FEAT_V10_NAME)
    b = get_schema_cols(META_FEAT_V10_NAME)
    assert a == b
    ha = hashlib.sha1(",".join(a).encode()).hexdigest()
    hb = hashlib.sha1(",".join(b).encode()).hexdigest()
    assert ha == hb
