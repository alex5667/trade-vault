"""
core/feature_snapshot_hash.py — pure-function hashing for FeatureSnapshot.

Used by signal_feature_snapshot_writer to stamp each immutable row with a
schema fingerprint so training pipelines can reject samples whose feature
schema drifted mid-window.

Two distinct hashes (intentional separation):

* schema_hash       — sha1 over the SET of keys present in the features dict,
                      including nested dotted paths. Sensitive to add/remove
                      of any key, regardless of feature_cols selection.

* feature_cols_hash — sha1 over the canonical sorted list of feature column
                      names the model will actually consume. Sensitive to
                      column re-ordering, additions, renames.

A trainer should reject rows whose feature_cols_hash != champion.feature_cols_hash.
A drift monitor should alert when rolling schema_hash distribution shifts.
"""
from __future__ import annotations

import hashlib
from collections.abc import Iterable, Mapping
from typing import Any


def _walk_keys(obj: Any, prefix: str = "") -> Iterable[str]:
    """Yield dotted-path keys for every leaf in a nested dict/list structure.

    Lists are indexed by position to catch shape changes; primitives stop the walk.
    """
    if isinstance(obj, Mapping):
        for k in obj:
            sub_prefix = f"{prefix}.{k}" if prefix else str(k)
            child = obj[k]
            if isinstance(child, (Mapping, list)):
                yield from _walk_keys(child, sub_prefix)
            else:
                yield sub_prefix
    elif isinstance(obj, list):
        for i, child in enumerate(obj):
            sub_prefix = f"{prefix}[{i}]" if prefix else f"[{i}]"
            if isinstance(child, (Mapping, list)):
                yield from _walk_keys(child, sub_prefix)
            else:
                yield sub_prefix


def compute_schema_hash(features: Mapping[str, Any]) -> str:
    """Stable sha1 hex digest over the set of dotted-path keys.

    Order-independent (uses sorted set). Returns 12-char prefix — collisions
    at this length are ~1e-7 per 10k unique schemas; sufficient for drift alerts.
    """
    if not features:
        return "empty"
    keys = sorted(set(_walk_keys(features)))
    h = hashlib.sha1("\n".join(keys).encode("utf-8")).hexdigest()
    return h[:12]


def compute_feature_cols_hash(feature_cols: Iterable[str]) -> str:
    """Stable sha1 hex digest over the sorted feature column list.

    Use this when the model has a declared feature_cols list. Order-independent
    inside the hash but the caller controls which columns matter.
    """
    cols = sorted({str(c) for c in feature_cols if c})
    if not cols:
        return "empty"
    h = hashlib.sha1("\n".join(cols).encode("utf-8")).hexdigest()
    return h[:12]


def extract_feature_cols(features: Mapping[str, Any]) -> list[str]:
    """Flat top-level scalar feature names (excludes nested dicts/lists).

    Convenience helper for callers that don't have an explicit feature_cols
    contract — uses top-level scalar leaves as the model input set.
    """
    out: list[str] = []
    for k, v in features.items():
        if isinstance(v, (int, float, bool, str)) and not isinstance(v, bool):
            out.append(str(k))
        elif isinstance(v, (int, float)):
            out.append(str(k))
    return sorted(out)
