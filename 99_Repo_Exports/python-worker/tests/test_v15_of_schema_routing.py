"""test_v15_of_schema_routing.py — verifies ML_FEATURE_SCHEMA_VER=v15_of routing.

P0 checks:
  1. get_schema_info("v15_of") routes to V15_OF_NUMERIC_KEYS (531 numeric features).
  2. v15_of and v14_of schemas are disjoint objects in the cache (no aliasing).
  3. Schema cache is keyed correctly — v14_of call after v15_of still returns 359.
  4. select_features drops low-coverage features but does NOT remove them from
     the schema list (schema integrity: 531 keys always present in V15_OF_NUMERIC_KEYS).
  5. Model pack produced by train_v15_lgbm stores feature_schema_ver="v15_of"
     so the shape-guard can validate it against the registry.
  6. Shape guard accepts a subset (training drops low-coverage cols) and rejects
     an oversized pack for a known schema.
"""
from __future__ import annotations

import pytest


# ── helpers ───────────────────────────────────────────────────────────────────

def _n_numeric(schema_info) -> int:
    """Count numeric (n:*) feature columns in a FeatureSchemaInfo."""
    return sum(1 for n in schema_info.feature_names if n.startswith("n:"))


# ── 1. Routing: v15_of → 531 numeric keys ─────────────────────────────────────

def test_v15_of_routing_key_count():
    from core.feature_registry import get_schema_info
    info = get_schema_info("v15_of")
    n = _n_numeric(info)
    assert n == 531, (
        f"v15_of routing returned {n} numeric keys; expected 531. "
        "Check that ML_FEATURE_SCHEMA_VER=v15_of selects V15_OF_NUMERIC_KEYS, "
        "not a fallback schema."
    )


def test_v15_of_routing_ver_string():
    from core.feature_registry import get_schema_info
    info = get_schema_info("v15_of")
    assert info.ver == "v15_of"


def test_v15_of_routing_alias_v15():
    from core.feature_registry import get_schema_info
    assert _n_numeric(get_schema_info("v15_of")) == _n_numeric(get_schema_info("v15"))


# ── 2. Schema integrity: v15_of ⊇ v14_of (append-only) ───────────────────────

def test_v15_of_superset_of_v14():
    from core.ml_feature_schema_v14_of import V14_OF_NUMERIC_KEYS
    from core.ml_feature_schema_v15_of import V15_OF_NUMERIC_KEYS
    missing = set(V14_OF_NUMERIC_KEYS) - set(V15_OF_NUMERIC_KEYS)
    assert missing == set(), (
        f"v15_of dropped keys from v14_of (append-only violated): {sorted(missing)[:20]}"
    )


def test_v15_of_new_key_count():
    from core.ml_feature_schema_v14_of import V14_OF_NUMERIC_KEYS
    from core.ml_feature_schema_v15_of import V15_OF_NUMERIC_KEYS
    new = set(V15_OF_NUMERIC_KEYS) - set(V14_OF_NUMERIC_KEYS)
    # 531 total − len(v14) new keys (v14 may drift; enforce min new count = 150)
    assert len(new) >= 150, (
        f"Expected ≥150 new v15_of keys beyond v14_of base, got {len(new)}"
    )


# ── 3. Cache isolation ─────────────────────────────────────────────────────────

def test_cache_isolation_v14_unchanged_after_v15():
    from core.feature_registry import get_schema_info
    from core.ml_feature_schema_v14_of import V14_OF_NUMERIC_KEYS
    info_v15 = get_schema_info("v15_of")
    info_v14 = get_schema_info("v14_of")
    expected_v14 = len(V14_OF_NUMERIC_KEYS)
    assert _n_numeric(info_v14) == expected_v14, (
        f"v14_of schema returned {_n_numeric(info_v14)} keys after v15_of was loaded; "
        "schema cache may be aliased."
    )
    assert _n_numeric(info_v15) == 531


def test_cache_not_aliased():
    from core.feature_registry import get_schema_info
    a = get_schema_info("v14_of")
    b = get_schema_info("v15_of")
    assert a is not b
    assert a.feature_names is not b.feature_names


# ── 4. Schema immutability: select_features does not mutate V15_OF_NUMERIC_KEYS ─

def test_select_features_does_not_mutate_schema():
    from core.ml_feature_schema_v15_of import V15_OF_NUMERIC_KEYS

    n_before = len(V15_OF_NUMERIC_KEYS)

    try:
        from tools.train_v15_lgbm import select_features, Sample
        samples = [
            Sample(sid=f"s{i}", ts_ms=i * 1000, symbol="BTC", regime="na",
                   features={"f_present": 1.0}, r=0.5, hit=1)
            for i in range(10)
        ]
        select_features(samples, min_coverage=0.80)
    except ImportError:
        pytest.skip("train_v15_lgbm not importable; schema-immutability check only")

    assert len(V15_OF_NUMERIC_KEYS) == n_before == 531, (
        "select_features must not mutate V15_OF_NUMERIC_KEYS. "
        "Features are excluded from training only, not dropped from the schema."
    )


# ── 5. Model pack: feature_schema_ver must be "v15_of" ─────────────────────────

def test_model_pack_schema_ver_is_v15_of():
    """train_v15_lgbm.py must store feature_schema_ver='v15_of' so the shape guard
    validates the model against the 531-key registry."""
    import pathlib

    src = pathlib.Path(__file__).parent.parent / "tools" / "train_v15_lgbm.py"
    if not src.exists():
        pytest.skip("train_v15_lgbm.py not found")

    text = src.read_text()
    # Must NOT contain the old "v15_lgbm" schema version string in the model pack
    assert '"v15_lgbm"' not in text, (
        "train_v15_lgbm.py still contains feature_schema_ver='v15_lgbm'. "
        "Shape guard cannot validate these models against the v15_of registry. "
        "Change both 'feature_schema_version' and 'feature_schema_ver' to 'v15_of'."
    )
    assert '"v15_of"' in text, (
        "train_v15_lgbm.py must set feature_schema_ver='v15_of' in the model pack."
    )


# ── 6. Shape guard ─────────────────────────────────────────────────────────────

def test_shape_guard_accepts_v15_of_subset():
    from services.ml_confirm.model_loader import _validate_edge_stack_shape
    pack = {"feature_schema_ver": "v15_of", "feature_cols": ["f"] * 200}
    assert _validate_edge_stack_shape(pack, "/tmp/m.joblib") is True


def test_shape_guard_rejects_v15_of_oversized():
    from services.ml_confirm.model_loader import _validate_edge_stack_shape
    pack = {"feature_schema_ver": "v15_of", "feature_cols": ["f"] * 99999}
    assert _validate_edge_stack_shape(pack, "/tmp/m.joblib") is False


def test_shape_guard_exact_531():
    from services.ml_confirm.model_loader import _validate_edge_stack_shape
    pack = {"feature_schema_ver": "v15_of", "feature_cols": ["f"] * 531}
    assert _validate_edge_stack_shape(pack, "/tmp/m.joblib") is True


# ── 7. Coverage: select_features contract ─────────────────────────────────────

def test_select_features_coverage_gate():
    try:
        from tools.train_v15_lgbm import select_features, Sample
    except ImportError:
        pytest.skip("train_v15_lgbm not importable")

    samples = []
    for i in range(100):
        feats: dict[str, float] = {"always_present": float(i)}  # variant: different per sample
        if i < 79:  # 79% coverage — below 0.80
            feats["below_threshold"] = float(i)
        if i < 90:  # 90% coverage — above 0.80
            feats["above_threshold"] = float(i)
        samples.append(Sample(sid=f"s{i}", ts_ms=i * 1000, symbol="X", regime="na",
                               features=feats, r=float(i % 2), hit=i % 2))

    selected = select_features(samples, min_coverage=0.80)
    assert "always_present" in selected
    assert "above_threshold" in selected
    assert "below_threshold" not in selected


def test_select_features_nan_counts_as_missing():
    try:
        from tools.train_v15_lgbm import select_features, Sample
    except ImportError:
        pytest.skip("train_v15_lgbm not importable")

    samples = []
    for i in range(10):
        samples.append(Sample(
            sid=f"s{i}", ts_ms=i * 1000, symbol="X", regime="na",
            features={"solid": float(i), "stale_feature": float("nan")},
            r=0.5, hit=i % 2,
        ))

    selected = select_features(samples, min_coverage=0.80)
    assert "solid" in selected
    assert "stale_feature" not in selected
