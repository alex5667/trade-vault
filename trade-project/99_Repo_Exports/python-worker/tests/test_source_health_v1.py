"""test_source_health_v1.py — unit tests for the canonical source-health helper.

Covers:
  1. SOURCE_REGISTRY shape — every spec has the expected three feature keys.
  2. compute_source_health — empty / no-ts / bad-ts / fresh / stale cases.
  3. make_source_health_features — unknown source returns {}, age omitted
     when ts missing.
  4. build_all_source_health_features — emits available/stale for every
     registered source even when no snapshot is provided.
  5. source_health_feature_keys — deterministic and matches registry.
  6. External payload integration — cg_data_* and cp_data_* now present
     in `_V12_BASE_OPTIONAL_KEYS` (closes the audit gap).
"""
from __future__ import annotations

import pytest


def test_registry_specs_have_three_feature_keys():
    from core.source_health_v1 import SOURCE_REGISTRY

    for spec in SOURCE_REGISTRY:
        a, age, s = spec.feature_keys
        assert a == f"{spec.prefix}_data_available"
        assert age == f"{spec.prefix}_data_age_ms"
        assert s == f"{spec.prefix}_data_stale"


def test_registry_prefixes_unique():
    from core.source_health_v1 import SOURCE_REGISTRY
    prefixes = [s.prefix for s in SOURCE_REGISTRY]
    assert len(prefixes) == len(set(prefixes)), "duplicate prefix in registry"


def test_compute_source_health_empty():
    from core.source_health_v1 import compute_source_health
    assert compute_source_health(None, now_ms=1000, max_lag_ms=500) == (0.0, 0.0, 1.0)
    assert compute_source_health({}, now_ms=1000, max_lag_ms=500) == (0.0, 0.0, 1.0)


def test_compute_source_health_no_ts():
    from core.source_health_v1 import compute_source_health
    a, age, s = compute_source_health({"value": 42}, now_ms=1000, max_lag_ms=500)
    assert a == 1.0 and age == 0.0 and s == 0.0


def test_compute_source_health_fresh():
    from core.source_health_v1 import compute_source_health
    a, age, s = compute_source_health({"ts_ms": 800}, now_ms=1000, max_lag_ms=500)
    assert a == 1.0
    assert age == 200.0
    assert s == 0.0


def test_compute_source_health_stale():
    from core.source_health_v1 import compute_source_health
    a, age, s = compute_source_health({"ts_ms": 100}, now_ms=1000, max_lag_ms=500)
    assert a == 1.0
    assert age == 900.0
    assert s == 1.0


def test_compute_source_health_bad_ts():
    from core.source_health_v1 import compute_source_health
    a, age, s = compute_source_health({"ts_ms": "not-a-number"}, now_ms=1000, max_lag_ms=500)
    # Bad ts → treat as no-ts (cannot prove staleness)
    assert a == 1.0 and age == 0.0 and s == 0.0


def test_compute_source_health_clock_skew():
    from core.source_health_v1 import compute_source_health
    a, age, s = compute_source_health({"ts_ms": 2000}, now_ms=1000, max_lag_ms=500)
    # Future ts → clamped to 0 age
    assert a == 1.0 and age == 0.0 and s == 0.0


def test_make_features_unknown_source():
    from core.source_health_v1 import make_source_health_features
    out = make_source_health_features("nonexistent_source", {"ts_ms": 100}, now_ms=200)
    assert out == {}


def test_make_features_age_omitted_when_zero():
    from core.source_health_v1 import make_source_health_features
    # No ts in snapshot → no _age_ms key
    out = make_source_health_features("coingecko", {"x": 1}, now_ms=1000)
    assert "cg_data_age_ms" not in out
    assert out["cg_data_available"] == 1.0
    assert out["cg_data_stale"] == 0.0


def test_make_features_emits_full_triple():
    from core.source_health_v1 import make_source_health_features
    out = make_source_health_features(
        "coingecko", {"ts_ms": 1000}, now_ms=1500, max_lag_ms=10_000
    )
    assert out["cg_data_available"] == 1.0
    assert out["cg_data_age_ms"] == 500.0
    assert out["cg_data_stale"] == 0.0


def test_build_all_emits_every_registered_source():
    from core.source_health_v1 import (
        SOURCE_REGISTRY,
        build_all_source_health_features,
    )
    out = build_all_source_health_features({}, now_ms=1000)
    for spec in SOURCE_REGISTRY:
        assert f"{spec.prefix}_data_available" in out
        assert f"{spec.prefix}_data_stale" in out
        # All missing → unavailable+stale
        assert out[f"{spec.prefix}_data_available"] == 0.0
        assert out[f"{spec.prefix}_data_stale"] == 1.0


def test_build_all_mixes_real_and_missing_sources():
    from core.source_health_v1 import build_all_source_health_features
    snapshots = {
        "coingecko": {"ts_ms": 900},
        "deribit": {"ts_ms": 990},
    }
    out = build_all_source_health_features(snapshots, now_ms=1000)
    assert out["cg_data_available"] == 1.0
    assert out["cg_data_stale"] == 0.0
    assert out["deribit_data_available"] == 1.0
    # bybit was not supplied
    assert out["bybit_data_available"] == 0.0
    assert out["bybit_data_stale"] == 1.0


def test_source_health_feature_keys_deterministic():
    from core.source_health_v1 import (
        SOURCE_REGISTRY,
        SOURCE_HEALTH_FEATURE_KEYS,
        source_health_feature_keys,
    )
    expected = []
    for spec in SOURCE_REGISTRY:
        expected.extend(spec.feature_keys)
    assert source_health_feature_keys() == tuple(expected)
    assert SOURCE_HEALTH_FEATURE_KEYS == tuple(expected)


def test_external_payload_now_declares_cg_cp_health():
    """Closes the audit gap: cg_data_* and cp_data_* must appear in the
    canonical optional-key list so the schema-routing layer can surface them."""
    from core.external_features_payload_v1 import _V12_BASE_OPTIONAL_KEYS
    for k in (
        "cg_data_available", "cg_data_age_ms", "cg_data_stale",
        "cp_data_available", "cp_data_age_ms", "cp_data_stale",
    ):
        assert k in _V12_BASE_OPTIONAL_KEYS, f"{k!r} missing from external payload optional keys"
