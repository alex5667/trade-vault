"""Tests for core.ml_feature_schema_v7_of.MLFeatureSchemaV7OF."""

from __future__ import annotations
import pytest
from core.ml_feature_schema_v7_of import MLFeatureSchemaV7OF
from core.ml_feature_schema_v6_of import MLFeatureSchemaV6OF

SCHEMA_HASH = "3cb5b9874cd7"


try:
    from core.ml_feature_schema_v7_of import MLFeatureSchemaV7OF
    from core.ml_feature_schema_v6_of import MLFeatureSchemaV6OF
except ImportError as e:
    pytest.skip(f"MLFeatureSchemaV7OF/V6OF недоступен: {e}", allow_module_level=True)


@pytest.fixture
def schema_v7():
    return MLFeatureSchemaV7OF()


@pytest.fixture
def schema_v6():
    return MLFeatureSchemaV6OF()


# ---------------------------------------------------------------------------
# Tests
# ---------------------------------------------------------------------------

def test_v7_of_inherits_v6_of():
    """v7_of must be a subclass of v6_of."""
    assert issubclass(MLFeatureSchemaV7OF, MLFeatureSchemaV6OF)


def test_v7_of_has_more_features_than_v6_of(schema_v7, schema_v6):
    """v7_of must have more num_keys than v6_of."""
    assert len(schema_v7.num_keys) > len(schema_v6.num_keys), (
        f"v7_of has {len(schema_v7.num_keys)} num_keys, v6_of has {len(schema_v6.num_keys)}"
    )


def test_v7_of_contains_hawkes_split_keys(schema_v7):
    """Required Hawkes split keys must be present."""
    required = [
        "hawkes_taker_buy_lam",
        "hawkes_taker_sell_lam",
        "hawkes_cancel_bid_lam",
        "hawkes_cancel_ask_lam",
        "hawkes_limit_add_lam",
        "hawkes_taker_lam",
        "hawkes_cancel_lam",
        "hawkes_churn_lam",
        "hawkes_dt_s",
    ]
    missing = [k for k in required if k not in schema_v7.num_keys]
    assert not missing, f"Missing Hawkes keys in v7_of: {missing}"


def test_v7_of_contains_vpin_keys(schema_v7):
    """VPIN toxicity keys must be present."""
    assert "vpin_tox_ema" in schema_v7.num_keys
    assert "vpin_tox_z" in schema_v7.num_keys


def test_v7_of_contains_add_rate_keys(schema_v7):
    """Addition rate keys must be present."""
    assert "added_bid_rate_ema" in schema_v7.num_keys
    assert "added_ask_rate_ema" in schema_v7.num_keys
    assert "added_total_rate_ema" in schema_v7.num_keys


def test_v6_of_keys_are_subset_of_v7_of(schema_v7, schema_v6):
    """All v6_of num_keys must be present in v7_of."""
    v7_set = set(schema_v7.num_keys)
    missing = [k for k in schema_v6.num_keys if k not in v7_set]
    assert not missing, f"v7_of missing v6_of keys: {missing[:10]}"


def test_num_keys_unique(schema_v7):
    """No duplicates in num_keys."""
    dup = [k for k in set(schema_v7.num_keys) if schema_v7.num_keys.count(k) > 1]
    assert not dup, f"Duplicate keys in v7_of.num_keys: {dup}"


def test_n_features_positive(schema_v7):
    """n_features must be a positive integer."""
    assert isinstance(schema_v7.n_features, int)
    assert schema_v7.n_features > 0


def test_raw_state_keys_present(schema_v7):
    """Raw S_* state keys for observability must be present."""
    raw_keys = [
        "hawkes_S_taker_buy",
        "hawkes_S_taker_sell",
        "hawkes_S_cancel_bid",
        "hawkes_S_cancel_ask",
        "hawkes_S_limit_add",
    ]
    missing = [k for k in raw_keys if k not in schema_v7.num_keys]
    assert not missing, f"Missing raw state keys: {missing}"
