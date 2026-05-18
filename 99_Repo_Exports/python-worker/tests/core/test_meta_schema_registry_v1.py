import pytest

from core.meta_features_v1 import META_FEAT_V1_COLS, META_FEAT_V1_NAME
from core.meta_features_v3 import META_FEAT_V3_COLS, META_FEAT_V3_NAME
from core.meta_schema_registry import META_SCHEMA_REGISTRY, get_schema_info


def test_registry_contains_known_schemas():
    assert META_FEAT_V1_NAME in META_SCHEMA_REGISTRY
    assert META_FEAT_V3_NAME in META_SCHEMA_REGISTRY

    v1_info = META_SCHEMA_REGISTRY[META_FEAT_V1_NAME]
    assert v1_info.cols == tuple(META_FEAT_V1_COLS)

    v3_info = META_SCHEMA_REGISTRY[META_FEAT_V3_NAME]
    assert v3_info.cols == tuple(META_FEAT_V3_COLS)

def test_get_schema_info():
    v1_ver, v1_cols, v1_hash = get_schema_info(META_FEAT_V1_NAME)
    assert v1_cols == META_FEAT_V1_COLS
    assert len(v1_hash) > 0

def test_get_schema_info_missing():
    with pytest.raises(KeyError):
        get_schema_info("non_existent_schema")
