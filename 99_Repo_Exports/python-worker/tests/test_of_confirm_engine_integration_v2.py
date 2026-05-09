from types import SimpleNamespace
from unittest.mock import patch

import pytest

from core.meta_features_v2 import META_FEAT_V2_NAME, META_FEAT_V2_VERSION
from core.of_confirm_engine import OFConfirmEngine
from domain.evidence_keys import MetaKeys


class MockMetaModel:
    def __init__(self, schema_name, schema_version):
        self.schema_name = schema_name
        self.schema_version = schema_version
        self.features = []
        self.intercept = 0.0
        self.coef = []

    def predict_proba(self, feat):
        return 0.6

@pytest.fixture
def engine():
    return OFConfirmEngine()

def test_engine_selects_v2_schema(engine):
    # Mock configuration to enable meta model
    cfg = {
        "meta_model_enable": 1,
        "meta_model_path": "dummy_path"
    }

    # Mock runtime
    runtime = SimpleNamespace(
        book_state=SimpleNamespace(
            snap=None, prev_snap=None
        ), # empty book state -> features 0.0
        dynamic_cfg={}
    )

    # Mock model loader to return V2 model
    mock_model = MockMetaModel(META_FEAT_V2_NAME, META_FEAT_V2_VERSION)

    # Patch the builder to verify it's called
    with patch.object(engine, "_load_meta_model_slot", return_value=mock_model), \
         patch("core.of_confirm_engine.build_meta_features_v2") as mock_build:

        # Setup mock return for builder so engine doesn't crash on tuple unpacking
        mock_build.return_value = ({}, [])

        indicators = {"sid": "test_sid"}

        res, gate = engine.build(
            symbol="BTCUSDT",
            tf="1m",
            direction="LONG",
            tick_ts_ms=1000,
            price=100.0,
            delta_z=1.0,
            runtime=runtime,
            cfg=cfg,
            indicators=indicators
        )

        assert res is not None
        evidence = res.evidence

        assert evidence[MetaKeys.SCHEMA_NAME] == META_FEAT_V2_NAME
        assert evidence[MetaKeys.SCHEMA_VERSION] == META_FEAT_V2_VERSION

        # Verify v2 builder was called
        mock_build.assert_called_once()

def test_engine_fallback_v1_schema(engine):
    # Mock configuration
    cfg = {
        "meta_model_enable": 1,
        "meta_model_path": "dummy_path_v1"
    }

    runtime = SimpleNamespace(book_state=None, dynamic_cfg={})

    # Mock model loader to return V1 model
    mock_model = MockMetaModel("meta_feat_v1", 1)

    with patch.object(engine, "_load_meta_model_slot", return_value=mock_model):
        res, gate = engine.build(
            symbol="BTCUSDT",
            tf="1m",
            direction="LONG",
            tick_ts_ms=1000,
            price=100.0,
            delta_z=1.0,
            runtime=runtime,
            cfg=cfg,
            indicators={"sid": "test_sid_v1"}
        )

        assert res is not None
        evidence = res.evidence
        assert evidence[MetaKeys.SCHEMA_NAME] == "meta_feat_v1"
        assert evidence[MetaKeys.SCHEMA_VERSION] == 1
