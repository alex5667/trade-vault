import pytest
from unittest.mock import MagicMock, patch
from core.of_confirm_engine import OFConfirmEngine
from core.meta_features_v1 import META_FEAT_V1_NAME, META_FEAT_V1_VERSION

@pytest.fixture
def mock_engine():
    return OFConfirmEngine()

def test_schema_legacy_downgrade(mock_engine):
    # Setup legacy model (no schema info)
    mm_mock = MagicMock()
    mm_mock.predict_proba.return_value = 0.8
    # Missing schema attributes simulates legacy model
    del mm_mock.schema_name
    del mm_mock.schema_version
    
    with patch("core.of_confirm_engine.OFConfirmEngine._load_meta_model_slot", return_value=mm_mock):
        cfg = {
            "meta_model_enable": 1,
            "meta_model_mode": "ENFORCE",
            "meta_min_feature_coverage": 0.0,
            "meta_model_path": "dummy.json",
            "meta_allow_legacy_schema": 0 # Default behavior
        }
        
        # Build should return evidence with SHADOW mode due to legacy schema
        ofc, _ = mock_engine.build(
            symbol="BTCUSDT", tf="1m", direction="LONG", tick_ts_ms=1000, price=100.0, delta_z=1.0,
            runtime=MagicMock(), cfg=cfg, indicators={"sid": "test"}
        )
        
        assert ofc.evidence["meta_mode"] == "SHADOW"
        assert "SCHEMA_MISMATCH" in ofc.evidence["meta_reason"]
        assert ofc.evidence["meta_model_schema_name"] == "legacy"

def test_schema_compatible_enforce(mock_engine):
    # Setup compatible model
    mm_mock = MagicMock()
    mm_mock.predict_proba.return_value = 0.8
    mm_mock.schema_name = META_FEAT_V1_NAME
    mm_mock.schema_version = META_FEAT_V1_VERSION
    mm_mock.schema_hash = ""
    mm_mock.feature_cols_hash = ""
    
    
    with patch("core.of_confirm_engine.OFConfirmEngine._load_meta_model_slot", return_value=mm_mock):
        cfg = {
            "meta_model_enable": 1,
            "meta_model_mode": "ENFORCE",
            "meta_min_feature_coverage": 0.0,
            "meta_model_path": "dummy.json",
            "meta_p_min": 0.5
        }
        
        ofc, _ = mock_engine.build(
            symbol="BTCUSDT", tf="1m", direction="LONG", tick_ts_ms=1000, price=100.0, delta_z=1.0,
            runtime=MagicMock(), cfg=cfg, indicators={"sid": "test"}
        )
        
        assert ofc.evidence["meta_mode"] == "ENFORCE"
        assert ofc.evidence["meta_model_schema_name"] == META_FEAT_V1_NAME

def test_schema_mismatch_downgrade(mock_engine):
    # Setup model with wrong version
    mm_mock = MagicMock()
    mm_mock.predict_proba.return_value = 0.8
    mm_mock.schema_name = META_FEAT_V1_NAME
    mm_mock.schema_version = META_FEAT_V1_VERSION + 1 # Future version
    mm_mock.schema_hash = ""
    mm_mock.feature_cols_hash = ""
    
    with patch("core.of_confirm_engine.OFConfirmEngine._load_meta_model_slot", return_value=mm_mock):
        cfg = {
            "meta_model_enable": 1,
            "meta_model_mode": "ENFORCE",
            "meta_min_feature_coverage": 0.0,
            "meta_model_path": "dummy.json",
        }
        
        ofc, _ = mock_engine.build(
            symbol="BTCUSDT", tf="1m", direction="LONG", tick_ts_ms=1000, price=100.0, delta_z=1.0,
            runtime=MagicMock(), cfg=cfg, indicators={"sid": "test"}
        )
        
        assert ofc.evidence["meta_mode"] == "SHADOW"
        assert "SCHEMA_MISMATCH" in ofc.evidence["meta_reason"]

def test_schema_legacy_allowed(mock_engine):
    # Setup legacy model but allow it
    mm_mock = MagicMock()
    mm_mock.predict_proba.return_value = 0.8
    del mm_mock.schema_name
    
    
    with patch("core.of_confirm_engine.OFConfirmEngine._load_meta_model_slot", return_value=mm_mock):
        cfg = {
            "meta_model_enable": 1,
            "meta_model_mode": "ENFORCE",
            "meta_min_feature_coverage": 0.0,
            "meta_model_path": "dummy.json",
            "meta_allow_legacy_schema": 1 # Override
        }
        
        ofc, _ = mock_engine.build(
            symbol="BTCUSDT", tf="1m", direction="LONG", tick_ts_ms=1000, price=100.0, delta_z=1.0,
            runtime=MagicMock(), cfg=cfg, indicators={"sid": "test"}
        )
        
        assert ofc.evidence["meta_mode"] == "ENFORCE"
