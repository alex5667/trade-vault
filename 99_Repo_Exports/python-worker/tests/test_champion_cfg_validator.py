from __future__ import annotations
"""
Unit тесты для champion_cfg_validator.
"""


import json
import pytest

from core.champion_cfg_validator import (
    validate_champion_cfg,
    CfgError,
    ChampionCfg,
    ALLOWED_MODES,
)


def test_valid_champion_cfg_minimal() -> None:
    """Тест минимального валидного конфига."""
    cfg_json = json.dumps({
        "schema_version": 1,
        "kind": "util_mh_v1",
        "run_id": "tb_v10_4_20260204_180848_830d27",
        "created_ms": 1770440031075,
        "model_path": "/var/lib/trade/ml_models/tb_v10_4_20260204_180848_830d27/model.joblib",
        "mode": "CANARY",
        "enforce_share": 0.05,
    })
    
    cfg, info = validate_champion_cfg(cfg_json)
    assert isinstance(cfg, ChampionCfg)
    assert cfg.schema_version == 1
    assert cfg.kind == "util_mh_v1"
    assert cfg.run_id == "tb_v10_4_20260204_180848_830d27"
    assert cfg.mode == "CANARY"
    assert cfg.enforce_share == 0.05
    assert info["defaulted_fields"] == {}


def test_valid_champion_cfg_full() -> None:
    """Тест полного валидного конфига."""
    cfg_json = json.dumps({
        "schema_version": 1,
        "kind": "util_mh_v1",
        "run_id": "tb_v10_4_20260204_180848_830d27",
        "created_ms": 1770440031075,
        "model_path": "/var/lib/trade/ml_models/tb_v10_4_20260204_180848_830d27/model.joblib",
        "calibrator_path": "/var/lib/trade/ml_models/tb_v10_4_20260204_180848_830d27/calibrator.json",
        "feature_version": "tb_v10_4.features.v2",
        "model_type": "UtilMHModelV1",
        "mode": "ENFORCE",
        "enforce_share": 1.0,
        "checksum": "sha256:abc123",
        "min_data_ts_ms": 1770000000000,
        "max_data_ts_ms": 1770440000000,
    })
    
    cfg, info = validate_champion_cfg(cfg_json)
    assert cfg.mode == "ENFORCE"
    assert cfg.enforce_share == 1.0
    assert cfg.calibrator_path == "/var/lib/trade/ml_models/tb_v10_4_20260204_180848_830d27/calibrator.json"
    assert cfg.feature_version == "tb_v10_4.features.v2"
    assert cfg.model_type == "UtilMHModelV1"
    assert cfg.checksum == "sha256:abc123"
    assert cfg.min_data_ts_ms == 1770000000000
    assert cfg.max_data_ts_ms == 1770440000000


def test_mode_shadow_requires_enforce_share_zero() -> None:
    """Тест: SHADOW требует enforce_share=0.0."""
    cfg_json = json.dumps({
        "schema_version": 1,
        "kind": "util_mh_v1",
        "run_id": "test",
        "created_ms": 1770440031075,
        "model_path": "/path/to/model.joblib",
        "mode": "SHADOW",
        "enforce_share": 0.05,  # Должно быть 0.0
    })
    
    with pytest.raises(CfgError, match="mode=SHADOW requires enforce_share=0.0"):
        validate_champion_cfg(cfg_json)


def test_mode_enforce_requires_enforce_share_one() -> None:
    """Тест: ENFORCE требует enforce_share=1.0."""
    cfg_json = json.dumps({
        "schema_version": 1,
        "kind": "util_mh_v1",
        "run_id": "test",
        "created_ms": 1770440031075,
        "model_path": "/path/to/model.joblib",
        "mode": "ENFORCE",
        "enforce_share": 0.5,  # Должно быть 1.0
    })
    
    with pytest.raises(CfgError, match="mode=ENFORCE requires enforce_share=1.0"):
        validate_champion_cfg(cfg_json)


def test_mode_canary_requires_enforce_share_between_zero_and_one() -> None:
    """Тест: CANARY требует 0.0 < enforce_share < 1.0."""
    # enforce_share = 0.0 (недопустимо для CANARY)
    cfg_json = json.dumps({
        "schema_version": 1,
        "kind": "util_mh_v1",
        "run_id": "test",
        "created_ms": 1770440031075,
        "model_path": "/path/to/model.joblib",
        "mode": "CANARY",
        "enforce_share": 0.0,
    })
    
    with pytest.raises(CfgError, match="mode=CANARY requires 0.0 < enforce_share < 1.0"):
        validate_champion_cfg(cfg_json)
    
    # enforce_share = 1.0 (недопустимо для CANARY)
    cfg_json = json.dumps({
        "schema_version": 1,
        "kind": "util_mh_v1",
        "run_id": "test",
        "created_ms": 1770440031075,
        "model_path": "/path/to/model.joblib",
        "mode": "CANARY",
        "enforce_share": 1.0,
    })
    
    with pytest.raises(CfgError, match="mode=CANARY requires 0.0 < enforce_share < 1.0"):
        validate_champion_cfg(cfg_json)


def test_missing_enforce_share_raises_error() -> None:
    """Тест: отсутствие enforce_share вызывает ошибку."""
    cfg_json = json.dumps({
        "schema_version": 1,
        "kind": "util_mh_v1",
        "run_id": "test",
        "created_ms": 1770440031075,
        "model_path": "/path/to/model.joblib",
        "mode": "CANARY",
        # enforce_share отсутствует
    })
    
    with pytest.raises(CfgError, match="enforce_share: missing"):
        validate_champion_cfg(cfg_json, default_enforce_share=None)


def test_missing_enforce_share_with_default() -> None:
    """Тест: отсутствие enforce_share с default_enforce_share."""
    cfg_json = json.dumps({
        "schema_version": 1,
        "kind": "util_mh_v1",
        "run_id": "test",
        "created_ms": 1770440031075,
        "model_path": "/path/to/model.joblib",
        "mode": "CANARY",
        # enforce_share отсутствует
    })
    
    cfg, info = validate_champion_cfg(cfg_json, default_enforce_share=0.05)
    assert cfg.enforce_share == 0.05
    assert "enforce_share" in info["defaulted_fields"]
    assert info["defaulted_fields"]["enforce_share"] == 0.05


def test_invalid_schema_version() -> None:
    """Тест: неверная schema_version."""
    cfg_json = json.dumps({
        "schema_version": 2,  # Должно быть 1
        "kind": "util_mh_v1",
        "run_id": "test",
        "created_ms": 1770440031075,
        "model_path": "/path/to/model.joblib",
        "mode": "SHADOW",
        "enforce_share": 0.0,
    })
    
    with pytest.raises(CfgError, match="schema_version: unsupported"):
        validate_champion_cfg(cfg_json)


def test_invalid_mode() -> None:
    """Тест: неверный mode."""
    cfg_json = json.dumps({
        "schema_version": 1,
        "kind": "util_mh_v1",
        "run_id": "test",
        "created_ms": 1770440031075,
        "model_path": "/path/to/model.joblib",
        "mode": "INVALID",  # Неверный mode
        "enforce_share": 0.0,
    })
    
    with pytest.raises(CfgError, match="mode: expected one of"):
        validate_champion_cfg(cfg_json)


def test_missing_required_fields() -> None:
    """Тест: отсутствие обязательных полей."""
    # Отсутствует kind
    cfg_json = json.dumps({
        "schema_version": 1,
        "run_id": "test",
        "created_ms": 1770440031075,
        "model_path": "/path/to/model.joblib",
        "mode": "SHADOW",
        "enforce_share": 0.0,
    })
    
    with pytest.raises(CfgError, match="kind: expected non-empty string"):
        validate_champion_cfg(cfg_json)


def test_empty_string_fields() -> None:
    """Тест: пустые строковые поля."""
    cfg_json = json.dumps({
        "schema_version": 1,
        "kind": "",  # Пустая строка
        "run_id": "test",
        "created_ms": 1770440031075,
        "model_path": "/path/to/model.joblib",
        "mode": "SHADOW",
        "enforce_share": 0.0,
    })
    
    with pytest.raises(CfgError, match="kind: expected non-empty string"):
        validate_champion_cfg(cfg_json)


def test_enforce_share_out_of_range() -> None:
    """Тест: enforce_share вне диапазона [0..1]."""
    # enforce_share > 1.0
    cfg_json = json.dumps({
        "schema_version": 1,
        "kind": "util_mh_v1",
        "run_id": "test",
        "created_ms": 1770440031075,
        "model_path": "/path/to/model.joblib",
        "mode": "CANARY",
        "enforce_share": 1.5,  # > 1.0
    })
    
    with pytest.raises(CfgError, match="enforce_share: out of range"):
        validate_champion_cfg(cfg_json)
    
    # enforce_share < 0.0
    cfg_json = json.dumps({
        "schema_version": 1,
        "kind": "util_mh_v1",
        "run_id": "test",
        "created_ms": 1770440031075,
        "model_path": "/path/to/model.joblib",
        "mode": "CANARY",
        "enforce_share": -0.1,  # < 0.0
    })
    
    with pytest.raises(CfgError, match="enforce_share: out of range"):
        validate_champion_cfg(cfg_json)


def test_mode_case_insensitive() -> None:
    """Тест: mode нормализуется в uppercase."""
    cfg_json = json.dumps({
        "schema_version": 1,
        "kind": "util_mh_v1",
        "run_id": "test",
        "created_ms": 1770440031075,
        "model_path": "/path/to/model.joblib",
        "mode": "canary",  # lowercase
        "enforce_share": 0.05,
    })
    
    cfg, _ = validate_champion_cfg(cfg_json)
    assert cfg.mode == "CANARY"  # Должно быть uppercase


def test_invalid_json() -> None:
    """Тест: невалидный JSON."""
    with pytest.raises(CfgError, match="json: invalid"):
        validate_champion_cfg("not a json")


def test_not_dict() -> None:
    """Тест: JSON не является объектом."""
    with pytest.raises(CfgError, match="json: expected object"):
        validate_champion_cfg('["array", "not", "object"]')

