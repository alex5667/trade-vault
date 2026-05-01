from __future__ import annotations
"""
Integration тесты для MLConfirmGate с валидатором champion конфига.
"""

from utils.time_utils import get_ny_time_millis

import json
import os
import tempfile
import time
from typing import Any, Dict

import pytest
import redis

from services.ml_confirm_gate import MLConfirmGate


@pytest.fixture
def redis_client() -> redis.Redis:
    """Фикстура Redis клиента."""
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = redis.Redis.from_url(redis_url, decode_responses=True)
    # Очистка тестовых ключей
    r.delete("cfg:ml_confirm:champion")
    r.delete("cfg:ml_confirm:challenger")
    r.delete("cfg:ml_confirm")
    yield r
    # Очистка после теста
    r.delete("cfg:ml_confirm:champion")
    r.delete("cfg:ml_confirm:challenger")
    r.delete("cfg:ml_confirm")


@pytest.fixture
def ml_gate(redis_client: redis.Redis) -> MLConfirmGate:
    """Фикстура MLConfirmGate."""
    return MLConfirmGate(
        r=redis_client,
        mode="SHADOW",
        fail_policy="OPEN",
        champion_key="cfg:ml_confirm:champion",
        challenger_key="cfg:ml_confirm:challenger",
    )


@pytest.fixture
def valid_champion_cfg() -> Dict[str, Any]:
    """Фикстура валидного champion конфига."""
    return {
        "schema_version": 1,
        "kind": "util_mh_v1",
        "run_id": "test_run_123",
        "created_ms": get_ny_time_millis(),
        "model_path": "/tmp/test_model.joblib",
        "mode": "SHADOW",
        "enforce_share": 0.0,
    }


def test_load_valid_champion_cfg(ml_gate: MLConfirmGate, redis_client: redis.Redis, valid_champion_cfg: Dict[str, Any]) -> None:
    """Тест: загрузка валидного champion конфига."""
    # Записываем валидный конфиг в Redis
    redis_client.set("cfg:ml_confirm:champion", json.dumps(valid_champion_cfg))
    
    # Загружаем конфиг
    cfg, model = ml_gate._load_cfg_and_model()
    
    # Проверяем, что конфиг загружен
    assert cfg
    assert cfg.get("kind") == "util_mh_v1"
    assert cfg.get("run_id") == "test_run_123"
    assert cfg.get("mode") == "SHADOW"
    assert cfg.get("enforce_share") == 0.0
    assert ml_gate._cfg_source == "champion"
    assert ml_gate._cfg_parse_err == ""


def test_load_invalid_champion_cfg_missing_enforce_share(
    ml_gate: MLConfirmGate,
    redis_client: redis.Redis,
    valid_champion_cfg: Dict[str, Any]
) -> None:
    """Тест: загрузка невалидного конфига (отсутствует enforce_share)."""
    # Удаляем enforce_share
    del valid_champion_cfg["enforce_share"]
    redis_client.set("cfg:ml_confirm:champion", json.dumps(valid_champion_cfg))
    
    # Загружаем конфиг
    cfg, model = ml_gate._load_cfg_and_model()
    
    # Проверяем, что конфиг загружен даже при ошибке валидации (lenient mode)
    assert cfg
    assert cfg.get("kind") == "util_mh_v1"


def test_load_invalid_champion_cfg_mode_mismatch(
    ml_gate: MLConfirmGate,
    redis_client: redis.Redis,
    valid_champion_cfg: Dict[str, Any]
) -> None:
    """Тест: загрузка невалидного конфига (mode/enforce_share mismatch)."""
    # SHADOW требует enforce_share=0.0, но ставим 0.05
    valid_champion_cfg["mode"] = "SHADOW"
    valid_champion_cfg["enforce_share"] = 0.05
    redis_client.set("cfg:ml_confirm:champion", json.dumps(valid_champion_cfg))
    
    # Загружаем конфиг
    cfg, model = ml_gate._load_cfg_and_model()
    
    # Проверяем, что конфиг загружен даже при ошибке валидации (lenient mode)
    assert cfg
    assert cfg.get("kind") == "util_mh_v1"


def test_load_missing_champion_cfg(ml_gate: MLConfirmGate, redis_client: redis.Redis) -> None:
    """Тест: отсутствие champion конфига."""
    # Не записываем конфиг в Redis
    
    # Загружаем конфиг
    cfg, model = ml_gate._load_cfg_and_model()
    
    # Проверяем, что конфиг не загружен
    assert not cfg
    assert ml_gate._cfg_parse_err == "missing" or ml_gate._cfg_source == "none"


def test_check_with_valid_cfg_no_model(
    ml_gate: MLConfirmGate,
    redis_client: redis.Redis,
    valid_champion_cfg: Dict[str, Any]
) -> None:
    """Тест: check() с валидным конфигом, но без модели."""
    # Записываем валидный конфиг
    redis_client.set("cfg:ml_confirm:champion", json.dumps(valid_champion_cfg))
    
    ml_gate._refresh_cache_if_needed()
    
    # Вызываем check()
    dec = ml_gate.check(
        symbol="BTCUSDT",
        ts_ms=get_ny_time_millis(),
        direction="LONG",
        scenario="trend_continuation",
        indicators={"spread_bps": 2.0, "expected_slippage_bps": 1.0},
        rule_score=1.0,
        rule_have=3,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )
    
    # Проверяем, что решение принято (но модель не загружена)
    assert dec.mode in ("ERR", "SHADOW", "OFF")
    # В SHADOW режиме без модели должно быть ERR или fallback
    if dec.mode == "ERR":
        assert "no_model" in dec.error.lower() or "load" in dec.error.lower()


def test_check_with_invalid_cfg(
    ml_gate: MLConfirmGate,
    redis_client: redis.Redis,
    valid_champion_cfg: Dict[str, Any]
) -> None:
    """Тест: check() с невалидным конфигом."""
    # Записываем невалидный конфиг (отсутствует enforce_share)
    del valid_champion_cfg["enforce_share"]
    redis_client.set("cfg:ml_confirm:champion", json.dumps(valid_champion_cfg))
    
    ml_gate._refresh_cache_if_needed()
    
    # Вызываем check()
    dec = ml_gate.check(
        symbol="BTCUSDT",
        ts_ms=get_ny_time_millis(),
        direction="LONG",
        scenario="trend_continuation",
        indicators={"spread_bps": 2.0, "expected_slippage_bps": 1.0},
        rule_score=1.0,
        rule_have=3,
        rule_need=2,
        cancel_spike_veto=0,
        ok_rule=1,
    )
    
    # Проверяем, что решение ALLOW/ERR/BLOCK в зависимости от lenient mode и наличия модели
    # Так как конфиг загружается в lenient режиме, но модель отсутствует, будет ERR_NO_MODEL или просто ERR
    assert dec.mode in ("ERR", "SHADOW", "OFF")


def test_canary_mode_enforce_share_validation(
    ml_gate: MLConfirmGate,
    redis_client: redis.Redis,
    valid_champion_cfg: Dict[str, Any]
) -> None:
    """Тест: валидация CANARY режима с enforce_share."""
    # Устанавливаем CANARY режим
    valid_champion_cfg["mode"] = "CANARY"
    valid_champion_cfg["enforce_share"] = 0.05
    redis_client.set("cfg:ml_confirm:champion", json.dumps(valid_champion_cfg))
    
    # Загружаем конфиг
    cfg, model = ml_gate._load_cfg_and_model()
    
    # Проверяем, что конфиг загружен
    assert cfg
    assert cfg.get("mode") == "CANARY"
    assert cfg.get("enforce_share") == 0.05


def test_enforce_mode_enforce_share_validation(
    ml_gate: MLConfirmGate,
    redis_client: redis.Redis,
    valid_champion_cfg: Dict[str, Any]
) -> None:
    """Тест: валидация ENFORCE режима с enforce_share=1.0."""
    # Устанавливаем ENFORCE режим
    valid_champion_cfg["mode"] = "ENFORCE"
    valid_champion_cfg["enforce_share"] = 1.0
    redis_client.set("cfg:ml_confirm:champion", json.dumps(valid_champion_cfg))
    
    # Загружаем конфиг
    cfg, model = ml_gate._load_cfg_and_model()
    
    # Проверяем, что конфиг загружен
    assert cfg
    assert cfg.get("mode") == "ENFORCE"
    assert cfg.get("enforce_share") == 1.0

