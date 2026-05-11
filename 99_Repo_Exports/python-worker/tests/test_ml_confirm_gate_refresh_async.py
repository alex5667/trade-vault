"""
Regression pack — MLConfirmGate.refresh_async (2026-04-18 wave).

Проверяет:
1. Повторный вызов в пределах TTL → не обращается к Redis.
2. При Redis timeout → fail-open (cfg/model не обнуляются).
3. mode==OFF → cfg и model обнуляются немедленно.
4. Тестовый override (_cfg/_model установлены, _cache_loaded_ms=0) → 
   не перезагружает, выставляет timestamp.
"""
import time
from unittest.mock import AsyncMock, patch

import pytest

from services.ml_confirm import MLConfirmGate


def _make_gate_async(**overrides) -> MLConfirmGate:
    """Хелпер для создания гейта без зависимостей/Metrics."""
    gate = MLConfirmGate.__new__(MLConfirmGate)
    gate.mode = "ENFORCE"
    gate.fail_policy = "OPEN"
    gate.champion_key = "cfg:ml_confirm:champion"
    gate.challenger_key = "cfg:ml_confirm:challenger"
    gate.ab_variant = "champion"

    gate._cfg = {}
    gate._model = None
    gate._cache_loaded_ms = 0
    gate._cache_ttl_ms = 60_000
    gate._model_load_error = ""
    gate._cfg_key_used = ""
    gate._cfg_source = ""
    gate._cfg_hash_key = "cfg:ml_confirm:hash"
    gate._metrics_enable = False
    gate._replay_capture = False

    # Overrides
    for k, v in overrides.items():
        setattr(gate, k, v)
    return gate


@pytest.mark.asyncio
async def test_refresh_async_ttl_hit_no_redis_call():
    """Второй вызов в рамках TTL → Redis.get НЕ вызывается."""
    gate = _make_gate_async()
    gate._cache_loaded_ms = int(time.time() * 1000)  # только что загружено

    mock_redis = AsyncMock()
    await gate.refresh_async(mock_redis)

    mock_redis.get.assert_not_called()
    mock_redis.hgetall.assert_not_called()


@pytest.mark.asyncio
async def test_refresh_async_mode_off_clears_state():
    """mode=OFF → немедленно обнуляет стейт."""
    gate = _make_gate_async(
        mode="OFF",
        _cfg={"kind": "util_mh_v1"},
        _model=object(),
    )
    mock_redis = AsyncMock()

    await gate.refresh_async(mock_redis)

    assert gate._cfg == {}
    assert gate._model is None
    mock_redis.get.assert_not_called()


@pytest.mark.asyncio
async def test_refresh_async_test_override_preserved():
    """
    Тестовый override: если _cfg и _model установлены вручную 
    (например в conftest), но timestamp пустой — метод просто
    выставит timestamp без обращения к Redis.
    """
    gate = _make_gate_async(
        _cfg={"kind": "util_mh_v1"},
        _model=object(),
        _cache_loaded_ms=0  # не загружено
    )
    mock_redis = AsyncMock()

    await gate.refresh_async(mock_redis)

    # Пломбирует кеш
    assert gate._cache_loaded_ms is not None
    # Сохраняет стейт
    assert gate._cfg == {"kind": "util_mh_v1"}
    assert gate._model is not None
    # В редис не ходит
    mock_redis.get.assert_not_called()


@pytest.mark.asyncio
async def test_refresh_async_redis_timeout_fail_open():
    """
    Если Redis недоступен (TimeoutError/ConnectionError), 
    refresh_async НЕ крашится, не стирает существующий _cfg / _model (Fail-Open).
    """
    import redis.exceptions

    gate = _make_gate_async()
    gate._cfg = {"kind": "existing_cfg"}
    gate._model = object()

    # Делаем Redis полностью недоступным
    mock_redis = AsyncMock()
    mock_redis.get.side_effect = redis.exceptions.TimeoutError("simulated timeout")
    mock_redis.hgetall.side_effect = redis.exceptions.TimeoutError("simulated timeout")

    try:
        await gate.refresh_async(mock_redis)
    except Exception as e:
        pytest.fail(f"refresh_async crashed on TimeoutError: {e}")

    # ❌ Не должен стереть кэш! Fail-open означает мы работаем по старому
    assert gate._cfg == {"kind": "existing_cfg"}, "Cfg должен сохраниться"
    assert gate._model is not None, "Модель должна сохраниться"


@pytest.mark.asyncio
async def test_refresh_async_redis_empty_fallback():
    """
    Если Redis пуст (get вернул None, hgetall пуст) и кэш тоже пуст,
    gate пишет ошибку 'no_cfg'.
    """
    gate = _make_gate_async()
    mock_redis = AsyncMock()
    mock_redis.get.return_value = None
    mock_redis.hgetall.return_value = {}

    await gate.refresh_async(mock_redis)

    assert gate._model_load_error == "no_cfg"
    assert gate._cfg == {}
