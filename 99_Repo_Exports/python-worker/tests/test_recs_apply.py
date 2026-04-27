"""
Тесты для применения и отката bundle рекомендаций.

Проверяет:
- apply_bundle() делает HSET и пишет audit
- rollback_bundle() возвращает значения
- Идемпотентность применения
- Защита от повторного применения
"""
import json
import pytest
# Используем локальный FakeStrictRedis из tests/fakeredis.py
from tests.fakeredis import FakeStrictRedis
from core.recs_contract import RecOp, RecBundle
from services.recs_store import (
    store_bundle, get_bundle, get_status, set_status, append_audit,
    BUNDLE_KEY, STATUS_KEY, AUDIT_KEY
)


@pytest.fixture
def redis_client():
    """Создает fake Redis клиент для тестов."""
    # Используем decode_responses=True для совместимости с recs_callback_worker
    return FakeStrictRedis(decode_responses=True)


def test_apply_bundle_hset_and_audit(redis_client):
    """Проверяет, что apply_bundle() делает HSET и пишет audit."""
    from services.recs_callback_worker import _apply_bundle as apply_bundle
    
    # Создаем bundle с одной операцией
    bundle_id = "test123"
    bundle = RecBundle(
        id=bundle_id,
        created_ms=1000000,
        ttl_sec=3600,
        who="test",
        ops=[
            RecOp(op="HSET", key="config:orderflow:BTCUSDT", field="w_exec_risk", value="0.200")
        ],
        meta={}
    )
    
    # Сохраняем bundle
    store_bundle(redis_client, bundle)
    
    # Устанавливаем начальное значение в Redis Hash
    redis_client.hset("config:orderflow:BTCUSDT", "w_exec_risk", "0.180")
    
    # Применяем bundle
    who = {"timestamp": "", "chat_id": "123", "user_id": "456", "username": "test_user"}
    result = apply_bundle(redis_client, bundle_id, who)
    
    assert result == "applied", f"Ожидался статус 'applied', получен '{result}'"
    
    # Проверяем, что значение изменилось
    new_val = redis_client.hget("config:orderflow:BTCUSDT", "w_exec_risk")
    assert new_val == "0.200", "Значение должно быть обновлено"
    
    # Проверяем статус
    status = get_status(redis_client, bundle_id)
    assert status == "APPLIED", "Статус должен быть APPLIED"
    
    # Проверяем audit log
    audit_entries = redis_client.lrange(AUDIT_KEY + bundle_id, 0, -1)
    assert len(audit_entries) == 1, "Должна быть одна запись в audit"
    
    audit_entry = json.loads(audit_entries[0])
    assert audit_entry["key"] == "config:orderflow:BTCUSDT"
    assert audit_entry["field"] == "w_exec_risk"
    assert audit_entry["old"] == "0.180", "Старое значение должно быть сохранено"
    assert audit_entry["new"] == "0.200", "Новое значение должно быть сохранено"


def test_apply_bundle_multiple_ops(redis_client):
    """Проверяет применение bundle с несколькими операциями."""
    from services.recs_callback_worker import _apply_bundle as apply_bundle
    
    bundle_id = "test456"
    bundle = RecBundle(
        id=bundle_id,
        created_ms=1000000,
        ttl_sec=3600,
        who="test",
        ops=[
            RecOp(op="HSET", key="config:orderflow:BTCUSDT", field="w_exec_risk", value="0.200"),
            RecOp(op="HSET", key="config:orderflow:BTCUSDT", field="exec_risk_ref_bps", value="9.0"),
        ],
        meta={}
    )
    
    store_bundle(redis_client, bundle)
    redis_client.hset("config:orderflow:BTCUSDT", "w_exec_risk", "0.180")
    redis_client.hset("config:orderflow:BTCUSDT", "exec_risk_ref_bps", "10.0")
    
    who = {"timestamp": "", "chat_id": "123", "user_id": "456", "username": "test_user"}
    result = apply_bundle(redis_client, bundle_id, who)
    assert result == "applied"
    
    # Проверяем оба значения
    val1 = redis_client.hget("config:orderflow:BTCUSDT", "w_exec_risk")
    val2 = redis_client.hget("config:orderflow:BTCUSDT", "exec_risk_ref_bps")
    assert val1 == "0.200"
    assert val2 == "9.0"
    
    # Проверяем audit (должно быть 2 записи)
    audit_entries = redis_client.lrange(AUDIT_KEY + bundle_id, 0, -1)
    assert len(audit_entries) == 2


def test_apply_bundle_idempotent(redis_client):
    """Проверяет идемпотентность применения (повторный apply не должен пере-применять)."""
    from services.recs_callback_worker import _apply_bundle as apply_bundle
    
    bundle_id = "test789"
    bundle = RecBundle(
        id=bundle_id,
        created_ms=1000000,
        ttl_sec=3600,
        who="test",
        ops=[
            RecOp(op="HSET", key="config:orderflow:BTCUSDT", field="w_exec_risk", value="0.200")
        ],
        meta={}
    )
    
    store_bundle(redis_client, bundle)
    redis_client.hset("config:orderflow:BTCUSDT", "w_exec_risk", "0.180")
    
    who = {"chat_id": "123", "user_id": "456", "username": "test_user"}
    # Первое применение
    result1 = apply_bundle(redis_client, bundle_id, who)
    assert result1 == "applied"
    
    # Второе применение (должно вернуть already_applied)
    result2 = apply_bundle(redis_client, bundle_id, who)
    assert result2 == "already_applied", "Повторное применение должно быть отклонено"
    
    # Значение не должно измениться
    val = redis_client.hget("config:orderflow:BTCUSDT", "w_exec_risk")
    assert val == "0.200"


def test_rollback_bundle_restores_values(redis_client):
    """Проверяет, что rollback_bundle() возвращает старые значения."""
    from services.recs_callback_worker import _apply_bundle as apply_bundle, _rollback_bundle as rollback_bundle
    
    bundle_id = "test_rollback"
    bundle = RecBundle(
        id=bundle_id,
        created_ms=1000000,
        ttl_sec=3600,
        who="test",
        ops=[
            RecOp(op="HSET", key="config:orderflow:BTCUSDT", field="w_exec_risk", value="0.200")
        ],
        meta={}
    )
    
    store_bundle(redis_client, bundle)
    
    # Устанавливаем начальное значение
    old_val = "0.180"
    redis_client.hset("config:orderflow:BTCUSDT", "w_exec_risk", old_val)
    
    who = {"chat_id": "123", "user_id": "456", "username": "test_user"}
    # Применяем bundle
    apply_result = apply_bundle(redis_client, bundle_id, who)
    assert apply_result == "applied"
    
    # Проверяем, что значение изменилось
    new_val = redis_client.hget("config:orderflow:BTCUSDT", "w_exec_risk")
    assert new_val == "0.200"
    
    # Откатываем
    rollback_result = rollback_bundle(redis_client, bundle_id, who)
    assert rollback_result == "rolled_back"
    
    # Проверяем, что значение восстановлено
    restored_val = redis_client.hget("config:orderflow:BTCUSDT", "w_exec_risk")
    assert restored_val == old_val
    
    # Проверяем статус
    status = get_status(redis_client, bundle_id)
    assert status == "ROLLED_BACK"


def test_rollback_bundle_not_applied(redis_client):
    """Проверяет, что rollback не работает для не примененного bundle."""
    from services.recs_callback_worker import _rollback_bundle as rollback_bundle
    
    bundle_id = "test_not_applied"
    bundle = RecBundle(
        id=bundle_id,
        created_ms=1000000,
        ttl_sec=3600,
        who="test",
        ops=[
            RecOp(op="HSET", key="config:orderflow:BTCUSDT", field="w_exec_risk", value="0.200")
        ],
        meta={}
    )
    
    store_bundle(redis_client, bundle)
    # Не применяем bundle (статус остается PENDING)
    
    who = {"chat_id": "123", "user_id": "456", "username": "test_user"}
    result = rollback_bundle(redis_client, bundle_id, who)
    assert result.startswith("not_applied"), "Rollback не должен работать для не примененного bundle"


def test_apply_bundle_missing_bundle(redis_client):
    """Проверяет обработку отсутствующего bundle."""
    from services.recs_callback_worker import _apply_bundle as apply_bundle
    
    who = {"chat_id": "123", "user_id": "456", "username": "test_user"}
    result = apply_bundle(redis_client, "nonexistent", who)
    assert result == "missing_bundle"


def test_apply_bundle_rejected(redis_client):
    """Проверяет, что отклоненный bundle не может быть применен."""
    from services.recs_callback_worker import _apply_bundle as apply_bundle
    
    bundle_id = "test_rejected"
    bundle = RecBundle(
        id=bundle_id,
        created_ms=1000000,
        ttl_sec=3600,
        who="test",
        ops=[
            RecOp(op="HSET", key="config:orderflow:BTCUSDT", field="w_exec_risk", value="0.200")
        ],
        meta={}
    )
    
    store_bundle(redis_client, bundle)
    set_status(redis_client, bundle_id, "REJECTED", bundle.ttl_sec)
    
    who = {"timestamp": "", "chat_id": "123", "user_id": "456", "username": "test_user"}
    result = apply_bundle(redis_client, bundle_id, who)
    assert result == "already_rejected", "Отклоненный bundle не должен применяться"


def test_preview_bundle_shows_diff(redis_client):
    """Проверяет, что preview_bundle показывает diff old→new."""
    from services.recs_callback_worker import _preview_bundle as preview_bundle
    
    bundle_id = "test_preview"
    bundle = RecBundle(
        id=bundle_id,
        created_ms=1000000,
        ttl_sec=3600,
        who="test",
        ops=[
            RecOp(op="HSET", key="config:orderflow:BTCUSDT", field="w_exec_risk", value="0.200"),
            RecOp(op="HSET", key="config:orderflow:ETHUSDT", field="of_score_min", value="0.700"),
        ],
        meta={}
    )
    
    store_bundle(redis_client, bundle)
    
    # Устанавливаем начальные значения
    redis_client.hset("config:orderflow:BTCUSDT", "w_exec_risk", "0.180")
    redis_client.hset("config:orderflow:ETHUSDT", "of_score_min", "0.650")
    
    who = {"timestamp": "", "chat_id": "123", "user_id": "456", "username": "test_user"}
    result = preview_bundle(redis_client, bundle_id, who)
    
    assert result == "previewed", f"Ожидался статус 'previewed', получен '{result}'"
    
    # Проверяем статус
    status = get_status(redis_client, bundle_id)
    assert status == "PREVIEWED", "Статус должен быть PREVIEWED"
    
    # Проверяем, что значения НЕ изменились (preview не применяет изменения)
    val1 = redis_client.hget("config:orderflow:BTCUSDT", "w_exec_risk")
    val2 = redis_client.hget("config:orderflow:ETHUSDT", "of_score_min")
    assert val1 == "0.180", "Значение не должно измениться после preview"
    assert val2 == "0.650", "Значение не должно измениться после preview"
    
    # Проверяем audit log (должна быть запись о preview)
    audit_entries = redis_client.lrange(AUDIT_KEY + bundle_id, 0, -1)
    assert len(audit_entries) >= 1, "Должна быть запись в audit о preview"
    preview_entry = json.loads(audit_entries[-1])
    assert preview_entry.get("previewed") is True, "Audit должен содержать запись о preview"


def test_preview_confirm_flow(redis_client):
    """Проверяет полный flow: preview → confirm → rollback."""
    from services.recs_callback_worker import (
        _preview_bundle as preview_bundle,
        _apply_bundle as apply_bundle,
        _rollback_bundle as rollback_bundle
    )
    
    bundle_id = "test_flow"
    bundle = RecBundle(
        id=bundle_id,
        created_ms=1000000,
        ttl_sec=3600,
        who="test",
        ops=[
            RecOp(op="HSET", key="config:orderflow:BTCUSDT", field="w_exec_risk", value="0.200")
        ],
        meta={}
    )
    
    store_bundle(redis_client, bundle)
    redis_client.hset("config:orderflow:BTCUSDT", "w_exec_risk", "0.180")
    
    who = {"timestamp": "", "chat_id": "123", "user_id": "456", "username": "test_user"}
    
    # 1. Preview
    preview_result = preview_bundle(redis_client, bundle_id, who)
    assert preview_result == "previewed"
    status = get_status(redis_client, bundle_id)
    assert status == "PREVIEWED"
    # Значение не изменилось
    assert redis_client.hget("config:orderflow:BTCUSDT", "w_exec_risk") == "0.180"
    
    # 2. Confirm
    confirm_result = apply_bundle(redis_client, bundle_id, who)
    assert confirm_result == "applied"
    status = get_status(redis_client, bundle_id)
    assert status == "APPLIED"
    # Значение изменилось
    assert redis_client.hget("config:orderflow:BTCUSDT", "w_exec_risk") == "0.200"
    
    # 3. Rollback
    rollback_result = rollback_bundle(redis_client, bundle_id, who)
    assert rollback_result == "rolled_back"
    status = get_status(redis_client, bundle_id)
    assert status == "ROLLED_BACK"
    # Значение восстановлено
    assert redis_client.hget("config:orderflow:BTCUSDT", "w_exec_risk") == "0.180"


def test_preview_cancel_flow(redis_client):
    """Проверяет flow: preview → cancel → preview снова."""
    from services.recs_callback_worker import (
        _preview_bundle as preview_bundle,
        _cancel_bundle as cancel_bundle
    )
    
    bundle_id = "test_cancel"
    bundle = RecBundle(
        id=bundle_id,
        created_ms=1000000,
        ttl_sec=3600,
        who="test",
        ops=[
            RecOp(op="HSET", key="config:orderflow:BTCUSDT", field="w_exec_risk", value="0.200")
        ],
        meta={}
    )
    
    store_bundle(redis_client, bundle)
    
    who = {"timestamp": "", "chat_id": "123", "user_id": "456", "username": "test_user"}
    
    # 1. Preview
    preview_result = preview_bundle(redis_client, bundle_id, who)
    assert preview_result == "previewed"
    status = get_status(redis_client, bundle_id)
    assert status == "PREVIEWED"
    
    # 2. Cancel (используем функцию cancel_bundle)
    cancel_result = cancel_bundle(redis_client, bundle_id, who)
    assert cancel_result == "pending"
    status = get_status(redis_client, bundle_id)
    assert status == "PENDING"
    
    # 3. Preview снова (должно работать)
    preview_result2 = preview_bundle(redis_client, bundle_id, who)
    assert preview_result2 == "previewed"
    status = get_status(redis_client, bundle_id)
    assert status == "PREVIEWED"


def test_preview_rejected_bundle(redis_client):
    """Проверяет, что preview не работает для отклоненного bundle."""
    from services.recs_callback_worker import _preview_bundle as preview_bundle
    
    bundle_id = "test_rejected_preview"
    bundle = RecBundle(
        id=bundle_id,
        created_ms=1000000,
        ttl_sec=3600,
        who="test",
        ops=[
            RecOp(op="HSET", key="config:orderflow:BTCUSDT", field="w_exec_risk", value="0.200")
        ],
        meta={}
    )
    
    store_bundle(redis_client, bundle)
    set_status(redis_client, bundle_id, "REJECTED", bundle.ttl_sec)
    
    who = {"timestamp": "", "chat_id": "123", "user_id": "456", "username": "test_user"}
    result = preview_bundle(redis_client, bundle_id, who)
    assert result.startswith("not_available"), "Preview не должен работать для отклоненного bundle"


def test_allowed_function(redis_client):
    """Проверяет функцию _allowed для проверки allowlist."""
    import os
    import importlib
    from services import recs_callback_worker
    
    # Сохраняем старые значения
    old_user_ids = os.environ.get("RECS_ALLOWED_USER_IDS", "")
    old_chat_ids = os.environ.get("RECS_ALLOWED_CHAT_IDS", "")
    
    try:
        # Тест 1: allowlist пустой - разрешаем всем
        os.environ["RECS_ALLOWED_USER_IDS"] = ""
        os.environ["RECS_ALLOWED_CHAT_IDS"] = ""
        # Перезагружаем модуль для обновления глобальных переменных
        importlib.reload(recs_callback_worker)
        who1 = {"user_id": "123", "chat_id": "456", "username": "test"}
        assert recs_callback_worker._allowed(who1) is True, "При пустом allowlist должны разрешать всем"
        
        # Тест 2: user_id в allowlist
        os.environ["RECS_ALLOWED_USER_IDS"] = "123,789"
        os.environ["RECS_ALLOWED_CHAT_IDS"] = ""
        importlib.reload(recs_callback_worker)
        who2 = {"user_id": "123", "chat_id": "456", "username": "test"}
        assert recs_callback_worker._allowed(who2) is True, "User ID в allowlist должен быть разрешен"
        
        # Тест 3: user_id не в allowlist
        who3 = {"user_id": "999", "chat_id": "456", "username": "test"}
        assert recs_callback_worker._allowed(who3) is False, "User ID не в allowlist должен быть отклонен"
        
        # Тест 4: chat_id в allowlist
        os.environ["RECS_ALLOWED_USER_IDS"] = ""
        os.environ["RECS_ALLOWED_CHAT_IDS"] = "456,789"
        importlib.reload(recs_callback_worker)
        who4 = {"user_id": "123", "chat_id": "456", "username": "test"}
        assert recs_callback_worker._allowed(who4) is True, "Chat ID в allowlist должен быть разрешен"
        
        # Тест 5: оба в allowlist
        os.environ["RECS_ALLOWED_USER_IDS"] = "123"
        os.environ["RECS_ALLOWED_CHAT_IDS"] = "456"
        importlib.reload(recs_callback_worker)
        who5 = {"user_id": "123", "chat_id": "456", "username": "test"}
        assert recs_callback_worker._allowed(who5) is True, "Оба ID в allowlist должны быть разрешены"
        
        # Тест 6: один не в allowlist
        who6 = {"user_id": "123", "chat_id": "999", "username": "test"}
        assert recs_callback_worker._allowed(who6) is False, "Если chat_id не в allowlist, должен быть отклонен"
        
    finally:
        # Восстанавливаем старые значения
        if old_user_ids:
            os.environ["RECS_ALLOWED_USER_IDS"] = old_user_ids
        else:
            os.environ.pop("RECS_ALLOWED_USER_IDS", None)
        if old_chat_ids:
            os.environ["RECS_ALLOWED_CHAT_IDS"] = old_chat_ids
        else:
            os.environ.pop("RECS_ALLOWED_CHAT_IDS", None)
        # Перезагружаем модуль для восстановления
        importlib.reload(recs_callback_worker)


def test_cancel_bundle(redis_client):
    """Проверяет функцию _cancel_bundle."""
    from services.recs_callback_worker import _cancel_bundle as cancel_bundle
    
    bundle_id = "test_cancel_bundle"
    bundle = RecBundle(
        id=bundle_id,
        created_ms=1000000,
        ttl_sec=3600,
        who="test",
        ops=[
            RecOp(op="HSET", key="config:orderflow:BTCUSDT", field="w_exec_risk", value="0.200")
        ],
        meta={}
    )
    
    store_bundle(redis_client, bundle)
    set_status(redis_client, bundle_id, "PREVIEWED", bundle.ttl_sec)
    
    who = {"timestamp": "", "chat_id": "123", "user_id": "456", "username": "test_user"}
    result = cancel_bundle(redis_client, bundle_id, who)
    
    assert result == "pending", f"Ожидался статус 'pending', получен '{result}'"
    status = get_status(redis_client, bundle_id)
    assert status == "PENDING", "Статус должен быть PENDING"


def test_cancel_bundle_not_cancelable(redis_client):
    """Проверяет, что cancel не работает для примененного bundle."""
    from services.recs_callback_worker import _cancel_bundle as cancel_bundle
    
    bundle_id = "test_cancel_not_cancelable"
    bundle = RecBundle(
        id=bundle_id,
        created_ms=1000000,
        ttl_sec=3600,
        who="test",
        ops=[
            RecOp(op="HSET", key="config:orderflow:BTCUSDT", field="w_exec_risk", value="0.200")
        ],
        meta={}
    )
    
    store_bundle(redis_client, bundle)
    set_status(redis_client, bundle_id, "APPLIED", bundle.ttl_sec)
    
    who = {"timestamp": "", "chat_id": "123", "user_id": "456", "username": "test_user"}
    result = cancel_bundle(redis_client, bundle_id, who)
    
    assert result.startswith("not_cancelable"), "Cancel не должен работать для примененного bundle"


def test_reject_bundle(redis_client):
    """Проверяет функцию _reject_bundle."""
    from services.recs_callback_worker import _reject_bundle as reject_bundle
    
    bundle_id = "test_reject_bundle"
    bundle = RecBundle(
        id=bundle_id,
        created_ms=1000000,
        ttl_sec=3600,
        who="test",
        ops=[
            RecOp(op="HSET", key="config:orderflow:BTCUSDT", field="w_exec_risk", value="0.200")
        ],
        meta={}
    )
    
    store_bundle(redis_client, bundle)
    
    who = {"timestamp": "", "chat_id": "123", "user_id": "456", "username": "test_user"}
    result = reject_bundle(redis_client, bundle_id, who)
    
    assert result == "rejected", f"Ожидался статус 'rejected', получен '{result}'"
    status = get_status(redis_client, bundle_id)
    assert status == "REJECTED", "Статус должен быть REJECTED"


def test_reject_bundle_not_rejectable(redis_client):
    """Проверяет, что reject не работает для примененного bundle."""
    from services.recs_callback_worker import _reject_bundle as reject_bundle
    
    bundle_id = "test_reject_not_rejectable"
    bundle = RecBundle(
        id=bundle_id,
        created_ms=1000000,
        ttl_sec=3600,
        who="test",
        ops=[
            RecOp(op="HSET", key="config:orderflow:BTCUSDT", field="w_exec_risk", value="0.200")
        ],
        meta={}
    )
    
    store_bundle(redis_client, bundle)
    set_status(redis_client, bundle_id, "APPLIED", bundle.ttl_sec)
    
    who = {"timestamp": "", "chat_id": "123", "user_id": "456", "username": "test_user"}
    result = reject_bundle(redis_client, bundle_id, who)
    
    assert result.startswith("not_rejectable"), "Reject не должен работать для примененного bundle"

