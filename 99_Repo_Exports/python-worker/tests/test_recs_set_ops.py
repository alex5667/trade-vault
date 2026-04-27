"""Тесты для поддержки SET операций в recs_callback_worker.

Проверяет:
- apply_bundle() поддерживает SET операции (кроме HSET)
- preview_bundle() показывает SET операции
- rollback_bundle() восстанавливает SET операции
- Смешанные HSET и SET операции
"""

import json
import pytest
from tests.fakeredis import FakeStrictRedis
from core.recs_contract import RecOp, RecBundle
from services.recs_store import (
    store_bundle, get_bundle, get_status, set_status, append_audit,
    BUNDLE_KEY, STATUS_KEY, AUDIT_KEY
)


@pytest.fixture
def redis_client():
    """Создает fake Redis клиент для тестов."""
    return FakeStrictRedis(decode_responses=True)


def test_apply_bundle_set_operation(redis_client):
    """Проверяет, что apply_bundle() поддерживает SET операции."""
    from services.recs_callback_worker import _apply_bundle as apply_bundle
    
    bundle_id = "test_set"
    # Используем dict формат для SET операций (RecOp не поддерживает SET без field)
    bundle_dict = {
        "id": bundle_id,
        "created_ms": 1000000,
        "ttl_sec": 3600,
        "who": "test",
        "ops": [
            {"op": "SET", "key": "cfg:entry_policy:overrides:A", "value": '{"version":1,"overrides":{"ENTRY_POLICY_SHADOW":"1"}}'}
        ],
        "meta": {}
    }
    
    redis_client.set(BUNDLE_KEY + bundle_id, json.dumps(bundle_dict))
    redis_client.set(STATUS_KEY + bundle_id, "PREVIEWED", ex=3600)
    
    # Устанавливаем начальное значение
    redis_client.set("cfg:entry_policy:overrides:A", '{"version":1,"overrides":{}}')
    
    who = {"timestamp": "", "chat_id": "123", "user_id": "456", "username": "test_user"}
    result = apply_bundle(redis_client, bundle_id, who)
    
    assert result == "applied", f"Ожидался статус 'applied', получен '{result}'"
    
    # Проверяем, что значение изменилось
    new_val = redis_client.get("cfg:entry_policy:overrides:A")
    assert new_val == '{"version":1,"overrides":{"ENTRY_POLICY_SHADOW":"1"}}', "Значение должно быть обновлено"
    
    # Проверяем статус
    status = get_status(redis_client, bundle_id)
    assert status == "APPLIED", "Статус должен быть APPLIED"
    
    # Проверяем audit log
    audit_entries = redis_client.lrange(AUDIT_KEY + bundle_id, 0, -1)
    assert len(audit_entries) >= 1, "Должна быть запись в audit"
    
    audit_entry = json.loads(audit_entries[0])
    assert audit_entry["op"] == "SET", "Audit должен содержать op=SET"
    assert audit_entry["key"] == "cfg:entry_policy:overrides:A"
    assert audit_entry["old"] == '{"version":1,"overrides":{}}', "Старое значение должно быть сохранено"
    assert audit_entry["new"] == '{"version":1,"overrides":{"ENTRY_POLICY_SHADOW":"1"}}', "Новое значение должно быть сохранено"


def test_apply_bundle_mixed_hset_and_set(redis_client):
    """Проверяет применение bundle с смешанными HSET и SET операциями."""
    from services.recs_callback_worker import _apply_bundle as apply_bundle
    
    bundle_id = "test_mixed"
    bundle_dict = {
        "id": bundle_id,
        "created_ms": 1000000,
        "ttl_sec": 3600,
        "who": "test",
        "ops": [
            {"op": "HSET", "key": "config:orderflow:BTCUSDT", "field": "w_exec_risk", "value": "0.200"},
            {"op": "SET", "key": "cfg:entry_policy:overrides:A", "value": '{"version":1,"overrides":{"ENTRY_POLICY_SHADOW":"1"}}'},
        ],
        "meta": {}
    }
    
    redis_client.set(BUNDLE_KEY + bundle_id, json.dumps(bundle_dict))
    redis_client.set(STATUS_KEY + bundle_id, "PREVIEWED", ex=3600)
    
    # Устанавливаем начальные значения
    redis_client.hset("config:orderflow:BTCUSDT", "w_exec_risk", "0.180")
    redis_client.set("cfg:entry_policy:overrides:A", '{"version":1,"overrides":{}}')
    
    who = {"timestamp": "", "chat_id": "123", "user_id": "456", "username": "test_user"}
    result = apply_bundle(redis_client, bundle_id, who)
    assert result == "applied"
    
    # Проверяем оба значения
    val1 = redis_client.hget("config:orderflow:BTCUSDT", "w_exec_risk")
    val2 = redis_client.get("cfg:entry_policy:overrides:A")
    assert val1 == "0.200"
    assert val2 == '{"version":1,"overrides":{"ENTRY_POLICY_SHADOW":"1"}}'
    
    # Проверяем audit (должно быть 2 записи)
    audit_entries = redis_client.lrange(AUDIT_KEY + bundle_id, 0, -1)
    assert len(audit_entries) == 2, f"Ожидалось 2 записи в audit, получено {len(audit_entries)}"


def test_preview_bundle_set_operation(redis_client):
    """Проверяет, что preview_bundle показывает SET операции."""
    from services.recs_callback_worker import _preview_bundle as preview_bundle
    
    bundle_id = "test_preview_set"
    bundle_dict = {
        "id": bundle_id,
        "created_ms": 1000000,
        "ttl_sec": 3600,
        "who": "test",
        "ops": [
            {"op": "SET", "key": "cfg:entry_policy:overrides:A", "value": '{"version":1,"overrides":{"ENTRY_POLICY_SHADOW":"1"}}'}
        ],
        "meta": {}
    }
    
    redis_client.set(BUNDLE_KEY + bundle_id, json.dumps(bundle_dict))
    redis_client.set("cfg:entry_policy:overrides:A", '{"version":1,"overrides":{}}')
    
    who = {"timestamp": "", "chat_id": "123", "user_id": "456", "username": "test_user"}
    result = preview_bundle(redis_client, bundle_id, who)
    
    assert result == "previewed", f"Ожидался статус 'previewed', получен '{result}'"
    
    # Проверяем, что значение НЕ изменилось (preview не применяет изменения)
    val = redis_client.get("cfg:entry_policy:overrides:A")
    assert val == '{"version":1,"overrides":{}}', "Значение не должно измениться после preview"


def test_rollback_bundle_set_operation(redis_client):
    """Проверяет, что rollback_bundle восстанавливает SET операции."""
    from services.recs_callback_worker import _apply_bundle as apply_bundle, _rollback_bundle as rollback_bundle
    
    bundle_id = "test_rollback_set"
    bundle_dict = {
        "id": bundle_id,
        "created_ms": 1000000,
        "ttl_sec": 3600,
        "who": "test",
        "ops": [
            {"op": "SET", "key": "cfg:entry_policy:overrides:A", "value": '{"version":1,"overrides":{"ENTRY_POLICY_SHADOW":"1"}}'}
        ],
        "meta": {}
    }
    
    redis_client.set(BUNDLE_KEY + bundle_id, json.dumps(bundle_dict))
    redis_client.set(STATUS_KEY + bundle_id, "PREVIEWED", ex=3600)
    
    # Устанавливаем начальное значение
    old_val = '{"version":1,"overrides":{}}'
    redis_client.set("cfg:entry_policy:overrides:A", old_val)
    
    who = {"timestamp": "", "chat_id": "123", "user_id": "456", "username": "test_user"}
    # Применяем bundle
    apply_result = apply_bundle(redis_client, bundle_id, who)
    assert apply_result == "applied"
    
    # Проверяем, что значение изменилось
    new_val = redis_client.get("cfg:entry_policy:overrides:A")
    assert new_val == '{"version":1,"overrides":{"ENTRY_POLICY_SHADOW":"1"}}'
    
    # Откатываем
    rollback_result = rollback_bundle(redis_client, bundle_id, who)
    assert rollback_result == "rolled_back"
    
    # Проверяем, что значение восстановлено
    restored_val = redis_client.get("cfg:entry_policy:overrides:A")
    assert restored_val == old_val
    
    # Проверяем статус
    status = get_status(redis_client, bundle_id)
    assert status == "ROLLED_BACK"


def test_rollback_bundle_set_operation_null_old(redis_client):
    """Проверяет rollback SET операции когда старое значение было None (ключ не существовал)."""
    from services.recs_callback_worker import _apply_bundle as apply_bundle, _rollback_bundle as rollback_bundle
    
    bundle_id = "test_rollback_set_null"
    bundle_dict = {
        "id": bundle_id,
        "created_ms": 1000000,
        "ttl_sec": 3600,
        "who": "test",
        "ops": [
            {"op": "SET", "key": "cfg:entry_policy:overrides:A", "value": '{"version":1,"overrides":{"ENTRY_POLICY_SHADOW":"1"}}'}
        ],
        "meta": {}
    }
    
    redis_client.set(BUNDLE_KEY + bundle_id, json.dumps(bundle_dict))
    redis_client.set(STATUS_KEY + bundle_id, "PREVIEWED", ex=3600)
    
    # Ключ не существует (None)
    assert redis_client.get("cfg:entry_policy:overrides:A") is None
    
    who = {"timestamp": "", "chat_id": "123", "user_id": "456", "username": "test_user"}
    # Применяем bundle
    apply_result = apply_bundle(redis_client, bundle_id, who)
    assert apply_result == "applied"
    
    # Проверяем, что значение установлено
    new_val = redis_client.get("cfg:entry_policy:overrides:A")
    assert new_val == '{"version":1,"overrides":{"ENTRY_POLICY_SHADOW":"1"}}'
    
    # Откатываем (должен удалить ключ, т.к. old_null=1)
    rollback_result = rollback_bundle(redis_client, bundle_id, who)
    assert rollback_result == "rolled_back"
    
    # Проверяем, что ключ удален
    restored_val = redis_client.get("cfg:entry_policy:overrides:A")
    assert restored_val is None, "Ключ должен быть удален при rollback если old_null=1"

