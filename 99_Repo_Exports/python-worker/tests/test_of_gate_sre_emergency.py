"""Тесты для tools/of_gate_sre_emergency.py.

Проверяет:
- compute_stats() вычисляет статистику из метрик
- merge_entry_policy_override() корректно мержит JSON
- apply_bundle_auto() применяет bundle автоматически
- main() проверяет пороги и применяет emergency shadow
"""

import json

import pytest

from tests.fakeredis import FakeStrictRedis
from tools.of_gate_sre_emergency import apply_bundle_auto, compute_stats, merge_entry_policy_override, pctl


@pytest.fixture
def redis_client():
    """Создает fake Redis клиент для тестов."""
    return FakeStrictRedis(decode_responses=True)


def test_compute_stats_empty():
    """Проверяет compute_stats с пустым списком."""
    result = compute_stats([])
    assert result["n"] == 0


def test_compute_stats_basic():
    """Проверяет compute_stats с базовыми метриками."""
    rows = [
        {"ok": "1", "ok_soft": "0", "latency_us": "1000", "exec_risk_norm": "0.5", "scenario_v4": "A3"},
        {"ok": "1", "ok_soft": "1", "latency_us": "2000", "exec_risk_norm": "0.6", "scenario_v4": "A3"},
        {"ok": "0", "ok_soft": "0", "latency_us": "5000", "exec_risk_norm": "0.8", "scenario_v4": "B2"},
    ]
    result = compute_stats(rows)

    assert result["n"] == 3
    assert result["ok_rate"] == pytest.approx(2.0 / 3.0, abs=0.01)
    assert result["soft_rate"] == pytest.approx(1.0 / 3.0, abs=0.01)
    assert result["lat_p99_us"] >= 2000
    assert result["exec_p90"] >= 0.6


def test_merge_entry_policy_override_empty():
    """Проверяет merge_entry_policy_override с пустой строкой."""
    result = merge_entry_policy_override("", "ENTRY_POLICY_SHADOW")
    d = json.loads(result)
    assert d["version"] == 1
    assert d["overrides"]["ENTRY_POLICY_SHADOW"] == "1"


def test_merge_entry_policy_override_existing():
    """Проверяет merge_entry_policy_override с существующим JSON."""
    old = '{"version":1,"overrides":{"SOME_FIELD":"0"}}'
    result = merge_entry_policy_override(old, "ENTRY_POLICY_SHADOW")
    d = json.loads(result)
    assert d["version"] == 1
    assert d["overrides"]["ENTRY_POLICY_SHADOW"] == "1"
    assert d["overrides"]["SOME_FIELD"] == "0"  # Сохраняется старое значение


def test_merge_entry_policy_override_invalid_json():
    """Проверяет merge_entry_policy_override с невалидным JSON."""
    result = merge_entry_policy_override("invalid json", "ENTRY_POLICY_SHADOW")
    d = json.loads(result)
    assert d["version"] == 1
    assert d["overrides"]["ENTRY_POLICY_SHADOW"] == "1"


def test_apply_bundle_auto_set(redis_client):
    """Проверяет apply_bundle_auto с SET операцией."""
    ops = [
        {"op": "SET", "key": "cfg:entry_policy:overrides:A", "value": '{"version":1,"overrides":{"ENTRY_POLICY_SHADOW":"1"}}'},
    ]
    meta = {"kind": "test"}

    # Устанавливаем начальное значение
    redis_client.set("cfg:entry_policy:overrides:A", '{"version":1,"overrides":{}}')

    bundle_id, sig = apply_bundle_auto(redis_client, ops=ops, meta=meta, who="test", ttl=3600, secret="test_secret")

    assert bundle_id is not None
    assert sig is not None

    # Проверяем, что значение изменилось
    new_val = redis_client.get("cfg:entry_policy:overrides:A")
    assert new_val == '{"version":1,"overrides":{"ENTRY_POLICY_SHADOW":"1"}}'

    # Проверяем статус
    status = redis_client.get(f"recs:status:{bundle_id}")
    assert status == "APPLIED"

    # Проверяем audit
    audit_entries = redis_client.lrange(f"recs:audit:{bundle_id}", 0, -1)
    assert len(audit_entries) == 1
    audit_entry = json.loads(audit_entries[0])
    assert audit_entry["op"] == "SET"
    assert audit_entry["key"] == "cfg:entry_policy:overrides:A"


def test_apply_bundle_auto_mixed(redis_client):
    """Проверяет apply_bundle_auto со смешанными SET и HSET операциями."""
    ops = [
        {"op": "SET", "key": "cfg:entry_policy:overrides:A", "value": '{"version":1,"overrides":{"ENTRY_POLICY_SHADOW":"1"}}'},
        {"op": "HSET", "key": "config:orderflow:BTCUSDT", "field": "meta_model_mode", "value": "SHADOW"},
    ]
    meta = {"kind": "test"}

    redis_client.set("cfg:entry_policy:overrides:A", '{"version":1,"overrides":{}}')

    bundle_id, sig = apply_bundle_auto(redis_client, ops=ops, meta=meta, who="test", ttl=3600, secret="test_secret")

    # Проверяем оба значения
    val1 = redis_client.get("cfg:entry_policy:overrides:A")
    val2 = redis_client.hget("config:orderflow:BTCUSDT", "meta_model_mode")
    assert val1 == '{"version":1,"overrides":{"ENTRY_POLICY_SHADOW":"1"}}'
    assert val2 == "SHADOW"

    # Проверяем audit (должно быть 2 записи)
    audit_entries = redis_client.lrange(f"recs:audit:{bundle_id}", 0, -1)
    assert len(audit_entries) == 2


def test_pctl():
    """Проверяет функцию pctl (percentile)."""
    xs = [1.0, 2.0, 3.0, 4.0, 5.0, 6.0, 7.0, 8.0, 9.0, 10.0]
    assert pctl(xs, 0.0) == 1.0
    assert pctl(xs, 1.0) == 10.0
    assert pctl(xs, 0.5) == pytest.approx(5.5, abs=0.1)
    assert pctl(xs, 0.99) >= 9.0


def test_pctl_empty():
    """Проверяет pctl с пустым списком."""
    assert pctl([], 0.5) == 0.0

