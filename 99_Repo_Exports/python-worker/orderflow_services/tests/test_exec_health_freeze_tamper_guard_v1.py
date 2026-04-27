from __future__ import annotations
from utils.time_utils import get_ny_time_millis

from orderflow_services.exec_health_freeze_tamper_guard_v1 import Guard, should_refreeze


class FakeRedis:
    def __init__(self):
        self.hashes = {
            'cfg:orderflow:exec_health:freeze_control:v1': {
                'manual_override_active': '1',
                'manual_override_action': 'thaw',
                'active_thaw_request_id': 'r1',
                'manual_commit_request_id': 'r1',
                'expected_ack_nonce': 'n1',
                'manual_ack_nonce': 'n1',
                'thaw_request_nonce': 'n1',
                'thaw_prepared_by': 'alice',
                'thaw_approved_by': 'bob',
                'manual_commit_by': 'bob',
                'manual_commit_sig': 'bad',
                'manual_ack_sig': 'bad',
                'last_trigger_ts_ms': '10000',
                'updated_ts_ms': '11000',
            },
            'metrics:exec_health:slo:autoguard:state': {},
            'metrics:exec_health:freeze_tamper_guard:last': {},
        }
        self.values = {}
        self.stream = []

    def hgetall(self, key: str):
        return dict(self.hashes.get(key, {}))

    def hset(self, key: str, mapping):
        self.hashes.setdefault(key, {}).update({str(k): str(v) for k, v in dict(mapping).items()})

    def expire(self, key: str, ttl: int):
        return True

    def set(self, key: str, value: str):
        self.values[key] = value

    def pexpire(self, key: str, ttl: int):
        return True

    def xrevrange(self, key: str, count: int = 10):
        rows = [(eid, payload) for k, eid, payload in self.stream if k == key]
        return list(reversed(rows))[:count]

    def xadd(self, key: str, mapping, maxlen=0):
        eid = f'{len(self.stream)+1}-0'
        self.stream.append((key, eid, {str(k): str(v) for k, v in dict(mapping).items()}))
        return eid


def _guard() -> Guard:
    g = Guard.__new__(Guard)
    g.control_key = 'cfg:orderflow:exec_health:freeze_control:v1'
    g.state_key = 'metrics:exec_health:slo:autoguard:state'
    g.freeze_key = 'cfg:orderflow:exec_health:auto_freeze:v1'
    g.event_stream = 'ops:exec_health:freeze_events:v1'
    g.request_stream = 'ops:exec_health:freeze_requests:v1'
    g.notify_stream = 'notify:telegram'
    g.guard_state_key = 'metrics:exec_health:freeze_tamper_guard:last'
    g.interval_s = 10
    g.cooldown_s = 300
    g.freeze_minutes = 30
    g.event_count = 100
    g.request_count = 100
    g.r = FakeRedis()
    return g


def test_should_refreeze_for_tamper_kinds() -> None:
    # control_request_mismatch → нужна повторная заморозка
    assert should_refreeze(['control_request_mismatch']) is True
    # 'none' → заморозка не нужна
    assert should_refreeze(['none']) is False


def test_guard_refreezes_on_direct_hash_edit_without_request_log() -> None:
    """P10: control hash содержит thaw без backing request log → tamper → автозаморозка."""
    g = _guard()
    out = g.run_once()
    assert out['tamper_active'] == 1
    assert out['refreeze_performed'] == 1
    # Должен быть записан raw freeze key
    assert 'cfg:orderflow:exec_health:auto_freeze:v1' in g.r.values
    # Лейбл autoguard должен быть выставлен в control
    control = g.r.hashes[g.control_key]
    assert control['control_source'] == 'autoguard'
    assert control['manual_ack_required'] == '1'
    # В стриме событий должен быть tamper_refreeze_latch
    assert any(k == 'ops:exec_health:freeze_events:v1' and payload['kind'] == 'tamper_refreeze_latch'
               for k, _, payload in g.r.stream)


def test_guard_no_refreeze_if_no_tamper() -> None:
    """Если control пуст (нет thaw без request log) → tamper_active=0."""
    g = _guard()
    g.r.hashes['cfg:orderflow:exec_health:freeze_control:v1'] = {}
    out = g.run_once()
    # Нет активного thaw в control → нет tamper
    assert out['tamper_active'] == 0
    assert out['refreeze_performed'] == 0


def test_guard_respects_cooldown() -> None:
    """Если cooldown ещё активен → повторная заморозка не выполняется, даже при tamper."""
    g = _guard()
    # Установить cooldown_until_ts_ms в будущем
    import time
    future_ts = get_ny_time_millis() + 999_000
    g.r.hashes['metrics:exec_health:freeze_tamper_guard:last'] = {
        'cooldown_until_ts_ms': str(future_ts),
        'last_refreeze_ts_ms': str(future_ts - 1000),
        'refreeze_total': '1',
    }
    out = g.run_once()
    assert out['tamper_active'] == 1   # tamper всё ещё виден
    assert out['refreeze_performed'] == 0  # но cooldown блокирует
