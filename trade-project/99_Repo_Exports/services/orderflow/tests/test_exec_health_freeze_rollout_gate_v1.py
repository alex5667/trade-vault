from services.orderflow.exec_health_freeze_rollout_gate import (
    assert_rollout_gate_open,
    get_rollout_gate_state,
    manual_ack_rollout_gate,
    update_rollout_gate_from_report,
)


class FakeRedis:
    def __init__(self):
        self.kv = {}
        self.hashes = {}
        self.events = []

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value):
        self.kv[key] = str(value)
        return 'OK'

    def delete(self, key):
        self.kv.pop(key, None)
        return 1

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def hset(self, key, mapping=None):
        self.hashes.setdefault(key, {}).update({str(k): str(v) for k, v in (mapping or {}).items()})
        return 1

    def expire(self, key, ttl):
        return 1

    def xadd(self, key, mapping, maxlen=None):
        self.events.append((key, dict(mapping)))
        return f'{len(self.events)}-0'


def test_failed_report_latches_gate_until_manual_ack():
    r = FakeRedis()
    report = {
        'ts_ms': 1234,
        'host': 'host-a',
        'ok': False,
        'cases': [
            {'service': 'exec_health_freeze_override_v1', 'scenario': 'reconnect-both', 'ok': False, 'check_reason': 'repairable_reconnect_failed'}
        ],
    }
    st = update_rollout_gate_from_report(r, report=report, report_path='/tmp/report.json', ops_event_id='1-0', telegram_event_id='2-0')
    assert st['active'] is True
    assert 'cfg:orderflow:exec_health:reconnect_smoke:rollout_gate:v1' == st['gate_key']
    assert 'nightly_reconnect_smoke_failed' in r.kv[st['gate_key']]
    try:
        assert_rollout_gate_open(r, purpose='unit-test')
    except RuntimeError as exc:
        assert 'nightly_reconnect_smoke_failed' in str(exc)
    else:
        raise AssertionError('expected rollout gate runtime error')

    ack = manual_ack_rollout_gate(r, operator='alice', reason='investigated', ticket='INC-1')
    assert ack['active'] is False
    assert st['gate_key'] not in r.kv
    assert r.events[-1][1]['kind'] == 'exec_health_reconnect_nightly_rollout_gate_ack'


def test_success_does_not_clear_existing_unacked_gate():
    r = FakeRedis()
    r.kv['cfg:orderflow:exec_health:reconnect_smoke:rollout_gate:v1'] = '{"reason":"nightly_reconnect_smoke_failed","ts_ms":1}'
    r.hashes['metrics:exec_health:freeze_reconnect_smoke:gate:last'] = {'gate_active': '1', 'last_fail_ts_ms': '1'}
    report = {'ts_ms': 2222, 'host': 'host-b', 'ok': True, 'cases': []}
    st = update_rollout_gate_from_report(r, report=report)
    assert st['active'] is True
    assert get_rollout_gate_state(r)['active'] is True
