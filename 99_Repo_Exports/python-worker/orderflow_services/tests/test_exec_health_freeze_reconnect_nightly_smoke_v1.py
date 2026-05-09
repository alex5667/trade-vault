from types import SimpleNamespace

import orderflow_services.exec_health_freeze_reconnect_nightly_smoke_v1 as mod


class FakeHarness:
    def __init__(self, redis_url, *, service, wrong_user_url=''):
        self.redis_url = redis_url
        self.service = service
        self.wrong_user_url = wrong_user_url

    def run_repairable(self, mode):
        expected = mod.get_expected_service(self.service)
        return {
            'ok': True,
            'recovered': True,
            'event_id': '1-0',
            'after_entry': {'name': expected.client_name, 'lib-name': expected.lib_name},
            'state': {'recovery_total': '1'},
        }

    def run_wrong_user(self):
        return {
            'ok': False,
            'unexpected_success': False,
            'error': 'wrong_user violation',
            'state': {'last_result_ok': '0'},
        }


class FailingHarness(FakeHarness):
    def run_repairable(self, mode):
        out = super().run_repairable(mode)
        out['after_entry']['lib-name'] = 'bad-lib'
        return out


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

    def expire(self, key, ttl):
        return 1

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def hset(self, key, mapping=None):
        self.hashes.setdefault(key, {}).update({str(k): str(v) for k, v in (mapping or {}).items()})
        return 1

    def xadd(self, key, mapping, maxlen=None):
        self.events.append((key, dict(mapping)))
        return f'{len(self.events)}-0'


class RedisFactory:
    def __init__(self, fake):
        self.fake = fake

    def from_url(self, url, decode_responses=True):
        return self.fake


def _redis_ns(fake):
    return SimpleNamespace(Redis=SimpleNamespace(from_url=staticmethod(RedisFactory(fake).from_url)))


def test_nightly_smoke_runner_success(monkeypatch, tmp_path):
    fake = FakeRedis()
    monkeypatch.setattr(mod, 'ChaosHarness', FakeHarness)
    monkeypatch.setattr(mod, 'redis', _redis_ns(fake))
    monkeypatch.setenv('REDIS_URL', 'redis://writer@unit/0')
    monkeypatch.setenv('EXEC_HEALTH_REDIS_AUDIT_URL', 'redis://audit@unit/0')
    monkeypatch.setenv('EXEC_HEALTH_REDIS_BOOTSTRAP_URL', 'redis://bootstrap@unit/0')
    monkeypatch.setenv('EXEC_HEALTH_REDIS_WRONG_USER_URL', 'redis://default@unit/0')
    monkeypatch.setenv('EXEC_HEALTH_FREEZE_RECONNECT_SMOKE_NOTIFY_ALWAYS', '1')

    report = tmp_path / 'report.json'
    prom = tmp_path / 'smoke.prom'
    rc = mod.main(['--report-path', str(report), '--textfile-path', str(prom)])
    assert rc == 0
    text = prom.read_text()
    assert 'exec_health_freeze_reconnect_smoke_last_run_ok 1' in text
    assert 'exec_health_freeze_reconnect_rollout_gate_active 0' in text
    assert 'exec_health_freeze_reconnect_smoke_ops_event_emitted 1' in text
    assert 'exec_health_freeze_reconnect_smoke_telegram_event_emitted 1' in text
    body = report.read_text()
    assert '"ok": true' in body
    assert '"ops_event_id": "1-0"' in body
    assert '"rollout_gate_active": false' in body
    assert fake.events[0][1]['kind'] == 'exec_health_reconnect_nightly_summary'


def test_nightly_smoke_runner_failure_latches_gate(monkeypatch, tmp_path):
    fake = FakeRedis()
    monkeypatch.setattr(mod, 'ChaosHarness', FailingHarness)
    monkeypatch.setattr(mod, 'redis', _redis_ns(fake))
    monkeypatch.setenv('REDIS_URL', 'redis://writer@unit/0')
    monkeypatch.setenv('EXEC_HEALTH_REDIS_AUDIT_URL', 'redis://audit@unit/0')
    monkeypatch.setenv('EXEC_HEALTH_FREEZE_RECONNECT_SMOKE_INCLUDE_BOOTSTRAP', '0')
    monkeypatch.setenv('EXEC_HEALTH_REDIS_WRONG_USER_URL', 'redis://default@unit/0')

    report = tmp_path / 'report.json'
    prom = tmp_path / 'smoke.prom'
    rc = mod.main(['--report-path', str(report), '--textfile-path', str(prom)])
    assert rc == 2
    text = prom.read_text()
    assert 'exec_health_freeze_reconnect_smoke_last_run_ok 0' in text
    assert 'exec_health_freeze_reconnect_rollout_gate_active 1' in text
    assert 'exec_health_freeze_reconnect_smoke_ops_event_emitted 1' in text
    assert 'exec_health_freeze_reconnect_smoke_telegram_event_emitted 1' in text
    body = report.read_text()
    assert '"ok": false' in body
    assert '"rollout_gate_active": true' in body
    assert 'cfg:orderflow:exec_health:reconnect_smoke:rollout_gate:v1' in fake.kv
