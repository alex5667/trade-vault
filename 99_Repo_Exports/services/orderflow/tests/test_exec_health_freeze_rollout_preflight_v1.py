from types import SimpleNamespace

import orderflow_services.exec_health_freeze_rollout_preflight_v1 as mod
from services.orderflow.exec_health_freeze_deploy_contract import render_sensitive_deploy_env_templates


class FakeRedis:
    def __init__(self):
        self.kv = {}
        self.hashes = {}
        self.client_id = 51
        self.client_name = ''
        self.lib_name = ''

    def execute_command(self, *argv):
        cmd = tuple(str(x) for x in argv)
        if cmd[:2] == ('CLIENT', 'SETNAME'):
            self.client_name = cmd[2]
            return 'OK'
        if cmd[:3] == ('CLIENT', 'SETINFO', 'LIB-NAME'):
            self.lib_name = cmd[3]
            return 'OK'
        if cmd[:2] == ('CLIENT', 'ID'):
            return self.client_id
        if cmd[:2] == ('CLIENT', 'LIST') and len(cmd) >= 4 and cmd[2] == 'ID':
            return f'id={self.client_id} user=exec_health_freeze_audit name={self.client_name} lib-name={self.lib_name}'
        raise AssertionError(cmd)

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value):
        self.kv[key] = str(value)
        return 'OK'

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def hset(self, key, mapping=None):
        self.hashes.setdefault(key, {}).update({str(k): str(v) for k, v in (mapping or {}).items()})
        return 1

    def expire(self, key, ttl):
        return 1

    def xadd(self, key, mapping, maxlen=None):
        return '1-0'


def _redis_ns(fake):
    return SimpleNamespace(Redis=SimpleNamespace(from_url=staticmethod(lambda *a, **k: fake)))


def _seed_env(monkeypatch, purpose: str):
    """Seed monkeypatch environment from the P20 deploy env templates for the given purpose."""
    env = render_sensitive_deploy_env_templates()[purpose]
    for k, v in env.items():
        monkeypatch.setenv(k, str(v))


def test_preflight_main_ok(monkeypatch):
    fake = FakeRedis()
    # P20: seed required deploy env contract vars for the preflight to validate
    _seed_env(monkeypatch, 'exec_health_freeze_acl_policy_apply')
    monkeypatch.setattr(mod, 'redis', _redis_ns(fake))
    assert mod.main(['--purpose', 'exec_health_freeze_acl_policy_apply']) == 0
    assert fake.client_name == 'exec-health-freeze-rollout-preflight-v1'
    assert fake.lib_name == 'exec-health-freeze-audit'


def test_preflight_main_blocked(monkeypatch):
    fake = FakeRedis()
    # P20: seed required deploy env contract vars
    _seed_env(monkeypatch, 'exec_health_freeze_acl_policy_apply')
    fake.kv['cfg:orderflow:exec_health:reconnect_smoke:rollout_gate:v1'] = '{"reason":"nightly_reconnect_smoke_failed","ts_ms":1}'
    monkeypatch.setattr(mod, 'redis', _redis_ns(fake))
    try:
        mod.main(['--purpose', 'exec_health_freeze_acl_policy_apply'])
    except SystemExit as exc:
        assert int(exc.code) == 24
    else:
        raise AssertionError('expected SystemExit(24)')


def test_preflight_main_fails_on_schema_mismatch(monkeypatch):
    """P20: preflight raises RuntimeError when schema version env is stale."""
    fake = FakeRedis()
    _seed_env(monkeypatch, 'exec_health_freeze_acl_policy_apply')
    # Override schema version with a stale value — contract check must fail
    monkeypatch.setenv('EXEC_HEALTH_ROLLOUT_PREFLIGHT_SCHEMA_VERSION', 'stale-v0')
    monkeypatch.setattr(mod, 'redis', _redis_ns(fake))
    try:
        mod.main(['--purpose', 'exec_health_freeze_acl_policy_apply'])
    except RuntimeError as exc:
        assert 'deploy env contract mismatch' in str(exc)
    else:
        raise AssertionError('expected deploy contract mismatch')

