from types import SimpleNamespace

import orderflow_services.exec_health_freeze_reconnect_chaos_harness_v1 as mod
from orderflow_services.exec_health_freeze_reconnect_chaos_harness_v1 import ChaosHarness
from orderflow_services.exec_health_freeze_client_name_audit_exporter_v1 import Exporter
from services.orderflow.exec_health_freeze_reconnect_healing import get_heal_state_key


class FakePool:
    def __init__(self, redis):
        self.redis = redis

    def disconnect(self):
        self.redis.client_id += 1


class FakeRedis:
    def __init__(self, *, user='exec_health_freeze_writer', name='', lib_name='', client_id=501):
        self.user = user
        self.client_name = name
        self.lib_name = lib_name
        self.client_id = client_id
        self.hashes = {}
        self.events = []
        self.extra_lines = []
        self.connection_pool = FakePool(self)

    def ping(self):
        return True

    def delete(self, key):
        self.hashes.pop(key, None)
        return 1

    def execute_command(self, *argv):
        cmd = tuple(str(x) for x in argv)
        if cmd[:2] == ('CLIENT', 'ID'):
            return self.client_id
        if cmd[:2] == ('CLIENT', 'SETNAME'):
            self.client_name = cmd[2]
            return 'OK'
        if cmd[:3] == ('CLIENT', 'SETINFO', 'LIB-NAME'):
            self.lib_name = cmd[3]
            return 'OK'
        if cmd[:2] == ('CLIENT', 'LIST') and len(cmd) >= 4 and cmd[2] == 'ID':
            return f'id={self.client_id} user={self.user} addr=10.0.0.9:9999 name={self.client_name} lib-name={self.lib_name}'
        if cmd[:2] == ('CLIENT', 'LIST'):
            rows = [
                f'id={self.client_id} user={self.user} addr=10.0.0.9:9999 name={self.client_name} lib-name={self.lib_name}',
                *list(self.extra_lines),
                'id=601 user=exec_health_freeze_writer addr=10.0.0.2:2222 name=exec-health-slo-autoguard-v1 lib-name=exec-health-freeze-writer',
                'id=602 user=exec_health_freeze_writer addr=10.0.0.3:3333 name=exec-health-freeze-tamper-guard-v1 lib-name=exec-health-freeze-writer',
                'id=603 user=exec_health_freeze_audit addr=10.0.0.4:4444 name=exec-health-freeze-acl-audit-exporter-v1 lib-name=exec-health-freeze-audit',
                'id=604 user=exec_health_freeze_audit addr=10.0.0.5:5555 name=exec-health-freeze-acl-drift-exporter-v1 lib-name=exec-health-freeze-audit',
                'id=605 user=exec_health_freeze_audit addr=10.0.0.6:6666 name=exec-health-freeze-client-name-audit-exporter-v1 lib-name=exec-health-freeze-audit',
                'id=606 user=exec_health_freeze_bootstrap addr=10.0.0.7:7777 name=exec-health-freeze-acl-policy-v1 lib-name=exec-health-freeze-bootstrap',
            ]
            if self.client_name == 'exec-health-freeze-client-name-audit-exporter-v1':
                rows = [r for r in rows if 'id=605 ' not in r]
            return "\n".join(rows)
        raise AssertionError(cmd)

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


class RedisFactory:
    def __init__(self, primary, wrong):
        self.primary = primary
        self.wrong = wrong

    def from_url(self, url, decode_responses=True):
        if 'default@' in url or 'wrong-user' in url:
            return self.wrong
        return self.primary


def _redis_ns(factory):
    return SimpleNamespace(Redis=SimpleNamespace(from_url=factory.from_url))


def test_harness_repairable_reconnect_and_exporter(monkeypatch):
    monkeypatch.setenv('EXEC_HEALTH_FREEZE_CLIENT_HEAL_CHECK_MS', '0')
    primary = FakeRedis()
    wrong = FakeRedis(user='default', name='exec-health-freeze-override-v1', lib_name='exec-health-freeze-writer')
    monkeypatch.setattr(mod, 'redis', _redis_ns(RedisFactory(primary, wrong)))
    audit = FakeRedis(user='exec_health_freeze_audit')
    audit.hashes = primary.hashes
    audit.extra_lines = [
        f'id={primary.client_id} user={primary.user} addr=10.0.0.1:1111 name=exec-health-freeze-override-v1 lib-name=exec-health-freeze-writer'
    ]
    import orderflow_services.exec_health_freeze_client_name_audit_exporter_v1 as exp_mod
    monkeypatch.setattr(exp_mod, 'redis', _redis_ns(RedisFactory(audit, wrong)))

    h = ChaosHarness('redis://writer@unit/0', service='exec_health_freeze_override_v1', wrong_user_url='redis://wrong-user@unit/0')
    out = h.run_repairable('reconnect-both')
    assert out['ok'] is True
    assert out['recovered'] is True
    assert out['after_entry']['name'] == 'exec-health-freeze-override-v1'
    assert out['after_entry']['lib-name'] == 'exec-health-freeze-writer'
    assert int(out['state']['recovery_total']) == 1
    assert primary.events and primary.events[-1][1]['kind'] == 'redis_client_identity_recovered'

    ex = Exporter()
    ex.run_once()
    st = primary.hashes[ex.state_key]
    assert int(st['violation_count']) == 0
    heal_state = primary.hashes[get_heal_state_key('exec_health_freeze_override_v1')]
    assert int(heal_state['recovery_total']) == 1


def test_harness_wrong_user_is_not_self_healed(monkeypatch):
    monkeypatch.setenv('EXEC_HEALTH_FREEZE_CLIENT_HEAL_CHECK_MS', '0')
    primary = FakeRedis()
    wrong = FakeRedis(user='default', name='exec-health-freeze-override-v1', lib_name='exec-health-freeze-writer')
    monkeypatch.setattr(mod, 'redis', _redis_ns(RedisFactory(primary, wrong)))

    h = ChaosHarness('redis://writer@unit/0', service='exec_health_freeze_override_v1', wrong_user_url='redis://wrong-user@unit/0')
    out = h.run_wrong_user()
    assert out['ok'] is False
    assert out['unexpected_success'] is False
    assert 'wrong_user' in out['error']
    assert int(out['state']['last_result_ok']) == 0
    assert int(out['state'].get('recovery_total', 0) or 0) == 0
    assert wrong.events == []
