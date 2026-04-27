from types import SimpleNamespace

import orderflow_services.exec_health_freeze_client_name_audit_exporter_v1 as exp_mod
from orderflow_services.exec_health_freeze_client_name_audit_exporter_v1 import Exporter


class FakeRedis:
    def __init__(self):
        self.hashes = {}
        self.client_name = ''
        self.lib_name = ''
        self.client_id = 88

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
            return f'id={self.client_id} user=exec_health_freeze_audit addr=10.0.0.9:9999 name={self.client_name} lib-name={self.lib_name}'
        if cmd[:2] == ('CLIENT', 'LIST'):
            return "\n".join([
                'id=1 user=exec_health_freeze_writer addr=10.0.0.1:1111 name=exec-health-freeze-override-v1 lib-name=exec-health-freeze-writer',
                'id=2 user=exec_health_freeze_writer addr=10.0.0.2:2222 name=exec-health-slo-autoguard-v1 lib-name=exec-health-freeze-writer',
                'id=3 user=exec_health_freeze_writer addr=10.0.0.3:3333 name=exec-health-freeze-tamper-guard-v1 lib-name=exec-health-freeze-writer',
                'id=4 user=exec_health_freeze_audit addr=10.0.0.4:4444 name=exec-health-freeze-acl-audit-exporter-v1 lib-name=exec-health-freeze-audit',
                'id=5 user=exec_health_freeze_audit addr=10.0.0.5:5555 name=exec-health-freeze-acl-drift-exporter-v1 lib-name=exec-health-freeze-audit',
                'id=6 user=exec_health_freeze_audit addr=10.0.0.6:6666 name=exec-health-freeze-client-name-audit-exporter-v1 lib-name=exec-health-freeze-audit',
                'id=7 user=exec_health_freeze_bootstrap addr=10.0.0.7:7777 name=exec-health-freeze-acl-policy-v1 lib-name=exec-health-freeze-bootstrap',
            ])
        raise AssertionError(cmd)

    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))

    def hset(self, key, mapping=None):
        self.hashes.setdefault(key, {}).update({str(k): str(v) for k, v in (mapping or {}).items()})
        return 1

    def expire(self, key, ttl):
        return 1


def _redis_ns(fake):
    return SimpleNamespace(Redis=SimpleNamespace(from_url=staticmethod(lambda *a, **k: fake)))


def test_exporter_run_once(monkeypatch):
    fake = FakeRedis()
    monkeypatch.setattr(exp_mod, 'redis', _redis_ns(fake))
    ex = Exporter()
    out = ex.run_once()
    assert out['ok'] is True
    assert int(fake.hashes[ex.state_key]['violation_count']) == 0
