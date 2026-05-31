from services.orderflow.exec_health_freeze_reconnect_healing import get_heal_state_key, heal_service_identity_sync


class FakeRedis:
    def __init__(self, *, user='exec_health_freeze_writer', name='bad', lib_name='bad-lib', client_id=77):
        self.user = user
        self.client_name = name
        self.lib_name = lib_name
        self.client_id = client_id
        self.hashes = {}
        self.events = []

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
            return f'id={self.client_id} user={self.user} name={self.client_name} lib-name={self.lib_name}'
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


def test_reconnect_healing_repairs_name_and_libname(monkeypatch):
    monkeypatch.setenv('EXEC_HEALTH_FREEZE_CLIENT_HEAL_CHECK_MS', '0')
    r = FakeRedis(name='', lib_name='wrong-lib')
    out = heal_service_identity_sync(r, 'exec_health_freeze_override_v1', force=True)
    assert out['ok'] is True
    assert out['recovered'] is True
    assert r.client_name == 'exec-health-freeze-override-v1'
    assert r.lib_name == 'exec-health-freeze-writer'
    st = r.hashes[get_heal_state_key('exec_health_freeze_override_v1')]
    assert int(st['recovery_total']) == 1
    assert st['last_recovery_reason'] == 'wrong_name,wrong_lib_name'
    assert r.events and r.events[0][1]['kind'] == 'redis_client_identity_recovered'


def test_reconnect_healing_wrong_user_is_not_repairable(monkeypatch):
    monkeypatch.setenv('EXEC_HEALTH_FREEZE_CLIENT_HEAL_CHECK_MS', '0')
    r = FakeRedis(user='default', name='exec-health-freeze-override-v1', lib_name='exec-health-freeze-writer')
    try:
        heal_service_identity_sync(r, 'exec_health_freeze_override_v1', force=True)
    except RuntimeError as exc:
        assert 'wrong_user' in str(exc)
    else:
        raise AssertionError('expected RuntimeError')
    st = r.hashes[get_heal_state_key('exec_health_freeze_override_v1')]
    assert int(st['last_result_ok']) == 0
    assert int(st.get('repair_failed_total', 0) or 0) == 0
