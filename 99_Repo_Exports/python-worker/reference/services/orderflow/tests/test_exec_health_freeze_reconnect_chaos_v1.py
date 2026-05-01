import asyncio

from services.orderflow.exec_health_freeze_reconnect_healing import (
    get_heal_state_key,
    heal_service_identity_async,
    heal_service_identity_sync,
)
from services.orderflow.exec_health_freeze_service_identity import ensure_service_identity_sync


class SyncChaosRedis:
    def __init__(self, *, user='exec_health_freeze_writer', client_name='', lib_name='', client_id=1001):
        self.user = user
        self.client_name = client_name
        self.lib_name = lib_name
        self.client_id = client_id
        self.hashes = {}
        self.events = []

    def reconnect(self, *, name='', lib_name=''):
        self.client_id += 1
        self.client_name = name
        self.lib_name = lib_name

    def ping(self):
        return True

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
            return f'id={self.client_id} user={self.user} addr=10.0.0.1:1234 name={self.client_name} lib-name={self.lib_name}'
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


class AsyncChaosRedis(SyncChaosRedis):
    async def execute_command(self, *argv):
        return super().execute_command(*argv)

    async def hgetall(self, key):
        return super().hgetall(key)

    async def hset(self, key, mapping=None):
        return super().hset(key, mapping=mapping)

    async def expire(self, key, ttl):
        return super().expire(key, ttl)

    async def xadd(self, key, mapping, maxlen=None):
        return super().xadd(key, mapping, maxlen=maxlen)


def test_sync_reconnect_chaos_e2e_repairs_and_emits_event(monkeypatch):
    monkeypatch.setenv('EXEC_HEALTH_FREEZE_CLIENT_HEAL_CHECK_MS', '0')
    r = SyncChaosRedis()
    ensure_service_identity_sync(r, 'exec_health_freeze_override_v1')
    assert r.client_name == 'exec-health-freeze-override-v1'
    assert r.lib_name == 'exec-health-freeze-writer'
    heal_service_identity_sync(r, 'exec_health_freeze_override_v1', force=True)

    r.reconnect(name='', lib_name='broken-lib')
    out = heal_service_identity_sync(r, 'exec_health_freeze_override_v1')
    assert out['ok'] is True
    assert out['recovered'] is True
    assert r.client_name == 'exec-health-freeze-override-v1'
    assert r.lib_name == 'exec-health-freeze-writer'
    st = r.hashes[get_heal_state_key('exec_health_freeze_override_v1')]
    assert int(st['recovery_total']) == 1
    assert int(st['reconnect_seen_total']) == 1
    assert r.events and r.events[-1][1]['kind'] == 'redis_client_identity_recovered'


def test_sync_reconnect_chaos_wrong_user_stays_violation(monkeypatch):
    monkeypatch.setenv('EXEC_HEALTH_FREEZE_CLIENT_HEAL_CHECK_MS', '0')
    r = SyncChaosRedis(user='default', client_name='exec-health-freeze-override-v1', lib_name='exec-health-freeze-writer')
    r.reconnect(name='exec-health-freeze-override-v1', lib_name='exec-health-freeze-writer')
    try:
        heal_service_identity_sync(r, 'exec_health_freeze_override_v1')
    except RuntimeError as exc:
        assert 'wrong_user' in str(exc)
    else:
        raise AssertionError('expected RuntimeError')
    st = r.hashes[get_heal_state_key('exec_health_freeze_override_v1')]
    assert int(st['last_result_ok']) == 0
    assert int(st.get('recovery_total', 0) or 0) == 0
    assert r.events == []


def test_async_reconnect_chaos_e2e_repairs_and_emits_event(monkeypatch):
    monkeypatch.setenv('EXEC_HEALTH_FREEZE_CLIENT_HEAL_CHECK_MS', '0')
    r = AsyncChaosRedis(client_name='exec-health-slo-autoguard-v1', lib_name='exec-health-freeze-writer')
    asyncio.run(heal_service_identity_async(r, 'exec_health_slo_autoguard_v1', force=True))
    r.reconnect(name='', lib_name='bad-lib')
    out = asyncio.run(heal_service_identity_async(r, 'exec_health_slo_autoguard_v1', force=True))
    assert out['ok'] is True
    assert out['recovered'] is True
    assert r.client_name == 'exec-health-slo-autoguard-v1'
    assert r.lib_name == 'exec-health-freeze-writer'
    st = r.hashes[get_heal_state_key('exec_health_slo_autoguard_v1')]
    assert int(st['recovery_total']) == 1
    assert r.events and r.events[-1][1]['kind'] == 'redis_client_identity_recovered'
