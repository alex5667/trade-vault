from __future__ import annotations

import json

from services.binance_dust_cleanup_admin import BinanceDustCleanupAdmin


class FakeRedis:
    def __init__(self):
        self.kv = {}
        self.expiry_ms = {}
        self.sets = {}
        self.streams = {}
        self.now_ms = 0

    def _purge_key(self, key):
        exp = self.expiry_ms.get(key)
        if exp is not None and exp <= self.now_ms:
            self.kv.pop(key, None)
            self.expiry_ms.pop(key, None)

    def get(self, key):
        key = str(key)
        self._purge_key(key)
        return self.kv.get(key)

    def set(self, key, value):
        self.kv[str(key)] = str(value)
        self.expiry_ms.pop(str(key), None)
        return True

    def setex(self, key, ttl_sec, value):
        self.kv[str(key)] = str(value)
        self.expiry_ms[str(key)] = self.now_ms + (int(ttl_sec) * 1000)
        return True

    def delete(self, key):
        self.kv.pop(str(key), None)
        self.expiry_ms.pop(str(key), None)
        return 1

    def pttl(self, key):
        key = str(key)
        self._purge_key(key)
        if key not in self.kv:
            return -2
        exp = self.expiry_ms.get(key)
        if exp is None:
            return -1
        return max(0, exp - self.now_ms)

    def sadd(self, key, *values):
        bucket = self.sets.setdefault(str(key), set())
        for v in values:
            bucket.add(str(v))
        return len(values)

    def srem(self, key, *values):
        bucket = self.sets.setdefault(str(key), set())
        removed = 0
        for v in values:
            if str(v) in bucket:
                bucket.remove(str(v))
                removed += 1
        return removed

    def smembers(self, key):
        return set(self.sets.get(str(key), set()))

    def scan_iter(self, match=None):
        prefix = (match or '').rstrip('*')
        for key in sorted(self.kv.keys()):
            self._purge_key(key)
            if not prefix or str(key).startswith(prefix):
                yield key

    def xadd(self, stream, fields, **kwargs):
        bucket = self.streams.setdefault(str(stream), [])
        item_id = f"{len(bucket)+1}-0"
        bucket.append((item_id, dict(fields), dict(kwargs)))
        return item_id

    def xrevrange(self, stream, count=50):
        bucket = list(self.streams.get(str(stream), []))
        out = []
        for item_id, fields, _ in reversed(bucket[:]):
            out.append((item_id, dict(fields)))
            if len(out) >= int(count):
                break
        return out


def test_admin_add_remove_denylist_and_symbol_state():
    r = FakeRedis()
    admin = BinanceDustCleanupAdmin(redis_client=r)
    add = admin.add_denylist_symbol('aptusdt', operator='alice', reason='manual_hold', ticket='INC-1', ttl_sec=120)
    assert add['ok'] is True
    state = admin.symbol_state('APTUSDT')
    assert state['dynamic_set_member'] is True
    assert state['dynamic_override']['exists'] is True
    assert state['effective_denylisted'] is True

    rem = admin.remove_denylist_symbol('APTUSDT', operator='alice', reason='done', ticket='INC-1')
    assert rem['ok'] is True
    state2 = admin.symbol_state('APTUSDT')
    assert state2['dynamic_set_member'] is False
    assert state2['dynamic_override']['exists'] is False


def test_admin_clear_cooldown_and_current_state():
    r = FakeRedis()
    r.setex('orders:dust_cleanup:cooldown:SUIUSDT', 300, json.dumps({'until_ms': 9999999999999, 'reason': 'closed'}))
    admin = BinanceDustCleanupAdmin(redis_client=r)
    before = admin.current_state()
    assert before['cooldowns'][0]['symbol'] == 'SUIUSDT'
    cleared = admin.clear_cooldown('SUIUSDT', operator='bob', reason='retry_now', ticket='OPS-7')
    assert cleared['cleared'] is True
    after = admin.current_state()
    assert after['cooldowns'] == []


def test_admin_recent_audit_contains_operator_reason_ticket():
    r = FakeRedis()
    admin = BinanceDustCleanupAdmin(redis_client=r)
    admin.add_denylist_symbol('ETHUSDT', operator='carol', reason='investigate', ticket='T-9', ttl_sec=0)
    rows = admin.recent_audit(limit=10)
    assert rows
    top = rows[0]
    assert top['action'] == 'add_denylist'
    assert top['operator'] == 'carol'
    assert top['reason'] == 'investigate'
    assert top['ticket'] == 'T-9'
