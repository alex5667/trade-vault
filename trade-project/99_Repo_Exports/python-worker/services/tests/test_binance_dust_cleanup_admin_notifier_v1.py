from __future__ import annotations

import json

from services.binance_dust_cleanup_admin_notifier import BinanceDustCleanupAdminNotifier
from utils.time_utils import get_ny_time_millis


class FakeRedis:
    def __init__(self):
        self.kv = {}
        self.expiry_ms = {}
        self.sets = {}
        self.streams = {}

    def _now_ms(self):
        return get_ny_time_millis()

    def get(self, key):
        if key in self.expiry_ms and self.expiry_ms[key] <= self._now_ms():
            self.delete(key)
            return None
        return self.kv.get(key)

    def set(self, key, value):
        self.kv[key] = str(value)

    def setex(self, key, ttl, value):
        self.set(key, value)
        self.expiry_ms[key] = self._now_ms() + int(ttl) * 1000

    def delete(self, key):
        self.kv.pop(key, None)
        self.expiry_ms.pop(key, None)

    def pttl(self, key):
        if key not in self.kv:
            return -2
        if key not in self.expiry_ms:
            return -1
        rem = self.expiry_ms[key] - self._now_ms()
        return rem if rem > 0 else -2

    def sadd(self, key, *values):
        self.sets.setdefault(key, set()).update(str(v) for v in values)

    def smembers(self, key):
        return set(self.sets.get(key, set()))

    def scan_iter(self, match=None):
        prefix = str(match).rstrip('*') if match else ''
        for key in sorted(list(self.kv.keys()) + list(self.sets.keys()) + list(self.streams.keys())):
            if key.startswith(prefix):
                yield key

    def keys(self, patt):
        return list(self.scan_iter(patt))

    def xrange(self, stream, min='-', max='+', count=None):
        rows = self.streams.get(stream, [])
        out = []
        lower = min[1:] if str(min).startswith('(') else min
        exclusive = str(min).startswith('(')
        for entry_id, fields in rows:
            if lower not in ('-', ''):
                if exclusive and entry_id <= lower:
                    continue
                if not exclusive and entry_id < lower:
                    continue
            out.append((entry_id, dict(fields)))
        if count is not None:
            out = out[:count]
        return out

    def xadd(self, stream, fields, maxlen=None, approximate=True):
        rows = self.streams.setdefault(stream, [])
        entry_id = f"{len(rows)+1}-0"
        rows.append((entry_id, {str(k): str(v) for k, v in dict(fields).items()}))
        if maxlen is not None and len(rows) > int(maxlen):
            del rows[: len(rows) - int(maxlen)]
        return entry_id


def test_manual_admin_actions_are_forwarded_to_notify_stream(monkeypatch):
    r = FakeRedis()
    r.xadd('orders:dust_cleanup:audit', {
        'action': 'add_denylist',
        'symbol': 'APTUSDT',
        'operator': 'alex',
        'reason': 'manual hold',
        'ticket': 'INC-42',
        'result': 'ok',
        'payload_json': json.dumps({'ok': True}),
    })
    svc = BinanceDustCleanupAdminNotifier(redis_client=r, notify_stream='notify:telegram:test', audit_stream='orders:dust_cleanup:audit')
    out = svc.process_manual_actions_once()
    assert out['processed'] == 1
    rows = r.streams['notify:telegram:test']
    assert len(rows) == 1
    _, fields = rows[0]
    assert fields['symbol'] == 'APTUSDT'
    assert 'add_denylist' in fields['kind']
    assert 'alex' in fields['text']


def test_old_denylist_entry_emits_reminder(monkeypatch):
    r = FakeRedis()
    monkeypatch.setenv('BINANCE_DUST_ADMIN_OLD_DENYLIST_SEC', '1')
    monkeypatch.setenv('BINANCE_DUST_ADMIN_REMINDER_REPEAT_SEC', '60')
    payload = {
        'symbol': 'SUIUSDT',
        'operator': 'alex',
        'reason': 'manual hold',
        'ticket': 'INC-55',
        'ts_ms': 1,
        'ttl_sec': 3600,
    }
    r.sadd('orders:dust_cleanup:denylist', 'SUIUSDT')
    r.set('orders:dust_cleanup:denylist:SUIUSDT', json.dumps(payload))
    svc = BinanceDustCleanupAdminNotifier(redis_client=r, notify_stream='notify:telegram:test')
    out = svc.scan_reminders_once()
    assert out['denylist_emitted'] == 1
    rows = r.streams['notify:telegram:test']
    assert any(fields['kind'] == 'old_denylist' for _, fields in rows)


def test_cooldown_loop_emits_reminder_after_symbol_persists(monkeypatch):
    r = FakeRedis()
    monkeypatch.setenv('BINANCE_DUST_ADMIN_COOLDOWN_LOOP_SEC', '1')
    monkeypatch.setenv('BINANCE_DUST_ADMIN_REMINDER_REPEAT_SEC', '60')
    payload = {'symbol': 'APTUSDT', 'reason': 'cleanup_ok', 'until_ms': 9_999_999_999_999, 'ts_ms': 1}
    r.set('orders:dust_cleanup:cooldown:APTUSDT', json.dumps(payload))
    svc = BinanceDustCleanupAdminNotifier(redis_client=r, notify_stream='notify:telegram:test')
    # first scan establishes first_seen; second scan sees the same symbol as a loop
    svc.scan_reminders_once()
    # force ancient first_seen
    state = json.loads(r.get('orders:dust_cleanup:reminder:state:APTUSDT'))  # type: ignore
    state['cooldown_first_seen_ms'] = 1
    r.set('orders:dust_cleanup:reminder:state:APTUSDT', json.dumps(state))
    out = svc.scan_reminders_once()
    assert out['cooldown_emitted'] == 1
    rows = r.streams['notify:telegram:test']
    assert any(fields['kind'] == 'cooldown_loop' for _, fields in rows)
