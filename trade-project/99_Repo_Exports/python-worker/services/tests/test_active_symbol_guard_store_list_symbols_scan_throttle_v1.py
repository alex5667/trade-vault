"""
Tests for list_symbols() empty-scan sentinel (CPU fix):
- When no active guards exist, a Redis key prevents repeated SCAN for 90s.
- When a guard is added (write path), list_symbols finds it via index SET, not SCAN.
"""
import importlib
import sys
from pathlib import Path

root = Path(__file__).resolve().parents[2]
if str(root) not in sys.path:
    sys.path.insert(0, str(root))

store_mod = importlib.import_module('services.active_symbol_guard_store')
ActiveSymbolGuardStore = store_mod.ActiveSymbolGuardStore


class FakeRedis:
    def __init__(self):
        self.kv: dict = {}
        self.sets: dict = {}
        self.scan_calls = 0

    def get(self, key):
        return self.kv.get(key)

    def set(self, key, value, ex=None):
        self.kv[key] = value

    def exists(self, key):
        return 1 if (key in self.kv or key in self.sets) else 0

    def smembers(self, key):
        return self.sets.get(key, set())

    def sadd(self, key, *members):
        self.sets.setdefault(key, set()).update(members)

    def srem(self, key, *members):
        if key in self.sets:
            self.sets[key] -= set(members)

    def scan_iter(self, match=None, count=None):
        self.scan_calls += 1
        prefix = (match or '').rstrip('*')
        for key in list(self.kv.keys()):
            if key.startswith(prefix):
                yield key

    def setex(self, key, ttl, value):
        self.kv[key] = value

    def hgetall(self, key):
        return {}

    def hset(self, key, mapping=None, **kwargs):
        pass

    def hincrby(self, key, field, amount):
        pass

    def hincrbyfloat(self, key, field, amount):
        pass

    def xadd(self, stream, fields, maxlen=None, approximate=False):
        pass

    def eval(self, script, numkeys, *args):
        return [1, b'{}', b'1']

    def expire(self, key, ttl):
        pass


def _make_store(r):
    return ActiveSymbolGuardStore(r, key_prefix='orders:active_symbol_sid:', active_ttl_sec=86400, tombstone_ttl_sec=120)


def test_first_call_does_scan_and_returns_empty():
    r = FakeRedis()
    store = _make_store(r)
    result = store.list_symbols()
    assert result == []
    assert r.scan_calls >= 1


def test_second_call_skips_scan_due_to_sentinel():
    r = FakeRedis()
    store = _make_store(r)
    store.list_symbols()  # first call — sets sentinel
    calls_after_first = r.scan_calls
    store.list_symbols()  # second call — should skip SCAN
    assert r.scan_calls == calls_after_first, "SCAN should be skipped when sentinel present"


def test_sentinel_key_is_set_after_empty_scan():
    r = FakeRedis()
    store = _make_store(r)
    store.list_symbols()
    sentinel = store.key_prefix.rstrip(':') + '_scan_empty'
    assert r.exists(sentinel) == 1, "Sentinel must be set after empty SCAN"


def test_scan_uses_count_500_not_50000():
    """Ensure count parameter in scan_iter is bounded (original was 50000)."""
    captured = []
    original_scan_iter = FakeRedis.scan_iter

    class TrackingRedis(FakeRedis):
        def scan_iter(self, match=None, count=None):
            captured.append(count)
            return iter([])

        def setex(self, key, ttl, value):
            self.kv[key] = value

    r = TrackingRedis()
    store = _make_store(r)
    store.list_symbols()
    assert captured, "scan_iter must have been called"
    for c in captured:
        assert c is None or c <= 1000, f"count={c} too large — keeps slowlog entries"


def test_write_path_bypasses_scan_on_next_read():
    """After acquire_or_refresh, list_symbols uses index SET, not SCAN."""
    r = FakeRedis()
    store = _make_store(r)

    # Simulate guard acquisition populating index
    r.sets[store.index_key] = {'BTCUSDT'}
    r.kv[store.key('BTCUSDT')] = '{"guard_status": "active"}'

    result = store.list_symbols()
    assert 'BTCUSDT' in result
    assert r.scan_calls == 0, "SCAN must not run when index SET is populated"
