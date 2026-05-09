
from orderflow_services import orchestration_composite_preflight_history_rollup_v1 as mod


class FakePipeline:
    def __init__(self, r):
        self.r = r
        self.ops = []
    def hincrby(self, key, field, amount):
        self.ops.append(("hincrby", key, field, amount))
        return self
    def expire(self, key, ttl):
        self.ops.append(("expire", key, ttl))
        return self
    def set(self, key, value):
        self.ops.append(("set", key, value))
        return self
    def hset(self, key, mapping=None, **kwargs):
        self.ops.append(("hset", key, dict(mapping or {})))
        return self
    def hgetall(self, key):
        self.ops.append(("hgetall", key))
        return self
    def execute(self):
        out = []
        for op in self.ops:
            kind = op[0]
            if kind == "hincrby":
                _, key, field, amount = op
                self.r.hashes.setdefault(key, {})[field] = int(self.r.hashes.setdefault(key, {}).get(field, 0)) + int(amount)
                out.append(self.r.hashes[key][field])
            elif kind == "expire":
                _, key, ttl = op
                self.r.expiry[key] = int(ttl)
                out.append(True)
            elif kind == "set":
                _, key, value = op
                self.r.kv[key] = str(value)
                out.append(True)
            elif kind == "hset":
                _, key, mapping = op
                self.r.hashes.setdefault(key, {}).update({str(k): str(v) for k, v in mapping.items()})
                out.append(True)
            elif kind == "hgetall":
                _, key = op
                out.append(dict(self.r.hashes.get(key, {})))
        self.ops = []
        return out


class FakeRedis:
    def __init__(self):
        self.kv = {}
        self.hashes = {}
        self.expiry = {}
        self.streams = {}
    def get(self, key):
        return self.kv.get(key)
    def set(self, key, value):
        self.kv[key] = str(value)
        return True
    def hget(self, key, field):
        return self.hashes.get(key, {}).get(field)
    def hset(self, key, mapping=None, **kwargs):
        self.hashes.setdefault(key, {}).update({str(k): str(v) for k, v in dict(mapping or {}).items()})
        return True
    def hgetall(self, key):
        return dict(self.hashes.get(key, {}))
    def hincrby(self, key, field, amount):
        self.hashes.setdefault(key, {})[field] = int(self.hashes.setdefault(key, {}).get(field, 0)) + int(amount)
        return self.hashes[key][field]
    def expire(self, key, ttl):
        self.expiry[key] = int(ttl)
        return True
    def pipeline(self, transaction=False):
        return FakePipeline(self)
    def xrevrange(self, key, max='+', min='-', count=1):
        items = list(self.streams.get(key, []))
        items.sort(key=lambda x: x[0], reverse=True)
        return items[:count]
    def xread(self, streams, count=1, block=1):
        key, last_id = next(iter(streams.items()))
        items = list(self.streams.get(key, []))
        picked = [item for item in items if item[0] > last_id][:count]
        if not picked:
            return []
        return [(key, picked)]



def test_rollup_increments_hour_and_day_buckets() -> None:
    r = FakeRedis()
    stream = 'ops:orchestration:preflight:v1'
    r.streams[stream] = [
        ('1700000000000-0', {'purpose': 'promote', 'selected_source': 'deploy_lint', 'decision_status': 'block', 'selected_reason_code': 'deploy_lint:missing_env', 'ts_ms': '1700000000000'}),
        ('1700000300000-0', {'purpose': 'promote', 'selected_source': 'research_guard', 'decision_status': 'invalid', 'selected_reason_code': 'research_guard:pbo_high', 'ts_ms': '1700000300000'}),
    ]
    res = mod.rollup_incremental(
        r,
        stream_key=stream,
        cursor_key='cursor',
        state_key='state',
        hourly_prefix='hist:h',
        daily_prefix='hist:d',
        batch_size=10,
        bootstrap_skip_existing=False,
    )
    assert res['processed'] == 2
    hour_key = 'hist:h:1699999200000'
    day_key = 'hist:d:1699920000000'
    assert sum(int(v) for v in r.hashes[hour_key].values()) == 2
    assert sum(int(v) for v in r.hashes[day_key].values()) == 2
    assert r.kv['cursor'] == '1700000300000-0'


def test_bootstrap_skip_existing_sets_cursor_without_backfill() -> None:
    r = FakeRedis()
    stream = 'ops:orchestration:preflight:v1'
    r.streams[stream] = [
        ('1700000000000-0', {'purpose': 'apply'}),
    ]
    res = mod.rollup_incremental(
        r,
        stream_key=stream,
        cursor_key='cursor',
        state_key='state',
        hourly_prefix='hist:h',
        daily_prefix='hist:d',
        batch_size=10,
        bootstrap_skip_existing=True,
    )
    assert res['processed'] == 0
    assert r.kv['cursor'] == '1700000000000-0'
    assert 'hist:h:1699999200000' not in r.hashes


def test_normalize_source_maps_variants() -> None:
    """Low-cardinality source normalization covers all known prefixes."""
    assert mod.normalize_source("deploy_lint") == "deploy_lint"
    assert mod.normalize_source("deploy-something") == "deploy_lint"
    assert mod.normalize_source("latency_contract") == "latency_contract"
    assert mod.normalize_source("research_guard") == "research_guard"
    assert mod.normalize_source("unknown_xyz") == "unknown"


def test_normalize_status_maps_variants() -> None:
    """Status normalization collapses all block/invalid/ok variants."""
    assert mod.normalize_status("block") == "block"
    assert mod.normalize_status("blocked_by_rule") == "block"
    assert mod.normalize_status("invalid_data") == "invalid"
    assert mod.normalize_status("ok") == "ok"
    assert mod.normalize_status("ok_passed") == "ok"
    assert mod.normalize_status("other_status") == "unknown"


def test_encode_decode_field_roundtrip() -> None:
    """Field encoding is deterministic and decode is an exact inverse."""
    field = mod.encode_field(
        purpose="promote",
        selected_source="deploy_lint",
        decision_status="block",
        selected_reason_code="deploy_lint:missing_env",
    )
    purpose, source, status, reason = mod.decode_field(field)
    assert purpose == "promote"
    assert source == "deploy_lint"
    assert status == "block"
    assert reason == "deploy_lint:missing_env"


def test_rollup_state_persisted_after_batch() -> None:
    """After processing, the rollup state hash is updated with cursor and counts."""
    r = FakeRedis()
    stream = 'ops:orchestration:preflight:v1'
    r.streams[stream] = [
        ('1700001000000-0', {'purpose': 'apply', 'selected_source': 'latency_contract', 'decision_status': 'ok', 'ts_ms': '1700001000000'}),
    ]
    mod.rollup_incremental(
        r, stream_key=stream, cursor_key='c', state_key='s',
        hourly_prefix='h', daily_prefix='d', batch_size=10, bootstrap_skip_existing=False,
    )
    state = r.hashes.get('s', {})
    # Cursor persisted in kv and state
    assert r.kv.get('c') == '1700001000000-0'
    assert state.get('last_stream_id') == '1700001000000-0'
    assert state.get('processed_events_last_run') == '1'
