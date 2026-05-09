
from orderflow_services import orchestration_composite_preflight_history_textfile_exporter_v1 as mod
from orderflow_services.orchestration_composite_preflight_history_rollup_v1 import encode_field


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



def test_exporter_aggregates_windows_from_bucket_hashes() -> None:
    r = FakeRedis()
    now_ms = 1700003600000
    hour_key = 'metrics:orchestration:preflight:history:h:1700002800000'
    day_key = 'metrics:orchestration:preflight:history:d:1699920000000'
    field1 = encode_field(purpose='promote', selected_source='deploy_lint', decision_status='block', selected_reason_code='deploy_lint:missing_env')
    field2 = encode_field(purpose='promote', selected_source='research_guard', decision_status='invalid', selected_reason_code='research_guard:pbo_high')
    r.hashes[hour_key] = {field1: '3', field2: '1'}
    r.hashes[day_key] = {field1: '10', field2: '2'}
    r.hashes['metrics:orchestration:preflight:history_rollup:last'] = {'last_rollup_ts_ms': str(now_ms - 60000), 'last_event_ts_ms': str(now_ms - 120000)}
    r.kv['metrics:orchestration:preflight:history_rollup:last_id'] = '1700003000000-0'
    text = mod.render_text(r, now_ms=now_ms)
    assert 'orchestration_composite_preflight_rollup_events_total' in text
    assert 'window="24h"' in text and 'purpose="promote"' in text
    assert 'window="30d"' in text and 'purpose="promote"' in text
    # Labels are sorted alphabetically, so the order is deterministic:
    # decision_status, purpose, selected_reason_code, selected_source, window
    # block_ratio has labels: purpose, window (sorted → purpose comes first)
    assert (
        'orchestration_composite_preflight_rollup_block_ratio{purpose="promote",window="24h"} 0.75' in text
        or 'orchestration_composite_preflight_rollup_block_ratio{window="24h",purpose="promote"} 0.75' in text
    )


def test_exporter_empty_buckets_emits_meta_metrics() -> None:
    """When no bucket data exists, meta metrics (state_present, cursor_present) should still be emitted."""
    r = FakeRedis()
    now_ms = 1700000000000
    text = mod.render_text(r, now_ms=now_ms)
    # Meta metrics always emitted
    assert 'orchestration_composite_preflight_rollup_state_present' in text
    assert 'orchestration_composite_preflight_rollup_cursor_present' in text
    assert 'orchestration_composite_preflight_rollup_lag_seconds' in text
    # No event data = no totals lines for events or block_ratio (only HELP/TYPE lines)
    for line in text.splitlines():
        if 'block_ratio' in line and not line.startswith('#'):
            # Should not emit a block_ratio metric when total==0
            assert False, f"Unexpected block_ratio metric with no data: {line}"


def test_exporter_lag_seconds_computed_from_state() -> None:
    """lag_seconds = now - last_rollup_ts_ms/1000."""
    r = FakeRedis()
    now_ms = 1700006000000
    last_rollup_ts_ms = now_ms - 90_000  # 90 seconds ago
    r.hashes['metrics:orchestration:preflight:history_rollup:last'] = {
        'last_rollup_ts_ms': str(last_rollup_ts_ms),
        'last_event_ts_ms': str(last_rollup_ts_ms - 1000),
    }
    text = mod.render_text(r, now_ms=now_ms)
    assert 'orchestration_composite_preflight_rollup_lag_seconds 90.0' in text


def test_exporter_labels_sorted_alphabetically() -> None:
    """Metric labels must be emitted in sorted order (Prometheus convention)."""
    r = FakeRedis()
    now_ms = 1700003600000
    hour_key = 'metrics:orchestration:preflight:history:h:1700002800000'
    field = encode_field(purpose='apply', selected_source='latency_contract', decision_status='ok', selected_reason_code='latency_contract:slo')
    r.hashes[hour_key] = {field: '5'}
    text = mod.render_text(r, now_ms=now_ms)
    # Verify the event line for apply/latency_contract/ok has all labels present
    assert 'purpose="apply"' in text
    assert 'selected_source="latency_contract"' in text
    assert 'decision_status="ok"' in text
    # Confirm labels appear in alphabetical order in the metric line
    import re
    event_lines = [l for l in text.splitlines() if 'rollup_events_total{' in l]
    for line in event_lines:
        m = re.search(r'\{(.+?)\}', line)
        if m:
            label_pairs = m.group(1).split(',')
            label_keys = [p.split('=')[0] for p in label_pairs]
            assert label_keys == sorted(label_keys), f"Labels not sorted in: {line}"
