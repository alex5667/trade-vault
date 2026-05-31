"""Tests for P5.7 rollup consistency checker and rebuild tool.

Covers:
- check_consistency: detects bucket value drift and cursor mismatch
- rebuild_range: rewrites wrong bucket, clears extra (ghost) bucket
- render_text: correct Prometheus textfile output
"""
from orderflow_services import orchestration_composite_preflight_history_consistency_v1 as mod
from orderflow_services.orchestration_composite_preflight_history_rollup_v1 import encode_field


class FakeRedis:
    """Minimal Redis stub sufficient for the consistency checker tests."""

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

    def delete(self, key):
        self.hashes.pop(key, None)
        self.kv.pop(key, None)
        self.expiry.pop(key, None)
        return 1

    def expire(self, key, ttl):
        self.expiry[key] = int(ttl)
        return True

    def xrange(self, key, min='-', max='+', count=None):
        items = []
        count = int(count or 10**9)
        for sid, payload in sorted(self.streams.get(key, []), key=lambda x: x[0]):
            if sid < min or sid > max:
                continue
            items.append((sid, payload))
            if len(items) >= count:
                break
        return items


def test_consistency_check_detects_bucket_drift_and_cursor_mismatch() -> None:
    """check_consistency must flag mismatched bucket count AND cursor/state divergence."""
    r = FakeRedis()
    stream = 'ops:orchestration:preflight:v1'
    field = encode_field(
        purpose='promote',
        selected_source='deploy_lint',
        decision_status='block',
        selected_reason_code='deploy_lint:missing_env',
    )
    # Two stream events → expected count = 2
    r.streams[stream] = [
        ('1700000000000-0', {'purpose': 'promote', 'selected_source': 'deploy_lint', 'decision_status': 'block', 'selected_reason_code': 'deploy_lint:missing_env', 'ts_ms': '1700000000000'}),
        ('1700000100000-0', {'purpose': 'promote', 'selected_source': 'deploy_lint', 'decision_status': 'block', 'selected_reason_code': 'deploy_lint:missing_env', 'ts_ms': '1700000100000'}),
    ]
    # Redis bucket has count = 1 (drift!)
    bucket = 'metrics:orchestration:preflight:history:h:1699999200000'
    r.hashes[bucket] = {field: '1'}
    # Cursor points to latest event but state has stale id → mismatch
    r.kv['metrics:orchestration:preflight:history_rollup:last_id'] = '1700000100000-0'
    r.hashes['metrics:orchestration:preflight:history_rollup:last'] = {'last_stream_id': '1700000000000-0'}

    report = mod.check_consistency(
        r,
        stream_key=stream,
        start_ms=1699999200000,
        end_ms=1700003599999,
        hourly_prefix='metrics:orchestration:preflight:history:h',
        daily_prefix='metrics:orchestration:preflight:history:d',
        state_key='metrics:orchestration:preflight:history_rollup:last',
        cursor_key='metrics:orchestration:preflight:history_rollup:last_id',
        batch_size=10,
    )
    assert report['consistency_ok'] == 0
    assert report['drift_detected'] == 1
    assert report['state_cursor_match'] == 0
    assert report['hourly']['mismatched_value_fields'] == 1


def test_rebuild_range_rewrites_wrong_bucket_and_clears_extra_bucket() -> None:
    """rebuild_range must fix the correct bucket AND delete the extra (ghost) bucket."""
    r = FakeRedis()
    stream = 'ops:orchestration:preflight:v1'
    field = encode_field(
        purpose='apply',
        selected_source='research_guard',
        decision_status='invalid',
        selected_reason_code='research_guard:pbo_high',
    )
    r.streams[stream] = [
        ('1700000000000-0', {'purpose': 'apply', 'selected_source': 'research_guard', 'decision_status': 'invalid', 'selected_reason_code': 'research_guard:pbo_high', 'ts_ms': '1700000000000'}),
    ]
    wrong_hour = 'metrics:orchestration:preflight:history:h:1699999200000'
    extra_hour = 'metrics:orchestration:preflight:history:h:1700002800000'
    r.hashes[wrong_hour] = {field: '5'}   # wrong count — will be corrected
    r.hashes[extra_hour] = {field: '9'}   # ghost bucket — will be cleared

    report = mod.rebuild_range(
        r,
        stream_key=stream,
        start_ms=1699999200000,
        end_ms=1700003599999,
        hourly_prefix='metrics:orchestration:preflight:history:h',
        daily_prefix='metrics:orchestration:preflight:history:d',
        state_key='metrics:orchestration:preflight:history_rollup:last',
        batch_size=10,
    )
    assert report['stream_events'] == 1
    # Correct bucket now has count = 1
    assert r.hashes[wrong_hour][field] == '1'
    # Extra bucket with no stream events must be gone
    assert extra_hour not in r.hashes


def test_rebuild_range_updates_cursor_when_flag_set() -> None:
    """rebuild_range should update cursor_key + state last_stream_id when update_cursor=True."""
    r = FakeRedis()
    stream = 'ops:orchestration:preflight:v1'
    field = encode_field(
        purpose='apply',
        selected_source='deploy_lint',
        decision_status='ok',
        selected_reason_code='deploy_lint:none',
    )
    r.streams[stream] = [
        ('1700000000000-0', {'purpose': 'apply', 'selected_source': 'deploy_lint', 'decision_status': 'ok', 'selected_reason_code': 'deploy_lint:none', 'ts_ms': '1700000000000'}),
    ]
    cursor_key = 'metrics:orchestration:preflight:history_rollup:last_id'
    state_key = 'metrics:orchestration:preflight:history_rollup:last'
    mod.rebuild_range(
        r,
        stream_key=stream,
        start_ms=1699999200000,
        end_ms=1700003599999,
        hourly_prefix='metrics:orchestration:preflight:history:h',
        daily_prefix='metrics:orchestration:preflight:history:d',
        state_key=state_key,
        batch_size=10,
        update_cursor=True,
        cursor_key=cursor_key,
    )
    assert r.kv.get(cursor_key) == '1700000000000-0'
    assert r.hashes.get(state_key, {}).get('last_stream_id') == '1700000000000-0'


def test_consistency_check_no_drift_when_buckets_match() -> None:
    """check_consistency must report consistency_ok=1 when buckets exactly match stream.

    Both the hourly AND daily buckets must match the stream counts.
    """
    r = FakeRedis()
    stream = 'ops:orchestration:preflight:v1'
    field = encode_field(
        purpose='apply',
        selected_source='deploy_lint',
        decision_status='block',
        selected_reason_code='deploy_lint:missing_env',
    )
    # ts_ms=1700000000000 → hourly bucket: 1699999200000, daily bucket: 1699920000000
    r.streams[stream] = [
        ('1700000000000-0', {'purpose': 'apply', 'selected_source': 'deploy_lint', 'decision_status': 'block', 'selected_reason_code': 'deploy_lint:missing_env', 'ts_ms': '1700000000000'}),
    ]
    # Populate BOTH hourly and daily buckets to exact counts from stream
    r.hashes['metrics:orchestration:preflight:history:h:1699999200000'] = {field: '1'}
    r.hashes['metrics:orchestration:preflight:history:d:1699920000000'] = {field: '1'}
    # cursor and state match exactly
    r.kv['metrics:orchestration:preflight:history_rollup:last_id'] = '1700000000000-0'
    r.hashes['metrics:orchestration:preflight:history_rollup:last'] = {'last_stream_id': '1700000000000-0'}

    report = mod.check_consistency(
        r,
        stream_key=stream,
        start_ms=1699999200000,
        end_ms=1700003599999,
        hourly_prefix='metrics:orchestration:preflight:history:h',
        daily_prefix='metrics:orchestration:preflight:history:d',
        state_key='metrics:orchestration:preflight:history_rollup:last',
        cursor_key='metrics:orchestration:preflight:history_rollup:last_id',
        batch_size=10,
    )
    assert report['consistency_ok'] == 1
    assert report['drift_detected'] == 0
    assert report['state_cursor_match'] == 1


def test_render_text_exposes_consistency_metrics() -> None:
    """render_text must produce Prometheus-format gauge lines with correct label/value."""
    report = {
        'checked_at_ts_ms': 1700000000000,
        'consistency_ok': 0,
        'drift_detected': 1,
        'state_cursor_match': 1,
        'expected_stream_events': 7,
        'window_hours': 24.0,
        'hourly': {'mismatched_bucket_keys': 2, 'missing_fields': 1, 'extra_fields': 0, 'mismatched_value_fields': 3},
        'daily': {'mismatched_bucket_keys': 1, 'missing_fields': 0, 'extra_fields': 2, 'mismatched_value_fields': 0},
    }
    text = mod.render_text(report)
    assert 'orchestration_composite_preflight_rollup_consistency_ok 0.0' in text
    assert 'bucket_kind="hourly"' in text
    assert 'orchestration_composite_preflight_rollup_consistency_mismatched_value_fields_total' in text
    assert 'orchestration_composite_preflight_rollup_consistency_extra_fields_total{bucket_kind="daily"} 2.0' in text
