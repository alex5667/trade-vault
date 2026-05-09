from __future__ import annotations

from pathlib import Path

from orderflow_services.orchestration_composite_preflight_history_exporter_v1 import (
    HistoryEvent,
    aggregate_events,
    compute_window_summaries,
    export_history_textfile,
    parse_event,
    render_metrics,
)


class FakeRedis:
    """In-memory Redis stub that supports xrange with min/max/count filtering."""

    def __init__(self, events):
        self._events = events

    def xrange(self, stream_key, min='-', max='+', count=None):
        items = self._events
        if min not in ('-', None):
            exclusive = str(min).startswith('(')
            token = str(min)[1:] if exclusive else str(min)
            items = [item for item in items if (item[0] > token if exclusive else item[0] >= token)]
        if max not in ('+', None):
            items = [item for item in items if item[0] <= str(max)]
        if count is not None:
            items = items[: int(count)]
        return items


def test_parse_event_normalizes_reason_code() -> None:
    """Unknown reason codes must collapse into '<source>:other', not explode cardinality."""
    event = parse_event(
        {
            'purpose': 'conf_score_guardrails_promote',
            'decision_status': 'block',
            'selected_source': 'research_guard',
            'selected_reason_code': 'strange_runtime_problem',
            'ts_ms': '1000',
        },
        entry_id='1000-0',
    )
    assert event is not None
    assert event.reason_code == 'research_guard:other'


def test_parse_event_rejects_unknown_purpose() -> None:
    """Events with an unrecognized purpose must be discarded (purpose-bounded labels)."""
    event = parse_event(
        {
            'purpose': 'totally_unknown_purpose_xyz',
            'decision_status': 'block',
            'selected_source': 'deploy_lint',
            'ts_ms': '1000',
        },
        entry_id='1000-0',
    )
    assert event is None


def test_parse_event_falls_back_to_entry_id_timestamp() -> None:
    """When ts_ms is absent, the entry ID millisecond prefix should be used."""
    event = parse_event(
        {
            'purpose': 'conf_score_guardrails_promote',
            'decision_status': 'ok',
            'selected_source': 'none',
        },
        entry_id='5000-0',
    )
    assert event is not None
    assert event.ts_ms == 5000


def test_aggregate_events_counts_24h_and_7d() -> None:
    """Events inside 24h window must appear in 24h bucket; events 2d ago only in 7d."""
    now_ms = 8 * 24 * 60 * 60 * 1000  # 8 days as epoch ms
    events = [
        HistoryEvent('conf_score_guardrails_promote', 'block', 'deploy_lint', 'deploy_lint:persistent_config_drift', now_ms - 2 * 60 * 60 * 1000),
        HistoryEvent('conf_score_guardrails_promote', 'invalid', 'research_guard', 'research_guard:report_stale', now_ms - 2 * 24 * 60 * 60 * 1000),
    ]
    counts, totals, scanned = aggregate_events(events, now_ms=now_ms)
    assert counts[('24h', 'conf_score_guardrails_promote', 'deploy_lint', 'block', 'deploy_lint:persistent_config_drift')] == 1
    assert ('24h', 'conf_score_guardrails_promote', 'research_guard', 'invalid', 'research_guard:report_stale') not in counts
    assert counts[('7d', 'conf_score_guardrails_promote', 'research_guard', 'invalid', 'research_guard:report_stale')] == 1
    assert totals[('24h', 'conf_score_guardrails_promote')] == 1
    assert totals[('7d', 'conf_score_guardrails_promote')] == 2
    assert scanned['24h'] == 1
    assert scanned['7d'] == 2


def test_compute_window_summaries_reports_incomplete_history() -> None:
    """7d window must be marked complete=0 when oldest stream event is only 6 days old."""
    now_ms = 10 * 24 * 60 * 60 * 1000
    fake = FakeRedis([
        ('518400000-0', {'ts_ms': str(6 * 24 * 60 * 60 * 1000)}),
    ])
    summaries = compute_window_summaries(fake, 'ops:orchestration:preflight:v1', now_ms=now_ms, scanned={'24h': 1, '7d': 2})
    assert summaries['24h'].complete == 1.0
    assert summaries['7d'].complete == 0.0


def test_render_metrics_has_required_headers() -> None:
    """All mandatory metric families must appear in rendered output."""
    body = render_metrics({}, {}, {}, purposes=['conf_score_guardrails_promote'])
    assert 'orchestration_composite_preflight_history_events_total' in body
    assert 'orchestration_composite_preflight_history_block_ratio' in body
    assert 'orchestration_composite_preflight_history_window_complete' in body
    assert 'orchestration_composite_preflight_history_scan_truncated' in body
    assert 'orchestration_composite_preflight_history_last_export_unixtime' in body


def test_export_history_textfile_writes_metrics(monkeypatch, tmp_path: Path) -> None:
    """Integration: export_history_textfile must write a valid .prom file atomically."""
    now_ms = 9 * 24 * 60 * 60 * 1000
    events = [
        ('777599000-0', {
            'ts_ms': str(now_ms - 1000),
            'purpose': 'conf_score_guardrails_promote',
            'decision_status': 'block',
            'selected_source': 'deploy_lint',
            'selected_reason_code': 'deploy_lint:persistent_config_drift',
        }),
        ('86400000-0', {
            'ts_ms': str(now_ms - 8 * 24 * 60 * 60 * 1000),
            'purpose': 'conf_score_guardrails_promote',
            'decision_status': 'block',
            'selected_source': 'deploy_lint',
            'selected_reason_code': 'deploy_lint:persistent_config_drift',
        }),
    ]
    fake = FakeRedis(events)
    out = tmp_path / 'orchestration_composite_preflight_history.prom'

    monkeypatch.setenv('ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_EXPORT_PATH', str(out))
    monkeypatch.setenv('ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_PURPOSES', 'conf_score_guardrails_promote')
    monkeypatch.setenv('ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_MAX_SCAN', '100')
    monkeypatch.setattr(
        'orderflow_services.orchestration_composite_preflight_history_exporter_v1._redis_client',
        lambda: fake,
    )
    monkeypatch.setattr('time.time', lambda: now_ms / 1000.0)

    assert export_history_textfile() == 0
    body = out.read_text()
    # Only the recent event (within 24h) should appear in 24h window
    assert 'orchestration_composite_preflight_history_events_total{window="24h",purpose="conf_score_guardrails_promote",selected_source="deploy_lint",decision_status="block",selected_reason_code="deploy_lint:persistent_config_drift"} 1.0' in body
    # The 8d-old event falls in the 7d total (actually beyond 7d, so only 1 event in 7d too — the recent one)
    assert 'orchestration_composite_preflight_history_total{window="7d",purpose="conf_score_guardrails_promote"}' in body
    assert 'orchestration_composite_preflight_history_scan_truncated{window="24h"} 0.0' in body
    # window_complete must be emitted (value depends on stream age vs window)
    assert 'orchestration_composite_preflight_history_window_complete{window="7d"}' in body
