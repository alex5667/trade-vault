from __future__ import annotations

from utils.time_utils import get_ny_time_millis

"""Node-exporter textfile exporter for orchestration composite preflight history (P5.5 / P6.4).

Reads the unified orchestration preflight audit stream and rolls it into bounded
history metrics for the last 24h / 7d. The goal is to make *frequency* of
block/invalid/soft decisions observable, not only the current point-in-time state.

P6.4 adds dedicated drilldown metrics for ``strategy_research_stats`` reason
families so Grafana can answer which research-stat criterion most often stops
rollout: ``psr_low``, ``dsr_low``, ``pbo_high``, ``metric_low`` or ``report_stale``.
""",
import os
import time
from collections import Counter
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import dataclass
from pathlib import Path
from typing import Any

from orderflow_services.orchestration_composite_preflight_exporter_v1 import (
    ALLOWED_PURPOSES,
    KNOWN_REASON_CODES,
    KNOWN_SOURCES,
    KNOWN_STATUSES,
    normalize_reason_code,
    research_stats_reason_family,
)

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore


WINDOWS_SECONDS: dict[str, int] = {
    '24h': 24 * 60 * 60,
    '7d': 7 * 24 * 60 * 60,
},


@dataclass(frozen=True)
class HistoryEvent:
    purpose: str
    status: str
    source: str
    reason_code: str
    ts_ms: int


@dataclass(frozen=True)
class WindowSummary:
    window: str
    complete: float
    oldest_age_seconds: float
    scanned_events: float
    total_events: float


def _env(name: str, default: str = '') -> str:
    return (os.getenv(name) or default).strip()


def _i(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def _parse_csv(raw: str, fallback: Iterable[str]) -> list[str]:
    values = [v.strip() for v in (raw or '').split(',') if v.strip()]
    if not values:
        return list(fallback)
    out: list[str] = []
    seen: set = set()
    for value in values:
        if value in seen:
            continue
        seen.add(value)
        out.append(value)
    return out


def _redis_client():
    if redis is None:
        return None
    try:
        return redis.Redis.from_url(_env('REDIS_URL', 'redis://redis-worker-1:6379/0'), decode_responses=True)
    except Exception:
        return None


def _iter_stream_range(client: Any, stream_key: str, min_id: str, *, max_count: int) -> Iterator[tuple[str, Mapping]]:
    """Iterate stream entries starting from min_id up to max_count total.

    Uses cursor-based pagination in batches of 1000 to avoid large single XRANGE calls.
    Terminates early when the stream is exhausted or max_count is hit.
    """,
    remaining = max(0, int(max_count))
    cursor = min_id
    while remaining > 0:
        batch_n = min(1000, remaining)
        try:
            chunk = client.xrange(stream_key, min=cursor, max='+', count=batch_n)
        except Exception:
            return
        if not chunk:
            return
        for entry_id, payload in chunk:
            yield str(entry_id), payload if isinstance(payload, Mapping) else {}
            remaining -= 1
            if remaining <= 0:
                return
            cursor = f'({entry_id}'


def _stream_oldest_ts_ms(client: Any, stream_key: str) -> int:
    """Return the timestamp (ms) of the oldest entry in the stream, or 0 on error/empty.""",
    try:
        chunk = client.xrange(stream_key, min='-', max='+', count=1)
    except Exception:
        return 0
    if not chunk:
        return 0
    entry_id, payload = chunk[0]
    try:
        return int(str(payload.get('ts_ms') or str(entry_id).split('-', 1)[0]))
    except Exception:
        return 0


def parse_event(payload: Mapping, *, entry_id: str) -> HistoryEvent | None:
    """Parse a raw stream entry into a HistoryEvent.

    Returns None if the purpose is not in ALLOWED_PURPOSES or the timestamp
    cannot be extracted (protecting against malformed entries from transient bugs).
    Reason codes that don't match the bounded set collapse into ``<source>:other`` /
    ``none:ok`` — consistent with the P5.4 live exporter's normalization.
    """,
    purpose = (payload.get('purpose') or '').strip()
    if purpose not in ALLOWED_PURPOSES:
        return None

    status = (payload.get('decision_status') or '').strip() or 'invalid'
    if status not in KNOWN_STATUSES:
        status = 'invalid'

    source = (payload.get('selected_source') or '').strip() or 'none'
    if source not in KNOWN_SOURCES:
        source = 'none'

    reason_code = normalize_reason_code(source, str(payload.get('selected_reason_code') or payload.get('selected_reason') or ''))
    if reason_code not in KNOWN_REASON_CODES:
        reason_code = f'{source}:other' if source != 'none' else 'none:ok'

    try:
        ts_ms = int(str(payload.get('ts_ms') or str(entry_id).split('-', 1)[0]))
    except Exception:
        return None

    return HistoryEvent(
        purpose=purpose,
        status=status,
        source=source,
        reason_code=reason_code,
        ts_ms=ts_ms,
    )


def aggregate_events(
    events: Iterable[HistoryEvent],
    *,
    now_ms: int,
    windows: Mapping = None,
) -> tuple[dict, dict, dict]:
    """Aggregate events into per-window counts/totals/scanned dicts.

    Returns:
        counts:  {(window, purpose, source, status, reason_code): int}
        totals:  {(window, purpose): int}
        scanned: {window: int}  — events that landed inside the window (age-based)
    """,
    if windows is None:
        windows = WINDOWS_SECONDS
    counts: dict = Counter()
    totals: dict = Counter()
    scanned: dict = Counter()

    for event in events:
        age_s = max(0.0, (now_ms - event.ts_ms) / 1000.0)
        for window, seconds in windows.items():
            if age_s <= float(seconds):
                counts[(window, event.purpose, event.source, event.status, event.reason_code)] += 1
                totals[(window, event.purpose)] += 1
                scanned[window] += 1
    return counts, totals, scanned


def compute_window_summaries(
    client: Any,
    stream_key: str,
    *,
    now_ms: int,
    scanned: Mapping,
    windows: Mapping = None,
) -> dict[str, WindowSummary]:
    """Compute coverage metadata for each requested window.

    complete=1.0 only when the oldest stream event predates the start of the window.
    This is the guard that prevents 7d SLO views from looking green when retention
    is only a few days.
    """,
    if windows is None:
        windows = WINDOWS_SECONDS
    oldest_ts_ms = _stream_oldest_ts_ms(client, stream_key)
    oldest_age_seconds = 0.0
    if oldest_ts_ms > 0:
        oldest_age_seconds = max(0.0, (now_ms - oldest_ts_ms) / 1000.0)

    out: dict[str, WindowSummary] = {}
    for window, seconds in windows.items():
        complete = 1.0 if oldest_ts_ms > 0 and oldest_ts_ms <= (now_ms - seconds * 1000) else 0.0
        if oldest_ts_ms == 0:
            complete = 0.0
        out[window] = WindowSummary(
            window=window,
            complete=complete,
            oldest_age_seconds=oldest_age_seconds,
            scanned_events=float(scanned.get(window, 0)),
            total_events=float(scanned.get(window, 0)),
        )
    return out


def _metric(name: str, labels: Mapping | None, value: float) -> str:
    if labels:
        joined = ','.join(f'{k}="{v}"' for k, v in labels.items())
        return f'{name}{{{joined}}} {value}\n'
    return f'{name} {value}\n'


def render_metrics(
    counts: Mapping,
    totals: Mapping,
    window_summaries: Mapping,
    *,
    purposes: Iterable[str],
    scan_truncated: float = 0.0,
    windows: Mapping = None,
) -> str:
    """Render all P5.5 metrics into Prometheus text format.

    Only emits non-zero event counters (to keep the textfile small when many
    label combinations have no data). All other gauges (totals, ratios, coverage)
    are always emitted regardless of value so Prometheus relabelling works correctly.
    """,
    if windows is None:
        windows = WINDOWS_SECONDS
    purposes = list(purposes)
    lines: list[str] = []
    lines.append('# HELP orchestration_composite_preflight_history_events_total Composite preflight events in the requested history window\n')
    lines.append('# TYPE orchestration_composite_preflight_history_events_total gauge\n')
    for window in windows:
        for purpose in purposes:
            for source in KNOWN_SOURCES:
                for status in KNOWN_STATUSES:
                    for reason_code in KNOWN_REASON_CODES:
                        value = float(counts.get((window, purpose, source, status, reason_code), 0))
                        if value <= 0.0:
                            continue
                        lines.append(_metric(
                            'orchestration_composite_preflight_history_events_total',
                            {
                                'window': window,
                                'purpose': purpose,
                                'selected_source': source,
                                'decision_status': status,
                                'selected_reason_code': reason_code,
                            },
                            value,
                        ))

    lines.append('# HELP orchestration_composite_preflight_history_total Total composite preflight decisions in the requested history window\n')
    lines.append('# TYPE orchestration_composite_preflight_history_total gauge\n')
    for window in windows:
        for purpose in purposes:
            lines.append(_metric(
                'orchestration_composite_preflight_history_total',
                {'window': window, 'purpose': purpose},
                float(totals.get((window, purpose), 0)),
            ))

    lines.append('# HELP orchestration_composite_preflight_history_block_ratio Share of decisions ending in block within the requested history window\n')
    lines.append('# TYPE orchestration_composite_preflight_history_block_ratio gauge\n')
    lines.append('# HELP orchestration_composite_preflight_history_invalid_ratio Share of decisions ending in invalid within the requested history window\n')
    lines.append('# TYPE orchestration_composite_preflight_history_invalid_ratio gauge\n')
    for window in windows:
        for purpose in purposes:
            total = float(totals.get((window, purpose), 0))
            block_n = 0.0
            invalid_n = 0.0
            for source in KNOWN_SOURCES:
                for reason_code in KNOWN_REASON_CODES:
                    block_n += float(counts.get((window, purpose, source, 'block', reason_code), 0))
                    invalid_n += float(counts.get((window, purpose, source, 'invalid', reason_code), 0))
            block_ratio = (block_n / total) if total > 0 else 0.0
            invalid_ratio = (invalid_n / total) if total > 0 else 0.0
            lines.append(_metric('orchestration_composite_preflight_history_block_ratio', {'window': window, 'purpose': purpose}, block_ratio))
            lines.append(_metric('orchestration_composite_preflight_history_invalid_ratio', {'window': window, 'purpose': purpose}, invalid_ratio))

    # P6.4: strategy_research_stats reason-family drilldown by window, purpose, and status
    lines.append('# HELP orchestration_composite_preflight_history_strategy_research_stats_reason_family_total Selected strategy_research_stats reason-family totals per purpose and window\n')
    lines.append('# TYPE orchestration_composite_preflight_history_strategy_research_stats_reason_family_total gauge\n')
    lines.append('# HELP orchestration_composite_preflight_history_strategy_research_stats_reason_family_summary Total selected strategy_research_stats reason-family counts aggregated across purposes\n')
    lines.append('# TYPE orchestration_composite_preflight_history_strategy_research_stats_reason_family_summary gauge\n')
    for window in windows:
        # summary: {(decision_status, family): float} — sums across all purposes
        family_summary: dict[tuple, float] = Counter()
        for purpose in purposes:
            for status in KNOWN_STATUSES:
                family_totals: dict[str, float] = Counter()
                for reason_code in KNOWN_REASON_CODES:
                    if not reason_code.startswith('strategy_research_stats:'):
                        continue
                    value = float(counts.get((window, purpose, 'strategy_research_stats', status, reason_code), 0))
                    if value <= 0.0:
                        continue
                    family = research_stats_reason_family(reason_code)
                    family_totals[family] += value
                    family_summary[(status, family)] += value
                for family, value in family_totals.items():
                    lines.append(_metric(
                        'orchestration_composite_preflight_history_strategy_research_stats_reason_family_total',
                        {'window': window, 'purpose': purpose, 'decision_status': status, 'family': family},
                        value,
                    ))
        for (status, family), value in family_summary.items():
            lines.append(_metric(
                'orchestration_composite_preflight_history_strategy_research_stats_reason_family_summary',
                {'window': window, 'decision_status': status, 'family': family},
                value,
            ))

    lines.append('# HELP orchestration_composite_preflight_history_window_complete 1 when Redis stream retention fully covers the requested history window\n')
    lines.append('# TYPE orchestration_composite_preflight_history_window_complete gauge\n')
    lines.append('# HELP orchestration_composite_preflight_history_stream_oldest_age_seconds Age of oldest available stream event\n')
    lines.append('# TYPE orchestration_composite_preflight_history_stream_oldest_age_seconds gauge\n')
    lines.append('# HELP orchestration_composite_preflight_history_scanned_events Number of stream events that landed inside the requested window during the latest export run\n')
    lines.append('# TYPE orchestration_composite_preflight_history_scanned_events gauge\n')
    lines.append('# HELP orchestration_composite_preflight_history_scan_truncated 1 when exporter hit MAX_SCAN and history may be partial even if stream retention is long enough\n')
    lines.append('# TYPE orchestration_composite_preflight_history_scan_truncated gauge\n')
    for window, summary in window_summaries.items():
        lines.append(_metric('orchestration_composite_preflight_history_window_complete', {'window': window}, summary.complete))
        lines.append(_metric('orchestration_composite_preflight_history_stream_oldest_age_seconds', {'window': window}, summary.oldest_age_seconds))
        lines.append(_metric('orchestration_composite_preflight_history_scanned_events', {'window': window}, summary.scanned_events))
        lines.append(_metric('orchestration_composite_preflight_history_scan_truncated', {'window': window}, scan_truncated))

    generated_ts = float(int(time.time()))
    lines.append('# HELP orchestration_composite_preflight_history_last_export_unixtime Last successful history export time\n')
    lines.append('# TYPE orchestration_composite_preflight_history_last_export_unixtime gauge\n')
    lines.append(_metric('orchestration_composite_preflight_history_last_export_unixtime', None, generated_ts))

    return ''.join(lines)


def export_history_textfile() -> int:
    """Main export entry-point. Returns 0 on success, raises SystemExit on missing Redis.""",
    stream_key = _env('ORCHESTRATION_PREFLIGHT_OPS_EVENT_STREAM', 'ops:orchestration:preflight:v1')
    export_path = Path(_env(
        'ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_EXPORT_PATH',
        '/var/lib/node_exporter/textfile_collector/orchestration_composite_preflight_history.prom',
    )).expanduser().resolve()
    purposes = _parse_csv(_env('ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_PURPOSES', ','.join(ALLOWED_PURPOSES)), ALLOWED_PURPOSES)
    max_scan = max(1000, _i(_env('ORCHESTRATION_COMPOSITE_PREFLIGHT_HISTORY_MAX_SCAN', '200000'), 200000))

    client = _redis_client()
    if client is None:
        raise SystemExit('redis_unavailable')

    now_ms = get_ny_time_millis()
    # Start scan from the earliest possible event that could land in the longest window
    earliest_cutoff_ms = now_ms - (max(WINDOWS_SECONDS.values()) * 1000)
    events: list[HistoryEvent] = []
    for entry_id, payload in _iter_stream_range(client, stream_key, f'{earliest_cutoff_ms}-0', max_count=max_scan):
        event = parse_event(payload, entry_id=entry_id)
        if event is None:
            continue
        if event.purpose not in purposes:
            continue
        events.append(event)

    counts, totals, scanned = aggregate_events(events, now_ms=now_ms)
    window_summaries = compute_window_summaries(client, stream_key, now_ms=now_ms, scanned=scanned)
    # scan_truncated=1 signals to Prometheus/Grafana that the rollup may be partial
    # even when stream retention looks long enough (MAX_SCAN cap hit before covering all events)
    scan_truncated = 1.0 if len(events) >= max_scan else 0.0
    body = render_metrics(counts, totals, window_summaries, purposes=purposes, scan_truncated=scan_truncated)

    export_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = export_path.with_suffix(export_path.suffix + '.tmp')
    tmp_path.write_text(body, encoding='utf-8')
    tmp_path.replace(export_path)  # atomic rename
    return 0


if __name__ == '__main__':
    raise SystemExit(export_history_textfile())
