#!/usr/bin/env python3
from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""Rebuild ``orders:state:*`` materialized view keys from the ``orders:exec`` stream.

P3.3-ops-complete additions:
- Writes a rebuild report to RUNBOOK_REPORT_DIR/latest_rebuild_state.json (for
  the runbook server /api/rebuild/latest endpoint)
- Report includes: source_counts, truncated_count, retention_guard_count,
  replay_latency_p95_ms, retention_guard_triggered and latency_ms per item

Usage
-----
  # Rebuild all SIDs found in last 20 000 stream entries:
  python3 scripts/rebuild_orders_state_from_exec.py

  # Rebuild specific SIDs only:
  python3 scripts/rebuild_orders_state_from_exec.py --sid sid-1 --sid sid-2

  # Dry-run (no Redis writes):
  python3 scripts/rebuild_orders_state_from_exec.py --dry-run

ENV
---
  REDIS_URL             (default redis://localhost:6379/0)
  EXEC_STREAM           (default orders:exec)
  ORDERS_STATE_KEY_PREFIX (default orders:state:)
  EXEC_REPLAY_SCAN_COUNT (default 20000)
  ORDERS_STATE_TTL_SEC   (default 86400)
  RUNBOOK_REPORT_DIR     (default /var/lib/trade-runbook/reports)
"""

import argparse
import json
import os
import time
from pathlib import Path
from typing import Any, Dict, List

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

try:
    from services.execution_state_replay import rebuild_state_with_fallback, persist_state_snapshot
except Exception:  # pragma: no cover
    try:
        from binance_execution.execution_state_replay import rebuild_state_with_fallback, persist_state_snapshot  # type: ignore
    except Exception:
        from execution_state_replay import rebuild_state_with_fallback, persist_state_snapshot  # type: ignore


def render_prometheus_textfile(report: Dict[str, Any]) -> str:
    """Convert rebuild report to Prometheus node-exporter textfile format.

    P3.3-autonomy: exposes rebuild last-run metrics for Prometheus scraping
    via node_exporter textfile_collector.  Status code 0 means full rebuild
    succeeded; 1 means at least one SID was not rebuilt.
    """
    status_code = 0 if int(report.get('rebuilt_count') or 0) == int(report.get('items_total') or 0) else 1
    pairs = {
        'trade_execution_rebuild_last_status_code': status_code,
        'trade_execution_rebuild_last_items_total': int(report.get('items_total') or 0),
        'trade_execution_rebuild_last_rebuilt_count': int(report.get('rebuilt_count') or 0),
        'trade_execution_rebuild_last_truncated_count': int(report.get('truncated_count') or 0),
        'trade_execution_rebuild_last_retention_guard_count': int(report.get('retention_guard_count') or 0),
        'trade_execution_rebuild_last_replay_latency_p95_ms': int(report.get('replay_latency_p95_ms') or 0),
        'trade_execution_rebuild_last_checked_at_ms': int(report.get('checked_at_ms') or 0),
    }
    lines = []
    for name, value in pairs.items():
        lines.append(f'# TYPE {name} gauge')
        lines.append(f'{name} {value}')
    return '\n'.join(lines) + '\n'


def main() -> int:
    parser = argparse.ArgumentParser(description='Rebuild orders:state:* materialized views from orders:exec stream.')
    parser.add_argument('--redis-url', default=os.getenv('REDIS_URL', 'redis://localhost:6379/0'))
    parser.add_argument('--exec-stream', default=os.getenv('EXEC_STREAM', 'orders:exec'))
    parser.add_argument('--state-prefix', default=(os.getenv('ORDERS_STATE_KEY_PREFIX') or 'orders:state:'))
    parser.add_argument('--checkpoint-prefix', default=os.getenv('EXEC_REPLAY_CHECKPOINT_KEY_PREFIX', 'orders:exec:replay:cursor:'))
    parser.add_argument('--scan-count', type=int, default=int(os.getenv('EXEC_REPLAY_SCAN_COUNT', '20000')))
    parser.add_argument('--sid', action='append', default=[])
    parser.add_argument('--ttl-sec', type=int, default=int(os.getenv('ORDERS_STATE_TTL_SEC', '86400')))
    parser.add_argument('--dry-run', action='store_true')
    # P3.3-ops-complete: report dir for latest_rebuild_state.json
    parser.add_argument('--report-dir', default=os.getenv('RUNBOOK_REPORT_DIR', '/var/lib/trade-runbook/reports'))
    # P3.3-autonomy: optional Prometheus textfile export for node_exporter
    parser.add_argument('--textfile-output', default=os.getenv('EXEC_REBUILD_TEXTFILE_PATH', ''))
    args = parser.parse_args()
    if redis is None:
        raise RuntimeError('redis package required')
    r = redis.from_url(args.redis_url, decode_responses=True)
    state_prefix = args.state_prefix.rstrip(':') + ':'
    checkpoint_prefix = args.checkpoint_prefix.rstrip(':') + ':'
    sids: List[str] = list(args.sid or [])
    has_prefetched_rows = False
    if not sids:
        # Discover unique SIDs from recent stream entries
        has_prefetched_rows = True
        rows = r.xrevrange(args.exec_stream, '+', '-', count=int(args.scan_count))
        seen = []
        seen_set = set()
        for _stream_id, fields in rows:
            sid = str(dict(fields or {}).get('sid') or '').strip()
            if sid and sid not in seen_set:
                seen.append(sid)
                seen_set.add(sid)
        sids = seen

    # Optional optimizations for batch mode
    oldest_stream_id = ""
    norm_rows_by_sid = {}
    if has_prefetched_rows:
        try:
            old_rows = r.xrange(args.exec_stream, '-', '+', count=1)
            if old_rows:
                oldest_stream_id = str(old_rows[0][0])
            from services.execution_state_replay import _stream_sort_key, extract_sid_events, replay_sid_state, normalize_stream_rows
            
            # Pre-group the rows to avoid O(M*N) decoding overhead
            norm_evs = normalize_stream_rows(rows)
            for ev in norm_evs:
                ev_sid = str(ev.get('sid') or '').strip()
                if ev_sid:
                    norm_rows_by_sid.setdefault(ev_sid, []).append(ev)
        except Exception as prefetch_err:
            pass
        
    rebuilt: List[Dict[str, Any]] = []
    # P3.3-ops-complete: aggregate stats for report
    latencies: List[int] = []
    sources: Dict[str, int] = {}
    truncated_count = 0
    retention_guard_count = 0
    for sid in sids:
        started = time.perf_counter()
        checkpoint_id = str(r.get(f'{checkpoint_prefix}{sid}') or '')
        
        if has_prefetched_rows and oldest_stream_id:
            # Fully in-memory processing to bypass O(N) Redis XREVRANGE calls
            retention_guard = False
            if checkpoint_id and oldest_stream_id:
                retention_guard = _stream_sort_key(checkpoint_id) < _stream_sort_key(oldest_stream_id)
            
            # extract_sid_events sorts them so oldest is first
            events = norm_rows_by_sid.get(sid, [])
            events.sort(key=lambda d: _stream_sort_key(str(d.get('stream_id') or '')))
            
            # Simple fallback struct matching ReplayBuildResult
            class DummyResult:
                def __init__(self, state_doc, source, checkpoint_id, retention_guard_triggered, latency_ms, truncated):
                    self.state_doc = state_doc
                    self.source = source
                    self.checkpoint_id = checkpoint_id
                    self.retention_guard_triggered = retention_guard_triggered
                    self.latency_ms = latency_ms
                    self.truncated = truncated
                    
            if events:
                state_doc = replay_sid_state(events)
                latency_ms = int((time.perf_counter() - started) * 1000)
                result = DummyResult(state_doc, "stream", checkpoint_id, retention_guard, latency_ms, False)
            else:
                latency_ms = int((time.perf_counter() - started) * 1000)
                result = DummyResult({}, "none", checkpoint_id, retention_guard, latency_ms, False)
        else:
            result = rebuild_state_with_fallback(
                r,
                exec_stream=args.exec_stream,
                sid=sid,
                scan_count=args.scan_count,
                checkpoint_id=checkpoint_id,
            )
            
        if not result.state_doc:
            rebuilt.append({'sid': sid, 'rebuilt': False, 'reason': 'stream_events_not_found',
                            'source': result.source, 'checkpoint_id': result.checkpoint_id,
                            'retention_guard_triggered': bool(result.retention_guard_triggered),
                            'latency_ms': int(result.latency_ms)})
            latencies.append(int(result.latency_ms))
            sources[result.source] = sources.get(result.source, 0) + 1
            truncated_count += int(bool(result.truncated))
            retention_guard_count += int(bool(result.retention_guard_triggered))
            continue
        if not args.dry_run:
            persist_state_snapshot(
                r,
                state_key=f'{state_prefix}{sid}',
                state_doc=result.state_doc,
                ttl_sec=args.ttl_sec,
                checkpoint_key=f'{checkpoint_prefix}{sid}',
            )
        # P3.3-ops-complete: per-item stats
        latencies.append(int(result.latency_ms))
        sources[result.source] = sources.get(result.source, 0) + 1
        truncated_count += int(bool(result.truncated))
        retention_guard_count += int(bool(result.retention_guard_triggered))
        rebuilt.append({
            'sid': sid,
            'rebuilt': True,
            'fsm_state': result.state_doc.get('fsm_state'),
            'stream_last_id': result.state_doc.get('stream_last_id'),
            'events': result.state_doc.get('stream_replayed_events', 0),
            'source': result.source,
            'checkpoint_id': result.checkpoint_id,
            'truncated': result.truncated,
            # P3.3-ops-complete additions
            'retention_guard_triggered': bool(result.retention_guard_triggered),
            'latency_ms': int(result.latency_ms),
        })

    # P3.3-ops-complete: compute p95 latency and write report
    p95 = 0
    if latencies:
        latencies_sorted = sorted(latencies)
        idx = max(0, int((len(latencies_sorted) - 1) * 0.95))
        p95 = int(latencies_sorted[idx])

    report = {
        'rebuilt_count': len([x for x in rebuilt if x.get('rebuilt')]),
        'items_total': len(rebuilt),
        'truncated_count': truncated_count,
        'retention_guard_count': retention_guard_count,
        'source_counts': sources,
        'replay_latency_p95_ms': p95,
        'checked_at_ms': get_ny_time_millis(),
        'items': rebuilt,
    }

    # Write report for runbook server /api/rebuild/latest endpoint
    report_dir = Path(args.report_dir)
    report_dir.mkdir(parents=True, exist_ok=True)
    (report_dir / 'latest_rebuild_state.json').write_text(
        json.dumps(report, ensure_ascii=False, indent=2), encoding='utf-8'
    )
    # P3.3-autonomy: optional Prometheus textfile export
    if args.textfile_output:
        Path(args.textfile_output).parent.mkdir(parents=True, exist_ok=True)
        Path(args.textfile_output).write_text(render_prometheus_textfile(report), encoding='utf-8')

    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
