#!/usr/bin/env python3
from __future__ import annotations

"""Periodic scrubber for orphan/stale replay checkpoint keys.

P3.3-ops-complete: new script.

Removes checkpoint cursor keys (``orders:exec:replay:cursor:{sid}``) that are
either:
  - orphan: no corresponding ``orders:state:{sid}`` key AND no stream events found
  - stale:  checkpoint is beyond the Redis stream retention window (retention guard)
            and the stream-only rebuild returns no events

Also reports the full stream_retention_guard_report to stdout so systemd
captures it in the journal.

Designed to run under the ``trade-execution-checkpoint-scrubber.timer`` every 20 min.

ENV
---
  REDIS_URL                          (default redis://localhost:6379/0)
  EXEC_STREAM                        (default orders:exec)
  EXEC_REPLAY_CHECKPOINT_KEY_PREFIX  (default orders:exec:replay:cursor:)
  ORDERS_STATE_KEY_PREFIX            (default orders:state:)
  EXECUTION_JOURNAL_DSN              (optional SQL fallback DSN)
  EXEC_REPLAY_SCAN_COUNT             (default 20000)
  EXEC_REPLAY_CHECKPOINT_SCRUB_SAMPLE_LIMIT (default 5000)
"""

import argparse
import json
import os
from typing import Any, Dict, List

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

try:
    from services.execution_state_replay import rebuild_state_with_fallback, stream_retention_guard_report, _stream_oldest_id, normalize_stream_rows, _stream_sort_key, replay_sid_state, _load_sql_state_snapshot
except Exception:  # pragma: no cover
    try:
        from binance_execution.execution_state_replay import rebuild_state_with_fallback, stream_retention_guard_report, _stream_oldest_id, normalize_stream_rows, _stream_sort_key, replay_sid_state, _load_sql_state_snapshot  # type: ignore
    except Exception:
        from execution_state_replay import rebuild_state_with_fallback, stream_retention_guard_report, _stream_oldest_id, normalize_stream_rows, _stream_sort_key, replay_sid_state, _load_sql_state_snapshot  # type: ignore


def run_scrub(
    redis_client: Any
    *
    exec_stream: str
    checkpoint_prefix: str
    state_prefix: str
    journal_dsn: str
    scan_count: int
    sample_limit: int
    dry_run: bool
) -> Dict[str, Any]:
    """Run the checkpoint scrub and return the report dict.

    P3.3-autonomy: extracted from main() so that auto_trigger_checkpoint_scrubber
    can call this without spawning a subprocess.
    """
    cprefix = checkpoint_prefix.rstrip(':') + ':'
    sprefix = state_prefix.rstrip(':') + ':'

    # Collect retention guard report upfront (used for delete decisions)
    report: Dict[str, Any] = {
        'checked': 0
        'deleted': 0
        'retention_guard': stream_retention_guard_report(
            redis_client
            exec_stream=exec_stream
            checkpoint_prefix=cprefix
            sample_limit=sample_limit
        )
        'items': []
    }

    # Prefetch rows to bypass O(N) XREVRANGE calls
    has_prefetched_rows = False
    norm_rows_by_sid = {}
    oldest_stream_id = ""
    try:
        oldest_stream_id = _stream_oldest_id(redis_client, exec_stream)
        all_rows = redis_client.xrevrange(exec_stream, '+', '-', count=int(scan_count))
        norm_evs = normalize_stream_rows(all_rows)
        for ev in norm_evs:
            ev_sid = str(ev.get('sid') or '').strip()
            if ev_sid:
                norm_rows_by_sid.setdefault(ev_sid, []).append(ev)
        has_prefetched_rows = True
    except Exception:
        pass

    class DummyResult:
        def __init__(self, state_doc, source, checkpoint_id, retention_guard_triggered, latency_ms, truncated):
            self.state_doc = state_doc
            self.source = source
            self.checkpoint_id = checkpoint_id
            self.retention_guard_triggered = retention_guard_triggered
            self.latency_ms = latency_ms
            self.truncated = truncated

    keys = list(redis_client.scan_iter(match=f'{cprefix}*'))[:int(sample_limit)]
    for key in keys:
        report['checked'] += 1
        sid = str(key).split(cprefix, 1)[-1]
        # Check if state document exists
        state_raw = redis_client.get(f'{sprefix}{sid}')
        state_exists = bool(state_raw)
        checkpoint_id = str(redis_client.get(key) or '')
        
        # Try rebuilding from stream to determine viability
        if has_prefetched_rows and oldest_stream_id:
            retention_guard = False
            if checkpoint_id and oldest_stream_id:
                retention_guard = _stream_sort_key(checkpoint_id) < _stream_sort_key(oldest_stream_id)
            
            events = norm_rows_by_sid.get(sid, [])
            events.sort(key=lambda d: _stream_sort_key(str(d.get('stream_id') or '')))
            
            if events:
                state_doc = replay_sid_state(events)
                result = DummyResult(state_doc, "stream", checkpoint_id, retention_guard, 0, False)
            else:
                sql_state = _load_sql_state_snapshot(dsn=journal_dsn, sid=sid) if journal_dsn else {}
                if sql_state:
                    result = DummyResult(sql_state, "sql", checkpoint_id, retention_guard, 0, False)
                else:
                    result = DummyResult({}, "none", checkpoint_id, retention_guard, 0, False)
        else:
            result = rebuild_state_with_fallback(
                redis_client
                exec_stream=exec_stream
                sid=sid
                scan_count=scan_count
                checkpoint_id=checkpoint_id
                sql_dsn=journal_dsn
            )
        # Decide whether to delete the checkpoint key
        delete_reason = ''
        if not state_exists and not result.state_doc:
            # No state and no stream events — orphan checkpoint
            delete_reason = 'orphan_checkpoint'
        elif result.retention_guard_triggered and result.source == 'none':
            # Checkpoint beyond stream retention AND no SQL fallback — stale
            delete_reason = 'checkpoint_beyond_stream_retention'

        if delete_reason:
            report['items'].append({
                'sid': sid
                'delete_reason': delete_reason
                'retention_guard_triggered': bool(result.retention_guard_triggered)
                'source': result.source
            })
            if not dry_run:
                redis_client.delete(key)
                report['deleted'] += 1

    return report


def main() -> int:
    parser = argparse.ArgumentParser(description='Periodic scrubber for replay checkpoint keys.')
    parser.add_argument('--redis-url', default=os.getenv('REDIS_URL', 'redis://localhost:6379/0'))
    parser.add_argument('--exec-stream', default=os.getenv('EXEC_STREAM', 'orders:exec'))
    parser.add_argument('--checkpoint-prefix', default=os.getenv('EXEC_REPLAY_CHECKPOINT_KEY_PREFIX', 'orders:exec:replay:cursor:'))
    parser.add_argument('--state-prefix', default=os.getenv('ORDERS_STATE_KEY_PREFIX', 'orders:state:'))
    parser.add_argument('--journal-dsn', default=os.getenv('EXECUTION_JOURNAL_DSN', ''))
    parser.add_argument('--scan-count', type=int, default=int(os.getenv('EXEC_REPLAY_SCAN_COUNT', '20000')))
    parser.add_argument('--sample-limit', type=int, default=int(os.getenv('EXEC_REPLAY_CHECKPOINT_SCRUB_SAMPLE_LIMIT', '5000')))
    parser.add_argument('--dry-run', action='store_true')
    args = parser.parse_args()
    if redis is None:
        raise RuntimeError('redis package required')
    r = redis.from_url(args.redis_url, decode_responses=True)
    report = run_scrub(
        r
        exec_stream=args.exec_stream
        checkpoint_prefix=args.checkpoint_prefix
        state_prefix=args.state_prefix
        journal_dsn=args.journal_dsn
        scan_count=args.scan_count
        sample_limit=args.sample_limit
        dry_run=bool(args.dry_run)
    )
    print(json.dumps(report, ensure_ascii=False, indent=2))
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
