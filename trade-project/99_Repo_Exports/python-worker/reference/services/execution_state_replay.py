from __future__ import annotations

from utils.time_utils import get_ny_time_millis

"""Replay Redis ``orders:exec`` facts into a materialized ``orders:state:{sid}`` snapshot.

This module closes the P3.3 gap where Redis state keys are treated as a fast cache
while the execution stream is the authoritative source of facts. The replay logic
is intentionally deterministic and side-effect free so it can be reused by the
executor on restart, by maintenance scripts, and by unit tests.

P3.3-ops-complete additions:
- stream_retention_guard_report(): scan checkpoint keys for Redis stream retention drift
- _stream_oldest_id() / _retention_guard_triggered(): internal retention helpers
- _bounded_rows() now returns (rows, truncated, retention_guard) triple
- rebuild_state_with_fallback() tracks latency_ms and retention_guard_triggered
- Prometheus: TRADE_EXECUTION_REPLAY_RETENTION_GUARD_TOTAL (Counter), TRADE_EXECUTION_REPLAY_LATENCY_MS (Histogram)
"""

import json
import time
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from typing import Any


def _metric(cls, name, doc, labels=None, **kwargs):
    """Safe Prometheus metric factory — returns None when prometheus_client is absent."""
    try:
        if cls is None:
            return None
        if labels:
            return cls(name, doc, labels, **kwargs)
        return cls(name, doc, **kwargs)
    except Exception:
        return None


try:  # pragma: no cover
    from prometheus_client import REGISTRY, Counter, Histogram
except Exception:  # pragma: no cover
    Counter = None  # type: ignore
    Histogram = None  # type: ignore
    REGISTRY = None  # type: ignore


# --- Prometheus metrics (P3.3-hardening) ---

TRADE_EXECUTION_REHYDRATE_TOTAL = _metric(
    Counter,
    'trade_execution_rehydrate_total',
    'Total execution state rehydration attempts, labelled by source and result.',
    ['source', 'result'],
)

TRADE_EXECUTION_REPLAY_TRUNCATED_TOTAL = _metric(
    Counter,
    'trade_execution_replay_truncated_total',
    'Replay attempts where the scan window hit the scan_count cap.',
    ['result'],
)

TRADE_EXECUTION_REPLAY_SQL_FALLBACK_TOTAL = _metric(
    Counter,
    'trade_execution_replay_sql_fallback_total',
    'Replay attempts that fell back to the SQL snapshot.',
    ['result'],
)

# P3.3-ops-complete: retention guard counter
TRADE_EXECUTION_REPLAY_RETENTION_GUARD_TOTAL = _metric(
    Counter,
    'trade_execution_replay_retention_guard_total',
    'Replay attempts where the checkpoint fell behind Redis stream retention.',
    ['result'],
)

# P3.3-ops-complete: replay latency histogram
TRADE_EXECUTION_REPLAY_LATENCY_MS = _metric(
    Histogram,
    'trade_execution_replay_latency_ms',
    'Execution replay/rehydrate latency in milliseconds.',
    buckets=(1, 2.5, 5, 10, 25, 50, 100, 250, 500, 1000, 2500, 5000),
)


try:  # pragma: no cover
    import psycopg  # type: ignore
except Exception:  # pragma: no cover
    try:
        import psycopg2 as psycopg  # type: ignore
    except Exception:
        psycopg = None  # type: ignore


# ---------------------------------------------------------------------------
# Core data structures
# ---------------------------------------------------------------------------

@dataclass
class ReplayBuildResult:
    """Result of a single rebuild_state_with_fallback() call."""
    state_doc: dict[str, Any]
    source: str  # 'stream' | 'sql' | 'none'
    used_checkpoint: bool
    checkpoint_id: str
    replayed_events: int
    truncated: bool
    # P3.3-ops-complete additions
    retention_guard_triggered: bool = False
    latency_ms: int = 0


def _s(v: Any) -> str:
    """Convert any value to str, decoding bytes with UTF-8."""
    if v is None:
        return ''
    if isinstance(v, bytes):
        return v.decode('utf-8', 'replace')
    return str(v)


def _i(v: Any, default: int = 0) -> int:
    try:
        if v is None:
            return default
        return int(float(v))
    except Exception:
        return default


def _loads(value: Any) -> dict[str, Any]:
    if value is None:
        return {}
    if isinstance(value, dict):
        return dict(value)
    if isinstance(value, bytes):
        value = value.decode('utf-8', 'replace')
    try:
        obj = json.loads(value)
        return obj if isinstance(obj, dict) else {}
    except Exception:
        return {}


# Keys that should never be copied into the materialized state document.
TRANSIENT_FIELDS = {
    'stream_id',
    'event_type',
    'severity',
    'error_class',
    'msg',
    'reason',
}


STATE_PRIORITY_FIELDS = {
    'sid', 'symbol', 'action', 'status', 'venue', 'execution_policy',
    'fsm_state', 'fsm_prev_state', 'entry_client_order_id', 'binance_order_id',
    'filled_qty', 'avg_price', 'entry_status', 'side', 'qty', 'exec_price',
    'sl_algo_id', 'tp1_algo_id', 'tp2_algo_id', 'tp3_algo_id', 'trail_algo_id'
    'tp1_state', 'tp2_state', 'tp3_state',
}


def normalize_stream_rows(rows: Sequence[tuple[Any, Mapping[str, Any]]]) -> list[dict[str, Any]]:
    """Normalize XRANGE/XREVRANGE rows into plain dictionaries.

    Each output item contains the original ``stream_id`` plus decoded field/value
    pairs. The function accepts either bytes or strings and preserves ordering
    from the input sequence.
    """
    out: list[dict[str, Any]] = []
    for stream_id, fields in rows:
        doc: dict[str, Any] = {'stream_id': _s(stream_id)}
        for k, v in dict(fields or {}).items():
            key = _s(k)
            if isinstance(v, bytes):
                v = v.decode('utf-8', 'replace')
            doc[key] = v
        out.append(doc)
    return out


def extract_sid_events(rows: Sequence[tuple[Any, Mapping[str, Any]]], sid: str) -> list[dict[str, Any]]:
    sid = _s(sid)
    events = [doc for doc in normalize_stream_rows(rows) if _s(doc.get('sid')) == sid]
    # XRANGE returns oldest-first; XREVRANGE newest-first. Sort by stream id to
    # guarantee deterministic oldest->newest replay regardless of caller.
    events.sort(key=lambda d: _stream_sort_key(_s(d.get('stream_id'))))
    return events


def _stream_sort_key(stream_id: str) -> tuple[int, int]:
    try:
        left, right = stream_id.split('-', 1)
        return int(left), int(right)
    except Exception:
        return (0, 0)


def replay_sid_state(events: Sequence[Mapping[str, Any]], *, base_state: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Replay one SID's event list into a materialized state snapshot.

    Replay strategy:
    - process events oldest->newest
    - copy stable fields into the state document
    - keep the latest non-empty value for the important execution fields
    - track replay metadata so operators can see when the state was rebuilt from
      the stream rather than directly loaded from Redis.
    """
    state: dict[str, Any] = dict(base_state or {})
    replay_count = 0
    last_stream_id = ''
    for ev in events:
        replay_count += 1
        last_stream_id = _s(ev.get('stream_id'))
        for key, value in ev.items():
            if key in TRANSIENT_FIELDS:
                continue
            if value in (None, ''):
                continue
            if key in STATE_PRIORITY_FIELDS or key.startswith('tp') or key.startswith('trail_') or key.startswith('fsm_'):
                state[key] = value
        # Preserve action/status even when only emitted in generic fact events.
        if _s(ev.get('action')):
            state['action'] = _s(ev.get('action'))
        if _s(ev.get('status')):
            state['status'] = _s(ev.get('status'))
        if _s(ev.get('symbol')):
            state['symbol'] = _s(ev.get('symbol'))
        if _s(ev.get('sid')):
            state['sid'] = _s(ev.get('sid'))
        # A state_transition event is authoritative for fsm_state.
        if _s(ev.get('event_type')) == 'state_transition' and _s(ev.get('fsm_state')):
            state['fsm_state'] = _s(ev.get('fsm_state'))
            state['fsm_prev_state'] = _s(ev.get('prev_state') or ev.get('fsm_prev_state'))
        if _s(ev.get('ts_ms')):
            state['ts_ms'] = _i(ev.get('ts_ms'))
        if _s(ev.get('mono_ms')):
            state['last_event_mono_ms'] = _i(ev.get('mono_ms'))
    if replay_count:
        state['rehydrated_from_stream'] = True
        state['stream_replayed_events'] = replay_count
        state['stream_last_id'] = last_stream_id
        state['state_source_stream'] = 'orders:exec'
        state['rehydrated_ts_ms'] = get_ny_time_millis()
    return state


# ---------------------------------------------------------------------------
# P3.3-ops-complete: Redis stream retention guard helpers
# ---------------------------------------------------------------------------

def _stream_oldest_id(redis_client: Any, exec_stream: str) -> str:
    """Return the oldest stream entry ID, or '' on failure."""
    try:
        rows = redis_client.xrange(exec_stream, '-', '+', count=1)
        if rows:
            return _s(rows[0][0])
    except Exception:
        pass
    try:
        rows = redis_client.xrevrange(exec_stream, '+', '-', count=1000000)
        if rows:
            return _s(rows[-1][0])
    except Exception:
        pass
    return ''


def _retention_guard_triggered(redis_client: Any, *, exec_stream: str, checkpoint_id: str) -> bool:
    """Return True when the checkpoint_id is older than the stream's oldest entry.

    This indicates that Redis already evicted events the checkpoint needs for an
    accurate replay — the checkpoint is stale and should be scrubbed.
    """
    if not checkpoint_id:
        return False
    oldest = _stream_oldest_id(redis_client, exec_stream)
    if not oldest:
        return False
    return _stream_sort_key(checkpoint_id) < _stream_sort_key(oldest)


def stream_retention_guard_report(
    redis_client: Any,
    *,
    exec_stream: str,
    checkpoint_prefix: str,
    sample_limit: int = 2000,
) -> dict[str, Any]:
    """Scan checkpoint keys and report how many have drifted behind stream retention.

    Used by execution_healthcheck.py and scrub_replay_checkpoints.py.
    """
    prefix = checkpoint_prefix.rstrip(':') + ':'
    oldest = _stream_oldest_id(redis_client, exec_stream)
    total = 0
    breached = 0
    examples: list[dict[str, Any]] = []
    try:
        keys = list(redis_client.scan_iter(match=f'{prefix}*'))[:int(sample_limit)]
    except Exception:
        keys = []
    for key in keys:
        total += 1
        sid = _s(key).split(prefix, 1)[-1]
        try:
            checkpoint_id = _s(redis_client.get(key))
        except Exception:
            checkpoint_id = ''
        if checkpoint_id and oldest and _stream_sort_key(checkpoint_id) < _stream_sort_key(oldest):
            breached += 1
            if len(examples) < 20:
                examples.append({'sid': sid, 'checkpoint_id': checkpoint_id, 'oldest_stream_id': oldest})
    return {
        'exec_stream': exec_stream,
        'checkpoint_prefix': prefix,
        'oldest_stream_id': oldest,
        'checked_checkpoint_keys': total,
        'breached_checkpoints': breached,
        'breached_examples': examples,
        'status': 'critical' if breached > 0 else 'ok',
    }


# ---------------------------------------------------------------------------
# Bounded row fetch with checkpoint and retention guard
# ---------------------------------------------------------------------------

def _bounded_rows(
    redis_client: Any,
    *,
    exec_stream: str,
    checkpoint_id: str,
    scan_count: int,
) -> tuple[list[tuple[Any, Mapping[str, Any]]], bool, bool]:
    """Fetch at most ``scan_count`` stream rows relative to the checkpoint.

    Returns (rows, truncated, retention_guard_triggered).

    retention_guard_triggered is True when the checkpoint_id is older than the
    oldest entry in the stream (meaning Redis has already evicted some entries
    the checkpoint pointed at).
    """
    retention_guard = _retention_guard_triggered(redis_client, exec_stream=exec_stream, checkpoint_id=checkpoint_id)
    if not checkpoint_id:
        rows = redis_client.xrevrange(exec_stream, '+', '-', count=int(scan_count))
        return rows, False, retention_guard
    recent = redis_client.xrevrange(exec_stream, '+', checkpoint_id, count=int(scan_count))
    if recent:
        return recent, False, retention_guard
    older = redis_client.xrevrange(exec_stream, checkpoint_id, '-', count=int(scan_count))
    return older, bool(older), retention_guard


def rebuild_state_from_stream(redis_client: Any, *, exec_stream: str, sid: str, scan_count: int = 20000) -> dict[str, Any]:
    """Load latest rows from the execution stream and rebuild one ``sid`` state."""
    rows = redis_client.xrevrange(exec_stream, '+', '-', count=int(scan_count))
    events = extract_sid_events(rows, sid)
    return replay_sid_state(events)


def _load_sql_state_snapshot(*, dsn: str, sid: str) -> dict[str, Any]:
    """Load the most recent execution state snapshot from the SQL journal.

    Returns an empty dict when no matching snapshot is found or on any error.
    """
    if not dsn or psycopg is None:
        return {}
    try:
        conn = psycopg.connect(dsn)
        with conn.cursor() as cur:
            cur.execute(
                'SELECT snapshot FROM execution_state_snapshots WHERE sid = %s ORDER BY created_at DESC LIMIT 1',
                (sid,),
            )
            row = cur.fetchone()
            if row:
                return _loads(row[0])
    except Exception:
        pass
    return {}


def rebuild_state_with_fallback(
    redis_client: Any,
    *,
    exec_stream: str,
    sid: str,
    scan_count: int = 20000,
    checkpoint_id: str = '',
    sql_dsn: str = '',
) -> ReplayBuildResult:
    """Rebuild SID state from stream (with SQL fallback).

    P3.3-ops-complete: tracks latency_ms and retention_guard_triggered on all
    exit paths. Increments TRADE_EXECUTION_REPLAY_RETENTION_GUARD_TOTAL when
    the checkpoint is stale, and observes TRADE_EXECUTION_REPLAY_LATENCY_MS.
    """
    started = time.perf_counter()
    rows, truncated, retention_guard = _bounded_rows(
        redis_client, exec_stream=exec_stream, checkpoint_id=checkpoint_id, scan_count=scan_count
    )
    events = extract_sid_events(rows, sid)
    # Emit metric for retention guard (stream-path; may also fire on sql/none path below)
    if retention_guard and TRADE_EXECUTION_REPLAY_RETENTION_GUARD_TOTAL:
        TRADE_EXECUTION_REPLAY_RETENTION_GUARD_TOTAL.labels(result='triggered').inc()
    if events:
        state = replay_sid_state(events)
        latency_ms = int((time.perf_counter() - started) * 1000)
        if TRADE_EXECUTION_REPLAY_LATENCY_MS:
            TRADE_EXECUTION_REPLAY_LATENCY_MS.observe(latency_ms)
        if TRADE_EXECUTION_REHYDRATE_TOTAL:
            TRADE_EXECUTION_REHYDRATE_TOTAL.labels(source='stream', result='ok').inc()
        if truncated and TRADE_EXECUTION_REPLAY_TRUNCATED_TOTAL:
            TRADE_EXECUTION_REPLAY_TRUNCATED_TOTAL.labels(result='truncated').inc()
        return ReplayBuildResult(
            state_doc=state,
            source='stream',
            used_checkpoint=bool(checkpoint_id),
            checkpoint_id=checkpoint_id,
            replayed_events=int(state.get('stream_replayed_events') or len(events)),
            truncated=truncated,
            retention_guard_triggered=retention_guard,
            latency_ms=latency_ms,
        )
    sql_state = _load_sql_state_snapshot(dsn=sql_dsn, sid=sid) if sql_dsn else {}
    if sql_state:
        latency_ms = int((time.perf_counter() - started) * 1000)
        if TRADE_EXECUTION_REPLAY_LATENCY_MS:
            TRADE_EXECUTION_REPLAY_LATENCY_MS.observe(latency_ms)
        if TRADE_EXECUTION_REPLAY_SQL_FALLBACK_TOTAL:
            TRADE_EXECUTION_REPLAY_SQL_FALLBACK_TOTAL.labels(result='ok').inc()
        if TRADE_EXECUTION_REHYDRATE_TOTAL:
            TRADE_EXECUTION_REHYDRATE_TOTAL.labels(source='sql', result='ok').inc()
        return ReplayBuildResult(
            state_doc=sql_state,
            source='sql',
            used_checkpoint=bool(checkpoint_id),
            checkpoint_id=checkpoint_id,
            replayed_events=0,
            truncated=truncated,
            retention_guard_triggered=retention_guard,
            latency_ms=latency_ms,
        )
    latency_ms = int((time.perf_counter() - started) * 1000)
    if TRADE_EXECUTION_REPLAY_LATENCY_MS:
        TRADE_EXECUTION_REPLAY_LATENCY_MS.observe(latency_ms)
    if TRADE_EXECUTION_REPLAY_SQL_FALLBACK_TOTAL:
        TRADE_EXECUTION_REPLAY_SQL_FALLBACK_TOTAL.labels(result='miss').inc()
    if TRADE_EXECUTION_REHYDRATE_TOTAL:
        TRADE_EXECUTION_REHYDRATE_TOTAL.labels(source='none', result='miss').inc()
    if retention_guard and TRADE_EXECUTION_REPLAY_RETENTION_GUARD_TOTAL:
        TRADE_EXECUTION_REPLAY_RETENTION_GUARD_TOTAL.labels(result='miss').inc()
    return ReplayBuildResult(
        state_doc={},
        source='none',
        used_checkpoint=bool(checkpoint_id),
        checkpoint_id=checkpoint_id,
        replayed_events=0,
        truncated=truncated,
        retention_guard_triggered=retention_guard,
        latency_ms=latency_ms,
    )


def persist_state_snapshot(
    redis_client: Any,
    *,
    state_key: str,
    state_doc: Mapping[str, Any],
    ttl_sec: int = 0,
    checkpoint_key: str = '',
) -> bool:
    """Write state_doc to Redis.

    Optionally update a checkpoint key to the stream_last_id from state_doc.
    Returns True on success.
    """
    if not state_doc:
        return False
    payload = json.dumps(dict(state_doc), ensure_ascii=False, default=str)
    if int(ttl_sec) > 0:
        redis_client.set(state_key, payload, ex=int(ttl_sec))
    else:
        redis_client.set(state_key, payload)
    if checkpoint_key:
        last_id = _s(state_doc.get('stream_last_id'))
        if last_id:
            redis_client.set(checkpoint_key, last_id)
    return True


def compare_replayed_state(redis_state: Mapping[str, Any], replayed_state: Mapping[str, Any]) -> dict[str, Any]:
    """Return a compact mismatch report for operator/runbook use."""
    mismatches: dict[str, Any] = {}
    fields = [
        'symbol', 'status', 'fsm_state', 'execution_policy', 'binance_order_id',
        'sl_algo_id', 'tp1_algo_id', 'tp2_algo_id', 'tp3_algo_id', 'trail_algo_id'
    ]
    for field in fields:
        left = _s(redis_state.get(field))
        right = _s(replayed_state.get(field))
        if left != right:
            mismatches[field] = {'redis_state': left, 'replayed_state': right}
    return mismatches
