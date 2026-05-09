#!/usr/bin/env python3
from __future__ import annotations

from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS

"""Cross-check Redis execution state against Redis orders:exec and SQL journal.

Purpose
-------
P6 introduces an operator-friendly consistency checker that can be run from a
systemd timer, cron, or manually during incidents. The checker compares three
storage layers that may diverge during partial rollouts or transient failures:

* Redis state snapshots: ``orders:state:*``
* Redis execution facts: ``orders:exec`` stream
* SQL durable mirror: ``execution_orders`` / ``execution_protection_refs``

The checker is intentionally deterministic and read-only. It produces a compact
JSON summary, writes an optional report file consumed by the runbook server, and
returns a non-zero code only when configured critical thresholds are exceeded.
"""

import argparse
import json
import os
from collections.abc import Iterable, Iterator, Mapping
from dataclasses import asdict, dataclass
from typing import Any

# Fields that must be consistent across all three views
STATE_FIELDS = ("symbol", "status", "fsm_state", "execution_policy", "position_side")

# Protection algo references checked between Redis state and SQL refs table
PROTECTION_FIELDS = (
    "sl_algo_id",
    "tp1_algo_id",
    "tp2_algo_id",
    "tp3_algo_id",
    "trail_algo_id",
)


def _i(v: Any, default: int = 0) -> int:
    """Safe int conversion with fallback."""
    try:
        if v is None:
            return default
        return int(float(v))
    except Exception:
        return default


def _s(v: Any) -> str:
    """Safe str conversion."""
    return "" if v is None else str(v)


def _loads(value: Any) -> dict[str, Any]:
    """Deserialise a Redis value to dict; silently return {} on any error."""
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


@dataclass(frozen=True)
class ConsistencyMismatch:
    """A single consistency discrepancy across storage layers."""
    sid: str
    severity: str   # 'critical' | 'warning'
    category: str   # e.g. 'sql_missing', 'status_mismatch'
    detail: str


@dataclass(frozen=True)
class ConsistencySummary:
    """Aggregated result of one consistency run."""
    checked_at_ms: int
    redis_state_count: int
    stream_sid_count: int
    sql_order_count: int
    stream_scan_count: int
    mismatches_total: int
    critical_mismatches: int
    warning_mismatches: int
    stream_missing_suppressed: int
    redis_state_missing_suppressed: int
    mismatches: list[dict[str, Any]]


class RedisExecutionReader:
    """Reads execution state and stream events from Redis."""

    def __init__(self, redis_client: Any, state_prefix: str, exec_stream: str):
        self.redis = redis_client
        self.state_prefix = state_prefix
        self.exec_stream = exec_stream

    def iter_state(self) -> Iterator[tuple[str, dict[str, Any]]]:
        """Scan ``orders:state:*`` keys and yield (sid, doc)."""
        for key in self.redis.scan_iter(match=f"{self.state_prefix}*"):
            doc = _loads(self.redis.get(key))
            sid = _s(doc.get('sid')) or _s(key).split(self.state_prefix, 1)[-1]
            if sid:
                yield sid, doc

    def latest_stream_events(self, count: int = 20000) -> dict[str, dict[str, Any]]:
        """Return the latest ``orders:exec`` stream event per sid.

        Reads the most recent *count* entries via XREVRANGE (newest-first) and
        keeps only the first (= latest) payload seen for each sid.
        """
        latest: dict[str, dict[str, Any]] = {}
        rows = self.redis.xrevrange(self.exec_stream, '+', '-', count=count)
        # xrevrange returns newest-first. Keep first seen per sid as latest.
        for stream_id, fields in rows:
            payload = {str(k): v for k, v in dict(fields or {}).items()}
            sid = _s(payload.get('sid'))
            if sid and sid not in latest:
                payload['stream_id'] = stream_id
                latest[sid] = payload
        return latest


class SQLExecutionReader:
    """Reads execution data from the SQL journal (psycopg / psycopg2)."""

    def __init__(self, conn: Any):
        self.conn = conn

    def load_orders(self) -> dict[str, dict[str, Any]]:
        """Load all rows from ``execution_orders`` keyed by sid."""
        sql = (
            "SELECT sid, symbol, action, status, fsm_state, execution_policy, position_side, state_jsonb "
            "FROM execution_orders"
        )
        out: dict[str, dict[str, Any]] = {}
        with self.conn.cursor() as cur:
            cur.execute(sql)
            for sid, symbol, action, status, fsm_state, execution_policy, position_side, state_jsonb in cur.fetchall():
                doc = _loads(state_jsonb)
                doc.setdefault('sid', sid)
                doc.setdefault('symbol', symbol)
                doc.setdefault('action', action)
                doc.setdefault('status', status)
                doc.setdefault('fsm_state', fsm_state)
                doc.setdefault('execution_policy', execution_policy)
                doc.setdefault('position_side', position_side)
                out[_s(sid)] = doc
        return out

    def load_protection_refs(self) -> dict[str, dict[str, Any]]:
        """Load protection algo references from ``execution_protection_refs``."""
        sql = (
            "SELECT sid, symbol, sl_algo_id, tp1_algo_id, tp2_algo_id, tp3_algo_id, trail_algo_id "
            "FROM execution_protection_refs"
        )
        out: dict[str, dict[str, Any]] = {}
        with self.conn.cursor() as cur:
            cur.execute(sql)
            for sid, symbol, sl_algo_id, tp1_algo_id, tp2_algo_id, tp3_algo_id, trail_algo_id in cur.fetchall():
                out[_s(sid)] = {
                    'sid': sid,
                    'symbol': symbol,
                    'sl_algo_id': sl_algo_id,
                    'tp1_algo_id': tp1_algo_id,
                    'tp2_algo_id': tp2_algo_id,
                    'tp3_algo_id': tp3_algo_id,
                    'trail_algo_id': trail_algo_id,
                }
        return out


def compare_execution_views(
    redis_state: Mapping[str, Mapping[str, Any]],
    stream_latest: Mapping[str, Mapping[str, Any]],
    sql_orders: Mapping[str, Mapping[str, Any]],
    sql_refs: Mapping[str, Mapping[str, Any]] | None = None,
    sid_prefix_allowlist: tuple[str, ...] | None = None,
) -> tuple[list[ConsistencyMismatch], int, int]:
    """Compare the three execution views and return (mismatches, stream_missing_suppressed, redis_state_missing_suppressed).

    Parameters
    ----------
    sid_prefix_allowlist:
        When non-empty, only SIDs whose prefix matches one of the given strings
        are checked. SIDs that don't match are silently skipped.
        Use this to exclude openflow SIDs (e.g. ``crypto-of:``) which are
        appended to the exec stream but are never stored in ``orders:state:*``
        or ``execution_orders``, and therefore always produce spurious
        ``presence`` warnings.  Example: ``("crypto:",)`` or ``("ord:",)``.

    Rules
    -----
    * A sid present in at least 2 of 3 views is considered "known";
      missing from 0 or 1 views triggers individual warnings.
    * A sid present in only 1 view gets a single ``presence`` warning.
    * ``status`` and ``fsm_state`` divergences are always *critical*.
    * Protection field divergences (ignoring zeros/None) are *warning*.
    * ``stream_missing`` is suppressed for SQL-only SIDs (no Redis state):
      these are terminated/closed orders whose stream events have legitimately
      fallen outside the scan window. Reporting them as warnings creates noise
      and masks real anomalies. Only SIDs with an active Redis state entry but
      no stream event are flagged — that indicates a true gap.
    """
    sql_refs = sql_refs or {}
    mismatches: list[ConsistencyMismatch] = []
    stream_missing_suppressed = 0
    redis_state_missing_suppressed = 0
    all_sids = set(redis_state) | set(stream_latest) | set(sql_orders)

    for sid in sorted(all_sids):
        # Skip SIDs that don't belong to the executor namespace.  Openflow SIDs
        # (e.g. ``crypto-of:SOLUSDT:…``) are written to ``orders:exec`` by the
        # orderflow pipeline but are never stored in ``orders:state:*`` or
        # ``execution_orders``, so they always score present_count=1 and
        # generate spurious ``presence`` warnings.  When an allowlist is
        # configured, any SID whose prefix doesn't match is silently filtered.
        if sid_prefix_allowlist and not any(sid.startswith(p) for p in sid_prefix_allowlist):
            continue
        r = dict(redis_state.get(sid) or {})
        s = dict(stream_latest.get(sid) or {})
        q = dict(sql_orders.get(sid) or {})
        qr = dict(sql_refs.get(sid) or {})

        present_count = int(bool(r)) + int(bool(s)) + int(bool(q))
        if present_count < 2:
            # SQL-only with no Redis state: terminated/historical order whose stream
            # entry has scrolled past the scan window.  Expected for closed orders.
            if q and not r and not s:
                stream_missing_suppressed += 1
                continue
            # Stream-only with no Redis state and no SQL: intent_published signal that
            # was rejected by the executor before creating an order record.  Expected
            # noise for paper/virtual signals and executor-rejected entries.
            if s and not r and not q:
                stream_missing_suppressed += 1
                continue
            # Only found in one view – low-confidence report
            mismatches.append(ConsistencyMismatch(sid, 'warning', 'presence', 'sid missing from at least two mirrors'))
            continue

        # Report which individual views are missing
        if not r:
            # redis_state_missing suppression: completed orders have their Redis state key
            # expired/cleaned up while stream entries may still fall within the scan window.
            # This is expected noise; count silently instead of polluting the report.
            if s:
                redis_state_missing_suppressed += 1
        if not s:
            # stream_missing suppression: if Redis state is absent and SQL exists, the SID is a
            # terminated/historical order. Its stream events have likely fallen outside the scan
            # window (XREVRANGE COUNT limit). This is expected behaviour, not an anomaly.
            # Only report stream_missing when an active Redis state entry exists — meaning the
            # executor considers the order live but left no fact in the exec stream.
            if r:
                # Suppress: Redis state exists AND SQL record exists → completed order with
                # stale Redis state key (TTL not yet expired). Expected noise.
                if q:
                    stream_missing_suppressed += 1
                else:
                    mismatches.append(ConsistencyMismatch(
                        sid, 'warning', 'stream_missing',
                        'Redis orders:exec latest event not found in scan window (active sid)'
                    ))
            else:
                # SQL-only terminated SID: stream event outside scan window — suppressed
                stream_missing_suppressed += 1
        if not q:
            # Missing from SQL is the most critical gap – durable audit log incomplete
            mismatches.append(ConsistencyMismatch(sid, 'critical', 'sql_missing', 'SQL execution_orders row is missing'))

        # Check state field consistency across whichever views exist
        for field in STATE_FIELDS:
            values = {source: _s(doc.get(field)) for source, doc in [('redis', r), ('stream', s), ('sql', q)] if doc}
            if len(set(v for v in values.values() if v != '')) > 1:
                mismatches.append(ConsistencyMismatch(
                    sid,
                    'critical' if field in {'status', 'fsm_state'} else 'warning',
                    f'{field}_mismatch',
                    ', '.join(f'{k}={v}' for k, v in sorted(values.items())),
                ))

        # Protection refs are allowed to be absent until protection arming begins.
        # Only flag when non-zero/non-null values disagree.
        for field in PROTECTION_FIELDS:
            vals = {}
            if r:
                vals['redis'] = _s(r.get(field))
            if q:
                vals['sql_order'] = _s(q.get(field))
            if qr:
                vals['sql_refs'] = _s(qr.get(field))
            nz = {k: v for k, v in vals.items() if v not in {'', '0', 'None'}}
            if len(set(nz.values())) > 1:
                mismatches.append(ConsistencyMismatch(
                    sid,
                    'warning',
                    f'{field}_mismatch',
                    ', '.join(f'{k}={v}' for k, v in sorted(nz.items())),
                ))

    return mismatches, stream_missing_suppressed, redis_state_missing_suppressed


def summarise_mismatches(
    redis_state_count: int,
    stream_sid_count: int,
    sql_order_count: int,
    mismatches: Iterable[ConsistencyMismatch],
    *,
    stream_scan_count: int = 0,
    stream_missing_suppressed: int = 0,
    redis_state_missing_suppressed: int = 0,
) -> ConsistencySummary:
    """Aggregate a list of mismatches into a ``ConsistencySummary``."""
    items = list(mismatches)
    critical = sum(1 for m in items if m.severity == 'critical')
    warning = sum(1 for m in items if m.severity != 'critical')
    return ConsistencySummary(
        checked_at_ms=get_ny_time_millis(),
        redis_state_count=redis_state_count,
        stream_sid_count=stream_sid_count,
        sql_order_count=sql_order_count,
        stream_scan_count=stream_scan_count,
        mismatches_total=len(items),
        critical_mismatches=critical,
        warning_mismatches=warning,
        stream_missing_suppressed=stream_missing_suppressed,
        redis_state_missing_suppressed=redis_state_missing_suppressed,
        mismatches=[asdict(m) for m in items],
    )


def _connect_pg(dsn: str):
    """Connect to PostgreSQL; tries psycopg3, falls back to psycopg2."""
    try:
        import psycopg  # type: ignore
        return psycopg.connect(dsn)
    except Exception:  # pragma: no cover
        import psycopg2  # type: ignore
        return psycopg2.connect(dsn)


def run_check(
    *,
    redis_url: str,
    journal_dsn: str,
    state_prefix: str,
    exec_stream: str,
    stream_count: int = 50000,
    sid_prefix_allowlist: tuple[str, ...] | None = None,
) -> ConsistencySummary:
    """Full consistency check: connect, collect, compare, summarise.

    Parameters
    ----------
    sid_prefix_allowlist:
        Forwarded to :func:`compare_execution_views`.  Only SIDs whose string
        prefix matches one of the given values are checked; all others are
        silently skipped.  Leave as ``None`` (default) to check every SID.
    """
    import redis  # type: ignore
    r = redis.from_url(redis_url, decode_responses=True)
    redis_reader = RedisExecutionReader(r, state_prefix, exec_stream)
    redis_state = dict(redis_reader.iter_state())
    stream_latest = redis_reader.latest_stream_events(count=stream_count)
    conn = _connect_pg(journal_dsn)
    sql_reader = SQLExecutionReader(conn)
    sql_orders = sql_reader.load_orders()
    sql_refs = sql_reader.load_protection_refs()
    mismatches, stream_missing_suppressed, redis_state_missing_suppressed = compare_execution_views(
        redis_state, stream_latest, sql_orders, sql_refs,
        sid_prefix_allowlist=sid_prefix_allowlist,
    )
    return summarise_mismatches(
        len(redis_state), len(stream_latest), len(sql_orders), mismatches,
        stream_scan_count=stream_count,
        stream_missing_suppressed=stream_missing_suppressed,
        redis_state_missing_suppressed=redis_state_missing_suppressed,
    )


def _parse_prefix_allowlist(raw: str) -> tuple[str, ...] | None:
    """Parse a comma-separated SID prefix allowlist from an env/CLI string.

    Returns ``None`` (= no filter) when the string is empty or whitespace-only.
    Strips surrounding whitespace from each token and drops empty tokens.

    Example: ``"crypto:,ord:"`` → ``("crypto:", "ord:")``
    """
    parts = [p.strip() for p in raw.split(',') if p.strip()]
    return tuple(parts) if parts else None


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        description='Check consistency between Redis orders state/stream and SQL execution journal.'
    )
    parser.add_argument('--redis-url', default=os.getenv('REDIS_URL', 'redis://localhost:6379/0'))
    parser.add_argument('--journal-dsn', default=os.getenv('EXECUTION_JOURNAL_DSN', ''))
    parser.add_argument('--state-prefix', default=os.getenv('ORDERS_STATE_KEY_PREFIX', 'orders:state:'))
    parser.add_argument('--exec-stream', default=os.getenv('EXEC_STREAM', RS.ORDERS_EXEC))
    parser.add_argument('--stream-count', type=int, default=int(os.getenv('EXEC_CONSISTENCY_STREAM_COUNT', '50000')))
    parser.add_argument('--report-path', default=os.getenv('EXEC_CONSISTENCY_REPORT_PATH', ''))
    parser.add_argument('--critical-threshold', type=int, default=int(os.getenv('EXEC_CONSISTENCY_CRITICAL_THRESHOLD', '1')))
    parser.add_argument('--warning-threshold', type=int, default=int(os.getenv('EXEC_CONSISTENCY_WARNING_THRESHOLD', '10')))
    parser.add_argument(
        '--sid-prefix-allowlist',
        default=os.getenv('EXEC_CONSISTENCY_SID_PREFIX_ALLOWLIST', ''),
        help=(
            'Comma-separated SID prefix allowlist.  Only SIDs that start with '
            'one of these prefixes are checked; all others are silently skipped. '
            'Use this to suppress openflow SIDs (e.g. crypto-of:) that appear '
            'in orders:exec but are never stored in orders:state:* or SQL. '
            'Example: "crypto:,ord:".  Empty = check all SIDs (default).'
        )
    )
    args = parser.parse_args(argv)

    if not args.journal_dsn:
        raise SystemExit('EXECUTION_JOURNAL_DSN/--journal-dsn is required')

    sid_prefix_allowlist = _parse_prefix_allowlist(args.sid_prefix_allowlist)

    summary = run_check(
        redis_url=args.redis_url,
        journal_dsn=args.journal_dsn,
        state_prefix=args.state_prefix,
        exec_stream=args.exec_stream,
        stream_count=args.stream_count,
        sid_prefix_allowlist=sid_prefix_allowlist,
    )
    payload = asdict(summary)
    text = json.dumps(payload, ensure_ascii=False, indent=2, sort_keys=True)
    print(text)

    if args.report_path:
        os.makedirs(os.path.dirname(args.report_path), exist_ok=True)
        with open(args.report_path, 'w', encoding='utf-8') as fh:
            fh.write(text + '\n')

    # Exit codes: 0=ok, 1=warning threshold exceeded, 2=critical threshold exceeded
    if summary.critical_mismatches >= args.critical_threshold:
        return 2
    if summary.warning_mismatches >= args.warning_threshold:
        return 1
    return 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
