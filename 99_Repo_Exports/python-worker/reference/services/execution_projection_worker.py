from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""Deterministic HA-safe projection worker for ``orders:exec`` -> ``orders:state:{sid}``.

P1.2.1: single-node deterministic materialisation (journal-first).
P1.2.2: HA leader-lease + fencing token, health endpoint, rebuild CLI.

The executor appends only to the primary execution journal. This worker consumes
journal events in stream order and materializes the latest SID snapshot into the
Redis cache used by dashboards, downstream readers and restart fast-paths.

Design goals
------------
- deterministic ordering: one global cursor over ``orders:exec``
- idempotent projection: replaying the same stream window yields the same state
- fail-open: projection lag must not block execution appends
- journal-first: executor never needs to mutate the derived cache inline
- HA-safe (P1.2.2): leader lease + fencing token prevent split-brain writes;
  standby replicas wait silently until they win the lease
- backward-compatible: set EXEC_PROJECTION_LEASE_ENABLE=0 for single-node mode

HA lease model
--------------
Leader elections use Redis ``SET key worker_id NX PX ttl_ms`` (atomic, no Lua).
A background thread renews the lease at ``renew_interval_ms`` before it expires.
Each successful acquisition atomically INCRs ``fence_key`` to produce a monotonic
fencing token. Before each write batch the worker re-checks the token against Redis
to detect a stale-writer (e.g. GC pause longer than lease TTL).

Rebuild CLI (--mode rebuild)
----------------------------
  --rebuild-all                replay entire orders:exec into orders:state:*
  --rebuild-sid SID            rebuild one SID only
  --reset-derived-state        DEL all orders:state:* keys (dangerous - confirm first)
  --set-cursor-to-tip          advance projection cursor to latest stream ID
  --print-health               dump health JSON and exit
"""

import argparse
import json
import os
import socket
import sys
import threading
import time
from dataclasses import dataclass, field
from typing import Any, Dict, Iterable, List, Mapping, Optional, Sequence, Tuple

try:  # pragma: no cover
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

try:  # pragma: no cover
    from prometheus_client import Counter, Gauge, REGISTRY
except Exception:  # pragma: no cover
    Counter = None  # type: ignore
    Gauge = None  # type: ignore
    REGISTRY = None  # type: ignore

try:  # pragma: no cover
    from services.execution_contracts import build_materialized_state_view
    from services.execution_journal import ExecutionJournalSink
    from services.execution_state_replay import project_event_into_state
except Exception:  # pragma: no cover
    from execution_contracts import build_materialized_state_view
    from execution_journal import ExecutionJournalSink
    from execution_state_replay import project_event_into_state


# ---------------------------------------------------------------------------
# Prometheus metric helpers
# ---------------------------------------------------------------------------

def _metric(factory, name: str, *args, **kwargs):
    """Create or retrieve an existing Prometheus metric safely."""
    if factory is None:
        return None
    try:
        return factory(name, *args, **kwargs)
    except ValueError:
        # Already registered — retrieve from registry
        return getattr(REGISTRY, '_names_to_collectors', {}).get(name) if REGISTRY is not None else None


def _ms_now() -> int:
    return get_ny_time_millis()


def _s(v: Any) -> str:
    if v is None:
        return ''
    if isinstance(v, bytes):
        return v.decode('utf-8', 'replace')
    return str(v)


def _stream_sort_key(stream_id: str) -> Tuple[int, int]:
    """Convert Redis stream ID '1700000000000-0' to comparable tuple."""
    try:
        left, right = str(stream_id).split('-', 1)
        return int(left), int(right)
    except Exception:
        return (0, 0)


# ---------------------------------------------------------------------------
# Prometheus metrics (P1.2.1 + P1.2.2)
# ---------------------------------------------------------------------------

TRADE_EXECUTION_PROJECTION_EVENT_TOTAL = _metric(
    Counter,
    'trade_execution_projection_event_total',
    'Projected execution journal events processed by the derived-state worker.',
    ['result'],
)

TRADE_EXECUTION_PROJECTION_LAG_MS = _metric(
    Gauge,
    'trade_execution_projection_lag_ms',
    'Age of the most recently projected execution journal event.',
)

TRADE_EXECUTION_PROJECTION_CURSOR_TS_MS = _metric(
    Gauge,
    'trade_execution_projection_cursor_ts_ms',
    'Timestamp component of the latest projected orders:exec stream ID.',
)

# P1.2.2 HA metrics
TRADE_EXECUTION_PROJECTION_IS_LEADER = _metric(
    Gauge,
    'trade_execution_projection_is_leader',
    'Whether this worker instance currently holds the projection leader lease (1=leader, 0=standby).',
)

TRADE_EXECUTION_PROJECTION_FENCING_TOKEN = _metric(
    Gauge,
    'trade_execution_projection_fencing_token',
    'Current fencing token value (monotonically increasing on each lease acquisition).',
)

TRADE_EXECUTION_PROJECTION_STALE_WRITER_TOTAL = _metric(
    Counter,
    'trade_execution_projection_stale_writer_total',
    'Times a stale-writer (fencing token mismatch) was detected and the batch was aborted.',
)


# ---------------------------------------------------------------------------
# P1.2.2: Leader Lease with fencing token
# ---------------------------------------------------------------------------

class LeaderLease:
    """Distributed leader lease via Redis SET NX PX + monotonic fencing token.

    Usage::

        lease = LeaderLease(r, worker_id='worker-1')
        if lease.acquire():
            # we are leader; token is our fencing token
            token = lease.fencing_token()

    The lease is automatically renewed in a background thread every
    ``renew_interval_ms`` milliseconds. If the process is paused longer than
    the lease TTL the renewal fails and ``is_leader()`` returns False.
    """

    def __init__(
        self,
        redis_client: Any,
        *,
        lease_key: str = 'orders:exec:projection:leader',
        fence_key: str = 'orders:exec:projection:fence',
        lease_ttl_ms: int = 5000,
        renew_interval_ms: int = 2000,
        worker_id: str = '',
    ) -> None:
        self.r = redis_client
        self.lease_key = lease_key
        self.fence_key = fence_key
        self.lease_ttl_ms = max(int(lease_ttl_ms), 1000)
        self.renew_interval_ms = max(int(renew_interval_ms), 200)
        self.worker_id = worker_id or socket.gethostname()
        self._leader = threading.Event()
        self._fencing_token: int = 0
        self._stop = threading.Event()
        self._lock = threading.Lock()
        self._renew_thread: Optional[threading.Thread] = None

    def acquire(self) -> bool:
        """Try to acquire the lease. Returns True if this worker is now leader."""
        try:
            # SET lease_key worker_id NX PX ttl_ms
            ok = self.r.set(self.lease_key, self.worker_id, nx=True, px=self.lease_ttl_ms)
            if ok:
                # Atomically increment fencing counter
                token = self.r.incr(self.fence_key)
                with self._lock:
                    self._fencing_token = int(token)
                    self._leader.set()
                self._start_renew_thread()
                return True
            # Maybe we already own it (e.g. after fast restart within TTL)
            owner = _s(self.r.get(self.lease_key))
            if owner == self.worker_id:
                with self._lock:
                    self._leader.set()
                self._start_renew_thread()
                return True
        except Exception:
            pass
        with self._lock:
            self._leader.clear()
        return False

    def release(self) -> None:
        """Voluntarily release the lease."""
        self._stop.set()
        try:
            owner = _s(self.r.get(self.lease_key))
            if owner == self.worker_id:
                self.r.delete(self.lease_key)
        except Exception:
            pass
        with self._lock:
            self._leader.clear()

    def is_leader(self) -> bool:
        """Check if this instance currently holds the lease (local flag)."""
        return self._leader.is_set()

    def fencing_token(self) -> int:
        """Return our fencing token (0 if never acquired)."""
        with self._lock:
            return self._fencing_token

    def remote_fencing_token(self) -> int:
        """Read the current fencing token from Redis (authoritative)."""
        try:
            v = self.r.get(self.fence_key)
            return int(_s(v)) if v else 0
        except Exception:
            return 0

    def is_stale_writer(self) -> bool:
        """Return True if our fencing token is behind Redis (split-brain / eviction)."""
        local = self.fencing_token()
        if local == 0:
            return False  # never acquired → not considered stale
        remote = self.remote_fencing_token()
        return remote > local

    def _renew(self) -> bool:
        """Extend the lease TTL. Returns True if still leader."""
        try:
            owner = _s(self.r.get(self.lease_key))
            if owner != self.worker_id:
                with self._lock:
                    self._leader.clear()
                return False
            # Use PEXPIRE to extend without a new NX check
            self.r.pexpire(self.lease_key, self.lease_ttl_ms)
            return True
        except Exception:
            return False

    def _renew_loop(self) -> None:
        """Background renewal loop — runs until stop event is set."""
        sleep_s = max(0.05, self.renew_interval_ms / 1000.0)
        while not self._stop.wait(timeout=sleep_s):
            still_leader = self._renew()
            if not still_leader:
                with self._lock:
                    self._leader.clear()
                break

    def _start_renew_thread(self) -> None:
        """Start background renewal thread if not already running."""
        self._stop.clear()
        if self._renew_thread is not None and self._renew_thread.is_alive():
            return
        t = threading.Thread(target=self._renew_loop, daemon=True, name='projection-lease-renew')
        self._renew_thread = t
        t.start()


# ---------------------------------------------------------------------------
# P1.2.1 + P1.2.2 Batch result
# ---------------------------------------------------------------------------

@dataclass
class ProjectionBatchResult:
    processed: int = 0
    last_stream_id: str = ''
    idle: bool = True
    skipped_not_leader: bool = False  # P1.2.2: standby skip
    stale_writer: bool = False        # P1.2.2: fencing abort


# ---------------------------------------------------------------------------
# Main projection worker
# ---------------------------------------------------------------------------

class ExecutionProjectionWorker:
    """Read ``orders:exec`` sequentially and maintain ``orders:state:{sid}``.

    This is the sole writer of the derived state cache when
    ``EXEC_INLINE_STATE_PROJECTION=0`` (the default). The executor only
    appends to ``orders:exec``; this worker projects each event in order.

    P1.2.2 HA mode (EXEC_PROJECTION_LEASE_ENABLE=1)
    ------------------------------------------------
    A ``LeaderLease`` is injected at construction time. ``run_once()`` checks
    the lease before every batch:
    - not leader → returns idle=True, skipped_not_leader=True
    - stale writer (fencing token mismatch) → aborts batch, emits metric
    """

    def __init__(
        self,
        redis_client: Any,
        *,
        exec_stream: str = 'orders:exec',
        state_key_prefix: str = 'orders:state:',
        active_symbol_key_prefix: str = 'orders:active_symbol_sid:',
        state_ttl_sec: int = 86400,
        cursor_key: str = 'orders:exec:projection:cursor',
        batch_size: int = 500,
        execution_journal: Optional[ExecutionJournalSink] = None,
        leader_lease: Optional[LeaderLease] = None,  # P1.2.2: inject lease
    ) -> None:
        self.r = redis_client
        self.exec_stream = str(exec_stream or 'orders:exec')
        self.state_key_prefix = str(state_key_prefix or 'orders:state:').rstrip(':') + ':'
        self.active_symbol_key_prefix = str(active_symbol_key_prefix or 'orders:active_symbol_sid:').rstrip(':') + ':'
        self.state_ttl_sec = int(state_ttl_sec)
        self.cursor_key = str(cursor_key or 'orders:exec:projection:cursor')
        self.batch_size = max(int(batch_size or 500), 1)
        self.execution_journal = execution_journal
        self.leader_lease = leader_lease  # None → single-node / lease disabled

        # Runtime health tracking
        self._last_batch_processed = 0
        self._last_batch_ts_ms: int = 0

    # ------------------------------------------------------------------
    # Cursor management
    # ------------------------------------------------------------------

    def _cursor(self) -> str:
        try:
            return _s(self.r.get(self.cursor_key))
        except Exception:
            return ''

    def _set_cursor(self, stream_id: str) -> None:
        if not stream_id:
            return
        try:
            self.r.set(self.cursor_key, stream_id)
        except Exception:
            pass
        try:
            if TRADE_EXECUTION_PROJECTION_CURSOR_TS_MS:
                TRADE_EXECUTION_PROJECTION_CURSOR_TS_MS.set(float(_stream_sort_key(stream_id)[0]))
        except Exception:
            pass

    # ------------------------------------------------------------------
    # State persistence
    # ------------------------------------------------------------------

    def _load_state(self, sid: str) -> Dict[str, Any]:
        try:
            raw = self.r.get(f'{self.state_key_prefix}{sid}')
            if raw:
                doc = json.loads(raw)
                if isinstance(doc, dict):
                    return doc
        except Exception:
            pass
        return {}

    def _active_symbol_key(self, symbol: str) -> str:
        return f"{self.active_symbol_key_prefix}{str(symbol or '').strip().upper()}"

    def _state_is_terminalish(self, doc: Mapping[str, Any]) -> bool:
        state = dict(doc or {})
        fsm_state = str(state.get('fsm_state') or '').strip().upper()
        if fsm_state in {'EXIT_FILLED', 'EMERGENCY_FLATTENED', 'FAILED'}:
            return True
        status = str(state.get('status') or '').strip().lower()
        if status in {'closed', 'cancelled', 'canceled', 'failed', 'exited', 'exit_filled', 'emergency_flattened'}:
            return True
        if bool(state.get('closed')):
            return True
        return False

    def _persist_active_symbol_state(self, state_doc: Mapping[str, Any]) -> None:
        doc = dict(state_doc or {})
        symbol = _s(doc.get('symbol'))
        sid = _s(doc.get('sid'))
        if not symbol or not sid:
            return
        key = self._active_symbol_key(symbol)
        if self._state_is_terminalish(doc):
            try:
                raw = self.r.get(key)
                if raw:
                    current = json.loads(raw)
                    if not isinstance(current, dict) or _s(current.get('sid')) == sid:
                        self.r.delete(key)
            except Exception:
                pass
            return
        payload = json.dumps({
            'symbol': symbol,
            'sid': sid,
            'fsm_state': str(doc.get('fsm_state') or doc.get('status') or ''),
            'state': str(doc.get('fsm_state') or doc.get('status') or ''),
            'side': str(doc.get('side') or ''),
            'updated_at_ms': int(doc.get('updated_at_ms') or doc.get('ts_state_commit_ms') or _ms_now()),
            'ts_state_commit_ms': int(doc.get('ts_state_commit_ms') or _ms_now()),
        }, ensure_ascii=False, default=str)
        try:
            if self.state_ttl_sec > 0:
                self.r.set(key, payload, ex=self.state_ttl_sec)
            else:
                self.r.set(key, payload)
        except Exception:
            pass

    def _persist_state(self, sid: str, state_doc: Mapping[str, Any]) -> Dict[str, Any]:
        doc = build_materialized_state_view(dict(state_doc or {}))
        doc['updated_at_ms'] = int(doc.get('updated_at_ms') or _ms_now())
        if 'created_at_ms' not in doc:
            doc['created_at_ms'] = doc['updated_at_ms']
        payload = json.dumps(doc, ensure_ascii=False, default=str)
        try:
            if self.state_ttl_sec > 0:
                self.r.set(f'{self.state_key_prefix}{sid}', payload, ex=self.state_ttl_sec)
            else:
                self.r.set(f'{self.state_key_prefix}{sid}', payload)
        except Exception:
            pass
        try:
            if self.execution_journal is not None:
                self.execution_journal.upsert_order_snapshot(doc)
                self.execution_journal.upsert_protection_refs(doc)
        except Exception:
            pass
        self._persist_active_symbol_state(doc)
        return doc

    # ------------------------------------------------------------------
    # Stream reading
    # ------------------------------------------------------------------

    def _rows_after_cursor(self, cursor: str) -> List[Tuple[Any, Mapping[str, Any]]]:
        # Use cursor as the exclusive start position so XRANGE seeks directly to
        # entries after the cursor rather than scanning from the beginning of the
        # stream.  This fixes a stall when the cursor is positioned near the end
        # of a large stream: reading from '-' with a small count would never
        # return any entries beyond the cursor.
        start = f'({cursor}' if cursor else '-'
        try:
            rows = list(self.r.xrange(self.exec_stream, start, '+', count=self.batch_size))
        except Exception:
            rows = []
        if not rows:
            return []
        rows.sort(key=lambda row: _stream_sort_key(_s(row[0])))
        return rows[: self.batch_size]

    def _tip_stream_id(self) -> str:
        """Return the latest stream ID in orders:exec, or '' if empty."""
        try:
            rows = self.r.xrevrange(self.exec_stream, '+', '-', count=1)
            if rows:
                return _s(rows[0][0])
        except Exception:
            pass
        return ''

    # ------------------------------------------------------------------
    # Row projection
    # ------------------------------------------------------------------

    def _project_row(self, stream_id: str, fields: Mapping[str, Any]) -> Optional[Dict[str, Any]]:
        ev = {_s(k): _s(v) for k, v in dict(fields or {}).items()}
        sid = _s(ev.get('sid'))
        if not sid:
            return None
        base = self._load_state(sid)
        projected = project_event_into_state(ev, base_state=base, stream_id=_s(stream_id))
        projected['ts_state_commit_ms'] = _ms_now()
        doc = self._persist_state(sid, projected)
        try:
            ts_event_ms = int(ev.get('ts_event_ms') or ev.get('ts_ms') or 0)
            if ts_event_ms > 0 and TRADE_EXECUTION_PROJECTION_LAG_MS:
                TRADE_EXECUTION_PROJECTION_LAG_MS.set(max(0.0, float(_ms_now() - ts_event_ms)))
        except Exception:
            pass
        return doc

    # ------------------------------------------------------------------
    # P1.2.2: HA guards
    # ------------------------------------------------------------------

    def _update_leader_metrics(self, is_leader: bool, fencing_token: int) -> None:
        """Update Prometheus HA metrics."""
        try:
            if TRADE_EXECUTION_PROJECTION_IS_LEADER:
                TRADE_EXECUTION_PROJECTION_IS_LEADER.set(1.0 if is_leader else 0.0)
        except Exception:
            pass
        try:
            if TRADE_EXECUTION_PROJECTION_FENCING_TOKEN and fencing_token > 0:
                TRADE_EXECUTION_PROJECTION_FENCING_TOKEN.set(float(fencing_token))
        except Exception:
            pass

    # ------------------------------------------------------------------
    # Public run interface
    # ------------------------------------------------------------------

    def run_once(self) -> ProjectionBatchResult:
        """Process one batch. Honours leader lease when configured (P1.2.2)."""
        # --- P1.2.2: lease guard ---
        if self.leader_lease is not None:
            is_leader = self.leader_lease.is_leader()
            token = self.leader_lease.fencing_token()
            self._update_leader_metrics(is_leader, token)

            if not is_leader:
                # Standby: skip silently, let leader do the work
                cursor = self._cursor()
                return ProjectionBatchResult(
                    processed=0, last_stream_id=cursor,
                    idle=True, skipped_not_leader=True,
                )

            # Stale-writer protection: abort if fencing token is behind Redis
            if self.leader_lease.is_stale_writer():
                try:
                    if TRADE_EXECUTION_PROJECTION_STALE_WRITER_TOTAL:
                        TRADE_EXECUTION_PROJECTION_STALE_WRITER_TOTAL.inc()
                except Exception:
                    pass
                cursor = self._cursor()
                return ProjectionBatchResult(
                    processed=0, last_stream_id=cursor,
                    idle=True, stale_writer=True,
                )
        else:
            # Single-node mode: always act as leader
            self._update_leader_metrics(True, 0)

        # --- Core projection loop (identical to P1.2.1) ---
        cursor = self._cursor()
        rows = self._rows_after_cursor(cursor)
        if not rows:
            return ProjectionBatchResult(processed=0, last_stream_id=cursor, idle=True)

        processed = 0
        last_stream_id = cursor
        for stream_id, fields in rows:
            try:
                self._project_row(_s(stream_id), fields)
                processed += 1
                last_stream_id = _s(stream_id)
                self._set_cursor(last_stream_id)
                if TRADE_EXECUTION_PROJECTION_EVENT_TOTAL:
                    TRADE_EXECUTION_PROJECTION_EVENT_TOTAL.labels(result='ok').inc()
            except Exception:
                if TRADE_EXECUTION_PROJECTION_EVENT_TOTAL:
                    TRADE_EXECUTION_PROJECTION_EVENT_TOTAL.labels(result='error').inc()

        self._last_batch_processed = processed
        self._last_batch_ts_ms = _ms_now()
        return ProjectionBatchResult(processed=processed, last_stream_id=last_stream_id, idle=False)

    def run_until_idle(self, *, max_loops: int = 1000) -> int:
        total = 0
        for _ in range(max(int(max_loops or 1), 1)):
            batch = self.run_once()
            total += batch.processed
            if batch.idle or batch.processed <= 0:
                break
        return total

    # ------------------------------------------------------------------
    # P1.2.2: Health snapshot (used by health server and --print-health)
    # ------------------------------------------------------------------

    def health_snapshot(self, *, lag_readyz_max_ms: int = 30000) -> Dict[str, Any]:
        """Return a health snapshot dict for the health endpoint and CLI.

        Fields
        ------
        leader          bool     - whether this instance is the leader
        fencing_token   int      - current fencing token (0 if lease disabled)
        cursor          str      - last projected stream ID
        cursor_age_ms   int      - ms since cursor timestamp (staleness proxy)
        lag_ms          int      - lag from last event ts to now
        last_batch_ts_ms int     - epoch ms of last run_once() with processed > 0
        ready           bool     - leader AND lag < threshold
        lease_enabled   bool     - whether leader lease is configured
        worker_id       str      - this worker's identity
        """
        cursor = self._cursor()
        cursor_ts = _stream_sort_key(cursor)[0]  # ms epoch from stream ID
        cursor_age_ms = max(0, _ms_now() - cursor_ts) if cursor_ts > 0 else -1

        is_leader: bool
        fencing_token: int
        if self.leader_lease is not None:
            is_leader = self.leader_lease.is_leader()
            fencing_token = self.leader_lease.fencing_token()
            worker_id = self.leader_lease.worker_id
            # Read-only health probes (e.g. execution-bootstrap-health sidecar) create a
            # fresh worker object that never calls acquire(), so is_leader() is always False
            # and fencing_token() is 0 even when a real leader holds the Redis lease.
            # Fall back to the remote fencing token: if it's > 0, the cluster HAS a leader.
            if not is_leader and fencing_token == 0:
                remote_token = self.leader_lease.remote_fencing_token()
                if remote_token > 0:
                    is_leader = True
                    fencing_token = remote_token
        else:
            is_leader = True  # single-node: always leader
            fencing_token = 0
            worker_id = socket.gethostname()

        # Lag: age since last event's ts_event_ms (approximated via cursor_age_ms)
        lag_ms = cursor_age_ms if cursor_age_ms >= 0 else 0
        ready = is_leader and lag_ms < lag_readyz_max_ms

        return {
            'leader': is_leader,
            'fencing_token': fencing_token,
            'cursor': cursor,
            'cursor_age_ms': cursor_age_ms,
            'lag_ms': lag_ms,
            'last_batch_ts_ms': self._last_batch_ts_ms,
            'ready': ready,
            'lease_enabled': self.leader_lease is not None,
            'worker_id': worker_id,
        }

    # ------------------------------------------------------------------
    # P1.2.2: Rebuild operations (called from CLI)
    # ------------------------------------------------------------------

    def rebuild_sid(self, sid: str) -> int:
        """Replay all orders:exec events for one SID and rebuild orders:state:{sid}.

        Safe to run on a standby; does not advance the global cursor.
        Returns the number of events processed.
        """
        processed = 0
        try:
            all_rows = self.r.xrange(self.exec_stream, '-', '+')
        except Exception:
            return 0

        state: Dict[str, Any] = {}
        for stream_id, fields in (all_rows or []):
            ev = {_s(k): _s(v) for k, v in dict(fields or {}).items()}
            if _s(ev.get('sid')) != sid:
                continue
            try:
                state = project_event_into_state(ev, base_state=state, stream_id=_s(stream_id))
                processed += 1
            except Exception:
                pass

        if processed > 0:
            self._persist_state(sid, state)
        return processed

    def rebuild_all(self) -> Dict[str, int]:
        """Replay entire orders:exec stream, rebuilding all orders:state:{sid} keys.

        Returns dict mapping sid → event count.
        """
        try:
            all_rows = self.r.xrange(self.exec_stream, '-', '+')
        except Exception:
            return {}

        # Group events by SID, preserving stream order
        sid_states: Dict[str, Dict[str, Any]] = {}
        sid_counts: Dict[str, int] = {}

        for stream_id, fields in (all_rows or []):
            ev = {_s(k): _s(v) for k, v in dict(fields or {}).items()}
            sid = _s(ev.get('sid'))
            if not sid:
                continue
            base = sid_states.get(sid, {})
            try:
                sid_states[sid] = project_event_into_state(ev, base_state=base, stream_id=_s(stream_id))
                sid_counts[sid] = sid_counts.get(sid, 0) + 1
            except Exception:
                pass

        for sid, state in sid_states.items():
            self._persist_state(sid, state)

        return sid_counts

    def reset_derived_state(self, *, dry_run: bool = True) -> int:
        """DEL all orders:state:* keys.

        DANGER: this wipes the entire derived cache. Always confirms unless
        dry_run=False is explicitly passed. Returns count of deleted keys.
        """
        if dry_run:
            raise RuntimeError(
                'reset_derived_state() called with dry_run=True. '
                'Pass dry_run=False to actually delete.'
            )
        count = 0
        try:
            pattern = f'{self.state_key_prefix}*'
            cursor_val = 0
            while True:
                cursor_val, keys = self.r.scan(cursor_val, match=pattern, count=10000)
                if keys:
                    self.r.delete(*keys)
                    count += len(keys)
                if cursor_val == 0:
                    break
        except Exception:
            pass
        return count

    def set_cursor_to_tip(self) -> str:
        """Move projection cursor to the latest stream ID in orders:exec.

        Returns the new cursor value, or '' if stream is empty.
        """
        tip = self._tip_stream_id()
        if tip:
            self._set_cursor(tip)
        return tip


# ---------------------------------------------------------------------------
# Factory helpers
# ---------------------------------------------------------------------------

def _redis_from_env() -> Any:  # pragma: no cover
    url = os.getenv('REDIS_URL', 'redis://redis-worker-1:6379/0')
    if redis is None:
        raise RuntimeError('redis package is required for execution_projection_worker')
    return redis.from_url(url, decode_responses=True)


def _worker_from_env(r: Any) -> ExecutionProjectionWorker:  # pragma: no cover
    """Build ExecutionProjectionWorker from environment variables."""
    # P1.2.2: HA lease (opt-in, default 1 in production)
    lease_enabled = os.getenv('EXEC_PROJECTION_LEASE_ENABLE', '1').strip() == '1'
    leader_lease: Optional[LeaderLease] = None
    if lease_enabled:
        leader_lease = LeaderLease(
            r,
            lease_key=os.getenv('EXEC_PROJECTION_LEASE_KEY', 'orders:exec:projection:leader'),
            fence_key=os.getenv('EXEC_PROJECTION_FENCE_KEY', 'orders:exec:projection:fence'),
            lease_ttl_ms=int(os.getenv('EXEC_PROJECTION_LEASE_TTL_MS', '5000')),
            renew_interval_ms=int(os.getenv('EXEC_PROJECTION_LEASE_RENEW_INTERVAL_MS', '2000')),
            worker_id=os.getenv('EXEC_PROJECTION_WORKER_ID', '') or socket.gethostname(),
        )

    return ExecutionProjectionWorker(
        r,
        exec_stream=os.getenv('EXEC_STREAM', 'orders:exec'),
        state_key_prefix=os.getenv('ORDERS_STATE_KEY_PREFIX', 'orders:state:'),
        active_symbol_key_prefix=os.getenv('ORDERS_ACTIVE_SYMBOL_KEY_PREFIX', 'orders:active_symbol_sid:'),
        state_ttl_sec=int(os.getenv('ORDERS_STATE_TTL_SEC', '86400')),
        cursor_key=os.getenv('EXEC_PROJECTION_CURSOR_KEY', 'orders:exec:projection:cursor'),
        batch_size=int(os.getenv('EXEC_PROJECTION_BATCH_SIZE', '500')),
        execution_journal=ExecutionJournalSink(dsn=os.getenv('EXECUTION_JOURNAL_DSN', '')),
        leader_lease=leader_lease,
    )


# ---------------------------------------------------------------------------
# P1.2.2: CLI entrypoint (rebuild + main loop)
# ---------------------------------------------------------------------------

def _build_arg_parser() -> argparse.ArgumentParser:  # pragma: no cover
    p = argparse.ArgumentParser(
        description='Execution projection worker — rebuild CLI + daemon mode (P1.2.2)',
        formatter_class=argparse.RawDescriptionHelpFormatter,
    )
    p.add_argument(
        '--rebuild-all',
        action='store_true',
        help='Replay entire orders:exec and rebuild all orders:state:{sid} keys, then exit.',
    )
    p.add_argument(
        '--rebuild-sid',
        metavar='SID',
        default='',
        help='Rebuild orders:state for one SID from orders:exec, then exit.',
    )
    p.add_argument(
        '--reset-derived-state',
        action='store_true',
        help='DEL all orders:state:* keys (DANGER — requires explicit flag).',
    )
    p.add_argument(
        '--set-cursor-to-tip',
        action='store_true',
        help='Advance projection cursor to latest orders:exec stream ID, then exit.',
    )
    p.add_argument(
        '--print-health',
        action='store_true',
        help='Print health JSON and exit.',
    )
    return p


def main() -> int:  # pragma: no cover
    parser = _build_arg_parser()
    # Allow unknown args so that Docker CMD arrays don't need exact flags
    args, _unknown = parser.parse_known_args()

    r = _redis_from_env()
    worker = _worker_from_env(r)

    lag_readyz = int(os.getenv('EXEC_PROJECTION_LAG_READYZ_MAX_MS', '30000'))

    # --- Rebuild CLI modes ---
    if args.print_health:
        snap = worker.health_snapshot(lag_readyz_max_ms=lag_readyz)
        print(json.dumps(snap, indent=2, default=str))
        return 0

    if args.set_cursor_to_tip:
        tip = worker.set_cursor_to_tip()
        print(f'cursor set to tip: {tip!r}')
        return 0

    if args.rebuild_sid:
        n = worker.rebuild_sid(args.rebuild_sid)
        print(f'rebuilt {n} events for SID={args.rebuild_sid!r}')
        return 0

    if args.rebuild_all:
        counts = worker.rebuild_all()
        total = sum(counts.values())
        print(f'rebuilt {total} events across {len(counts)} SIDs')
        return 0

    if args.reset_derived_state:
        confirm = input('This will DELETE all orders:state:* keys. Type YES to confirm: ').strip()
        if confirm != 'YES':
            print('Aborted.')
            return 1
        deleted = worker.reset_derived_state(dry_run=False)
        print(f'Deleted {deleted} keys.')
        return 0

    # --- Daemon mode: try to acquire lease then loop ---
    if worker.leader_lease is not None:
        # Attempt initial lease acquisition (non-blocking; retry in loop)
        worker.leader_lease.acquire()

    sleep_ms = int(os.getenv('EXEC_PROJECTION_IDLE_SLEEP_MS', '500'))
    # Re-attempt lease every N idles when not leader
    lease_retry_idles = int(os.getenv('EXEC_PROJECTION_LEASE_RETRY_IDLES', '10'))
    idle_count = 0

    while True:
        batch = worker.run_once()
        if batch.processed <= 0:
            idle_count += 1
            time.sleep(max(0.05, sleep_ms / 1000.0))
            # Periodically re-attempt lease acquisition if we are standby
            if (
                worker.leader_lease is not None
                and not worker.leader_lease.is_leader()
                and idle_count % lease_retry_idles == 0
            ):
                worker.leader_lease.acquire()
        else:
            idle_count = 0


if __name__ == '__main__':  # pragma: no cover
    raise SystemExit(main())
