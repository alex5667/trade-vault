from __future__ import annotations

from utils.time_utils import get_ny_time_millis

"""Best-effort execution journal sink for Postgres.

Redis stream (orders:exec) remains the online source of truth.
These Postgres tables provide durable audit history for incident analysis.
All DB operations are fail-open: failures are logged to Prometheus and swallowed.

Tables (created by 036_execution_journal.sql + P5 migrations):
  execution_orders         — one row per signal ID (upserted on every state change)
  execution_order_events   — append-only event log
  execution_protection_refs — SL/TP/trail algo IDs per signal ID
  execution_watchdog_events — TP watchdog state events (P5)

P5 extends the journal contract so execution SQL mirrors can be used as a durable
join point between signal production, execution lifecycle and analytics.  The
Redis stream remains the online SoT; SQL mirrors are durable audit materialized
views that must never break the hot path.

Performance (fix):
  JOURNAL_EVENT_BATCH_ENABLED=1 — route record_event() through AsyncBatchWriter
  so high-frequency order events are batched (executemany) instead of opening a
  new connection per row. The shared pool from analytics_db.get_conn() is used
  for all other write paths to eliminate per-write TCP overhead.

ENV:
  EXECUTION_JOURNAL_DSN       — Postgres DSN (e.g. postgresql://user:pw@host/db).
                                If not set, all writes are silently no-ops.
  JOURNAL_EVENT_BATCH_ENABLED — '1' enables AsyncBatchWriter for record_event().
  JOURNAL_BATCH_SIZE          — rows per flush (default 200).
  JOURNAL_FLUSH_INTERVAL_S    — flush interval in seconds (default 2.0).
"""

import json
import math
import os
from dataclasses import dataclass
from typing import Any

def _sanitize_floats(obj: Any) -> Any:
    """Recursively replace NaN/Infinity with None so json.dumps produces valid JSON.

    PostgreSQL jsonb rejects the `NaN` token (non-standard JSON extension).
    """
    if isinstance(obj, float):
        return None if not math.isfinite(obj) else obj
    if isinstance(obj, dict):
        return {k: _sanitize_floats(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        sanitized = [_sanitize_floats(v) for v in obj]
        return sanitized if isinstance(obj, list) else tuple(sanitized)
    return obj

try:
    from prometheus_client import REGISTRY, Counter
except Exception:  # pragma: no cover
    Counter = None  # type: ignore
    REGISTRY = None  # type: ignore


def _metric(factory, name: str, *args, **kwargs):
    """Idempotent Prometheus metric factory — returns existing metric if already registered."""
    if factory is None:
        return None
    try:
        return factory(name, *args, **kwargs)
    except ValueError:
        return getattr(REGISTRY, "_names_to_collectors", {}).get(name) if REGISTRY is not None else None


TRADE_EXECUTION_JOURNAL_WRITE_FAIL_TOTAL = _metric(
    Counter,
    'trade_execution_journal_write_fail_total',
    'Number of execution journal DB write failures.',
    ['kind'],
)

# psycopg3 preferred, psycopg2 as fallback (both provide compatible context-manager API)
try:  # pragma: no cover
    import psycopg  # type: ignore
except Exception:  # pragma: no cover
    psycopg = None
    try:
        import psycopg2 as psycopg  # type: ignore
    except Exception:  # pragma: no cover
        psycopg = None

# Shared connection pool from analytics_db (created lazily on first use).
try:
    from services.analytics_db import get_conn as _get_shared_conn
except Exception:  # pragma: no cover — analytics_db not available in some test contexts
    _get_shared_conn = None  # type: ignore

# Optional async batch writer for high-frequency event inserts.
_event_batch_writer = None
_JOURNAL_EVENT_BATCH_ENABLED = os.getenv("JOURNAL_EVENT_BATCH_ENABLED", "0") == "1"
_JOURNAL_BATCH_SIZE = int(os.getenv("JOURNAL_BATCH_SIZE", "200"))
_JOURNAL_FLUSH_INTERVAL_S = float(os.getenv("JOURNAL_FLUSH_INTERVAL_S", "2.0"))

_EVENT_COLUMNS = (
    "sid", "symbol", "signal_id", "execution_plan_id",
    "event_type", "event_ts_ms", "payload_jsonb",
)


def _get_event_batch_writer(dsn: str):
    """Lazy-init the AsyncBatchWriter for execution_order_events."""
    global _event_batch_writer
    if _event_batch_writer is not None:
        return _event_batch_writer
    try:
        from services.db_batch_writer import get_or_create_writer
        _event_batch_writer = get_or_create_writer(
            table="execution_order_events",
            columns=_EVENT_COLUMNS,
            dsn=dsn,
            batch_size=_JOURNAL_BATCH_SIZE,
            flush_interval_s=_JOURNAL_FLUSH_INTERVAL_S,
            on_conflict_sql="ON CONFLICT DO NOTHING",  # ignore duplicate entries
        )
    except Exception as exc:
        import logging
        logging.getLogger("execution_journal").warning(
            "AsyncBatchWriter init failed, falling back to direct writes: %s", exc
        )
        _event_batch_writer = None
    return _event_batch_writer


def _s(v: Any) -> str:
    """Coerce to str, treating None/empty as empty string."""
    return (v or '')


def _i(v: Any, default: int = 0) -> int:
    """Safe int coercion with default."""
    try:
        return int(v)
    except Exception:
        return default


def _optional_text(v: Any) -> str | None:
    """Return stripped string or None if blank."""
    s = (v or '').strip()
    return s or None


def _first_text(doc: dict[str, Any], *keys: str) -> str | None:
    """Return the first non-blank string value from doc for the given keys."""
    for key in keys:
        s = _optional_text(doc.get(key))
        if s:
            return s
    return None


@dataclass
class ExecutionJournalSink:
    """Best-effort Postgres sink for execution journal events.

    Lifecycle: instantiated once in BinanceExecutor.__init__() and used
    synchronously in the executor main loop.  All public methods return bool
    (True = write succeeded, False = write failed or sink disabled).

    Performance:
      - Uses shared analytics_db ConnectionPool instead of a new connection per
        write. When _get_shared_conn is unavailable (test stubs), falls back to
        per-call connect.
      - When JOURNAL_EVENT_BATCH_ENABLED=1, record_event() batches via
        AsyncBatchWriter (executemany + pool) for very high write frequencies.
    """

    dsn: str = ''
    connect_factory: Any = None

    def __post_init__(self) -> None:
        self.dsn = self.dsn or os.getenv('EXECUTION_JOURNAL_DSN', '')
        if self.connect_factory is None and self.dsn and psycopg is not None:
            self.connect_factory = psycopg.connect

    @property
    def enabled(self) -> bool:
        """True if DSN is configured and a driver or shared pool is available."""
        return bool(self.dsn and (self.connect_factory or _get_shared_conn))

    def _get_conn_ctx(self):
        """Return a context-manager that yields an open psycopg connection.

        Preference order:
          1. Shared pool from analytics_db (get_conn) — only if pool is initialised.
          2. Direct connect via self.connect_factory (fallback for non-pooled envs).
        """
        # Use the shared pool only when it has been explicitly initialised.
        # This prevents test environments (where analytics_db._POOL is None)
        # from failing silently when pool.getconn() raises PoolError.
        try:
            import services.analytics_db as _adb  # local import to avoid circular
            if _get_shared_conn is not None:
                if _adb._POOL is None and hasattr(_adb, '_init_pool'):
                    _adb.TRADES_DB_DSN = self.dsn
                    _adb._init_pool()
                if _adb._POOL is not None:
                    return _get_shared_conn()
        except Exception:
            pass  # analytics_db unavailable or pool not ready — use fallback

        # Fallback: wrap a new connection in a minimal context manager.
        import contextlib
        @contextlib.contextmanager
        def _direct():
            conn = self.connect_factory(self.dsn, connect_timeout=3)
            try:
                yield conn
            finally:
                if hasattr(conn, 'close'):
                    conn.close()
        return _direct()

    def record_event(self, event: dict[str, Any]) -> bool:
        """Append one execution event to execution_order_events.

        When JOURNAL_EVENT_BATCH_ENABLED=1 the row is enqueued into an
        AsyncBatchWriter and flushed in a background thread (executemany),
        avoiding a per-row round-trip.  Falls back to direct insert otherwise.

        Params
        ------
        event: fields dict (same as what goes to orders:exec Redis stream)
        """
        if not self.enabled:
            return False
        payload = _sanitize_floats(dict(event or {}))
        payload_json = json.dumps(payload, ensure_ascii=False, default=str)

        # -- Async batch path (high-frequency optimisation) -------------------
        if _JOURNAL_EVENT_BATCH_ENABLED:
            writer = _get_event_batch_writer(self.dsn)
            if writer is not None:
                try:
                    writer.enqueue({
                        "sid": _s(payload.get("sid")),
                        "symbol": _s(payload.get("symbol")),
                        "signal_id": _first_text(payload, "signal_id", "decision_id", "id"),
                        "execution_plan_id": _first_text(
                            payload, "execution_plan_id", "decision_id", "signal_id", "id"
                        ),
                        "event_type": _s(
                            payload.get("event_type") or payload.get("action") or "event"
                        ),
                        "event_ts_ms": _i(payload.get("ts_ms") or get_ny_time_millis()),
                        "payload_jsonb": payload_json,
                    })
                    return True
                except Exception:
                    pass  # fall through to direct insert

        # -- Direct synchronous path (default) --------------------------------
        sql = (
            "INSERT INTO execution_order_events "
            "(sid, symbol, signal_id, execution_plan_id, event_type, event_ts_ms, payload_jsonb) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb) "
            "ON CONFLICT DO NOTHING"
        )
        try:
            with self._get_conn_ctx() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (
                        _s(payload.get("sid")),
                        _s(payload.get("symbol")),
                        _first_text(payload, "signal_id", "decision_id", "id"),
                        _first_text(
                            payload, "execution_plan_id", "decision_id", "signal_id", "id"
                        ),
                        _s(payload.get("event_type") or payload.get("action") or "event"),
                        _i(payload.get("ts_ms") or get_ny_time_millis()),
                        payload_json,
                    ))
                conn.commit()
            return True
        except Exception:
            if TRADE_EXECUTION_JOURNAL_WRITE_FAIL_TOTAL:
                TRADE_EXECUTION_JOURNAL_WRITE_FAIL_TOTAL.labels(kind="event").inc()
            return False

    def upsert_order_snapshot(self, state: dict[str, Any]) -> bool:
        """Upsert the current order state snapshot into execution_orders.

        Called on every FSM state transition so the table always reflects
        the latest known executor state for each signal ID.
        Uses shared analytics_db pool to avoid per-write TCP connections.
        """
        if not self.enabled:
            return False
        sql = (
            # P5: extended columns — entry_policy, exit_policy, signal_id, execution_plan_id,
            # entry_order_ref, exit_order_ref, closed_trade_id.  COALESCE on chain refs ensures
            # partial updates (TP watchdog, trail arming) do not wipe the original chain.
            "INSERT INTO execution_orders (sid, symbol, action, status, fsm_state, execution_policy, entry_policy, exit_policy, "
            "signal_id, execution_plan_id, entry_order_ref, exit_order_ref, closed_trade_id, venue, position_mode, position_side, "
            "working_type_policy, state_jsonb, created_at_ms, updated_at_ms) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb, %s, %s) "
            "ON CONFLICT (sid) DO UPDATE SET "
            "symbol = EXCLUDED.symbol, action = EXCLUDED.action, status = EXCLUDED.status, fsm_state = EXCLUDED.fsm_state, "
            "execution_policy = EXCLUDED.execution_policy, entry_policy = EXCLUDED.entry_policy, exit_policy = EXCLUDED.exit_policy, "
            "signal_id = COALESCE(EXCLUDED.signal_id, execution_orders.signal_id), "
            "execution_plan_id = COALESCE(EXCLUDED.execution_plan_id, execution_orders.execution_plan_id), "
            "entry_order_ref = COALESCE(EXCLUDED.entry_order_ref, execution_orders.entry_order_ref), "
            "exit_order_ref = COALESCE(EXCLUDED.exit_order_ref, execution_orders.exit_order_ref), "
            "closed_trade_id = COALESCE(EXCLUDED.closed_trade_id, execution_orders.closed_trade_id), "
            "venue = EXCLUDED.venue, position_mode = EXCLUDED.position_mode, position_side = EXCLUDED.position_side, "
            "working_type_policy = EXCLUDED.working_type_policy, state_jsonb = EXCLUDED.state_jsonb, updated_at_ms = EXCLUDED.updated_at_ms"
        )
        doc = _sanitize_floats(dict(state or {}))
        now_ms = _i(doc.get('ts_ms') or get_ny_time_millis())
        try:
            with self._get_conn_ctx() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (
                        _s(doc.get('sid')),
                        _s(doc.get('symbol')),
                        _s(doc.get('action')),
                        _s(doc.get('status')),
                        _s(doc.get('fsm_state')),
                        _s(doc.get('execution_policy')),
                        # entry_policy fallback to execution_policy for pre-P5 rows
                        _s(doc.get('entry_policy') or doc.get('execution_policy')),
                        _s(doc.get('exit_policy')),
                        _first_text(doc, 'signal_id', 'decision_id', 'id'),
                        _first_text(doc, 'execution_plan_id', 'decision_id', 'signal_id', 'id'),
                        _optional_text(doc.get('entry_order_ref')),
                        _optional_text(doc.get('exit_order_ref')),
                        _optional_text(doc.get('closed_trade_id')),
                        _s(doc.get('venue') or 'binance'),
                        _s(doc.get('position_mode')),
                        _s(doc.get('position_side')),
                        _s(doc.get('working_type_policy')),
                        json.dumps(doc, ensure_ascii=False, default=str),
                        _i(doc.get('created_at_ms') or now_ms),
                        _i(doc.get('updated_at_ms') or now_ms),
                    ))
                conn.commit()
            return True
        except Exception:
            if TRADE_EXECUTION_JOURNAL_WRITE_FAIL_TOTAL:
                TRADE_EXECUTION_JOURNAL_WRITE_FAIL_TOTAL.labels(kind='order_snapshot').inc()
            return False

    def upsert_protection_refs(self, state: dict[str, Any]) -> bool:
        """Upsert SL/TP/trail algo IDs into execution_protection_refs.

        Called together with upsert_order_snapshot whenever order state is saved.
        The table provides a fast lookup of Binance algo IDs by signal ID without
        parsing the full state_jsonb column.
        Uses shared analytics_db pool to avoid per-write TCP connections.
        """
        if not self.enabled:
            return False
        sid = _s((state or {}).get('sid'))
        if not sid:
            return False
        sql = (
            "INSERT INTO execution_protection_refs "
            "(sid, symbol, sl_algo_id, sl_client_algo_id, tp1_algo_id, tp2_algo_id, tp3_algo_id, trail_algo_id, trail_client_algo_id, updated_at_ms) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s) "
            "ON CONFLICT (sid) DO UPDATE SET symbol = EXCLUDED.symbol, sl_algo_id = EXCLUDED.sl_algo_id, "
            "sl_client_algo_id = EXCLUDED.sl_client_algo_id, "
            "tp1_algo_id = EXCLUDED.tp1_algo_id, tp2_algo_id = EXCLUDED.tp2_algo_id, "
            "tp3_algo_id = EXCLUDED.tp3_algo_id, "
            "trail_algo_id = EXCLUDED.trail_algo_id, trail_client_algo_id = EXCLUDED.trail_client_algo_id, "
            "updated_at_ms = EXCLUDED.updated_at_ms"
        )
        s = state or {}
        try:
            with self._get_conn_ctx() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (
                        sid,
                        _s(s.get('symbol')),
                        s.get('sl_algo_id'),
                        _optional_text(s.get('sl_client_algo_id')),
                        s.get('tp1_algo_id'),
                        s.get('tp2_algo_id'),
                        s.get('tp3_algo_id'),
                        s.get('trail_algo_id'),
                        _optional_text(s.get('trail_client_algo_id')),
                        _i(s.get('updated_at_ms') or get_ny_time_millis()),
                    ))
                conn.commit()
            return True
        except Exception:
            if TRADE_EXECUTION_JOURNAL_WRITE_FAIL_TOTAL:
                TRADE_EXECUTION_JOURNAL_WRITE_FAIL_TOTAL.labels(kind='protection_refs').inc()
            return False

    def record_watchdog_event(self, event: dict[str, Any]) -> bool:
        """Append one TP watchdog state event to execution_watchdog_events (P5).

        Called from _emit_tp_state() in the executor for durable forensic audit.
        Fail-open: SQL errors are swallowed and reflected in Prometheus.
        Uses shared analytics_db pool to avoid per-write TCP connections.
        """
        if not self.enabled:
            return False
        sql = (
            "INSERT INTO execution_watchdog_events "
            "(sid, symbol, signal_id, execution_plan_id, tp_level, watchdog_state, event_type, event_ts_ms, payload_jsonb) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb) "
            "ON CONFLICT DO NOTHING"
        )
        payload = _sanitize_floats(dict(event or {}))
        try:
            with self._get_conn_ctx() as conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (
                        _s(payload.get('sid')),
                        _s(payload.get('symbol')),
                        _first_text(payload, 'signal_id', 'decision_id', 'id'),
                        _first_text(payload, 'execution_plan_id', 'decision_id', 'signal_id', 'id'),
                        payload.get('tp_level'),
                        _s(payload.get('tp_state') or payload.get('watchdog_state') or ''),
                        _s(payload.get('event_type') or 'watchdog'),
                        _i(payload.get('ts_ms') or get_ny_time_millis()),
                        json.dumps(payload, ensure_ascii=False, default=str),
                    ))
                conn.commit()
            return True
        except Exception:
            if TRADE_EXECUTION_JOURNAL_WRITE_FAIL_TOTAL:
                TRADE_EXECUTION_JOURNAL_WRITE_FAIL_TOTAL.labels(kind='watchdog_event').inc()
            return False
