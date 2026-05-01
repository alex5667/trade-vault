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

ENV:
  EXECUTION_JOURNAL_DSN   — Postgres DSN (e.g. postgresql://user:pw@host/db)
                            If not set, all writes are silently no-ops.
"""

from dataclasses import dataclass
from typing import Any, Dict, Optional
import json
import os
import time

try:
    from prometheus_client import Counter, REGISTRY
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


def _s(v: Any) -> str:
    """Coerce to str, treating None/empty as empty string."""
    return str(v or '')


def _i(v: Any, default: int = 0) -> int:
    """Safe int coercion with default."""
    try:
        return int(v)
    except Exception:
        return int(default)


def _optional_text(v: Any) -> Optional[str]:
    """Return stripped string or None if blank."""
    s = str(v or '').strip()
    return s or None


def _first_text(doc: Dict[str, Any], *keys: str) -> Optional[str]:
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

    Thread-safety: each call opens and immediately closes a connection.
    No pooling — the sink is intentionally simple and low-frequency.
    """

    dsn: str = ''
    connect_factory: Any = None

    def __post_init__(self) -> None:
        self.dsn = self.dsn or os.getenv('EXECUTION_JOURNAL_DSN', '')
        if self.connect_factory is None and self.dsn and psycopg is not None:
            self.connect_factory = psycopg.connect

    @property
    def enabled(self) -> bool:
        """True if both DSN and driver are available."""
        return bool(self.dsn and self.connect_factory)

    def _connect(self):
        if not self.enabled:
            return None
        return self.connect_factory(self.dsn)

    def record_event(self, event: Dict[str, Any]) -> bool:
        """Append one execution event to execution_order_events.

        Params
        ------
        event: fields dict (same as what goes to orders:exec Redis stream)
        """
        if not self.enabled:
            return False
        sql = (
            # P5: include signal_id / execution_plan_id for durable cross-join chain
            "INSERT INTO execution_order_events (sid, symbol, signal_id, execution_plan_id, event_type, event_ts_ms, payload_jsonb) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s::jsonb)"
        )
        payload = dict(event or {})
        try:
            conn = self._connect()
            with conn:
                with conn.cursor() as cur:
                    cur.execute(sql, (
                        _s(payload.get('sid')),
                        _s(payload.get('symbol')),
                        _first_text(payload, 'signal_id', 'decision_id', 'id'),
                        _first_text(payload, 'execution_plan_id', 'decision_id', 'signal_id', 'id'),
                        _s(payload.get('event_type') or payload.get('action') or 'event'),
                        _i(payload.get('ts_ms') or get_ny_time_millis()),
                        json.dumps(payload, ensure_ascii=False, default=str),
                    ))
            return True
        except Exception:
            if TRADE_EXECUTION_JOURNAL_WRITE_FAIL_TOTAL:
                TRADE_EXECUTION_JOURNAL_WRITE_FAIL_TOTAL.labels(kind='event').inc()
            return False

    def upsert_order_snapshot(self, state: Dict[str, Any]) -> bool:
        """Upsert the current order state snapshot into execution_orders.

        Called on every FSM state transition so the table always reflects
        the latest known executor state for each signal ID.
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
        doc = dict(state or {})
        now_ms = _i(doc.get('ts_ms') or get_ny_time_millis())
        try:
            conn = self._connect()
            with conn:
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
            return True
        except Exception:
            if TRADE_EXECUTION_JOURNAL_WRITE_FAIL_TOTAL:
                TRADE_EXECUTION_JOURNAL_WRITE_FAIL_TOTAL.labels(kind='order_snapshot').inc()
            return False

    def upsert_protection_refs(self, state: Dict[str, Any]) -> bool:
        """Upsert SL/TP/trail algo IDs into execution_protection_refs.

        Called together with upsert_order_snapshot whenever order state is saved.
        The table provides a fast lookup of Binance algo IDs by signal ID without
        parsing the full state_jsonb column.
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
            conn = self._connect()
            with conn:
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
            return True
        except Exception:
            if TRADE_EXECUTION_JOURNAL_WRITE_FAIL_TOTAL:
                TRADE_EXECUTION_JOURNAL_WRITE_FAIL_TOTAL.labels(kind='protection_refs').inc()
            return False

    def record_watchdog_event(self, event: Dict[str, Any]) -> bool:
        """Append one TP watchdog state event to execution_watchdog_events (P5).

        Called from _emit_tp_state() in the executor for durable forensic audit.
        Fail-open: SQL errors are swallowed and reflected in Prometheus.
        """
        if not self.enabled:
            return False
        sql = (
            "INSERT INTO execution_watchdog_events "
            "(sid, symbol, signal_id, execution_plan_id, tp_level, watchdog_state, event_type, event_ts_ms, payload_jsonb) "
            "VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s::jsonb)"
        )
        payload = dict(event or {})
        try:
            conn = self._connect()
            with conn:
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
            return True
        except Exception:
            if TRADE_EXECUTION_JOURNAL_WRITE_FAIL_TOTAL:
                TRADE_EXECUTION_JOURNAL_WRITE_FAIL_TOTAL.labels(kind='watchdog_event').inc()
            return False
