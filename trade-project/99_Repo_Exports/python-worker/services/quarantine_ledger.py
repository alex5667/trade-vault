from __future__ import annotations

from utils.time_utils import get_ny_time_millis

"""Best-effort SQL sink for quarantine/repair audit trail.

The sink is intentionally fail-open: runtime execution and publish gates must not
block just because the audit mirror is unavailable.
"""

import json
import os
from dataclasses import dataclass
from typing import Any
import contextlib

try:
    from prometheus_client import REGISTRY, Counter
except Exception:  # pragma: no cover
    Counter = None  # type: ignore
    REGISTRY = None  # type: ignore


def _metric(factory, name: str, *args, **kwargs):
    if factory is None:
        return None
    try:
        return factory(name, *args, **kwargs)
    except ValueError:
        return getattr(REGISTRY, '_names_to_collectors', {}).get(name) if REGISTRY is not None else None


EXECUTION_QUARANTINE_EVENTS_TOTAL = _metric(
    Counter,
    'execution_quarantine_events_total',
    'Number of quarantine/repair ledger events mirrored to SQL.',
    ['action'],
)
TRADE_QUARANTINE_LEDGER_WRITE_FAIL_TOTAL = _metric(
    Counter,
    'trade_quarantine_ledger_write_fail_total',
    'Number of quarantine ledger write failures.',
    ['kind'],
)

try:  # pragma: no cover
    import psycopg  # type: ignore
except Exception:  # pragma: no cover
    psycopg = None
    try:
        import psycopg2 as psycopg  # type: ignore
    except Exception:  # pragma: no cover
        psycopg = None


@dataclass
class QuarantineLedgerSink:
    dsn: str = ''
    connect_factory: Any = None

    def __post_init__(self) -> None:
        self.dsn = self.dsn or os.getenv('EXECUTION_QUARANTINE_LEDGER_DSN', '') or os.getenv('EXECUTION_JOURNAL_DSN', '')
        if self.connect_factory is None and self.dsn and psycopg is not None:
            self.connect_factory = psycopg.connect

    @property
    def enabled(self) -> bool:
        return bool(self.dsn and self.connect_factory)

    def _connect(self):
        if not self.enabled:
            return None
        return self.connect_factory(self.dsn)

    def record_quarantine_event(self, payload: dict[str, Any]) -> bool:
        if not self.enabled:
            return False
        doc = dict(payload or {})
        now_ms = int(doc.get('event_ts_ms') or get_ny_time_millis())
        sql = (
            'INSERT INTO execution_quarantine_ledger '
            '(sid, symbol, action, severity, reason, source, quarantine_key, applied, state_jsonb, event_ts_ms, created_at_ms) '
            'VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s,%s)'
        )
        try:
            conn = self._connect()
            try:
                with conn, conn.cursor() as cur:  # type: ignore
                    cur.execute(sql, (
                        (doc.get('sid') or ''),
                        (doc.get('symbol') or ''),
                        (doc.get('action') or 'QUARANTINED'),
                        (doc.get('severity') or ''),
                        (doc.get('reason') or ''),
                        (doc.get('source') or ''),
                        (doc.get('quarantine_key') or ''),
                        bool(doc.get('applied', True)),
                        json.dumps(doc.get('state') or doc, ensure_ascii=False, default=str),
                        now_ms,
                        int(doc.get('created_at_ms') or now_ms),
                    ))
                if EXECUTION_QUARANTINE_EVENTS_TOTAL:
                    EXECUTION_QUARANTINE_EVENTS_TOTAL.labels(action=(doc.get('action') or 'QUARANTINED')).inc()
                return True
            finally:
                with contextlib.suppress(Exception):
                    conn.rollback()  # type: ignore
                conn.close()  # type: ignore
        except Exception:
            if TRADE_QUARANTINE_LEDGER_WRITE_FAIL_TOTAL:
                TRADE_QUARANTINE_LEDGER_WRITE_FAIL_TOTAL.labels(kind='quarantine_event').inc()
            return False

    def record_repair_run(self, payload: dict[str, Any]) -> bool:
        if not self.enabled:
            return False
        doc = dict(payload or {})
        started_at_ms = int(doc.get('started_at_ms') or get_ny_time_millis())
        finished_at_ms = int(doc.get('finished_at_ms') or started_at_ms)
        sql = (
            'INSERT INTO execution_repair_runs '
            '(run_kind, source, status, summary_jsonb, started_at_ms, finished_at_ms) '
            'VALUES (%s,%s,%s,%s::jsonb,%s,%s)'
        )
        try:
            conn = self._connect()
            try:
                with conn, conn.cursor() as cur:  # type: ignore
                    cur.execute(sql, (
                        (doc.get('run_kind') or 'automated_repair'),
                        (doc.get('source') or ''),
                        (doc.get('status') or ''),
                        json.dumps(doc.get('summary') or doc, ensure_ascii=False, default=str),
                        started_at_ms,
                        finished_at_ms,
                    ))
                if EXECUTION_QUARANTINE_EVENTS_TOTAL:
                    EXECUTION_QUARANTINE_EVENTS_TOTAL.labels(action='REPAIR_RUN').inc()
                return True
            finally:
                with contextlib.suppress(Exception):
                    conn.rollback()  # type: ignore
                conn.close()  # type: ignore
        except Exception:
            if TRADE_QUARANTINE_LEDGER_WRITE_FAIL_TOTAL:
                TRADE_QUARANTINE_LEDGER_WRITE_FAIL_TOTAL.labels(kind='repair_run').inc()
            return False
