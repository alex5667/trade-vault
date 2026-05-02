from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""Best-effort SQL sink for repeated risk mismatch quarantine events.

P4.8: Writes each repeated-mismatch quarantine event to risk_mismatch_quarantine_ledger.
Prometheus counters track successful and failed writes.
All fields are coerced to match the SQL schema; missing values default gracefully.

Environment variables
---------------------
RISK_AUDIT_SQL_DSN               – PostgreSQL DSN for the ledger table.
                                   Falls back to EXECUTION_JOURNAL_DSN.
TRADE_RISK_DRIFT_LEDGER_ENABLE   – '1'/'true'/'yes'/'on' to enable (default: 1).
"""

from dataclasses import dataclass
from typing import Any, Dict
import json
import os
import time

try:
    import psycopg  # type: ignore
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore

try:
    from prometheus_client import Counter, REGISTRY
except Exception:  # pragma: no cover
    Counter = None  # type: ignore
    REGISTRY = None  # type: ignore


def _metric(factory, name: str, *args, **kwargs):
    """Register a Prometheus metric, returning existing collector if already registered."""
    if factory is None:
        return None
    try:
        return factory(name, *args, **kwargs)
    except ValueError:
        # Already registered — retrieve from registry
        return getattr(REGISTRY, '_names_to_collectors', {}).get(name) if REGISTRY is not None else None


TRADE_RISK_DRIFT_LEDGER_WRITE_TOTAL = _metric(
    Counter,
    'trade_risk_drift_ledger_write_total',
    'Number of successful writes to risk mismatch quarantine ledger.',
    ['action'],
)
TRADE_RISK_DRIFT_LEDGER_WRITE_FAIL_TOTAL = _metric(
    Counter,
    'trade_risk_drift_ledger_write_fail_total',
    'Number of failed writes to risk mismatch quarantine ledger.',
    ['stage'],
)


@dataclass
class RiskDriftSqlSink:
    """Best-effort SQL sink: write risk mismatch quarantine events to ledger table.

    Usage::

        sink = RiskDriftSqlSink.from_env()
        sink.record_quarantine({
            'decision_id': '...',
            'sid': '...',
            'symbol': 'BTCUSDT',
            'tier': 'TIER1',
            'repeated_count': 5,
            'mismatch_rate': 0.12,
            'reasons': ['execution_policy'],
            'quarantine_action': 'REPEATED_MISMATCH_QUARANTINED',
        })

    Returns True on success, False on any error (never raises).
    """

    dsn: str = ''
    enabled: bool = False

    @classmethod
    def from_env(cls) -> 'RiskDriftSqlSink':
        """Construct from environment variables (RISK_AUDIT_SQL_DSN / EXECUTION_JOURNAL_DSN)."""
        dsn = str(os.getenv('RISK_AUDIT_SQL_DSN', os.getenv('EXECUTION_JOURNAL_DSN', '')) or '').strip()
        enabled = str(os.getenv('TRADE_RISK_DRIFT_LEDGER_ENABLE', '1')).strip().lower() in {'1', 'true', 'yes', 'on'}
        return cls(dsn=dsn, enabled=bool(enabled and dsn))

    def record_quarantine(self, payload: Dict[str, Any]) -> bool:
        """Insert one quarantine event into risk_mismatch_quarantine_ledger.

        Args:
            payload: dict with decision_id, sid, signal_id, symbol, tier,
                     repeated_count, mismatch_rate, reasons (list), quarantine_action.

        Returns:
            True if the INSERT succeeded, False otherwise (never raises).
        """
        if not self.enabled or not self.dsn or psycopg is None:
            return False
        doc = dict(payload or {})
        now_ms = int(doc.get('created_ts_ms') or get_ny_time_millis())
        try:
            with psycopg.connect(self.dsn) as conn:
                with conn.cursor() as cur:
                    cur.execute(
                        """
                        INSERT INTO risk_mismatch_quarantine_ledger (
                            decision_id, sid, signal_id, symbol, tier,
                            repeated_count, mismatch_rate, reasons_jsonb,
                            source, quarantine_action, created_ts_ms
                        ) VALUES (
                            %s,%s,%s,%s,%s,
                            %s,%s,%s::jsonb,
                            %s,%s,%s
                        )
                        """
                        (
                            str(doc.get('decision_id') or '') or None,
                            str(doc.get('sid') or '') or None,
                            str(doc.get('signal_id') or '') or None,
                            str(doc.get('symbol') or ''),
                            str(doc.get('tier') or ''),
                            int(doc.get('repeated_count') or 0),
                            float(doc.get('mismatch_rate') or 0.0),
                            json.dumps(doc.get('reasons') or [], ensure_ascii=False, default=str),
                            str(doc.get('source') or 'risk_consistency_checker'),
                            str(doc.get('quarantine_action') or 'REPEATED_MISMATCH_QUARANTINED'),
                            now_ms,
                        ),
                    )
                conn.commit()
            if TRADE_RISK_DRIFT_LEDGER_WRITE_TOTAL:
                TRADE_RISK_DRIFT_LEDGER_WRITE_TOTAL.labels(
                    action=str(doc.get('quarantine_action') or 'REPEATED_MISMATCH_QUARANTINED')
                ).inc()
            return True
        except Exception:
            if TRADE_RISK_DRIFT_LEDGER_WRITE_FAIL_TOTAL:
                TRADE_RISK_DRIFT_LEDGER_WRITE_FAIL_TOTAL.labels(stage='record_quarantine').inc()
            return False
