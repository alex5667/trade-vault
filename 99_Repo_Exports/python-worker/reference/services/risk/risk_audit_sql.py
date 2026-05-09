from __future__ import annotations

from utils.time_utils import get_ny_time_millis

"""Best-effort SQL audit sink for risk decisions (P4.4/P4.5).

The sink mirrors pre-publish risk decisions into Postgres without making the
signal path dependent on SQL availability. If the DB is unavailable, the
caller continues fail-open and Prometheus counters reflect the write failure.

Tables written:
  risk_decision_audit  — full audit trail per decision (immutable append)
  risk_snapshot        — latest state per decision_id (upsert, fast lookup)

Prometheus metrics emitted:
  trade_risk_audit_write_total{table}      — successful writes
  trade_risk_audit_write_fail_total{stage} — failed writes
"""

import json
import os
from dataclasses import dataclass
from typing import Any

try:
    import psycopg  # type: ignore
except Exception:  # pragma: no cover
    psycopg = None  # type: ignore

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
        return getattr(REGISTRY, '_names_to_collectors', {}).get(name) if REGISTRY is not None else None


TRADE_RISK_AUDIT_WRITE_FAIL_TOTAL = _metric(
    Counter,
    'trade_risk_audit_write_fail_total',
    'Number of failed SQL writes for risk decision audit storage.',
    ['stage'],
)
TRADE_RISK_AUDIT_WRITE_TOTAL = _metric(
    Counter,
    'trade_risk_audit_write_total',
    'Number of successful SQL writes for risk decision audit storage.',
    ['table'],
)


def _json(v: Any) -> str:
    """Serialize value to compact JSON string."""
    return json.dumps(v, ensure_ascii=False, sort_keys=True, separators=(',', ':'))


@dataclass
class RiskAuditSqlSink:
    """Best-effort SQL sink for risk decisions.

    Writes to risk_decision_audit and risk_snapshot tables.
    Fail-open: if the DB is unavailable the publish path is not blocked.
    """
    dsn: str = ''
    enabled: bool = False

    @classmethod
    def from_env(cls) -> RiskAuditSqlSink:
        """Construct sink from environment variables."""
        dsn = os.getenv('RISK_AUDIT_SQL_DSN', os.getenv('EXECUTION_JOURNAL_DSN', '') or '').strip()
        enabled = os.getenv('TRADE_RISK_SQL_AUDIT_ENABLE', '1').strip().lower() in {'1', 'true', 'yes', 'on'}
        return cls(dsn=dsn, enabled=bool(enabled and dsn))

    def _connect(self):
        """Open a new psycopg connection; returns None if sink is disabled or unavailable."""
        if not self.enabled or not self.dsn or psycopg is None:
            return None
        return psycopg.connect(self.dsn)

    def record_decision(
        self,
        *,
        decision_id: str,
        signal: dict[str, Any],
        risk_input: Any,
        risk_decision: Any,
    ) -> bool:
        """Mirror a risk decision into SQL.

        Writes to risk_decision_audit (INSERT … ON CONFLICT UPDATE) and
        risk_snapshot (upsert by decision_id).

        Returns True on success, False on any failure.
        All exceptions are swallowed — the caller must not block on this.
        """
        conn = self._connect()
        if conn is None:
            return False

        created_ts_ms = int(
            signal.get('ts_event_ms') or signal.get('ts_publish_ms') or get_ny_time_millis()
        )
        snapshot = dict(getattr(risk_decision, 'snapshot', {}) or {})
        snapshot.setdefault('decision_latency_ms', int(snapshot.get('decision_latency_ms') or 0))
        reasons = list(getattr(risk_decision, 'reasons', []) or [])

        symbol = str(signal.get('symbol') or getattr(risk_input, 'symbol', '') or '').upper()
        sid = (signal.get('sid') or '') or None
        signal_id = str(signal.get('signal_id') or signal.get('id') or '') or None
        tier = str(
            getattr(getattr(risk_decision, 'tier_policy', None), 'name', None)
            or signal.get('risk_tier') or signal.get('symbol_tier') or ''
        ) or 'B'
        cluster = str(
            signal.get('risk_cluster') or signal.get('cluster')
            or getattr(risk_input, 'cluster', symbol) or symbol
        )
        level = str(getattr(risk_decision, 'level', signal.get('risk_level') or 'UNKNOWN'))
        allow_trade_publish = bool(getattr(risk_decision, 'allow_trade_publish', False))
        effective_execution_policy = str(
            getattr(risk_decision, 'effective_execution_policy', signal.get('execution_policy') or 'SAFETY_FIRST')
        )
        requested_notional_usd = float(
            getattr(risk_input, 'requested_notional_usd', signal.get('requested_notional_usd') or 0.0) or 0.0
        )
        adjusted_notional_usd = float(
            getattr(risk_decision, 'adjusted_notional_usd', signal.get('planned_notional_usd') or 0.0) or 0.0
        )
        leverage_cap = float(getattr(risk_decision, 'leverage_cap', signal.get('risk_leverage_cap') or 0.0) or 0.0)
        risk_multiplier = float(getattr(risk_decision, 'risk_multiplier', 0.0) or 0.0)
        clamp_ratio = float(snapshot.get('clamp_ratio') or 0.0)
        decision_latency_ms = float(snapshot.get('decision_latency_ms') or 0.0)

        try:
            with conn, conn.cursor() as cur:
                # Full audit trail — immutable append (ON CONFLICT → UPDATE allow idempotency)
                cur.execute(
                    """
                        insert into risk_decision_audit (
                            decision_id, signal_id, sid, symbol, cluster, tier, level,
                            allow_trade_publish, effective_execution_policy,
                            requested_notional_usd, adjusted_notional_usd,
                            leverage_cap, risk_multiplier, clamp_ratio,
                            decision_latency_ms, reasons_jsonb, snapshot_jsonb,
                            signal_jsonb, created_ts_ms
                        ) values (
                            %s,%s,%s,%s,%s,%s,%s,
                            %s,%s,
                            %s,%s,
                            %s,%s,%s,
                            %s,%s::jsonb,%s::jsonb,
                            %s::jsonb,%s
                        )
                        on conflict (decision_id) do update set
                            signal_id = excluded.signal_id,
                            sid = excluded.sid,
                            symbol = excluded.symbol,
                            cluster = excluded.cluster,
                            tier = excluded.tier,
                            level = excluded.level,
                            allow_trade_publish = excluded.allow_trade_publish,
                            effective_execution_policy = excluded.effective_execution_policy,
                            requested_notional_usd = excluded.requested_notional_usd,
                            adjusted_notional_usd = excluded.adjusted_notional_usd,
                            leverage_cap = excluded.leverage_cap,
                            risk_multiplier = excluded.risk_multiplier,
                            clamp_ratio = excluded.clamp_ratio,
                            decision_latency_ms = excluded.decision_latency_ms,
                            reasons_jsonb = excluded.reasons_jsonb,
                            snapshot_jsonb = excluded.snapshot_jsonb,
                            signal_jsonb = excluded.signal_jsonb,
                            created_ts_ms = excluded.created_ts_ms
                        """
                    (
                        decision_id, signal_id, sid, symbol, cluster, tier, level,
                        allow_trade_publish, effective_execution_policy,
                        requested_notional_usd, adjusted_notional_usd,
                        leverage_cap, risk_multiplier, clamp_ratio,
                        decision_latency_ms, _json(reasons), _json(snapshot),
                        _json(signal), created_ts_ms,
                    )
                )
                if TRADE_RISK_AUDIT_WRITE_TOTAL:
                    TRADE_RISK_AUDIT_WRITE_TOTAL.labels(table='risk_decision_audit').inc()

                # Latest snapshot per decision — upsert for fast current-state lookup
                cur.execute(
                    """
                        insert into risk_snapshot (
                            decision_id, sid, signal_id, symbol, cluster, tier, level,
                            effective_execution_policy, adjusted_notional_usd, leverage_cap,
                            clamp_ratio, decision_latency_ms, snapshot_jsonb, updated_ts_ms
                        ) values (
                            %s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s
                        )
                        on conflict (decision_id) do update set
                            sid = excluded.sid,
                            signal_id = excluded.signal_id,
                            symbol = excluded.symbol,
                            cluster = excluded.cluster,
                            tier = excluded.tier,
                            level = excluded.level,
                            effective_execution_policy = excluded.effective_execution_policy,
                            adjusted_notional_usd = excluded.adjusted_notional_usd,
                            leverage_cap = excluded.leverage_cap,
                            clamp_ratio = excluded.clamp_ratio,
                            decision_latency_ms = excluded.decision_latency_ms,
                            snapshot_jsonb = excluded.snapshot_jsonb,
                            updated_ts_ms = excluded.updated_ts_ms
                        """
                    (
                        decision_id, sid, signal_id, symbol, cluster, tier, level,
                        effective_execution_policy, adjusted_notional_usd, leverage_cap,
                        clamp_ratio, decision_latency_ms, _json(snapshot), created_ts_ms,
                    )
                )
                if TRADE_RISK_AUDIT_WRITE_TOTAL:
                    TRADE_RISK_AUDIT_WRITE_TOTAL.labels(table='risk_snapshot').inc()
            return True
        except Exception:
            if TRADE_RISK_AUDIT_WRITE_FAIL_TOTAL:
                TRADE_RISK_AUDIT_WRITE_FAIL_TOTAL.labels(stage='record_decision').inc()
            return False
