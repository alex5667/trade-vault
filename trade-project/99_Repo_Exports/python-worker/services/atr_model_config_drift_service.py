import json
import logging
import os
import uuid
from datetime import UTC, datetime, timedelta
from typing import Any

import psycopg2.extras
from prometheus_client import Counter, Gauge

from services.analytics_db import get_conn as get_db_connection

logger = logging.getLogger("atr_model_config_drift_service")

try:
    from prometheus_client import REGISTRY as _PREG
    from prometheus_client import Counter as _PCounter
    from prometheus_client import Gauge as _PGauge

    def _pcounter(name, doc, labels=()):
        try:
            return _PCounter(name, doc, list(labels))
        except ValueError:
            return (_PREG._names_to_collectors or {}).get(name)

    def _pgauge(name, doc, labels=()):
        try:
            return _PGauge(name, doc, list(labels))
        except ValueError:
            return (_PREG._names_to_collectors or {}).get(name)

    DRIFT_GOVERNANCE_EVENTS_TOTAL = _pcounter("atr_drift_governance_total", "Total drift governance events", ["drift_family", "severity", "status"])
    DATASET_REFRESH_REQUESTS_TOTAL = _pcounter("atr_dataset_refresh_requests_total", "Total refresh requests", ["dataset_class", "status"])
    DATASET_VALIDITY_TOTAL = _pgauge("atr_dataset_validity_total", "Dataset validity status", ["status", "dataset_class"])
    DATASET_EXPIRING_TOTAL = _pgauge("atr_dataset_expiring_total", "Datasets expiring soon", ["dataset_class"])
    DATASET_REFRESH_ACTIVATED_TOTAL = _pcounter("atr_dataset_refresh_activated_total", "Total refresh activations", ["dataset_class"])
except ImportError:
    class _NullMetric:
        def inc(self, **_): pass
        def set(self, **_): pass
        def labels(self, **_): return self
    DRIFT_GOVERNANCE_EVENTS_TOTAL = _NullMetric()
    DATASET_REFRESH_REQUESTS_TOTAL = _NullMetric()
    DATASET_VALIDITY_TOTAL = _NullMetric()
    DATASET_EXPIRING_TOTAL = _NullMetric()
    DATASET_REFRESH_ACTIVATED_TOTAL = _NullMetric()

ATR_DRIFT_GOVERNANCE_ENABLE = os.getenv("ATR_DRIFT_GOVERNANCE_ENABLE", "1") == "1"
ATR_DRIFT_GOVERNANCE_ENFORCE = os.getenv("ATR_DRIFT_GOVERNANCE_ENFORCE", "0") == "1"

# Default validity windows (days)
VALIDITY_WINDOWS = {
    "SMOKE_GOLDEN": 30,
    "CANARY_GOLDEN": 45,
    "RUNTIME_GOLDEN": 30,
    "EXECUTION_GOLDEN": 14,
    "PROTECTIVE_GOLDEN": 30,
    "INCIDENT_GOLDEN": 365,
    "RELEASE_WINDOW_GOLDEN": 30,
    "DR_RESTORE_GOLDEN": 90
}

class ATRModelConfigDriftService:
    @staticmethod
    def detect_feature_drift(scope_value: str, drift_score: float, threshold: float, details: dict[str, Any]) -> None:
        """Detect and open governance event for feature distribution drift."""
        if not ATR_DRIFT_GOVERNANCE_ENABLE:
            return

        severity = "warn"
        if drift_score > threshold * 2:
            severity = "critical"
        elif drift_score > threshold:
            severity = "error"

        if severity in ["error", "critical"]:
            reason = f"Feature drift score {drift_score:.4f} exceeds threshold {threshold}"
            ATRModelConfigDriftService.open_drift_governance_event(
                drift_family="FEATURE_DISTRIBUTION_DRIFT",
                scope_value=scope_value,
                severity=severity,
                reason_code="FEATURE_DISTRICT_SHIFT",
                event_json={"drift_score": drift_score, "threshold": threshold, **details}
            )

    @staticmethod
    def detect_execution_cost_drift(scope_value: str, slippage_ema: float, approved_band: float, details: dict[str, Any]) -> None:
        """Detect and open governance event for execution cost drift (slippage)."""
        if not ATR_DRIFT_GOVERNANCE_ENABLE:
            return

        if abs(slippage_ema) > approved_band:
            severity = "critical" if abs(slippage_ema) > approved_band * 2 else "error"
            ATRModelConfigDriftService.open_drift_governance_event(
                drift_family="EXECUTION_COST_DRIFT",
                scope_value=scope_value,
                severity=severity,
                reason_code="SLIPPAGE_EMA_SHIFT",
                event_json={"slippage_ema": slippage_ema, "approved_band": approved_band, **details}
            )

    @staticmethod
    def detect_protective_outcome_drift(scope_value: str, metric_name: str, deviation: float, details: dict[str, Any]) -> None:
        """Detect and open governance event for protective outcome drift."""
        if not ATR_DRIFT_GOVERNANCE_ENABLE:
            return

        severity = "error" # Default for protective deviations
        ATRModelConfigDriftService.open_drift_governance_event(
            drift_family="PROTECTIVE_OUTCOME_DRIFT",
            scope_value=scope_value,
            severity=severity,
            reason_code=f"PROTECTIVE_{metric_name.upper()}_SHIFT",
            event_json={"metric": metric_name, "deviation": deviation, **details}
        )

    @staticmethod
    def open_drift_governance_event(drift_family: str, scope_value: str, severity: str, reason_code: str, event_json: dict[str, Any]) -> str:
        event_id = f"drift_{uuid.uuid4().hex[:8]}"
        status = "open"

        # Determine if refresh should be requested automatically
        if severity in ["error", "critical"]:
            status = "refresh_requested"

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO atr_drift_governance_events 
                    (event_id, drift_family, scope_value, severity, status, reason_code, event_json)
                    VALUES (%s, %s, %s, %s, %s, %s, %s)
                    ON CONFLICT DO NOTHING
                """, (event_id, drift_family, scope_value, severity, status, reason_code, json.dumps(event_json)))

                if status == "refresh_requested":
                    # Potentially open a refresh request immediately
                    ATRModelConfigDriftService.open_dataset_refresh_request(
                        drift_family=drift_family,
                        scope_value=scope_value,
                        trigger_event_id=event_id,
                        owner="system"
                    )
            conn.commit()
  # type: ignore
        DRIFT_GOVERNANCE_EVENTS_TOTAL.labels(drift_family=drift_family, severity=severity, status=status).inc()  # type: ignore
        return event_id

    @staticmethod
    def open_dataset_refresh_request(drift_family: str, scope_value: str, trigger_event_id: str, owner: str) -> str:
        request_id = f"refreq_{uuid.uuid4().hex[:8]}"
        # Map drift family to dataset class
        dataset_class_map = {
            "FEATURE_DISTRIBUTION_DRIFT": "RUNTIME_GOLDEN",
            "EXECUTION_COST_DRIFT": "EXECUTION_GOLDEN",
            "PROTECTIVE_OUTCOME_DRIFT": "PROTECTIVE_GOLDEN",
            "DATASET_REPRESENTATIVENESS_DRIFT": "RUNTIME_GOLDEN",
            "CONFIG_SURFACE_DRIFT": "SMOKE_GOLDEN"
        }
        dataset_class = dataset_class_map.get(drift_family, "RUNTIME_GOLDEN")

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO atr_dataset_refresh_requests 
                    (request_id, dataset_class, scope_json, trigger_event_id, status, owner, summary_json)
                    VALUES (%s, %s, %s, %s, 'requested', %s, %s)
                """, (request_id, dataset_class, json.dumps({"scope": scope_value}), trigger_event_id, owner, json.dumps({})))
            conn.commit()
  # type: ignore
        DATASET_REFRESH_REQUESTS_TOTAL.labels(dataset_class=dataset_class, status="requested").inc()  # type: ignore
        return request_id

    @staticmethod
    def update_baseline_validity(dataset_id: str, dataset_class: str) -> None:
        """Initialize or update validity window for a dataset."""
        days = VALIDITY_WINDOWS.get(dataset_class, 30)
        valid_from = datetime.now(UTC)
        valid_until = valid_from + timedelta(days=days)

        with get_db_connection() as conn:
            with conn.cursor() as cur:
                cur.execute("""
                    INSERT INTO atr_dataset_baseline_validity 
                    (dataset_id, valid_from, valid_until, status, summary_json)
                    VALUES (%s, %s, %s, 'valid', %s)
                    ON CONFLICT (dataset_id) DO UPDATE 
                    SET valid_until = EXCLUDED.valid_until, status = 'valid', updated_at = now()
                """, (dataset_id, valid_from, valid_until, json.dumps({"days": days})))
            conn.commit()

    @staticmethod
    def check_dataset_validity(dataset_id: str) -> tuple[str, datetime | None]:
        """Check if a dataset is still valid."""
        with get_db_connection() as conn:
            with conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
                cur.execute("SELECT status, valid_until FROM atr_dataset_baseline_validity WHERE dataset_id = %s", (dataset_id,))
                row = cur.fetchone()
                if not row:
                    return "missing", None

                status = row['status']
                until = row['valid_until']

                if status == 'valid' and datetime.now(UTC) > until:
                    # Auto-expire
                    cur.execute("UPDATE atr_dataset_baseline_validity SET status = 'expired' WHERE dataset_id = %s", (dataset_id,))
                    conn.commit()
                    return "expired", until

                return status, until

    @staticmethod  # type: ignore
    def get_active_drift_events(scope: str = None) -> list[dict[str, Any]]:  # type: ignore
        with get_db_connection() as conn, conn.cursor(cursor_factory=psycopg2.extras.DictCursor) as cur:
            query = "SELECT * FROM atr_drift_governance_events WHERE status <> 'resolved'"
            params = []
            if scope:
                query += " AND scope_value = %s"
                params.append(scope)
            cur.execute(query, params)
            return [dict(r) for r in cur.fetchall()]

    @staticmethod
    def is_release_blocked_by_drift(change_class: str, target_scope: str) -> list[str]:
        """Check if drift governance blocks a release."""
        if not ATR_DRIFT_GOVERNANCE_ENABLE:
            return []

        blockers = []
        events = ATRModelConfigDriftService.get_active_drift_events(target_scope)

        # Family mapping to change classes
        risk_mapping = {
            "FEATURE_DISTRIBUTION_DRIFT": ["RUNTIME_GOLDEN", "CANARY_GOLDEN"],
            "DECISION_BEHAVIOR_DRIFT": ["CRITICAL_RUNTIME_GATING"],
            "EXECUTION_COST_DRIFT": ["CRITICAL_EXECUTION_TOUCHING"],
            "PROTECTIVE_OUTCOME_DRIFT": ["PROTECTIVE_PATH_TOUCHING"],
            "CONFIG_SURFACE_DRIFT": ["MEDIUM_POLICY", "HIGH_GOVERNANCE"],
            "DATASET_REPRESENTATIVENESS_DRIFT": ["HIGH_GOVERNANCE", "CRITICAL_RUNTIME_GATING"]
        }

        for event in events:
            if event['severity'] == 'critical':
                # Critical drift blocks related scope
                related_classes = risk_mapping.get(event['drift_family'], [])
                if change_class in related_classes or event['drift_family'] == "DECISION_BEHAVIOR_DRIFT":
                    blockers.append(f"critical drift {event['drift_family']} on {event['scope_value']}")

        return blockers
