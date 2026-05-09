from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any

from utils.time_utils import get_ny_time_millis

try:  # pragma: no cover
    import redis.asyncio as redis
except Exception:  # pragma: no cover
    redis = None

try:  # pragma: no cover
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None

try:  # pragma: no cover
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
except Exception:  # pragma: no cover
    Counter = Gauge = Histogram = None
    def start_http_server(*args: Any, **kwargs: Any) -> None:
        return None


APP_NAME = "route_incident_rca_mirror_auto_escalation_summarizer_v3_10"
SLO_LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_SLO_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_slo:last",
)
RETRY_LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RETRY_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_retry:last",
)
OUTPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_ESCALATIONS_STREAM",
    "stream:ml:route_incident_rca_mirror_escalations",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_ESCALATIONS_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_escalations:last",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_ESCALATIONS_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_escalations_audit",
)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_ESCALATIONS_PORT", "9928"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_ESCALATIONS_MAXLEN", "20000"))
RUN_EVERY_SEC = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_ESCALATIONS_RUN_EVERY_SEC", "300"))


def _counter(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter("ml_route_incident_rca_mirror_escalations_runs_total", "Mirror escalation runs", ("status", "severity"))
LAT = _hist("ml_route_incident_rca_mirror_escalations_latency_seconds", "Mirror escalation latency seconds")
UP = _gauge("ml_route_incident_rca_mirror_escalations_up", "Mirror escalation up")
LAST_RUN_TS = _gauge("ml_route_incident_rca_mirror_escalations_last_run_ts_seconds", "Mirror escalation last run ts")


def now_ms() -> int:
    return get_ny_time_millis()


def as_dict(fields: dict[Any, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    for k, v in fields.items():
        kk = k.decode() if isinstance(k, (bytes, bytearray)) else str(k)
        if isinstance(v, (bytes, bytearray)):
            try:
                out[kk] = v.decode()
            except Exception:
                out[kk] = v.hex()
        else:
            out[kk] = v
    return out


def stable_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def parse_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def parse_float(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default


def decide_severity(slo: dict[str, Any], retry: dict[str, Any]) -> dict[str, Any]:
    promotion_apply_rate = parse_float(slo.get("promotion_apply_rate"), 1.0)
    rollback_apply_rate = parse_float(slo.get("rollback_apply_rate"), 1.0)
    mttr_p95 = parse_float(slo.get("rollback_mttr_p95_sec"), 0.0)
    retry_decision = (retry.get("decision") or "NOOP")
    retry_reason = (retry.get("reason_code") or "NO_ACTION")

    severity = "info"
    reason_codes = []
    if promotion_apply_rate < 1.0:
        reason_codes.append("PROMOTION_APPLY_RATE_LOW")
    if rollback_apply_rate < 1.0:
        reason_codes.append("ROLLBACK_APPLY_RATE_LOW")
    if mttr_p95 > 120.0:
        reason_codes.append("ROLLBACK_MTTR_P95_HIGH")
    if retry_decision == "EXHAUSTED":
        reason_codes.append("RETRY_EXHAUSTED")

    if "RETRY_EXHAUSTED" in reason_codes or "ROLLBACK_APPLY_RATE_LOW" in reason_codes:
        severity = "critical"
    elif reason_codes:
        severity = "warning"

    return {
        "severity": severity,
        "reason_codes": reason_codes or ["OK"],
        "retry_decision": retry_decision,
        "retry_reason_code": retry_reason,
        "promotion_apply_rate": promotion_apply_rate,
        "rollback_apply_rate": rollback_apply_rate,
        "rollback_mttr_p95_sec": mttr_p95,
    }


async def persist_if_configured(db_url: str, summary: dict[str, Any]) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """

                INSERT INTO llm_route_incident_rca_mirror_escalations (
                    ts_ms, severity, summary_json
                ) VALUES (
                    %(ts_ms)s, %(severity)s, %(summary_json)s
                )
                """,
                {"ts_ms": now_ms(), "severity": summary["severity"], "summary_json": json.dumps(summary)},
            )
            conn.commit()


async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PORT)
    if UP:
        UP.set(1)
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    db_url = os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL", "")

    while True:
        started = time.perf_counter()
        status = "ok"
        severity = "info"
        try:
            slo = as_dict(await r.hgetall(SLO_LAST_HASH))
            retry = as_dict(await r.hgetall(RETRY_LAST_HASH))
            summary = decide_severity(slo, retry)
            severity = summary["severity"]
            await persist_if_configured(db_url, summary)
            await r.xadd(OUTPUT_STREAM, {"schema_version": 1, "severity": severity, "summary_json": stable_json(summary), "ts_ms": str(now_ms())}, maxlen=MAXLEN, approximate=True)
            await r.hset(LAST_HASH, mapping={"severity": severity, "summary_json": stable_json(summary), "ts_ms": str(now_ms())})
            await r.xadd(AUDIT_STREAM, {"event_type": "ROUTE_INCIDENT_RCA_MIRROR_ESCALATION_SUMMARY", "severity": severity, "ts_ms": str(now_ms())}, maxlen=MAXLEN, approximate=True)
            if LAST_RUN_TS:
                LAST_RUN_TS.set(time.time())
        except Exception as exc:
            status = "error"
            await r.xadd(AUDIT_STREAM, {"event_type": "ROUTE_INCIDENT_RCA_MIRROR_ESCALATION_FAILED", "error": str(exc), "ts_ms": str(now_ms())}, maxlen=MAXLEN, approximate=True)
        finally:
            if RUNS:
                RUNS.labels(status=status, severity=severity).inc()
            if LAT:
                LAT.observe(max(time.perf_counter() - started, 0.0))
            await asyncio.sleep(max(RUN_EVERY_SEC, 5))


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
