from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

try:  # pragma: no cover
    import redis.asyncio as redis
except Exception:  # pragma: no cover
    redis = None

try:  # pragma: no cover
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
except Exception:  # pragma: no cover
    Counter = Gauge = Histogram = None
    def start_http_server(*args: Any, **kwargs: Any) -> None:
        return None


APP_NAME = "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_rollup_v3_57"
PROM_PREFIX = "ml_phase3_57_slo_rollup"

BASE_LABELS = {
    "pipeline": "route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply",
    "version": "v3"
}
LABEL_NAMES = list(BASE_LABELS.keys())

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
METRICS_PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_SLO_PORT", "9993"))
WINDOW_MIN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_SLO_WINDOW_MIN", "10080"))
POLL_SEC = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_SLO_POLL_SEC", "60"))
MTTR_SLO_SEC = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_INCIDENT_RCA_APPLY_MTTR_SLO_SEC", "900"))

VERIFICATION_STREAM = "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_verification_results"
ROLLBACK_STREAM = "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_rollback_journal"
RETRY_STREAM = "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_retry_results"
ESCALATION_STREAM = "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_escalations"
SLO_STREAM = "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_rollups"
SLO_AUDIT_STREAM = "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo_audit"
METRICS_HASH = "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_slo:last"

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()
logging.basicConfig(
    level=getattr(logging, LOG_LEVEL, logging.INFO),
    format="%(asctime)s %(levelname)s %(name)s %(message)s",
)
log = logging.getLogger(APP_NAME)


RUNS = Counter(f"{PROM_PREFIX}_runs_total", "SLO rollup runs", ["status"] + LABEL_NAMES) if Counter else None
LATENCY = Histogram(f"{PROM_PREFIX}_latency_seconds", "SLO rollup loop latency seconds", LABEL_NAMES) if Histogram else None
UP = Gauge(f"{PROM_PREFIX}_up", "SLO rollup service up", LABEL_NAMES) if Gauge else None
LAST_RUN = Gauge(f"{PROM_PREFIX}_last_run_ts_seconds", "SLO rollup last run ts seconds", LABEL_NAMES) if Gauge else None
VERIFY_KEEP_RATE = Gauge(f"{PROM_PREFIX}_verify_keep_rate", "Verification keep rate", LABEL_NAMES) if Gauge else None
ROLLBACK_PLAN_RATE = Gauge(f"{PROM_PREFIX}_rollback_plan_rate", "Rollback plan rate", LABEL_NAMES) if Gauge else None
ROLLBACK_APPLIED_RATE = Gauge(f"{PROM_PREFIX}_rollback_applied_rate", "Rollback applied rate", LABEL_NAMES) if Gauge else None
ROLLBACK_MTTR_P95 = Gauge(f"{PROM_PREFIX}_rollback_mttr_p95_sec", "Rollback MTTR p95 seconds", LABEL_NAMES) if Gauge else None
RETRY_RATE = Gauge(f"{PROM_PREFIX}_retry_rate", "Retry rate", LABEL_NAMES) if Gauge else None
ESCALATION_RATE = Gauge(f"{PROM_PREFIX}_escalation_rate", "Escalation rate", LABEL_NAMES) if Gauge else None
VERIFICATION_N = Gauge(f"{PROM_PREFIX}_verification_n", "Verification row count", LABEL_NAMES) if Gauge else None
ROLLBACK_PLANNED_N = Gauge(f"{PROM_PREFIX}_rollback_planned_n", "Rollback planned count", LABEL_NAMES) if Gauge else None
ROLLBACK_APPLIED_N = Gauge(f"{PROM_PREFIX}_rollback_applied_n", "Rollback applied count", LABEL_NAMES) if Gauge else None
RETRY_N = Gauge(f"{PROM_PREFIX}_retry_n", "Retry count", LABEL_NAMES) if Gauge else None
ESCALATION_N = Gauge(f"{PROM_PREFIX}_escalation_n", "Escalation count", LABEL_NAMES) if Gauge else None


def now_ms() -> int:
    return int(time.time() * 1000)


def as_dict(fields: Dict[Any, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
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


def maybe_json(value: Any, default: Any = None) -> Any:
    if value is None:
        return default
    if isinstance(value, (dict, list)):
        return value
    if isinstance(value, (bytes, bytearray)):
        try:
            value = value.decode()
        except Exception:
            return default
    if not isinstance(value, str):
        return value
    try:
        return json.loads(value)
    except Exception:
        return default if default is not None else value


def i64(x: Any, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default


def f01(x: Any) -> int:
    return 1 if i64(x, 0) != 0 else 0


def f64(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def normalize_row(row: Dict[str, Any]) -> Dict[str, Any]:
    payload = maybe_json(row.get("payload"), None)
    if isinstance(payload, dict):
        merged = dict(payload)
        merged.setdefault("_raw", row)
        return merged
    return row


def p95(values: List[float]) -> float:
    if not values:
        return 0.0
    arr = sorted(values)
    idx = min(len(arr) - 1, max(0, int(round(0.95 * (len(arr) - 1)))))
    return float(arr[idx])


async def read_stream_since(r: "redis.Redis", stream: str, min_id: str, count: int = 10000) -> List[Dict[str, Any]]:
    rows = await r.xrange(stream, min=min_id, max="+", count=count)
    out: List[Dict[str, Any]] = []
    for msg_id, fields in rows:
        row = as_dict(fields)
        row["_id"] = msg_id.decode() if isinstance(msg_id, (bytes, bytearray)) else str(msg_id)
        out.append(normalize_row(row))
    return out


def build_rollup(
    verification_rows: List[Dict[str, Any]],
    rollback_rows: List[Dict[str, Any]],
    retry_rows: List[Dict[str, Any]],
    escalation_rows: List[Dict[str, Any]],
) -> Dict[str, Any]:
    verification_n = len(verification_rows)
    verified_n = sum(1 for r in verification_rows if str(r.get("decision") or "") == "VERIFIED")
    rollback_planned_n = sum(1 for r in rollback_rows if str(r.get("decision") or "") == "ROLLBACK_TO_PREVIOUS_MODE")
    rollback_applied_n = sum(
        1
        for r in rollback_rows
        if str(r.get("decision") or "") == "ROLLBACK_TO_PREVIOUS_MODE" and f01(r.get("applied")) == 1
    )
    retry_n = sum(1 for r in retry_rows if str(r.get("decision") or "") == "RETRY_ROLLBACK_TO_PREVIOUS_MODE")
    escalation_n = sum(1 for r in escalation_rows if str(r.get("decision") or "") == "ESCALATE")

    mttr_vals: List[float] = []
    for r in rollback_rows:
        if str(r.get("decision") or "") != "ROLLBACK_TO_PREVIOUS_MODE":
            continue
        if f01(r.get("applied")) != 1:
            continue
        verification_ts_ms = i64(
            r.get("source_verification_ts_ms")
            or r.get("verification_ts_ms")
            or r.get("source_apply_verification_ts_ms")
        )
        rollback_ts_ms = i64(r.get("ts_ms"))
        if verification_ts_ms > 0 and rollback_ts_ms >= verification_ts_ms:
            mttr_vals.append((rollback_ts_ms - verification_ts_ms) / 1000.0)

    verify_keep_rate = float(verified_n) / float(verification_n) if verification_n else 0.0
    rollback_plan_rate = float(rollback_planned_n) / float(verification_n) if verification_n else 0.0
    rollback_applied_rate = float(rollback_applied_n) / float(rollback_planned_n) if rollback_planned_n else 0.0
    retry_rate = float(retry_n) / float(rollback_planned_n) if rollback_planned_n else 0.0
    escalation_rate = float(escalation_n) / float(rollback_planned_n) if rollback_planned_n else 0.0

    return {
        "schema_version": 1,
        "app_name": APP_NAME,
        "ts_ms": now_ms(),
        "window_min": WINDOW_MIN,
        "verification_n": verification_n,
        "verified_n": verified_n,
        "rollback_planned_n": rollback_planned_n,
        "rollback_applied_n": rollback_applied_n,
        "retry_n": retry_n,
        "escalation_n": escalation_n,
        "verify_keep_rate": round(verify_keep_rate, 6),
        "rollback_plan_rate": round(rollback_plan_rate, 6),
        "rollback_applied_rate": round(rollback_applied_rate, 6),
        "rollback_mttr_p95_sec": round(p95(mttr_vals), 6),
        "retry_rate": round(retry_rate, 6),
        "escalation_rate": round(escalation_rate, 6),
        "mttr_slo_sec": MTTR_SLO_SEC,
    }


def metrics_mapping(rollup: Dict[str, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in rollup.items():
        if isinstance(v, (dict, list)):
            out[k] = json.dumps(v, ensure_ascii=False, sort_keys=True)
        else:
            out[k] = str(v)
    return out


async def loop() -> None:
    if redis is None:
        raise RuntimeError("redis.asyncio is required")

    start_http_server(METRICS_PORT)
    if UP:
        UP.labels(**BASE_LABELS).set(1)

    r = redis.from_url(REDIS_URL, decode_responses=False)
    try:
        while True:
            t0 = time.time()
            status = "ok"
            try:
                cutoff_ms = now_ms() - WINDOW_MIN * 60 * 1000
                min_id = f"{cutoff_ms}-0"

                verification_rows = await read_stream_since(r, VERIFICATION_STREAM, min_id)
                rollback_rows = await read_stream_since(r, ROLLBACK_STREAM, min_id)
                retry_rows = await read_stream_since(r, RETRY_STREAM, min_id)
                escalation_rows = await read_stream_since(r, ESCALATION_STREAM, min_id)

                rollup = build_rollup(
                    verification_rows=verification_rows,
                    rollback_rows=rollback_rows,
                    retry_rows=retry_rows,
                    escalation_rows=escalation_rows,
                )

                payload = json.dumps(rollup, ensure_ascii=False, sort_keys=True)
                await r.xadd(SLO_STREAM, {"payload": payload}, maxlen=20000, approximate=True)
                await r.xadd(
                    SLO_AUDIT_STREAM,
                    {"payload": payload, "event": "slo_rollup"},
                    maxlen=20000,
                    approximate=True,
                )
                await r.hset(METRICS_HASH, mapping=metrics_mapping(rollup))

                if VERIFY_KEEP_RATE:
                    VERIFY_KEEP_RATE.labels(**BASE_LABELS).set(rollup["verify_keep_rate"])
                if ROLLBACK_PLAN_RATE:
                    ROLLBACK_PLAN_RATE.labels(**BASE_LABELS).set(rollup["rollback_plan_rate"])
                if ROLLBACK_APPLIED_RATE:
                    ROLLBACK_APPLIED_RATE.labels(**BASE_LABELS).set(rollup["rollback_applied_rate"])
                if ROLLBACK_MTTR_P95:
                    ROLLBACK_MTTR_P95.labels(**BASE_LABELS).set(rollup["rollback_mttr_p95_sec"])
                if RETRY_RATE:
                    RETRY_RATE.labels(**BASE_LABELS).set(rollup["retry_rate"])
                if ESCALATION_RATE:
                    ESCALATION_RATE.labels(**BASE_LABELS).set(rollup["escalation_rate"])
                if VERIFICATION_N:
                    VERIFICATION_N.labels(**BASE_LABELS).set(rollup["verification_n"])
                if ROLLBACK_PLANNED_N:
                    ROLLBACK_PLANNED_N.labels(**BASE_LABELS).set(rollup["rollback_planned_n"])
                if ROLLBACK_APPLIED_N:
                    ROLLBACK_APPLIED_N.labels(**BASE_LABELS).set(rollup["rollback_applied_n"])
                if RETRY_N:
                    RETRY_N.labels(**BASE_LABELS).set(rollup["retry_n"])
                if ESCALATION_N:
                    ESCALATION_N.labels(**BASE_LABELS).set(rollup["escalation_n"])
                if LAST_RUN:
                    LAST_RUN.labels(**BASE_LABELS).set(time.time())

                log.info(
                    "slo_rollup verification_n=%s rollback_planned_n=%s rollback_applied_n=%s retry_n=%s escalation_n=%s rollback_mttr_p95_sec=%.3f",
                    rollup["verification_n"],
                    rollup["rollback_planned_n"],
                    rollup["rollback_applied_n"],
                    rollup["retry_n"],
                    rollup["escalation_n"],
                    rollup["rollback_mttr_p95_sec"],
                )
            except Exception:
                status = "error"
                log.exception("slo_rollup loop failed")
            finally:
                if RUNS:
                    RUNS.labels(status=status, **BASE_LABELS).inc()
                if LATENCY:
                    LATENCY.labels(**BASE_LABELS).observe(max(0.0, time.time() - t0))
            await asyncio.sleep(POLL_SEC)
    finally:
        if UP:
            UP.labels(**BASE_LABELS).set(0)
        await r.close()


def main() -> None:
    asyncio.run(loop())


if __name__ == "__main__":
    main()
