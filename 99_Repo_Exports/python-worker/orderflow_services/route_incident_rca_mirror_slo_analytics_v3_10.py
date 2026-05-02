from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
from typing import Any, Dict, List, Tuple

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


APP_NAME = "route_incident_rca_mirror_slo_analytics_v3_10"
DECISIONS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_ROLLOUT_DECISIONS_STREAM",
    "stream:ml:route_incident_rca_mirror_rollout_decisions",
)
JOURNAL_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_ROLLOUT_JOURNAL_STREAM",
    "stream:ml:route_incident_rca_mirror_rollout_journal",
)
VERIFICATION_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_RESULTS_STREAM",
    "stream:ml:route_incident_rca_mirror_verification_results",
)
OUTPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_SLO_ROLLUPS_STREAM",
    "stream:ml:route_incident_rca_mirror_slo_rollups",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_SLO_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_slo:last",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_SLO_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_slo_audit",
)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_SLO_PORT", "9926"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_SLO_MAXLEN", "20000"))
RUN_EVERY_SEC = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_SLO_RUN_EVERY_SEC", "300"))
WINDOW_MIN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_SLO_WINDOW_MIN", "1440"))
LOOKBACK_COUNT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_SLO_LOOKBACK_COUNT", "1000"))
ROLLBACK_MTTR_SLO_SEC = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_ROLLBACK_MTTR_SLO_SEC", "120"))


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter("ml_route_incident_rca_mirror_slo_runs_total", "Mirror SLO runs", ("status",))
LAT = _hist("ml_route_incident_rca_mirror_slo_latency_seconds", "Mirror SLO latency seconds")
UP = _gauge("ml_route_incident_rca_mirror_slo_up", "Mirror SLO up")
LAST_RUN_TS = _gauge("ml_route_incident_rca_mirror_slo_last_run_ts_seconds", "Mirror SLO last run ts")
PROMOTION_APPLY_RATE = _gauge("ml_route_incident_rca_mirror_promotion_apply_rate", "Mirror promotion apply rate")
ROLLBACK_APPLY_RATE = _gauge("ml_route_incident_rca_mirror_rollback_apply_rate", "Mirror rollback apply rate")
ROLLBACK_MTTR_P95 = _gauge("ml_route_incident_rca_mirror_rollback_mttr_p95_seconds", "Mirror rollback MTTR p95")


def now_ms() -> int:
    return get_ny_time_millis()


def parse_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


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


def stable_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


async def xr_recent(client: Any, stream_key: str, count: int) -> List[Dict[str, Any]]:
    try:
        rows = await client.xrevrange(stream_key, count=count)
    except Exception:
        return []
    out: List[Dict[str, Any]] = []
    for entry_id, payload in rows:
        row = as_dict(payload)
        row["_stream_id"] = entry_id.decode() if isinstance(entry_id, (bytes, bytearray)) else str(entry_id)
        out.append(row)
    return out


def percentile(values: List[float], q: float) -> float:
    if not values:
        return 0.0
    values = sorted(values)
    if len(values) == 1:
        return float(values[0])
    idx = max(0, min(len(values) - 1, int(round((len(values) - 1) * q))))
    return float(values[idx])


def compute_rollup(decision_rows: List[Dict[str, Any]], journal_rows: List[Dict[str, Any]], verification_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    cutoff = now_ms() - WINDOW_MIN * 60 * 1000
    decisions = [r for r in decision_rows if parse_int(r.get("ts_ms"), 0) >= cutoff]
    journal = [r for r in journal_rows if parse_int(r.get("ts_ms"), 0) >= cutoff]
    verif = [r for r in verification_rows if parse_int(r.get("ts_ms"), 0) >= cutoff]

    requested_promotions = [r for r in decisions if str(r.get("controller_decision") or "") == "PROMOTE"]
    requested_rollbacks = [r for r in decisions if str(r.get("controller_decision") or "") == "ROLLBACK"]
    applied_promotions = [r for r in journal if str(r.get("transition_type") or "") == "AUDIT_TO_MIRROR"]
    applied_rollbacks = [r for r in journal if str(r.get("transition_type") or "") == "MIRROR_TO_AUDIT"]

    promotion_apply_rate = (len(applied_promotions) / len(requested_promotions)) if requested_promotions else 1.0
    rollback_apply_rate = (len(applied_rollbacks) / len(requested_rollbacks)) if requested_rollbacks else 1.0

    rollback_decisions = [r for r in verif if str(r.get("decision") or "") == "ROLLBACK_TO_AUDIT"]
    rollback_mtt_rs: List[float] = []
    rollback_decisions_sorted = sorted(rollback_decisions, key=lambda r: parse_int(r.get("ts_ms"), 0))
    applied_rollbacks_sorted = sorted(applied_rollbacks, key=lambda r: parse_int(r.get("ts_ms"), 0))
    j = 0
    for dec in rollback_decisions_sorted:
        dec_ts = parse_int(dec.get("ts_ms"), 0)
        while j < len(applied_rollbacks_sorted) and parse_int(applied_rollbacks_sorted[j].get("ts_ms"), 0) < dec_ts:
            j += 1
        if j < len(applied_rollbacks_sorted):
            dt_ms = max(0, parse_int(applied_rollbacks_sorted[j].get("ts_ms"), 0) - dec_ts)
            rollback_mtt_rs.append(dt_ms / 1000.0)
            j += 1

    mttr_p50 = percentile(rollback_mtt_rs, 0.50)
    mttr_p95 = percentile(rollback_mtt_rs, 0.95)
    reason_codes: List[str] = []
    if promotion_apply_rate < 1.0:
        reason_codes.append("PROMOTION_APPLY_RATE_LOW")
    if rollback_apply_rate < 1.0:
        reason_codes.append("ROLLBACK_APPLY_RATE_LOW")
    if mttr_p95 > ROLLBACK_MTTR_SLO_SEC:
        reason_codes.append("ROLLBACK_MTTR_P95_HIGH")
        reason_codes.append("ROLLBACK_MTTR_SLO_BREACH")

    return {
        "window_min": WINDOW_MIN,
        "requested_promotions": len(requested_promotions),
        "applied_promotions": len(applied_promotions),
        "requested_rollbacks": len(requested_rollbacks),
        "applied_rollbacks": len(applied_rollbacks),
        "promotion_apply_rate": round(promotion_apply_rate, 6),
        "rollback_apply_rate": round(rollback_apply_rate, 6),
        "rollback_mttr_p50_sec": round(mttr_p50, 6),
        "rollback_mttr_p95_sec": round(mttr_p95, 6),
        "rollback_mttr_samples": len(rollback_mtt_rs),
        "reason_codes": reason_codes,
    }


async def persist_if_configured(db_url: str, rollup: Dict[str, Any]) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """

                INSERT INTO llm_route_incident_rca_mirror_slo_rollups (
                    ts_ms, window_min, requested_promotions, applied_promotions,
                    requested_rollbacks, applied_rollbacks, promotion_apply_rate,
                    rollback_apply_rate, rollback_mttr_p50_sec, rollback_mttr_p95_sec,
                    rollback_mttr_samples, reason_codes_json
                ) VALUES (
                    %(ts_ms)s, %(window_min)s, %(requested_promotions)s, %(applied_promotions)s,
                    %(requested_rollbacks)s, %(applied_rollbacks)s, %(promotion_apply_rate)s,
                    %(rollback_apply_rate)s, %(rollback_mttr_p50_sec)s, %(rollback_mttr_p95_sec)s,
                    %(rollback_mttr_samples)s, %(reason_codes_json)s
                )
                """,
                {
                    "ts_ms": now_ms(),
                    **rollup,
                    "reason_codes_json": json.dumps(rollup["reason_codes"]),
                }
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
        try:
            decisions = await xr_recent(r, DECISIONS_STREAM, LOOKBACK_COUNT)
            journal = await xr_recent(r, JOURNAL_STREAM, LOOKBACK_COUNT)
            verification = await xr_recent(r, VERIFICATION_STREAM, LOOKBACK_COUNT)
            rollup = compute_rollup(decisions, journal, verification)
            await persist_if_configured(db_url, rollup)
            await r.xadd(
                OUTPUT_STREAM,
                {
                    "schema_version": 1,
                    "payload_json": stable_json(rollup),
                    "reason_codes_json": stable_json(rollup["reason_codes"]),
                    "ts_ms": str(now_ms()),
                }, maxlen=MAXLEN,
                approximate=True,
            )
            await r.hset(
                LAST_HASH,
                mapping={
                    "promotion_apply_rate": str(rollup["promotion_apply_rate"]),
                    "rollback_apply_rate": str(rollup["rollback_apply_rate"]),
                    "rollback_mttr_p95_sec": str(rollup["rollback_mttr_p95_sec"]),
                    "rollback_mttr_samples": str(rollup["rollback_mttr_samples"]),
                    "reason_codes_json": stable_json(rollup["reason_codes"]),
                    "ts_ms": str(now_ms()),
                }
            )
            if PROMOTION_APPLY_RATE:
                PROMOTION_APPLY_RATE.set(rollup["promotion_apply_rate"])
            if ROLLBACK_APPLY_RATE:
                ROLLBACK_APPLY_RATE.set(rollup["rollback_apply_rate"])
            if ROLLBACK_MTTR_P95:
                ROLLBACK_MTTR_P95.set(rollup["rollback_mttr_p95_sec"])
            if LAST_RUN_TS:
                LAST_RUN_TS.set(time.time())
        except Exception as exc:
            status = "error"
            await r.xadd(
                AUDIT_STREAM,
                {"event_type": "ROUTE_INCIDENT_RCA_MIRROR_SLO_FAILED", "error": str(exc), "ts_ms": str(now_ms())},
                maxlen=MAXLEN,
                approximate=True,
            )
        finally:
            if RUNS:
                RUNS.labels(status=status).inc()
            if LAT:
                LAT.observe(max(time.perf_counter() - started, 0.0))
            await asyncio.sleep(max(RUN_EVERY_SEC, 5))


if __name__ == "__main__":  # pragma: no cover
    import asyncio
    asyncio.run(main())
