from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
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


APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_slo_analytics_v3_34"
DECISIONS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_CONTROLLER_DECISIONS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_controller_decisions",
)
JOURNAL_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_CONTROLLER_JOURNAL_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_controller_journal",
)
VERIFICATION_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERIFICATION_RESULTS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_verification_results",
)
ROLLBACK_JOURNAL_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_ROLLBACK_JOURNAL_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_rollback_journal",
)
OUTPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_SLO_ROLLUPS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_slo_rollups",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_SLO_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_slo:last",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_SLO_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_slo_audit",
)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_SLO_PORT", "9959"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_SLO_MAXLEN", "20000"))
RUN_EVERY_SEC = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_SLO_RUN_EVERY_SEC", "300"))
WINDOW_MIN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_SLO_WINDOW_MIN", "1440"))
LOOKBACK_COUNT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_SLO_LOOKBACK_COUNT", "1000"))
ROLLBACK_MTTR_SLO_SEC = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_ROLLBACK_MTTR_SLO_SEC", "120"))


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter("ml_route_incident_rca_mirror_rca_winner_apply_apply_slo_runs_total", "Winner-apply apply SLO runs", ("status",))
LAT = _hist("ml_route_incident_rca_mirror_rca_winner_apply_apply_slo_latency_seconds", "Winner-apply apply SLO latency seconds")
UP = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_apply_slo_up", "Winner-apply apply SLO up")
LAST_RUN_TS = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_apply_slo_last_run_ts_seconds", "Winner-apply apply SLO last run ts")
APPLY_RATE = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_apply_rate", "Winner-apply apply rate")
VERIFY_KEEP_RATE = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_apply_verify_keep_rate", "Winner-apply apply verify keep rate")
ROLLBACK_MTTR_P95 = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_apply_rollback_mttr_p95_seconds", "Winner-apply apply rollback MTTR p95")


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


def compute_rollup(decision_rows: List[Dict[str, Any]], journal_rows: List[Dict[str, Any]], verification_rows: List[Dict[str, Any]], rollback_rows: List[Dict[str, Any]]) -> Dict[str, Any]:
    cutoff = now_ms() - WINDOW_MIN * 60 * 1000
    decisions = [r for r in decision_rows if parse_int(r.get("ts_ms"), 0) >= cutoff]
    journal = [r for r in journal_rows if parse_int(r.get("ts_ms"), 0) >= cutoff]
    verification = [r for r in verification_rows if parse_int(r.get("ts_ms"), 0) >= cutoff]
    rollback = [r for r in rollback_rows if parse_int(r.get("ts_ms"), 0) >= cutoff]

    apply_requests = [r for r in decisions if str(r.get("decision") or "") in {"APPLY_PRIMARY_ARM_SHADOW", "APPLY_SINGLE_ARM"}]
    applied = [r for r in journal if str(r.get("decision") or "") in {"APPLY_PRIMARY_ARM_SHADOW", "APPLY_SINGLE_ARM"}]
    verified_keep = [r for r in verification if str(r.get("decision") or "") == "KEEP_APPLIED"]
    rollback_results = [r for r in verification if str(r.get("decision") or "") == "ROLLBACK_PREVIOUS_POLICY"]

    apply_rate = (len(applied) / len(apply_requests)) if apply_requests else 1.0
    verify_keep_rate = (len(verified_keep) / len(applied)) if applied else 1.0

    rollback_decisions_sorted = sorted(rollback_results, key=lambda r: parse_int(r.get("ts_ms"), 0))
    rollback_applied_sorted = sorted(rollback, key=lambda r: parse_int(r.get("ts_ms"), 0))
    mttrs: List[float] = []
    j = 0
    for dec in rollback_decisions_sorted:
        dec_ts = parse_int(dec.get("ts_ms"), 0)
        while j < len(rollback_applied_sorted) and parse_int(rollback_applied_sorted[j].get("ts_ms"), 0) < dec_ts:
            j += 1
        if j < len(rollback_applied_sorted):
            mttrs.append(max(0, parse_int(rollback_applied_sorted[j].get("ts_ms"), 0) - dec_ts) / 1000.0)
            j += 1

    mttr_p50 = percentile(mttrs, 0.50)
    mttr_p95 = percentile(mttrs, 0.95)

    reason_codes: List[str] = []
    if apply_rate < 1.0:
        reason_codes.append("APPLY_RATE_LOW")
    if verify_keep_rate < 1.0:
        reason_codes.append("VERIFY_KEEP_RATE_LOW")
    if mttr_p95 > ROLLBACK_MTTR_SLO_SEC:
        reason_codes.append("ROLLBACK_MTTR_P95_HIGH")
        reason_codes.append("ROLLBACK_MTTR_SLO_BREACH")

    return {
        "window_min": WINDOW_MIN,
        "apply_requests": len(apply_requests),
        "applied_n": len(applied),
        "verified_keep_n": len(verified_keep),
        "rollback_decisions_n": len(rollback_results),
        "rollback_applied_n": len(rollback),
        "apply_rate": round(apply_rate, 6),
        "verify_keep_rate": round(verify_keep_rate, 6),
        "rollback_mttr_p50_sec": round(mttr_p50, 6),
        "rollback_mttr_p95_sec": round(mttr_p95, 6),
        "rollback_mttr_samples": len(mttrs),
        "reason_codes": reason_codes,
    },


async def persist_if_configured(db_url: str, rollup: Dict[str, Any]) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """,
                INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_apply_slo_rollups (
                    ts_ms, window_min, apply_requests, applied_n, verified_keep_n,
                    rollback_decisions_n, rollback_applied_n, apply_rate, verify_keep_rate,
                    rollback_mttr_p50_sec, rollback_mttr_p95_sec, rollback_mttr_samples, reason_codes_json
                ) VALUES (
                    %(ts_ms)s, %(window_min)s, %(apply_requests)s, %(applied_n)s, %(verified_keep_n)s,
                    %(rollback_decisions_n)s, %(rollback_applied_n)s, %(apply_rate)s, %(verify_keep_rate)s,
                    %(rollback_mttr_p50_sec)s, %(rollback_mttr_p95_sec)s, %(rollback_mttr_samples)s, %(reason_codes_json)s
                )
                """,
                {
                    "ts_ms": now_ms(),
                    **rollup,
                    "reason_codes_json": json.dumps(rollup["reason_codes"]),
                },
            )
            conn.commit()


async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PORT)
    if UP:
        UP.set(1)
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    db_url = os.getenv("DATABASE_URL", "")

    while True:
        started = time.perf_counter()
        status = "ok"
        try:
            decisions = await xr_recent(r, DECISIONS_STREAM, LOOKBACK_COUNT)
            journal = await xr_recent(r, JOURNAL_STREAM, LOOKBACK_COUNT)
            verification = await xr_recent(r, VERIFICATION_STREAM, LOOKBACK_COUNT)
            rollback = await xr_recent(r, ROLLBACK_JOURNAL_STREAM, LOOKBACK_COUNT)
            rollup = compute_rollup(decisions, journal, verification, rollback)
            await persist_if_configured(db_url, rollup)
            await r.xadd(
                OUTPUT_STREAM,
                {
                    "schema_version": 1,
                    "payload_json": stable_json(rollup),
                    "reason_codes_json": stable_json(rollup["reason_codes"]),
                    "ts_ms": str(now_ms()),
                },
                maxlen=MAXLEN,
                approximate=True,
            )
            await r.hset(
                LAST_HASH,
                mapping={
                    "apply_rate": str(rollup["apply_rate"]),
                    "verify_keep_rate": str(rollup["verify_keep_rate"]),
                    "rollback_mttr_p95_sec": str(rollup["rollback_mttr_p95_sec"]),
                    "rollback_mttr_samples": str(rollup["rollback_mttr_samples"]),
                    "reason_codes_json": stable_json(rollup["reason_codes"]),
                    "ts_ms": str(now_ms()),
                },
            )
            if APPLY_RATE:
                APPLY_RATE.set(rollup["apply_rate"])
            if VERIFY_KEEP_RATE:
                VERIFY_KEEP_RATE.set(rollup["verify_keep_rate"])
            if ROLLBACK_MTTR_P95:
                ROLLBACK_MTTR_P95.set(rollup["rollback_mttr_p95_sec"])
            if LAST_RUN_TS:
                LAST_RUN_TS.set(time.time())
        except Exception as exc:
            status = "error"
            await r.xadd(
                AUDIT_STREAM,
                {"event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_SLO_FAILED", "error": str(exc), "ts_ms": str(now_ms())},
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
    asyncio.run(main())
