from __future__ import annotations

import asyncio
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

APP_NAME = "route_incident_rca_mirror_rca_winner_apply_slo_analytics_v3_18"
DECISIONS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_DECISIONS_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_decisions")
JOURNAL_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_JOURNAL_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_journal")
VERIFICATION_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_RESULTS_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_verification_results")
ROLLBACK_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_ROLLBACK_JOURNAL_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_rollback_journal")

ROLLUPS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_SLO_ROLLUPS_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_slo_rollups")

PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_SLO_PORT", "9937"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_SLO_MAXLEN", "1000"))
POLL_INTERVAL_SEC = 30.0

def _counter(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None

def _gauge(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None

def _hist(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_route_incident_rca_mirror_rca_winner_apply_slo_runs_total", "Runs", ("status",))
LAT = _hist("ml_route_incident_rca_mirror_rca_winner_apply_slo_latency_seconds", "Latency")
UP = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_slo_up", "Up")
LAST_RUN = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_slo_last_run_ts_seconds", "Last run")

APPLY_RATE = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_apply_rate", "Apply Rate")
VERIFY_KEEP_RATE = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_verify_keep_rate", "Verify Keep Rate")
ROLLBACK_MTTR_P50 = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_rollback_mttr_p50_seconds", "Rollback MTTR P50 sec")
ROLLBACK_MTTR_P95 = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_rollback_mttr_p95_seconds", "Rollback MTTR P95 sec")

def now_ms() -> int:
    return get_ny_time_millis()

def decode_dict(d: dict[Any, Any]) -> dict[str, Any]:
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in d.items()
    }

async def load_stream_recent(r: Any, stream: str, count: int = 100) -> list[dict[str, Any]]:
    # Simple recent load for stats
    res = await r.xrevrange(stream, max="+", min="-", count=count)
    if not res:
        return []
    out = []
    for msg_id, fields in res:
        out.append(decode_dict(fields))
    return out

def calculate_p_metric(values: list[float], percentile: float) -> float:
    if not values:
        return 0.0
    sorted_v = sorted(values)
    k = (len(sorted_v) - 1) * percentile
    f = int(k)
    c = int(k) + (1 if k % 1 > 0 else 0)
    if f == c:
        return sorted_v[int(k)]
    d0 = sorted_v[f] * (c - k)
    d1 = sorted_v[c] * (k - f)
    return d0 + d1

async def calculate_mttr(applies: list[dict[str, Any]], rollbacks: list[dict[str, Any]]) -> tuple[float, float]:
    times = []
    # simplistic calculation: match closest rollback after an apply
    for a in applies:
        try:
            ts_a = int(a.get("ts_ms", 0))
        except Exception:
            continue
        valid_r = []
        for rb in rollbacks:
            try:
                ts_r = int(rb.get("ts_ms", 0))
            except Exception:
                continue
            if ts_r > ts_a:
                valid_r.append(ts_r)

        if valid_r:
            closest_r = min(valid_r)
            times.append((closest_r - ts_a) / 1000.0)

    if not times:
        return 0.0, 0.0

    return calculate_p_metric(times, 0.5), calculate_p_metric(times, 0.95)

async def check_rates(decisions: list[dict[str, Any]], validations: list[dict[str, Any]]) -> tuple[float, float]:
    apply_total = len(decisions)
    apply_count = sum(1 for x in decisions if x.get("decision") == "APPLY")
    apply_rate = apply_count / apply_total if apply_total > 0 else 0.0

    verify_total = len(validations)
    verify_keep = sum(1 for x in validations if x.get("decision") == "KEEP_APPLIED")
    verify_keep_rate = verify_keep / verify_total if verify_total > 0 else 0.0

    return apply_rate, verify_keep_rate

async def persist_rollup(db_url: str, apply_rate: float, verify_keep_rate: float, mttr_p50: float, mttr_p95: float) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """

                INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_slo_rollups (
                    apply_rate, verify_keep_rate, rollback_mttr_p50_sec, rollback_mttr_p95_sec, ts_ms
                ) VALUES (
                    %(apply_rate)s, %(verify_keep_rate)s, %(rollback_mttr_p50_sec)s, %(rollback_mttr_p95_sec)s, %(ts_ms)s
                )
                """,
                {
                    "apply_rate": apply_rate,
                    "verify_keep_rate": verify_keep_rate,
                    "rollback_mttr_p50_sec": mttr_p50,
                    "rollback_mttr_p95_sec": mttr_p95,
                    "ts_ms": now_ms(),
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
            decisions = await load_stream_recent(r, DECISIONS_STREAM)
            validations = await load_stream_recent(r, VERIFICATION_STREAM)
            applies = await load_stream_recent(r, JOURNAL_STREAM)
            rollbacks = await load_stream_recent(r, ROLLBACK_STREAM)

            apply_rate, verify_keep_rate = await check_rates(decisions, validations)
            mttr_p50, mttr_p95 = await calculate_mttr(applies, rollbacks)

            if APPLY_RATE:
                APPLY_RATE.set(apply_rate)
            if VERIFY_KEEP_RATE:
                VERIFY_KEEP_RATE.set(verify_keep_rate)
            if ROLLBACK_MTTR_P50:
                ROLLBACK_MTTR_P50.set(mttr_p50)
            if ROLLBACK_MTTR_P95:
                ROLLBACK_MTTR_P95.set(mttr_p95)

            await persist_rollup(db_url, apply_rate, verify_keep_rate, mttr_p50, mttr_p95)

            await r.xadd(ROLLUPS_STREAM, {
                "apply_rate": str(apply_rate),
                "verify_keep_rate": str(verify_keep_rate),
                "rollback_mttr_p50_sec": str(mttr_p50),
                "rollback_mttr_p95_sec": str(mttr_p95),
                "ts_ms": str(now_ms())
            }, maxlen=MAXLEN, approximate=True)

            if LAST_RUN:
                LAST_RUN.set(time.time())

        except Exception:
            status = "error"
        finally:
            if RUNS:
                RUNS.labels(status=status).inc()
            if LAT:
                LAT.observe(max(time.perf_counter() - started, 0.0))

            await asyncio.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
