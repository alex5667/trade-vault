from __future__ import annotations

import asyncio
import json
import math
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


APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_slo_rollup_v3_51"
INPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_VERIFICATION_RESULTS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_verification_results",
)
ROLLBACK_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_ROLLBACK_JOURNAL_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_rollback_journal",
)
RETRY_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_RETRY_RESULTS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_retry_results",
)
ESCALATIONS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_ESCALATIONS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_escalations",
)
OUTPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_SLO_ROLLUPS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_slo_rollups",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_SLO_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_slo_audit",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_SLO_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_slo:last",
)
GROUP = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_SLO_GROUP",
    APP_NAME,
)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_SLO_PORT", "9984"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_SLO_MAXLEN", "20000"))
LOOKBACK_COUNT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_SLO_LOOKBACK_COUNT", "1000"))
WINDOW_MIN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_APPLY_FLOW_EXPERIMENT_SLO_WINDOW_MIN", "10080"))


def _counter(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_slo_runs_total",
    "Apply-flow experiment SLO rollup runs",
    ("status",),
)
LAT = _hist(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_slo_latency_seconds",
    "Apply-flow experiment SLO rollup latency seconds",
)
UP = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_slo_up",
    "Apply-flow experiment SLO rollup up",
)
LAST_RUN_TS = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_slo_last_run_ts_seconds",
    "Apply-flow experiment SLO rollup last run timestamp",
)
VERIFY_KEEP_RATE = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_verify_keep_rate",
    "Apply-flow experiment verify keep rate",
)
ROLLBACK_PLAN_RATE = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_rollback_plan_rate",
    "Apply-flow experiment rollback plan rate",
)
ROLLBACK_APPLIED_RATE = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_rollback_applied_rate",
    "Apply-flow experiment rollback applied rate",
)
ROLLBACK_MTTR_P95 = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_rollback_mttr_p95_sec",
    "Apply-flow experiment rollback mttr p95 sec",
)
ESCALATIONS_RATE = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_escalation_rate",
    "Apply-flow experiment escalation rate",
)


def now_ms() -> int:
    return get_ny_time_millis()


def parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def parse_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def stable_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


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


def percentile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    vals = sorted(values)
    idx = max(0, min(len(vals) - 1, math.ceil(q * len(vals)) - 1))
    return round(vals[idx], 6)


def recent(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    cutoff = now_ms() - WINDOW_MIN * 60 * 1000
    return [r for r in rows if parse_int(r.get("ts_ms"), 0) >= cutoff]


async def ensure_group(client: Any, stream_key: str, group: str) -> None:
    try:
        await client.xgroup_create(stream_key, group, id="$", mkstream=True)
    except Exception:
        return


async def xr_recent(r: Any, stream_key: str, count: int) -> list[dict[str, Any]]:
    try:
        rows = await r.xrevrange(stream_key, count=count)
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for entry_id, payload in rows:
        row = as_dict(payload)
        row["_stream_id"] = entry_id.decode() if isinstance(entry_id, (bytes, bytearray)) else str(entry_id)
        out.append(row)
    return out


def build_rollup(
    verification_rows: list[dict[str, Any]],
    rollback_rows: list[dict[str, Any]],
    retry_rows: list[dict[str, Any]],
    escalation_rows: list[dict[str, Any]],
) -> dict[str, Any]:
    vr = recent(verification_rows)
    rr = recent(rollback_rows)
    tr = recent(retry_rows)
    er = recent(escalation_rows)

    verification_n = len(vr)
    verified_n = sum(1 for r in vr if (r.get("decision") or "") == "VERIFIED")
    rollback_planned_n = sum(1 for r in vr if (r.get("decision") or "") == "ROLLBACK_PREVIOUS_PROFILE")
    rollback_applied_n = sum(1 for r in rr if parse_int(r.get("applied"), 0) == 1)
    escalation_n = len(er)
    retry_n = len(tr)

    verify_keep_rate = round((verified_n / verification_n), 6) if verification_n else 0.0
    rollback_plan_rate = round((rollback_planned_n / verification_n), 6) if verification_n else 0.0
    rollback_applied_rate = round((rollback_applied_n / verification_n), 6) if verification_n else 0.0
    escalation_rate = round((escalation_n / verification_n), 6) if verification_n else 0.0

    mttr_vals: list[float] = []
    by_source_verification_ts = {
        parse_int(r.get("source_verification_ts_ms"), 0): r for r in rr if parse_int(r.get("applied"), 0) == 1
    }
    for v in vr:
        v_ts = parse_int(v.get("ts_ms"), 0)
        r = by_source_verification_ts.get(v_ts)
        if r:
            mttr_vals.append(max(parse_int(r.get("ts_ms"), 0) - v_ts, 0) / 1000.0)

    return {
        "verification_n": verification_n,
        "verified_n": verified_n,
        "rollback_planned_n": rollback_planned_n,
        "rollback_applied_n": rollback_applied_n,
        "retry_n": retry_n,
        "escalation_n": escalation_n,
        "verify_keep_rate": verify_keep_rate,
        "rollback_plan_rate": rollback_plan_rate,
        "rollback_applied_rate": rollback_applied_rate,
        "rollback_mttr_p95_sec": percentile(mttr_vals, 0.95),
        "escalation_rate": escalation_rate,
    }


async def persist_if_configured(db_url: str, rollup: dict[str, Any]) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """

                INSERT INTO ml_route_rca_experiment_slo_rollups_v51 (
                    ts_ms, window_min, verification_n, verified_n, rollback_planned_n, rollback_applied_n,
                    retry_n, escalation_n, verify_keep_rate, rollback_plan_rate, rollback_applied_rate,
                    rollback_mttr_p95_sec, escalation_rate, rollup_json
                ) VALUES (
                    %(ts_ms)s, %(window_min)s, %(verification_n)s, %(verified_n)s, %(rollback_planned_n)s, %(rollback_applied_n)s,
                    %(retry_n)s, %(escalation_n)s, %(verify_keep_rate)s, %(rollback_plan_rate)s, %(rollback_applied_rate)s,
                    %(rollback_mttr_p95_sec)s, %(escalation_rate)s, %(rollup_json)s
                )
                """,
                {
                    "ts_ms": now_ms(),
                    "window_min": WINDOW_MIN,
                    **rollup,
                    "rollup_json": json.dumps(rollup),
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
    await ensure_group(r, INPUT_STREAM, GROUP)
    db_url = os.getenv("DATABASE_URL", "")

    while True:
        rows = await r.xreadgroup(GROUP, CONSUMER, {INPUT_STREAM: ">"}, count=32, block=5000)
        if not rows:
            continue
        for _stream, messages in rows:
            for msg_id, _payload in messages:
                started = time.perf_counter()
                status = "ok"
                try:
                    verification_rows = await xr_recent(r, INPUT_STREAM, LOOKBACK_COUNT)
                    rollback_rows = await xr_recent(r, ROLLBACK_STREAM, LOOKBACK_COUNT)
                    retry_rows = await xr_recent(r, RETRY_STREAM, LOOKBACK_COUNT)
                    escalation_rows = await xr_recent(r, ESCALATIONS_STREAM, LOOKBACK_COUNT)
                    rollup = build_rollup(verification_rows, rollback_rows, retry_rows, escalation_rows)
                    await persist_if_configured(db_url, rollup)
                    await r.xadd(
                        OUTPUT_STREAM,
                        {"schema_version": 1, "rollup_json": stable_json(rollup), "ts_ms": str(now_ms())},
                        maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.hset(
                        LAST_HASH,
                        mapping={**{k: str(v) for k, v in rollup.items()}, "ts_ms": str(now_ms())},
                    )
                    await r.xadd(
                        AUDIT_STREAM,
                        {"event_type": "APPLY_FLOW_EXPERIMENT_SLO_ROLLUP", "rollup_json": stable_json(rollup), "ts_ms": str(now_ms())},
                        maxlen=MAXLEN,
                        approximate=True,
                    )
                    if VERIFY_KEEP_RATE:
                        VERIFY_KEEP_RATE.set(rollup["verify_keep_rate"])
                    if ROLLBACK_PLAN_RATE:
                        ROLLBACK_PLAN_RATE.set(rollup["rollback_plan_rate"])
                    if ROLLBACK_APPLIED_RATE:
                        ROLLBACK_APPLIED_RATE.set(rollup["rollback_applied_rate"])
                    if ROLLBACK_MTTR_P95:
                        ROLLBACK_MTTR_P95.set(rollup["rollback_mttr_p95_sec"])
                    if ESCALATIONS_RATE:
                        ESCALATIONS_RATE.set(rollup["escalation_rate"])
                    await r.xack(INPUT_STREAM, GROUP, msg_id)
                    if LAST_RUN_TS:
                        LAST_RUN_TS.set(time.time())
                except Exception as exc:
                    status = "error"
                    await r.xadd(
                        AUDIT_STREAM,
                        {"event_type": "APPLY_FLOW_EXPERIMENT_SLO_FAILED", "error": str(exc), "ts_ms": str(now_ms())},
                        maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.xack(INPUT_STREAM, GROUP, msg_id)
                finally:
                    if RUNS:
                        RUNS.labels(status=status).inc()
                    if LAT:
                        LAT.observe(max(time.perf_counter() - started, 0.0))


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
