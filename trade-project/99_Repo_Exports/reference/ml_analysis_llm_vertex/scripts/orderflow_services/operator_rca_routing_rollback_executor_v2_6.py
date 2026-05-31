from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict

try:
    import redis.asyncio as redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

from prometheus_client import Counter, Gauge, Histogram, start_http_server


STREAM_ROLLBACK_REQUESTS = os.getenv("ML_OPERATOR_RCA_ROUTING_ROLLBACK_REQUESTS_STREAM", "stream:ml:operator_rca_routing_rollback_requests")
STREAM_ROLLBACK_RESULTS = os.getenv("ML_OPERATOR_RCA_ROUTING_ROLLBACK_RESULTS_STREAM", "stream:ml:operator_rca_routing_rollback_results")
STREAM_ROLLBACK_JOURNAL = os.getenv("ML_OPERATOR_RCA_ROUTING_ROLLBACK_JOURNAL_STREAM", "stream:ml:operator_rca_routing_rollback_journal")
STREAM_AUDIT = os.getenv("ML_OPERATOR_RCA_ROUTING_AUDIT_STREAM", "stream:ml:operator_rca_routing_apply_audit")
HASH_ROUTE_DEFAULT = os.getenv("ML_OPERATOR_RCA_DEFAULT_ROUTE_KEY", "cfg:ml:operator_rca_routing:default")
GROUP = os.getenv("ML_OPERATOR_RCA_ROUTING_ROLLBACK_GROUP", "cg:ml_operator_rca_routing_rollback_v2_6")
CONSUMER = os.getenv("ML_OPERATOR_RCA_ROUTING_ROLLBACK_CONSUMER", os.getenv("HOSTNAME", "ml-operator-rca-routing-rollback-v2-6"))
PROM_PORT = int(os.getenv("ML_OPERATOR_RCA_ROUTING_ROLLBACK_METRICS_PORT", "9879"))
MODE = os.getenv("ML_OPERATOR_RCA_ROUTING_ROLLBACK_MODE", "DRY_RUN").upper()


ROLLBACK_TOTAL = Counter(
    "ml_operator_rca_routing_rollback_total",
    "Total routing rollback executions",
    ["status"],
)
LAST_RUN_TS = Gauge(
    "ml_operator_rca_routing_rollback_last_run_ts_seconds",
    "Last routing rollback execution ts",
)
LOOP_SECONDS = Histogram(
    "ml_operator_rca_routing_rollback_loop_seconds",
    "Loop duration for routing rollback executor",
)


def _decode(fields: Dict[Any, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in fields.items():
        kk = k.decode() if isinstance(k, (bytes, bytearray)) else str(k)
        vv = v.decode() if isinstance(v, (bytes, bytearray)) else str(v)
        out[kk] = vv
    return out


@dataclass
class RollbackRequest:
    recommendation_id: str
    ts_ms: int
    rollback_type: str
    baseline_route_json: str
    applied_route_json: str
    reason_codes_json: str


def _parse(fields: Dict[Any, Any]) -> RollbackRequest:
    d = _decode(fields)
    return RollbackRequest(
        recommendation_id=d.get("recommendation_id", ""),
        ts_ms=int(float(d.get("ts_ms", "0") or 0)),
        rollback_type=d.get("rollback_type", "ROUTE_DEFAULT_ROLLBACK"),
        baseline_route_json=d.get("baseline_route_json", "{}"),
        applied_route_json=d.get("applied_route_json", "{}"),
        reason_codes_json=d.get("reason_codes_json", "[]"),
    )


async def main() -> None:
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PROM_PORT)
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"), decode_responses=False)
    try:
        await r.xgroup_create(STREAM_ROLLBACK_REQUESTS, GROUP, id="0", mkstream=True)
    except Exception:
        pass

    while True:
        t0 = time.perf_counter()
        rows = await r.xreadgroup(GROUP, CONSUMER, {STREAM_ROLLBACK_REQUESTS: ">"}, count=50, block=5000)
        for _stream, items in rows:
            for msg_id, fields in items:
                req = _parse(fields)
                result_status = "DRY_RUN_READY"
                before_route = await r.hgetall(HASH_ROUTE_DEFAULT)
                before_decoded = {k.decode() if isinstance(k, (bytes, bytearray)) else str(k): v.decode() if isinstance(v, (bytes, bytearray)) else str(v) for k, v in before_route.items()}
                baseline = json.loads(req.baseline_route_json or "{}")
                if MODE == "COMMIT":
                    mapping = {str(k): str(v) for k, v in baseline.items()}
                    if mapping:
                        await r.delete(HASH_ROUTE_DEFAULT)
                        await r.hset(HASH_ROUTE_DEFAULT, mapping=mapping)
                    result_status = "ROLLED_BACK"
                journal_payload = {
                    "schema_version": 1,
                    "recommendation_id": req.recommendation_id,
                    "ts_ms": get_ny_time_millis(),
                    "mode": MODE,
                    "status": result_status,
                    "before_route_json": json.dumps(before_decoded, ensure_ascii=False),
                    "baseline_route_json": req.baseline_route_json,
                    "applied_route_json": req.applied_route_json,
                    "reason_codes_json": req.reason_codes_json,
                }
                await r.xadd(STREAM_ROLLBACK_JOURNAL, journal_payload, maxlen=100000, approximate=True)
                await r.xadd(STREAM_ROLLBACK_RESULTS, journal_payload, maxlen=100000, approximate=True)
                await r.xadd(STREAM_AUDIT, {
                    "event": "ROUTING_ROLLBACK_EXECUTED",
                    "recommendation_id": req.recommendation_id,
                    "status": result_status,
                    "mode": MODE,
                    "ts_ms": get_ny_time_millis(),
                }, maxlen=100000, approximate=True)
                ROLLBACK_TOTAL.labels(status=result_status).inc()
                LAST_RUN_TS.set(time.time())
                await r.xack(STREAM_ROLLBACK_REQUESTS, GROUP, msg_id)
        LOOP_SECONDS.observe(time.perf_counter() - t0)


if __name__ == "__main__":
    import asyncio

    asyncio.run(main())
