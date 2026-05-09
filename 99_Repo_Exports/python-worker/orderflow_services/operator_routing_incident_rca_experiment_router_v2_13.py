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
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
except Exception:  # pragma: no cover
    Counter = Gauge = Histogram = None
    def start_http_server(*args: Any, **kwargs: Any) -> None:
        return None


APP_NAME = "operator_routing_incident_rca_experiment_router_v2_13"
PORT = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_EXPERIMENT_ROUTER_PORT", "9891"))

ENABLE_EXPERIMENTS = os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_EXPERIMENT_ENABLE", "0") == "1"
EXPERIMENT_ID = os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_EXPERIMENT_ID", "routing_incident_rca_ab_v1")

REQUESTS_STREAM = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_RCA_REQUESTS_ROUTED_STREAM",
    "stream:ml:operator_rca_routing_rca_requests_routed",
)
OUT_STREAM = os.getenv(
    "ML_OPERATOR_ROUTING_INCIDENT_RCA_REQUESTS_EXPERIMENTED_STREAM",
    "stream:ml:operator_routing_incident_rca_requests_experimented",
)
EXPOSURE_STREAM = os.getenv(
    "ML_OPERATOR_ROUTING_INCIDENT_RCA_EXPOSURE_STREAM",
    "stream:ml:operator_routing_incident_rca_exposures",
)
AUDIT_STREAM = os.getenv(
    "ML_OPERATOR_ROUTING_INCIDENT_RCA_EXPERIMENT_AUDIT_STREAM",
    "stream:ml:operator_routing_incident_rca_experiment_audit",
)

GROUP = "operator_routing_incident_rca_experiment_router_v2_13"
CONSUMER = f"{GROUP}_{os.getpid()}"
POLL_INTERVAL = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_EXPERIMENT_POLL_INTERVAL", "5"))
MAX_BATCH = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_EXPERIMENT_MAX_BATCH", "50"))
MAXLEN = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_EXPERIMENT_MAXLEN", "10000"))


def _counter(name: str, doc: str, labels: tuple = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: tuple = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: tuple = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_operator_routing_incident_rca_experiment_router_runs_total",
    "Routing incident RCA experiment router runs",
    ("status",),
)
LAT = _hist(
    "ml_operator_routing_incident_rca_experiment_router_latency_seconds",
    "Routing incident RCA experiment router latency seconds",
)
LAST_RUN_TS = _gauge(
    "ml_operator_routing_incident_rca_experiment_router_last_run_ts_seconds",
    "Routing incident RCA experiment router last run ts",
)
EXPOSURES = _counter(
    "ml_operator_routing_incident_rca_experiment_exposures_total",
    "Routing incident RCA experiment exposures",
    ("experiment_id", "bucket"),
)


def now_ms() -> int:
    return get_ny_time_millis()


def as_dict(record: dict[bytes, bytes]) -> dict[str, str]:
    return {k.decode("utf-8"): v.decode("utf-8") for k, v in record.items()}


async def ensure_group(r: Any, stream: str, group: str) -> None:
    try:
        await r.xgroup_create(stream, group, mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            raise


def select_bucket(route_change_id: str) -> str:
    # extremely simple 50/50 modulo hash
    # in production use MurmurHash3
    h = hash(route_change_id)
    return "challenger" if h % 2 == 1 else "control"


async def experiment_loop(r: Any) -> None:
    started = time.perf_counter()
    status = "ok"
    try:
        await ensure_group(r, REQUESTS_STREAM, GROUP)
        messages = await r.xreadgroup(GROUP, CONSUMER, {REQUESTS_STREAM: ">"}, count=MAX_BATCH, block=10)
        if not messages:
            return

        for stream_name, records in messages:
            for msg_id, payload in records:
                try:
                    row = as_dict(payload)
                    route_change_id = row.get("route_change_id", "unknown")

                    bucket = "control"
                    overrides = {}
                    is_experimented = "0"

                    if ENABLE_EXPERIMENTS:
                        bucket = select_bucket(route_change_id)
                        is_experimented = "1"
                        if bucket == "challenger":
                            # mock override, ideally pulled from config
                            overrides["routed_model_name"] = "gemini-2.0-flash-lite-preview-02-05"
                            overrides["routed_policy_version"] = "policy_challenger_v1"

                    out = dict(row)
                    for k, v in overrides.items():
                        out[k] = v

                    out["experiment_id"] = EXPERIMENT_ID if ENABLE_EXPERIMENTS else "none"
                    out["experiment_bucket"] = bucket
                    out["is_experimented"] = is_experimented

                    exposure_event = {
                        "route_change_id": route_change_id,
                        "experiment_id": EXPERIMENT_ID if ENABLE_EXPERIMENTS else "none",
                        "bucket": bucket,
                        "base_provider": row.get("routed_provider", "unknown"),
                        "base_model": row.get("routed_model_name", "unknown"),
                        "ts_ms": now_ms(),
                    }

                    # track specific choices
                    for k, v in overrides.items():
                        exposure_event[f"override_{k}"] = v

                    if ENABLE_EXPERIMENTS:
                        await r.xadd(EXPOSURE_STREAM, exposure_event, maxlen=MAXLEN, approximate=True)
                        if EXPOSURES:
                            EXPOSURES.labels(experiment_id=EXPERIMENT_ID, bucket=bucket).inc()

                    await r.xadd(OUT_STREAM, out, maxlen=MAXLEN, approximate=True)
                    await r.xadd(AUDIT_STREAM, exposure_event, maxlen=MAXLEN, approximate=True)

                    await r.xack(REQUESTS_STREAM, GROUP, msg_id)
                except Exception:
                    status = "error"
                    await r.xack(REQUESTS_STREAM, GROUP, msg_id)

        if LAST_RUN_TS:
            LAST_RUN_TS.set(time.time())
    except Exception:
        status = "error"
    finally:
        if RUNS:
            RUNS.labels(status=status).inc()
        if LAT:
            LAT.observe(max(time.perf_counter() - started, 0.0))


async def main() -> None:  # pragma: no cover
    start_http_server(PORT)
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    while True:
        await experiment_loop(r)
        await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
