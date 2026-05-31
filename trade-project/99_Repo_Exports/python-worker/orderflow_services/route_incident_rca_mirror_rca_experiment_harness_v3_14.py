from __future__ import annotations

import asyncio
import hashlib
import json
import os
import time
from typing import Any

from utils.time_utils import get_ny_time_millis
import contextlib

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

APP_NAME = "route_incident_rca_mirror_rca_experiment_harness_v3_14"
BUNDLES_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_BUNDLES_STREAM", "stream:ml:route_incident_rca_mirror_incident_bundles")
EXPOSURES_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EXPERIMENT_EXPOSURES_STREAM", "stream:ml:route_incident_rca_mirror_rca_experiment_exposures")
DECISIONS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EXPERIMENT_DECISIONS_STREAM", "stream:ml:route_incident_rca_mirror_rca_experiment_decisions")
AUDIT_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EXPERIMENT_AUDIT_STREAM", "stream:ml:route_incident_rca_mirror_rca_experiment_audit")
LAST_HASH = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EXPERIMENT_LAST_HASH", "metrics:ml:route_incident_rca_mirror_rca_experiment:last")

PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EXPERIMENT_PORT", "9933"))
MODE = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EXPERIMENT_MODE", "SHADOW") # DISABLED, SHADOW, SINGLE_ARM, MULTI_ARM
PRIMARY_ARM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EXPERIMENT_PRIMARY_ARM", "deterministic")
SHADOW_ARMS_JSON = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EXPERIMENT_SHADOW_ARMS_JSON", '["vertex_candidate","local_fallback_candidate"]')
ARM_WEIGHTS_JSON = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EXPERIMENT_ARM_WEIGHTS_JSON", '{"deterministic":70,"vertex_candidate":20,"local_fallback_candidate":10}')
ALLOW_SEVERITIES = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EXPERIMENT_ALLOW_SEVERITIES", "warning,critical").split(",")
HASH_SALT = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EXPERIMENT_HASH_SALT", "rca_v1")
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EXPERIMENT_MAXLEN", "1000"))
POLL_INTERVAL_SEC = 2.0

def _counter(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None

def _gauge(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None

def _hist(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_route_incident_rca_mirror_rca_experiment_runs_total", "Runs", ("status", "decision"))
EXPOSURES = _counter("ml_route_incident_rca_mirror_rca_exposures_total", "Exposures", ("arm", "severity", "mode"))
LAT = _hist("ml_route_incident_rca_mirror_rca_experiment_latency_seconds", "Latency")
UP = _gauge("ml_route_incident_rca_mirror_rca_experiment_up", "Up")
LAST_RUN = _gauge("ml_route_incident_rca_mirror_rca_experiment_last_run_ts_seconds", "Last run")

def now_ms() -> int:
    return get_ny_time_millis()

def decode_dict(d: dict[Any, Any]) -> dict[str, Any]:
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in d.items()
    }

def deterministic_assignment(bundle_id: str, salt: str, weights: dict[str, float]) -> str:
    hash_input = f"{bundle_id}:{salt}".encode()
    h = int(hashlib.md5(hash_input).hexdigest()[:8], 16)

    total_weight = sum(weights.values())
    if total_weight <= 0:
        return "deterministic"

    point = h % int(total_weight)
    cum = 0.0
    for arm, w in weights.items():
        cum += w
        if point < cum:
            return arm
    return list(weights.keys())[0]

def decide_arms(bundle_id: str, mode: str, primary_arm: str, shadow_arms: list[str], weights: dict[str, float]) -> list[str]:
    if mode == "DISABLED":
        return []
    if mode == "SINGLE_ARM":
        return [primary_arm]
    if mode == "SHADOW":
        return [primary_arm] + shadow_arms
    if mode == "MULTI_ARM":
        winner = deterministic_assignment(bundle_id, HASH_SALT, weights)
        return [winner]
    return []

async def route_to_arm(r: Any, arm: str, bundle_id: str, bundle_json: str) -> None:
    payload = {
        "task_type": "rca_experiment_exposure",
        "experiment_arm": arm,
        "bundle_json": bundle_json,
        "bundle_id": bundle_id,
        "source_app": APP_NAME
    }

    stream_name = None
    if arm == "deterministic":
        stream_name = "stream:ml:route_incident_rca_mirror_rca_requests"
        payload["task_family"] = "route_incident_rca_mirror_rca"
    elif arm == "vertex_candidate":
        stream_name = "stream:ml:route_incident_rca_mirror_vertex_rca_requests"
        payload["task_family"] = "route_incident_rca_mirror_rca"
    elif arm == "local_fallback_candidate":
        stream_name = "stream:ml:local_fallback_requests"
        payload["task_type"] = "vertex_unavailable_fallback"

    if stream_name:
        await r.xadd(stream_name, payload, maxlen=MAXLEN, approximate=True)

async def persist_exposure(db_url: str, bundle_id: str, arm: str, mode: str, severity: str) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """

                INSERT INTO llm_route_incident_rca_mirror_rca_experiment_exposures (
                    bundle_id, arm, mode, severity, ts_ms
                ) VALUES (
                    %(bundle_id)s, %(arm)s, %(mode)s, %(severity)s, %(ts_ms)s
                )
                """,
                {
                    "bundle_id": bundle_id,
                    "arm": arm,
                    "mode": mode,
                    "severity": severity,
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

    shadow_arms = []
    with contextlib.suppress(Exception):
        shadow_arms = json.loads(SHADOW_ARMS_JSON)

    arm_weights = {}
    with contextlib.suppress(Exception):
        arm_weights = json.loads(ARM_WEIGHTS_JSON)

    last_bundle_id = "$"

    while True:
        started = time.perf_counter()
        status = "ok"
        decision = "none"

        try:
            streams = {BUNDLES_STREAM: last_bundle_id}
            results = await r.xread(streams, count=5, block=int(POLL_INTERVAL_SEC*1000))
            if results:
                for stream_name, events in results:
                    for msg_id, fields in events:
                        m_id = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
                        decoded = decode_dict(fields)

                        bundle_id = decoded.get("bundle_id")
                        severity = decoded.get("severity", "info")
                        bundle_json = decoded.get("bundle_json")

                        if bundle_id and bundle_json and severity in ALLOW_SEVERITIES:
                            arms = decide_arms(bundle_id, MODE, PRIMARY_ARM, shadow_arms, arm_weights)

                            for arm in arms:
                                await route_to_arm(r, arm, bundle_id, bundle_json)
                                await persist_exposure(db_url, bundle_id, arm, MODE, severity)

                                await r.xadd(EXPOSURES_STREAM, {"bundle_id": bundle_id, "arm": arm, "ts_ms": str(now_ms())}, maxlen=MAXLEN, approximate=True)
                                if EXPOSURES:
                                    EXPOSURES.labels(arm=arm, severity=severity, mode=MODE).inc()

                            await r.xadd(DECISIONS_STREAM, {"bundle_id": bundle_id, "arms": json.dumps(arms), "ts_ms": str(now_ms())}, maxlen=MAXLEN, approximate=True)
                            await r.hset(LAST_HASH, mapping={"bundle_id": bundle_id, "mode": MODE, "arms_count": str(len(arms)), "ts_ms": str(now_ms())})
                            decision = "routed"
                        else:
                            decision = "ignored"

                        last_bundle_id = m_id

            if LAST_RUN:
                LAST_RUN.set(time.time())

        except Exception as exc:
            status = "error"
            await r.xadd(AUDIT_STREAM, {"event_type": "EXPERIMENT_ROUTING_FAILED", "error": str(exc), "ts_ms": str(now_ms())}, maxlen=MAXLEN, approximate=True)
            await asyncio.sleep(2)
        finally:
            if RUNS:
                RUNS.labels(status=status, decision=decision).inc()
            if LAT:
                LAT.observe(max(time.perf_counter() - started, 0.0))

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
