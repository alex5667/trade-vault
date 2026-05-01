from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import json
import os
import time
from typing import Any, Dict, List, Optional

try:  # pragma: no cover
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None

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


APP_NAME = "operator_routing_incident_rca_routing_controller_v2_12"
PORT = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_PORT", "9890"))
MODE = os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_MODE", "DRY_RUN").strip().upper()

DEFAULT_PROVIDER = os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_DEFAULT_PROVIDER", "vertex")
DEFAULT_MODEL = os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_DEFAULT_MODEL", "gemini-2.5-flash-lite")
DEFAULT_PROMPT_VERSION = os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_DEFAULT_PROMPT_VERSION", "routing_incident_rca_v1")
DEFAULT_POLICY_VERSION = os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_DEFAULT_POLICY_VERSION", "policy_v1")

REDIS_POLICY_PREFIX = os.getenv(
    "ML_OPERATOR_ROUTING_INCIDENT_RCA_GOVERNOR_REDIS_PREFIX",
    "cfg:ml:operator_routing_incident_rca_governor",
)

REQUESTS_STREAM = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_RCA_REQUESTS_STREAM",
    "stream:ml:operator_rca_routing_rca_requests",
)
ROUTED_STREAM = os.getenv(
    "ML_OPERATOR_RCA_ROUTING_RCA_REQUESTS_ROUTED_STREAM",
    "stream:ml:operator_rca_routing_rca_requests_routed",
)
DECISIONS_STREAM = os.getenv(
    "ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_DECISIONS_STREAM",
    "stream:ml:operator_routing_incident_rca_routing_decisions",
)
AUDIT_STREAM = os.getenv(
    "ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_AUDIT_STREAM",
    "stream:ml:operator_routing_incident_rca_routing_audit",
)
METRICS_KEY = os.getenv(
    "ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_METRICS_KEY",
    "metrics:ml:operator_routing_incident_rca_routing:last",
)

GROUP = "operator_routing_incident_rca_routing_controller_v2_12"
CONSUMER = f"{GROUP}_{os.getpid()}"
POLL_INTERVAL = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_POLL_INTERVAL", "5"))
MAX_BATCH = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_MAX_BATCH", "50"))
MAXLEN = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_MAXLEN", "10000"))


def _counter(name: str, doc: str, labels: tuple = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: tuple = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: tuple = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_operator_routing_incident_rca_routing_runs_total",
    "Routing incident RCA routing runs",
    ("status",),
)
LAT = _hist(
    "ml_operator_routing_incident_rca_routing_latency_seconds",
    "Routing incident RCA routing latency seconds",
)
LAST_RUN_TS = _gauge(
    "ml_operator_routing_incident_rca_routing_last_run_ts_seconds",
    "Routing incident RCA routing last run timestamp",
)
ROUTED = _counter(
    "ml_operator_routing_incident_rca_routing_requests_total",
    "Routing incident RCA routing processed requests",
    ("provider", "model"),
)


def now_ms() -> int:
    return get_ny_time_millis()


def as_dict(record: Dict[bytes, bytes]) -> Dict[str, str]:
    return {k.decode("utf-8"): v.decode("utf-8") for k, v in record.items()}


async def ensure_group(r: Any, stream: str, group: str) -> None:
    try:
        await r.xgroup_create(stream, group, mkstream=True)
    except Exception as e:
        if "BUSYGROUP" not in str(e):
            raise


class RoutingRepo:
    def __init__(self, db_url: str) -> None:
        self.db_url = db_url

    def persist_decision(self, decision: Dict[str, Any]) -> None:
        if psycopg is None:
            return
        with psycopg.connect(self.db_url) as conn:
            with conn.cursor() as cur:
                cur.execute(
                    """,
                    INSERT INTO llm_operator_routing_incident_rca_routing_decisions (
                        route_change_id,
                        task_type,
                        provider,
                        model_name,
                        prompt_version,
                        policy_version,
                        routing_reason,
                        mode,
                        ts_ms
                    ) VALUES (
                        %(route_change_id)s,
                        %(task_type)s,
                        %(provider)s,
                        %(model_name)s,
                        %(prompt_version)s,
                        %(policy_version)s,
                        %(routing_reason)s,
                        %(mode)s,
                        %(ts_ms)s
                    )
                    """,
                    decision,
                )
            conn.commit()


async def get_governor_action(r: Any, scope_type: str, scope_key: str) -> Optional[str]:
    if scope_type == "action":
        key = f"{REDIS_POLICY_PREFIX}:action:{scope_key}"
    else:
        key = f"{REDIS_POLICY_PREFIX}:provider:{scope_key}"
    action_bytes = await r.hget(key, "action")
    if not action_bytes:
        return None
    return action_bytes.decode("utf-8")


async def determine_route(r: Any, row: Dict[str, str]) -> Dict[str, Any]:
    task_type = "routing_incident_root_cause_analysis"
    
    # default route
    provider = DEFAULT_PROVIDER
    model = DEFAULT_MODEL
    prompt_version = DEFAULT_PROMPT_VERSION
    policy_version = DEFAULT_POLICY_VERSION
    reason = "default_static_route"

    # check action scope (task_type:prompt:policy)
    action_scope = f"{task_type}:{prompt_version}:{policy_version}"
    action_decision = await get_governor_action(r, "action", action_scope)
    
    # check provider scope (provider:model:prompt)
    provider_scope = f"{provider}:{model}:{prompt_version}"
    provider_decision = await get_governor_action(r, "provider", provider_scope)

    if action_decision == "SUPPRESS" or provider_decision == "SUPPRESS":
        reason = f"suppressed_by_governor_action={action_decision}_prov={provider_decision}"
        # fallback to minimal stable model or dummy
        model = "gemini-2.0-flash-lite-preview-02-05" # hypothetical stable
        
    decision = {
        "route_change_id": row.get("route_change_id", "unknown"),
        "task_type": task_type,
        "provider": provider,
        "model_name": model,
        "prompt_version": prompt_version,
        "policy_version": policy_version,
        "routing_reason": reason,
        "mode": MODE,
        "ts_ms": now_ms(),
    },
    return decision


async def routing_loop(r: Any, repo: RoutingRepo) -> None:
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
                    decision = await determine_route(r, row)
                    
                    out = dict(row)
                    out.update({
                        "routed_provider": decision["provider"],
                        "routed_model_name": decision["model_name"],
                        "routed_prompt_version": decision["prompt_version"],
                        "routed_policy_version": decision["policy_version"],
                        "routed_reason": decision["routing_reason"],
                        "routed_mode": decision["mode"],
                        "routed_ts_ms": str(decision["ts_ms"]),
                    })

                    # persist DB
                    repo.persist_decision(decision)

                    # Redis audit + decisions
                    await r.xadd(DECISIONS_STREAM, decision, maxlen=MAXLEN, approximate=True)
                    await r.xadd(AUDIT_STREAM, decision, maxlen=MAXLEN, approximate=True)
                    
                    # Emit to routed stream for next stage (bridge to vertex)
                    await r.xadd(ROUTED_STREAM, out, maxlen=MAXLEN, approximate=True)

                    # Update metrics hash
                    await r.hset(
                        METRICS_KEY,
                        mapping={
                            "last_route_change_id": decision["route_change_id"],
                            "last_provider": decision["provider"],
                            "last_model": decision["model_name"],
                            "last_mode": decision["mode"],
                            "last_ts_ms": str(decision["ts_ms"]),
                        },
                    )
                    
                    if ROUTED:
                        ROUTED.labels(provider=decision["provider"], model=decision["model_name"]).inc()

                    await r.xack(REQUESTS_STREAM, GROUP, msg_id)
                except Exception as e:
                    import traceback
                    traceback.print_exc()
                    status = "error"
                    await r.xack(REQUESTS_STREAM, GROUP, msg_id)

        if LAST_RUN_TS:
            LAST_RUN_TS.set(time.time())
    except Exception as e:
        import traceback
        traceback.print_exc()
        status = "error"
    finally:
        if RUNS:
            RUNS.labels(status=status).inc()
        if LAT:
            LAT.observe(max(time.perf_counter() - started, 0.0))


async def main() -> None:  # pragma: no cover
    start_http_server(PORT)
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    repo = RoutingRepo(db_url=os.getenv("DATABASE_URL", ""))
    while True:
        await routing_loop(r, repo)
        await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
