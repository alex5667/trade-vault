from __future__ import annotations

import asyncio
import os
import time
from typing import Any

from core.redis_keys import RedisKeyPrefixes as RK
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


APP_NAME = "operator_routing_incident_rca_winner_routing_apply_controller_v2_14"
PORT = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_APPLY_PORT", "9893"))

ADVISORY_ONLY = os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_APPLY_ADVISORY_ONLY", "1") == "1"
EXECUTOR_MODE = os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_APPLY_EXECUTOR_MODE", "DRY_RUN")
MIN_SAMPLE = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_APPLY_MIN_SAMPLE", "8"))
MIN_UPLIFT = float(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_APPLY_MIN_UPLIFT", "0.05"))
COOLDOWN_SEC = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_APPLY_COOLDOWN_SEC", "21600"))

KILL_SWITCH_KEY = os.getenv("GLOBAL_EXEC_KILL_SWITCH", RK.EXEC_KILL_SWITCH)
ALLOWLIST_MODELS = os.getenv(
    "ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_APPLY_ALLOWLIST",
    "gemini-2.0-flash-lite-preview-02-05,gemini-2.5-flash-lite,gemini-2.5-pro",
).split(",")

IN_STREAM = os.getenv(
    "ML_OPERATOR_ROUTING_INCIDENT_RCA_EXPERIMENT_WINNER_DECISIONS_STREAM",
    "stream:ml:operator_routing_incident_rca_experiment_winner_decisions",
)
OUT_STREAM = os.getenv(
    "ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_APPLY_RESULTS_STREAM",
    "stream:ml:operator_routing_incident_rca_routing_apply_results",
)
AUDIT_STREAM = os.getenv(
    "ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_APPLY_AUDIT_STREAM",
    "stream:ml:operator_routing_incident_rca_routing_apply_audit",
)
GLOBAL_POLICY_HASH = os.getenv(
    "ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_DEFAULT_POLICY",
    "cfg:ml:operator_routing_incident_rca_routing:default",
)

GROUP = "operator_routing_incident_rca_routing_apply_v2_14"
CONSUMER = f"{GROUP}_{os.getpid()}"
POLL_INTERVAL = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_APPLY_POLL_INTERVAL", "5"))
MAX_BATCH = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_ROUTING_APPLY_MAX_BATCH", "50"))
MAXLEN = int(os.getenv("ML_OPERATOR_ROUTING_INCIDENT_RCA_MAXLEN", "10000"))


def _counter(name: str, doc: str, labels: tuple = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: tuple = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: tuple = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_operator_routing_incident_rca_winner_routing_apply_runs_total",
    "Routing apply runs",
    ("status",),
)
LAT = _hist(
    "ml_operator_routing_incident_rca_winner_routing_apply_latency_seconds",
    "Routing apply latency seconds",
)
LAST_RUN_TS = _gauge(
    "ml_operator_routing_incident_rca_winner_routing_apply_last_run_ts_seconds",
    "Routing apply last run ts",
)
APPLIES = _counter(
    "ml_operator_routing_incident_rca_winner_routing_applies_total",
    "Routing rules applied",
    ("experiment_id", "action"),
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


async def apply_loop(r: Any) -> None:
    started = time.perf_counter()
    status = "ok"
    try:
        await ensure_group(r, IN_STREAM, GROUP)
        messages = await r.xreadgroup(GROUP, CONSUMER, {IN_STREAM: ">"}, count=MAX_BATCH, block=10)
        if not messages:
            return

        for stream_name, records in messages:
            for msg_id, payload in records:
                try:
                    row = as_dict(payload)
                    exp_id = row.get("experiment_id", "unknown")
                    winner_bucket = row.get("winner_bucket", "none")
                    winner_score = float(row.get("winner_score", 0.0))
                    advisory_flag = row.get("advisory_only", "1")

                    sample_n_ch = int(row.get("bucket_challenger_sample_n", 0))
                    sample_n_ct = int(row.get("bucket_control_sample_n", 0))

                    score_ct = float(row.get("bucket_control_avg_quality", 0.0))*0.4 + float(row.get("bucket_control_avg_usefulness", 0.0))*0.6

                    action = "REJECTED"
                    reason = "none"

                    kill_val = await r.get(KILL_SWITCH_KEY)
                    is_killed = kill_val and kill_val.decode("utf-8") == "1"

                    last_apply = await r.hget(GLOBAL_POLICY_HASH, "last_updated_ms")
                    last_apply_sec = int(last_apply.decode("utf-8")) / 1000 if last_apply else 0

                    # Validations
                    if is_killed:
                        reason = "kill_switch_active"
                    elif advisory_flag == "1" and not ADVISORY_ONLY:
                        reason = "decision_is_advisory" # wait, if global is active, but decision is advisory?
                        # ignore, let global override # actually no, trust global
                        pass
                    elif winner_bucket != "challenger":
                        reason = "winner_not_challenger"
                    elif sample_n_ch < MIN_SAMPLE or sample_n_ct < MIN_SAMPLE:
                        reason = "insufficient_sample"
                    elif winner_score < score_ct + MIN_UPLIFT:
                        reason = "insufficient_uplift"
                    elif (time.time() - last_apply_sec) < COOLDOWN_SEC:
                        reason = "cooldown_active"
                    else:
                        action = "APPLIED"
                        reason = "validation_passed"

                    if ADVISORY_ONLY and action == "APPLIED":
                        action = "ADVISORY_APPLIED"
                        reason = "validation_passed_dry_run"

                    result = {
                        "experiment_id": exp_id,
                        "winner_bucket": winner_bucket,
                        "action": action,
                        "reason": reason,
                        "executor_mode": EXECUTOR_MODE,
                        "ts_ms": now_ms(),
                    }

                    if action == "APPLIED" and EXECUTOR_MODE == "COMMIT":
                        await r.hset(GLOBAL_POLICY_HASH, mapping={
                            "provider": "vertex",
                            "model_name": "gemini-2.0-flash-lite-preview-02-05", # mocked lookup
                            "prompt_version": "routing_incident_rca_v1_challenger",
                            "last_updated_ms": now_ms(),
                            "experiment_source": exp_id
                        })

                    await r.xadd(OUT_STREAM, result, maxlen=MAXLEN, approximate=True)
                    await r.xadd(AUDIT_STREAM, result, maxlen=MAXLEN, approximate=True)

                    if APPLIES:
                        APPLIES.labels(experiment_id=exp_id, action=action).inc()

                    await r.xack(IN_STREAM, GROUP, msg_id)
                except Exception:
                    status = "error"
                    await r.xack(IN_STREAM, GROUP, msg_id)

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
        await apply_loop(r)
        await asyncio.sleep(POLL_INTERVAL)


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
