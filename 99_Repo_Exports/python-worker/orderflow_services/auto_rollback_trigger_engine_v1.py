from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
from typing import Any, Dict, List

try:  # pragma: no cover
    import redis.asyncio as redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

from prometheus_client import Counter, Gauge, start_http_server


TRIGGER_RUNS = Counter("ml_auto_rollback_trigger_runs_total", "Rollback trigger runs", ["status"])
TRIGGERED = Counter("ml_auto_rollback_triggered_total", "Triggered rollbacks", ["action_type"])
SUPPRESSED = Counter("ml_auto_rollback_suppressed_total", "Suppressed rollbacks", ["reason"])
LAST_RUN = Gauge("ml_auto_rollback_trigger_last_run_ts_seconds", "Last trigger engine run")


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d


def _j(x: Any, d: Any) -> Any:
    try:
        if x is None:
            return d
        if isinstance(x, (list, dict)):
            return x
        return json.loads(x)
    except Exception:
        return d


async def run_once() -> None:
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    in_stream = os.getenv("ML_POST_COMMIT_AUDIT_STREAM", "stream:ml:recommendation_audit")
    out_stream = os.getenv("ML_RECOMMENDATION_ROLLBACK_REQUESTS_STREAM", "stream:ml:recommendation_rollback_requests")
    group = os.getenv("ML_AUTO_ROLLBACK_GROUP", "ml_auto_rollback_trigger_v1")
    consumer = os.getenv("ML_AUTO_ROLLBACK_CONSUMER", os.uname().nodename)
    cli = redis.from_url(redis_url, decode_responses=False)
    try:
        await cli.xgroup_create(in_stream, group, id="0", mkstream=True)
    except Exception:
        pass

    rows = await cli.xreadgroup(group, consumer, {in_stream: ">"}, count=100, block=1000)
    if not rows:
        TRIGGER_RUNS.labels(status="idle").inc()
        return

    for _, messages in rows:
        for msg_id, fields in messages:
            d = {str(k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v) for k, v in fields.items()}
            if str(d.get("event_type", "")) != "POST_COMMIT_VERIFICATION":
                await cli.xack(in_stream, group, msg_id)
                continue
            if str(d.get("verification_status", "")) != "ROLLBACK_REQUIRED":
                await cli.xack(in_stream, group, msg_id)
                continue

            reason_codes = _j(d.get("reason_codes_json", "[]"), [])
            action_type = str(d.get("action_type", "unknown"))
            cooldown_key = f"ml:auto_rollback:cooldown:{d.get('recommendation_id','')}"
            cooldown_sec = _i(os.getenv("ML_AUTO_ROLLBACK_COOLDOWN_SEC", "1800"), 1800)

            if await cli.exists(cooldown_key):
                SUPPRESSED.labels(reason="cooldown_active").inc()
                await cli.xack(in_stream, group, msg_id)
                continue

            if "LATENCY_P95_REGRESSION" not in reason_codes and "ERROR_RATE_SPIKE" not in reason_codes:
                SUPPRESSED.labels(reason="non_hard_failure").inc()
                await cli.xack(in_stream, group, msg_id)
                continue

            await cli.xadd(
                out_stream,
                {
                    "schema_version": 1,
                    "recommendation_id": str(d.get("recommendation_id", "")),
                    "ts_ms": get_ny_time_millis(),
                    "requested_by": "auto_rollback_trigger_engine_v1",
                    "rollback_reason_codes_json": json.dumps(reason_codes),
                    "action_type": action_type,
                    "target_kind": str(d.get("target_kind", "unknown")),
                    "target_ref": str(d.get("target_ref", "")),
                }, maxlen=_i(os.getenv("ML_ROLLBACK_REQUESTS_MAXLEN", "50000"), 50000),
                approximate=True,
            )
            await cli.set(cooldown_key, "1", ex=cooldown_sec)
            TRIGGERED.labels(action_type=action_type).inc()
            await cli.xack(in_stream, group, msg_id)

    LAST_RUN.set(time.time())
    TRIGGER_RUNS.labels(status="ok").inc()


def main() -> None:
    start_http_server(_i(os.getenv("ML_AUTO_ROLLBACK_TRIGGER_METRICS_PORT", "9873"), 9873))
    import asyncio

    while True:
        try:
            asyncio.run(run_once())
        except Exception:
            TRIGGER_RUNS.labels(status="error").inc()
            time.sleep(5)


if __name__ == "__main__":
    main()
