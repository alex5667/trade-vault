from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional

try:
    import redis.asyncio as redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

from prometheus_client import Counter, Gauge, Histogram, start_http_server

IN_STREAM = os.getenv("ML_OPERATOR_RCA_ROUTING_VERIFY_STREAM", "stream:ml:operator_rca_routing_verify_results")
OUT_STREAM = os.getenv("ML_OPERATOR_RCA_ROUTING_RETRY_REQUESTS_STREAM", "stream:ml:operator_rca_routing_retry_requests")
AUDIT_STREAM = os.getenv("ML_OPERATOR_RCA_ROUTING_RETRY_AUDIT_STREAM", "stream:ml:operator_rca_routing_retry_audit")
STATE_HASH = os.getenv("ML_OPERATOR_RCA_ROUTING_RETRY_STATE_HASH", "metrics:ml:operator_rca_routing_retry:last")
GROUP = os.getenv("ML_OPERATOR_RCA_ROUTING_RETRY_GROUP", "cg:ml:operator_rca_routing_retry")
CONSUMER = os.getenv("ML_OPERATOR_RCA_ROUTING_RETRY_CONSUMER", "retry-ctrl-1")

RUNS = Counter("ml_operator_rca_routing_retry_runs_total", "Retry controller runs", ["status"])
RETRY_REQ = Counter("ml_operator_rca_routing_retry_requests_total", "Retry requests emitted", ["reason_code"])
SUPPRESS = Counter("ml_operator_rca_routing_retry_suppressed_total", "Retry suppressed", ["reason"])
LAST = Gauge("ml_operator_rca_routing_retry_last_run_ts_seconds", "Last retry run")
LAT = Histogram("ml_operator_rca_routing_retry_loop_seconds", "Retry loop latency")

RETRYABLE = {
    "ROUTE_VERIFY_INCONCLUSIVE",
    "ROUTE_PROVIDER_TIMEOUT",
    "ROUTE_PROVIDER_UNAVAILABLE",
    "ROUTE_STATE_RACE",
}
NON_RETRYABLE = {
    "ROUTE_POLICY_DENIED",
    "ROUTE_BASELINE_MISSING",
    "ROUTE_TARGET_MISSING",
    "ROUTE_GOVERNOR_DENIED",
}


def _to_str_dict(raw: Dict[Any, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in raw.items():
        out[k.decode() if isinstance(k, bytes) else str(k)] = v.decode() if isinstance(v, bytes) else str(v)
    return out


def _now_ms() -> int:
    return get_ny_time_millis()


@dataclass
class VerifyEvent:
    recommendation_id: str
    route_change_id: str
    verify_status: str
    reason_code: str
    ts_ms: int


def _parse(raw: Dict[str, str]) -> Optional[VerifyEvent]:
    try:
        return VerifyEvent(
            recommendation_id=str(raw.get("recommendation_id", "")),
            route_change_id=str(raw.get("route_change_id", "")),
            verify_status=str(raw.get("verify_status", "")),
            reason_code=str(raw.get("reason_code", "")),
            ts_ms=int(raw.get("ts_ms", "0") or 0),
        )
    except Exception:
        return None


async def _state_get(cli: "redis.Redis", rid: str) -> Dict[str, str]:
    raw = await cli.hgetall(f"metrics:ml:operator_rca_routing_retry:{rid}")
    return _to_str_dict(raw)


async def _state_set(cli: "redis.Redis", rid: str, mapping: Dict[str, Any]) -> None:
    await cli.hset(f"metrics:ml:operator_rca_routing_retry:{rid}", mapping={k: str(v) for k, v in mapping.items()})


def _backoff_sec(attempt: int, base: float, max_backoff: float) -> float:
    return min(max_backoff, base * (2 ** max(0, attempt - 1)))


async def process_event(cli: "redis.Redis", ev: VerifyEvent, cfg: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    if ev.verify_status not in ("FAIL", "INCONCLUSIVE", "ROLLBACK_REQUIRED"):
        SUPPRESS.labels(reason="verify_status_not_retryable").inc()
        return None
    if ev.reason_code in NON_RETRYABLE:
        SUPPRESS.labels(reason="non_retryable_reason").inc()
        return None
    if ev.reason_code and ev.reason_code not in RETRYABLE and ev.verify_status != "INCONCLUSIVE":
        SUPPRESS.labels(reason="unknown_reason_not_retryable").inc()
        return None
    state = await _state_get(cli, ev.recommendation_id)
    attempts = int(state.get("attempts", "0") or 0)
    if attempts >= int(cfg["max_attempts"]):
        SUPPRESS.labels(reason="max_attempts").inc()
        return None
    now = _now_ms()
    not_before_ms = int(state.get("not_before_ms", "0") or 0)
    if now < not_before_ms:
        SUPPRESS.labels(reason="backoff_active").inc()
        return None

    attempt = attempts + 1
    delay_sec = _backoff_sec(attempt, float(cfg["base_backoff_sec"]), float(cfg["max_backoff_sec"]))
    req = {
        "schema_version": 1,
        "ts_ms": now,
        "recommendation_id": ev.recommendation_id,
        "route_change_id": ev.route_change_id,
        "retry_attempt": attempt,
        "reason_code": ev.reason_code or "ROUTE_VERIFY_INCONCLUSIVE",
        "retry_after_sec": round(delay_sec, 3),
    }
    await cli.xadd(OUT_STREAM, req, maxlen=int(cfg["stream_maxlen"]), approximate=True)
    await cli.xadd(AUDIT_STREAM, {**req, "event": "ROUTING_RETRY_REQUESTED"}, maxlen=int(cfg["stream_maxlen"]), approximate=True)
    await _state_set(
        cli,
        ev.recommendation_id,
        {
            "attempts": attempt,
            "last_reason_code": ev.reason_code or "ROUTE_VERIFY_INCONCLUSIVE",
            "last_ts_ms": now,
            "not_before_ms": now + int(delay_sec * 1000),
        }
    )
    RETRY_REQ.labels(reason_code=ev.reason_code or "ROUTE_VERIFY_INCONCLUSIVE").inc()
    return req


async def main() -> None:
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(int(os.getenv("ML_OPERATOR_RCA_ROUTING_RETRY_METRICS_PORT", "9881")))
    cli = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    try:
        await cli.xgroup_create(IN_STREAM, GROUP, id="0", mkstream=True)
    except Exception:
        pass
    cfg = {
        "max_attempts": int(os.getenv("ML_OPERATOR_RCA_ROUTING_RETRY_MAX_ATTEMPTS", "3")),
        "base_backoff_sec": float(os.getenv("ML_OPERATOR_RCA_ROUTING_RETRY_BASE_BACKOFF_SEC", "60")),
        "max_backoff_sec": float(os.getenv("ML_OPERATOR_RCA_ROUTING_RETRY_MAX_BACKOFF_SEC", "900")),
        "stream_maxlen": int(os.getenv("ML_OPERATOR_RCA_ROUTING_RETRY_STREAM_MAXLEN", "2000")),
    }
    while True:
        t0 = time.perf_counter()
        try:
            rows = await cli.xreadgroup(GROUP, CONSUMER, {IN_STREAM: ">"}, count=50, block=5000)
            for _, items in rows:
                for msg_id, payload in items:
                    raw = _to_str_dict(payload)
                    ev = _parse(raw)
                    if ev is not None:
                        await process_event(cli, ev, cfg)
                    await cli.xack(IN_STREAM, GROUP, msg_id)
            RUNS.labels(status="ok").inc()
        except Exception:
            RUNS.labels(status="err").inc()
        LAST.set(time.time())
        LAT.observe(time.perf_counter() - t0)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
