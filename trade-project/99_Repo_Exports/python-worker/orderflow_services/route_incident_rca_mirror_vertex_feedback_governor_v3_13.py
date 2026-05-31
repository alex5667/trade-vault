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

APP_NAME = "route_incident_rca_mirror_vertex_feedback_governor_v3_13"
FEEDBACK_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERTEX_RCA_FEEDBACK_STREAM", "stream:ml:route_incident_rca_mirror_vertex_rca_feedback")
ROLLUPS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERTEX_RCA_FEEDBACK_ROLLUPS_STREAM", "stream:ml:route_incident_rca_mirror_vertex_rca_feedback_rollups")
DECISIONS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERTEX_RCA_GOVERNANCE_DECISIONS_STREAM", "stream:ml:route_incident_rca_mirror_vertex_rca_governance_decisions")
LAST_HASH = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERTEX_RCA_GOVERNANCE_LAST_HASH", "metrics:ml:route_incident_rca_mirror_vertex_rca_governance:last")

PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERTEX_RCA_GOVERNANCE_PORT", "9932"))
MIN_SAMPLES = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERTEX_RCA_GOVERNANCE_MIN_SAMPLES", "10"))
MIN_AVG_QUALITY = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERTEX_RCA_GOVERNANCE_MIN_AVG_QUALITY", "0.55"))
MIN_AVG_USEFULNESS = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERTEX_RCA_GOVERNANCE_MIN_AVG_USEFULNESS", "0.60"))
MIN_ACCEPTED_RATE = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERTEX_RCA_GOVERNANCE_MIN_ACCEPTED_RATE", "0.60"))
MAX_LOW_QUALITY_RATE = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERTEX_RCA_GOVERNANCE_MAX_LOW_QUALITY_RATE", "0.35"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERTEX_RCA_GOVERNANCE_MAXLEN", "1000"))
POLL_INTERVAL_SEC = 2.0

def _counter(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None

def _gauge(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None

def _hist(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_route_incident_rca_mirror_vertex_governance_runs_total", "Runs", ("status", "decision"))
LAT = _hist("ml_route_incident_rca_mirror_vertex_governance_latency_seconds", "Latency")
UP = _gauge("ml_route_incident_rca_mirror_vertex_governance_up", "Up")
LAST_RUN = _gauge("ml_route_incident_rca_mirror_vertex_governance_last_run_ts_seconds", "Last run")

AVG_Q = _gauge("ml_route_incident_rca_mirror_vertex_governance_avg_quality", "Quality")
AVG_U = _gauge("ml_route_incident_rca_mirror_vertex_governance_avg_usefulness", "Usefulness")
ACC_RATE = _gauge("ml_route_incident_rca_mirror_vertex_governance_accepted_rate", "Accepted rate")

def now_ms() -> int:
    return get_ny_time_millis()

def decode_dict(d: dict[Any, Any]) -> dict[str, Any]:
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in d.items()
    }

def compute_decision(samples: list[dict[str, Any]]) -> tuple[str, dict[str, float]]:
    if len(samples) < MIN_SAMPLES:
        return "HOLD", {}

    avg_quality = sum(float(s.get("quality_score", 0)) for s in samples) / len(samples)
    avg_usefulness = sum(float(s.get("usefulness_score", 0)) for s in samples) / len(samples)
    accepted_rate = sum(1 for s in samples if (s.get("accepted", "0")) == "1") / len(samples)
    low_quality_rate = sum(1 for s in samples if float(s.get("quality_score", 1.0)) < 0.4) / len(samples)

    rollups = {
        "avg_quality": avg_quality,
        "avg_usefulness": avg_usefulness,
        "accepted_rate": accepted_rate,
        "low_quality_rate": low_quality_rate,
        "samples": float(len(samples))
    }

    if (avg_quality < MIN_AVG_QUALITY or
        avg_usefulness < MIN_AVG_USEFULNESS or
        accepted_rate < MIN_ACCEPTED_RATE or
        low_quality_rate > MAX_LOW_QUALITY_RATE):
        return "PREFER_LOCAL_ONLY", rollups

    return "KEEP_AUTO", rollups

async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PORT)
    if UP:
        UP.set(1)

    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    last_id = "0-0"  # To collect sliding window, we might just fetch the last N from the stream

    while True:
        started = time.perf_counter()
        status = "ok"
        decision = "none"

        try:
            # simple sliding window: get last MAXLEN elements
            res = await r.xrevrange(FEEDBACK_STREAM, count=100)
            samples = []
            for msg_id, fields in res:
                decoded = decode_dict(fields)
                samples.append(decoded)

            governance_decision, rollups = compute_decision(samples)
            decision = governance_decision

            if rollups:
                if AVG_Q: AVG_Q.set(rollups["avg_quality"])
                if AVG_U: AVG_U.set(rollups["avg_usefulness"])
                if ACC_RATE: ACC_RATE.set(rollups["accepted_rate"])

                await r.xadd(ROLLUPS_STREAM, rollups, maxlen=MAXLEN, approximate=True)

            await r.xadd(DECISIONS_STREAM, {"decision": decision, "ts_ms": now_ms()}, maxlen=MAXLEN, approximate=True)
            await r.hset(LAST_HASH, mapping={"decision": decision, "ts_ms": str(now_ms())})

            if LAST_RUN:
                LAST_RUN.set(time.time())

            await asyncio.sleep(POLL_INTERVAL_SEC)

        except Exception:
            status = "error"
            await asyncio.sleep(2)
        finally:
            if RUNS:
                RUNS.labels(status=status, decision=decision).inc()
            if LAT:
                LAT.observe(max(time.perf_counter() - started, 0.0))

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
