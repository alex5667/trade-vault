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

APP_NAME = "route_incident_rca_mirror_rca_winner_apply_vertex_feedback_governor_v3_21"

FEEDBACK_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERTEX_RCA_FEEDBACK_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_vertex_rca_feedback")
ROLLUPS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERTEX_RCA_FEEDBACK_ROLLUPS_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_vertex_rca_feedback_rollups")
DECISIONS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERTEX_RCA_GOVERNANCE_DECISIONS_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_vertex_rca_governance_decisions")
LAST_METRIC = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERTEX_RCA_GOVERNANCE_LAST", "metrics:ml:route_incident_rca_mirror_rca_winner_apply_vertex_rca_governance:last")

PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERTEX_GOVERNANCE_PORT", "9943"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERTEX_GOVERNANCE_MAXLEN", "2000"))

MIN_SAMPLES = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERTEX_RCA_GOVERNANCE_MIN_SAMPLES", "10"))
MIN_AVG_QUALITY = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERTEX_RCA_GOVERNANCE_MIN_AVG_QUALITY", "0.55"))
MIN_AVG_USEFULNESS = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERTEX_RCA_GOVERNANCE_MIN_AVG_USEFULNESS", "0.60"))
MIN_ACCEPTED_RATE = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERTEX_RCA_GOVERNANCE_MIN_ACCEPTED_RATE", "0.60"))
MAX_LOW_QUALITY_RATE = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERTEX_RCA_GOVERNANCE_MAX_LOW_QUALITY_RATE", "0.35"))

ADVISORY_ONLY = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERTEX_GOVERNANCE_ADVISORY_ONLY", "1"))
EXECUTOR_MODE = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERTEX_GOVERNANCE_EXECUTOR_MODE", "DRY_RUN")

POLL_INTERVAL_SEC = 5.0

def _counter(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None

def _gauge(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None

def _hist(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_route_incident_rca_mirror_rca_winner_apply_vertex_governance_runs_total", "Runs", ("status", "decision"))
LAT = _hist("ml_route_incident_rca_mirror_rca_winner_apply_vertex_governance_latency_seconds", "Latency")
UP = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_vertex_governance_up", "Up")
LAST_RUN = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_vertex_governance_last_run_ts_seconds", "Last run")

AVG_Q = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_vertex_governance_avg_quality", "Avg Q")
AVG_U = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_vertex_governance_avg_usefulness", "Avg U")
ACC_R = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_vertex_governance_accepted_rate", "Acc Rate")

def now_ms() -> int:
    return get_ny_time_millis()

def decode_dict(d: dict[Any, Any]) -> dict[str, Any]:
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in d.items()
    }

def calculate_rollups(feedbacks: list[dict[str, Any]]) -> dict[str, float]:
    if not feedbacks:
        return {"avg_q": 0.0, "avg_u": 0.0, "acc_r": 0.0, "low_q": 0.0, "n": 0.0}

    qs, us, accs, lqs = 0.0, 0.0, 0.0, 0.0
    for f in feedbacks:
        q = float(f.get("quality_score", "0"))
        u = float(f.get("usefulness_score", "0"))
        acc = int(f.get("accepted", "0"))

        qs += q
        us += u
        accs += acc
        lqs += 1 if q < 0.5 else 0

    n = len(feedbacks)
    return {
        "avg_q": qs / n,
        "avg_u": us / n,
        "acc_r": accs / n,
        "low_q": lqs / n,
        "n": n
    }

def decide_governance(rollups: dict[str, float], min_n: int, min_q: float, min_u: float, min_a: float, max_lq: float) -> str:
    n = rollups["n"]
    if n < min_n:
        return "HOLD"

    if rollups["avg_q"] < min_q or rollups["avg_u"] < min_u or rollups["acc_r"] < min_a or rollups["low_q"] > max_lq:
        return "PREFER_LOCAL_ONLY"

    return "KEEP_AUTO"

async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PORT)
    if UP:
        UP.set(1)

    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))

    last_id = "0-0"

    while True:
        started = time.perf_counter()
        status = "ok"
        decision = "HOLD"

        try:
            # We don't block here typically, just poll
            # We fetch up to 100 recent feedback samples to govern
            hist = await r.xrevrange(FEEDBACK_STREAM, max="+", min="-", count=100)

            feedbacks = [decode_dict(fields) for _, fields in hist] if hist else []

            rollups = calculate_rollups(feedbacks)

            if AVG_Q: AVG_Q.set(rollups["avg_q"])
            if AVG_U: AVG_U.set(rollups["avg_u"])
            if ACC_R: ACC_R.set(rollups["acc_r"])

            decision = decide_governance(rollups, MIN_SAMPLES, MIN_AVG_QUALITY, MIN_AVG_USEFULNESS, MIN_ACCEPTED_RATE, MAX_LOW_QUALITY_RATE)

            await r.xadd(ROLLUPS_STREAM, {
                "avg_quality": str(rollups["avg_q"]),
                "avg_usefulness": str(rollups["avg_u"]),
                "accepted_rate": str(rollups["acc_r"]),
                "low_quality_rate": str(rollups["low_q"]),
                "n_samples": str(rollups["n"]),
                "ts_ms": str(now_ms())
            }, maxlen=MAXLEN, approximate=True)

            await r.xadd(DECISIONS_STREAM, {
                "decision": decision,
                "advisory": str(ADVISORY_ONLY),
                "ts_ms": str(now_ms())
            }, maxlen=MAXLEN, approximate=True)

            await r.hset(LAST_METRIC, "decision", decision)
            await r.hset(LAST_METRIC, "n_samples", str(rollups["n"]))
            await r.hset(LAST_METRIC, "ts_ms", str(now_ms()))

            if LAST_RUN:
                LAST_RUN.set(time.time())

        except Exception:
            status = "error"
        finally:
            if RUNS:
                RUNS.labels(status=status, decision=decision).inc()
            if LAT:
                LAT.observe(max(time.perf_counter() - started, 0.0))

            await asyncio.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
