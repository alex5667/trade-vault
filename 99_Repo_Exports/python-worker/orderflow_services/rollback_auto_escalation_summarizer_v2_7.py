from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
from collections import Counter as CCounter
from typing import Any, Dict, List

try:
    import redis.asyncio as redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

from prometheus_client import Counter, Gauge, Histogram, start_http_server

IN_STREAMS = [
    os.getenv("ML_OPERATOR_RCA_ROUTING_VERIFY_STREAM", "stream:ml:operator_rca_routing_verify_results"),
    os.getenv("ML_OPERATOR_RCA_ROUTING_RETRY_AUDIT_STREAM", "stream:ml:operator_rca_routing_retry_audit"),
    os.getenv("ML_OPERATOR_RCA_ROUTING_ROLLBACK_RESULTS_STREAM", "stream:ml:operator_rca_routing_rollback_results"),
]
OUT_STREAM = os.getenv("ML_OPERATOR_RCA_ROUTING_ESCALATION_STREAM", "stream:ml:operator_rca_routing_escalations")
LAST_HASH = os.getenv("ML_OPERATOR_RCA_ROUTING_ESCALATION_LAST_HASH", "metrics:ml:operator_rca_routing_escalations:last")

RUNS = Counter("ml_operator_rca_routing_escalation_runs_total", "Escalation summarizer runs", ["status"])
OPEN = Gauge("ml_operator_rca_routing_escalation_open_items", "Open escalation items")
CRIT = Gauge("ml_operator_rca_routing_escalation_critical_items", "Critical escalation items")
LAST = Gauge("ml_operator_rca_routing_escalation_last_run_ts_seconds", "Last run")
LAT = Histogram("ml_operator_rca_routing_escalation_loop_seconds", "Loop seconds")


def _to_str_dict(raw: Dict[Any, Any]) -> Dict[str, str]:
    out: Dict[str, str] = {}
    for k, v in raw.items():
        out[k.decode() if isinstance(k, bytes) else str(k)] = v.decode() if isinstance(v, bytes) else str(v)
    return out


async def _read(cli: "redis.Redis", stream: str, count: int) -> List[Dict[str, str]]:
    rows = await cli.xrevrange(stream, max="+", min="-", count=count)
    out: List[Dict[str, str]] = []
    for _id, payload in rows:
        d = _to_str_dict(payload)
        d["stream_id"] = _id.decode() if isinstance(_id, bytes) else str(_id)
        out.append(d)
    return out


def _severity(open_n: int, critical_n: int, cfg: Dict[str, Any]) -> str:
    if critical_n >= int(cfg["critical_open_threshold"]):
        return "critical"
    if open_n >= int(cfg["warning_open_threshold"]):
        return "warning"
    return "info"


async def run_once(cli: "redis.Redis", cfg: Dict[str, Any]) -> Dict[str, Any]:
    t0 = time.perf_counter()
    events: List[Dict[str, str]] = []
    for s in IN_STREAMS:
        events.extend(await _read(cli, s, int(cfg["window_count"])))
    by_reason = CCounter()
    open_ids = set()
    critical_ids = set()
    for e in events:
        rid = str(e.get("recommendation_id", "") or e.get("route_change_id", ""))
        reason = str(e.get("reason_code", "UNKNOWN"))
        if rid:
            open_ids.add(rid)
        if reason in ("ERROR_RATE_SPIKE", "PARSE_FAIL_RATE_HIGH", "LATENCY_P95_REGRESSION"):
            if rid:
                critical_ids.add(rid)
        by_reason[reason] += 1
    sev = _severity(len(open_ids), len(critical_ids), cfg)
    payload = {
        "schema_version": 1,
        "ts_ms": get_ny_time_millis(),
        "severity": sev,
        "open_items_n": len(open_ids),
        "critical_items_n": len(critical_ids),
        "top_reason_codes_json": json.dumps(by_reason.most_common(5), separators=(",", ":")),
        "summary": f"route_change open={len(open_ids)} critical={len(critical_ids)}",
    }
    await cli.xadd(OUT_STREAM, payload, maxlen=int(cfg["stream_maxlen"]), approximate=True)
    await cli.hset(LAST_HASH, mapping={k: str(v) for k, v in payload.items()})
    OPEN.set(len(open_ids))
    CRIT.set(len(critical_ids))
    LAST.set(time.time())
    RUNS.labels(status="ok").inc()
    LAT.observe(time.perf_counter() - t0)
    return payload


async def main() -> None:
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(int(os.getenv("ML_OPERATOR_RCA_ROUTING_ESCALATION_METRICS_PORT", "9882")))
    cli = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    cfg = {
        "window_count": int(os.getenv("ML_OPERATOR_RCA_ROUTING_ESCALATION_WINDOW_COUNT", "500")),
        "stream_maxlen": int(os.getenv("ML_OPERATOR_RCA_ROUTING_ESCALATION_STREAM_MAXLEN", "2000")),
        "warning_open_threshold": int(os.getenv("ML_OPERATOR_RCA_ROUTING_ESCALATION_WARNING_OPEN_THRESHOLD", "3")),
        "critical_open_threshold": int(os.getenv("ML_OPERATOR_RCA_ROUTING_ESCALATION_CRITICAL_OPEN_THRESHOLD", "5")),
        "interval_sec": float(os.getenv("ML_OPERATOR_RCA_ROUTING_ESCALATION_INTERVAL_SEC", "60")),
    }
    while True:
        try:
            await run_once(cli, cfg)
        except Exception:
            RUNS.labels(status="err").inc()
        await asyncio.sleep(cfg["interval_sec"])


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
