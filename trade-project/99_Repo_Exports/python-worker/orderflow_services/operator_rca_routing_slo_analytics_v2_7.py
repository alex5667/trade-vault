from __future__ import annotations

import json
import os
import time
from collections.abc import Iterable
from dataclasses import dataclass
from statistics import median
from typing import Any

from utils.time_utils import get_ny_time_millis

try:
    import redis.asyncio as redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None  # type: ignore

from prometheus_client import Counter, Gauge, Histogram, start_http_server

METRIC_RUNS = Counter("ml_operator_rca_routing_slo_runs_total", "Routing SLO runs", ["status"])
METRIC_EVENTS = Gauge("ml_operator_rca_routing_slo_events_window", "Events in analysis window")
METRIC_SUCCESS_RATE = Gauge("ml_operator_rca_routing_slo_success_rate", "Routing verification success rate")
METRIC_MTTR_P50 = Gauge("ml_operator_rca_routing_slo_mttr_p50_seconds", "Routing MTTR p50")
METRIC_MTTR_P95 = Gauge("ml_operator_rca_routing_slo_mttr_p95_seconds", "Routing MTTR p95")
METRIC_BREACHES = Gauge("ml_operator_rca_routing_slo_breaches", "Routing SLO breaches")
METRIC_LAST = Gauge("ml_operator_rca_routing_slo_last_run_ts_seconds", "Last run")
METRIC_LAT = Histogram("ml_operator_rca_routing_slo_loop_seconds", "Loop seconds")

VERIFY_STREAM = os.getenv("ML_OPERATOR_RCA_ROUTING_VERIFY_STREAM", "stream:ml:operator_rca_routing_verify_results")
ROLLBACK_STREAM = os.getenv("ML_OPERATOR_RCA_ROUTING_ROLLBACK_RESULTS_STREAM", "stream:ml:operator_rca_routing_rollback_results")
OUT_STREAM = os.getenv("ML_OPERATOR_RCA_ROUTING_SLO_STREAM", "stream:ml:operator_rca_routing_slo_rollups")
LAST_HASH = os.getenv("ML_OPERATOR_RCA_ROUTING_SLO_LAST_HASH", "metrics:ml:operator_rca_routing_slo:last")


def _now_ms() -> int:
    return get_ny_time_millis()


def _to_str_dict(raw: dict[Any, Any]) -> dict[str, str]:
    out: dict[str, str] = {}
    for k, v in raw.items():
        ks = k.decode() if isinstance(k, bytes) else str(k)
        vs = v.decode() if isinstance(v, bytes) else str(v)
        out[ks] = vs
    return out


def _pctile(values: list[float], q: float) -> float:
    if not values:
        return 0.0
    arr = sorted(values)
    if len(arr) == 1:
        return float(arr[0])
    idx = (len(arr) - 1) * q
    lo = int(idx)
    hi = min(lo + 1, len(arr) - 1)
    frac = idx - lo
    return float(arr[lo] * (1.0 - frac) + arr[hi] * frac)


def _reason_codes(success_rate: float, mttr_p95: float, breaches: int, cfg: dict[str, Any]) -> list[str]:
    codes: list[str] = []
    if success_rate < float(cfg["success_rate_min"]):
        codes.append("ROUTE_VERIFY_SUCCESS_RATE_LOW")
    if mttr_p95 > float(cfg["mttr_p95_max_sec"]):
        codes.append("ROUTE_VERIFY_MTTR_P95_HIGH")
    if breaches > 0:
        codes.append("ROUTE_VERIFY_SLO_BREACH")
    return codes


@dataclass
class RoutingEvent:
    recommendation_id: str
    route_change_id: str
    ts_ms: int
    verify_status: str
    rollback_required: int
    reason_code: str


async def _read_recent(cli: redis.Redis, stream: str, count: int) -> list[dict[str, str]]:
    rows = await cli.xrevrange(stream, max="+", min="-", count=count)
    out: list[dict[str, str]] = []
    for _id, payload in rows:
        d = _to_str_dict(payload)
        d["stream_id"] = _id.decode() if isinstance(_id, bytes) else str(_id)
        out.append(d)
    return out


def _extract_events(rows: Iterable[dict[str, str]]) -> list[RoutingEvent]:
    out: list[RoutingEvent] = []
    for r in rows:
        try:
            out.append(
                RoutingEvent(
                    recommendation_id=(r.get("recommendation_id", "")),
                    route_change_id=(r.get("route_change_id", "")),
                    ts_ms=int(r.get("ts_ms", "0") or 0),
                    verify_status=(r.get("verify_status", "UNKNOWN")),
                    rollback_required=int(r.get("rollback_required", "0") or 0),
                    reason_code=(r.get("reason_code", "")),
                )
            )
        except Exception:
            continue
    return out


async def run_once(cli: redis.Redis, cfg: dict[str, Any]) -> dict[str, Any]:
    t0 = time.perf_counter()
    win = int(cfg["window_count"])
    verify_rows = await _read_recent(cli, VERIFY_STREAM, win)
    rb_rows = await _read_recent(cli, ROLLBACK_STREAM, win)
    events = _extract_events(verify_rows)
    rb = _extract_events(rb_rows)

    rb_by_change: dict[str, int] = {}
    for e in rb:
        if e.route_change_id:
            rb_by_change[e.route_change_id] = e.ts_ms

    mttrs: list[float] = []
    success = 0
    failed = 0
    breaches = 0
    for e in events:
        if e.verify_status == "PASS":
            success += 1
        elif e.verify_status in ("ROLLBACK_REQUIRED", "FAIL"):
            failed += 1
            breaches += 1
            rb_ts = rb_by_change.get(e.route_change_id)
            if rb_ts and rb_ts >= e.ts_ms > 0:
                mttrs.append((rb_ts - e.ts_ms) / 1000.0)
        elif e.verify_status == "INCONCLUSIVE":
            failed += 1

    total = success + failed
    success_rate = (success / total) if total > 0 else 1.0
    mttr_p50 = median(mttrs) if mttrs else 0.0
    mttr_p95 = _pctile(mttrs, 0.95) if mttrs else 0.0
    codes = _reason_codes(success_rate, mttr_p95, breaches, cfg)

    payload = {
        "schema_version": 1,
        "ts_ms": _now_ms(),
        "window_count": win,
        "events_n": total,
        "success_n": success,
        "failed_n": failed,
        "success_rate": round(success_rate, 6),
        "mttr_p50_sec": round(mttr_p50, 6),
        "mttr_p95_sec": round(mttr_p95, 6),
        "breaches": breaches,
        "reason_codes_json": json.dumps(codes, separators=(",", ":")),
    }
    await cli.xadd(OUT_STREAM, payload, maxlen=int(cfg["stream_maxlen"]), approximate=True)
    await cli.hset(LAST_HASH, mapping={k: str(v) for k, v in payload.items()})

    METRIC_EVENTS.set(total)
    METRIC_SUCCESS_RATE.set(success_rate)
    METRIC_MTTR_P50.set(mttr_p50)
    METRIC_MTTR_P95.set(mttr_p95)
    METRIC_BREACHES.set(breaches)
    METRIC_LAST.set(time.time())
    METRIC_RUNS.labels(status="ok").inc()
    METRIC_LAT.observe(time.perf_counter() - t0)
    return payload


async def main() -> None:
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(int(os.getenv("ML_OPERATOR_RCA_ROUTING_SLO_METRICS_PORT", "9880")))
    cli = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    cfg = {
        "window_count": int(os.getenv("ML_OPERATOR_RCA_ROUTING_SLO_WINDOW_COUNT", "500")),
        "stream_maxlen": int(os.getenv("ML_OPERATOR_RCA_ROUTING_SLO_STREAM_MAXLEN", "2000")),
        "success_rate_min": float(os.getenv("ML_OPERATOR_RCA_ROUTING_SLO_SUCCESS_RATE_MIN", "0.80")),
        "mttr_p95_max_sec": float(os.getenv("ML_OPERATOR_RCA_ROUTING_SLO_MTTR_P95_MAX_SEC", "900")),
        "interval_sec": float(os.getenv("ML_OPERATOR_RCA_ROUTING_SLO_INTERVAL_SEC", "60")),
    }
    while True:
        try:
            await run_once(cli, cfg)
        except Exception:
            METRIC_RUNS.labels(status="err").inc()
        await asyncio.sleep(cfg["interval_sec"])


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
