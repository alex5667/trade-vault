from __future__ import annotations

import hashlib
import json
import os
import time
from dataclasses import dataclass
from typing import Any

from prometheus_client import Counter, Gauge, Histogram, start_http_server

from utils.time_utils import get_ny_time_millis

SNAPSHOT_STREAM = os.getenv("ML_MODEL_SNAPSHOT_STREAM", "stream:ml:model_snapshot")
TRAINING_STREAM = os.getenv("ML_TRAINING_RUNS_STREAM", "stream:ml:training_runs")
REQUESTS_STREAM = os.getenv("ML_ANALYSIS_REQUESTS_STREAM", "stream:ml:analysis_requests")
LAST_HASH = os.getenv("ML_ANALYSIS_PACK_BUILDER_LAST_HASH", "metrics:ml:analysis_requests:last")
STATE_HASH = os.getenv("ML_ANALYSIS_PACK_BUILDER_STATE_HASH", "metrics:ml:analysis_pack_builder:state")

RUNS = Counter("ml_analysis_pack_builder_runs_total", "Pack builder runs", ["status"])
REQUESTS = Counter("ml_analysis_requests_built_total", "Requests built", ["family", "priority"])
LAST_RUN_TS = Gauge("ml_analysis_pack_builder_last_run_ts_seconds", "Last run ts")
UP = Gauge("ml_analysis_pack_builder_up", "Health")
LOOP_LAT = Histogram("ml_analysis_pack_builder_loop_seconds", "Loop latency")


def _now_ms() -> int:
    return get_ny_time_millis()


def _jloads(x: Any, default: Any) -> Any:
    if x is None:
        return default
    if isinstance(x, (dict, list)):
        return x
    try:
        if isinstance(x, bytes):
            x = x.decode("utf-8", "replace")
        return json.loads(str(x))
    except Exception:
        return default


def _s(x: Any, default: str = "") -> str:
    if x is None:
        return default
    if isinstance(x, bytes):
        return x.decode("utf-8", "replace")
    return str(x)


def _f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


@dataclass
class Snapshot:
    model_id: str
    family: str
    kind: str
    status: str
    champion_flag: bool
    promotion_state: str
    artifact_uri: str
    schema_ver: str
    schema_hash: str
    reason_codes: list[str]
    latest_runtime_ts_ms: int
    runtime_age_sec: float
    latency_p95_max_ms: float
    latency_p99_max_ms: float
    allow_rate_avg: float
    block_rate_avg: float
    abstain_rate_avg: float
    shadow_rate_avg: float
    error_rate_max: float
    ece_max: float | None
    brier_max: float | None
    missing_critical_rate_max: float
    hot_symbols: list[str]
    psi_top_json: list[Any]
    ks_top_json: list[Any]


def normalize_snapshot(d: dict[str, Any]) -> Snapshot:
    reason_codes = _jloads(d.get("reason_codes_json"), [])
    hot_symbols = _jloads(d.get("hot_symbols_json"), [])
    psi_top = _jloads(d.get("psi_top_json"), [])
    ks_top = _jloads(d.get("ks_top_json"), [])
    return Snapshot(
        model_id=_s(d.get("model_id")),
        family=_s(d.get("family")),
        kind=_s(d.get("kind")),
        status=_s(d.get("status"), "unknown"),
        champion_flag=_s(d.get("champion_flag"), "0") in ("1", "true", "True"),
        promotion_state=_s(d.get("promotion_state"), "unknown"),
        artifact_uri=_s(d.get("artifact_uri")),
        schema_ver=_s(d.get("schema_ver")),
        schema_hash=_s(d.get("schema_hash")),
        reason_codes=reason_codes if isinstance(reason_codes, list) else [],
        latest_runtime_ts_ms=int(_f(d.get("latest_runtime_ts_ms"), 0)),
        runtime_age_sec=_f(d.get("runtime_age_sec"), 0.0),
        latency_p95_max_ms=_f(d.get("latency_p95_max_ms"), 0.0),
        latency_p99_max_ms=_f(d.get("latency_p99_max_ms"), 0.0),
        allow_rate_avg=_f(d.get("allow_rate_avg"), 0.0),
        block_rate_avg=_f(d.get("block_rate_avg"), 0.0),
        abstain_rate_avg=_f(d.get("abstain_rate_avg"), 0.0),
        shadow_rate_avg=_f(d.get("shadow_rate_avg"), 0.0),
        error_rate_max=_f(d.get("error_rate_max"), 0.0),
        ece_max=None if _s(d.get("ece_max")) == "" else _f(d.get("ece_max"), 0.0),
        brier_max=None if _s(d.get("brier_max")) == "" else _f(d.get("brier_max"), 0.0),
        missing_critical_rate_max=_f(d.get("missing_critical_rate_max"), 0.0),
        hot_symbols=hot_symbols if isinstance(hot_symbols, list) else [],
        psi_top_json=psi_top if isinstance(psi_top, list) else [],
        ks_top_json=ks_top if isinstance(ks_top, list) else [],
    )


def severity_of_snapshot(s: Snapshot) -> tuple[str, list[str]]:
    reasons: list[str] = []
    severity = "normal"
    if s.status in ("critical", "warning"):
        severity = "high" if s.status == "critical" else "normal"
        reasons.extend(s.reason_codes)
    if s.error_rate_max >= float(os.getenv("ML_ANALYSIS_REQ_ERROR_RATE_MIN", "0.02")):
        severity = "high"
        reasons.append("ERR_RATE_HIGH")
    if s.missing_critical_rate_max >= float(os.getenv("ML_ANALYSIS_REQ_MISSING_CRIT_MIN", "0.01")):
        severity = "high"
        reasons.append("MISSING_CRITICAL_HIGH")
    if s.ece_max is not None and s.ece_max >= float(os.getenv("ML_ANALYSIS_REQ_ECE_MIN", "0.08")):
        severity = "high"
        reasons.append("ECE_HIGH")
    if s.runtime_age_sec >= float(os.getenv("ML_ANALYSIS_REQ_RUNTIME_STALE_SEC", "900")):
        severity = "high"
        reasons.append("RUNTIME_STALE")
    return severity, sorted(set(reasons))


def build_analysis_request(snapshot: Snapshot, training_run: dict[str, Any], window_min: int = 60) -> dict[str, Any]:
    severity, reasons = severity_of_snapshot(snapshot)
    task_type = "root_cause_degradation" if reasons else "health_summary"
    priority = "high" if severity == "high" else "normal"
    scope = {
        "model_id": snapshot.model_id,
        "family": snapshot.family,
        "kind": snapshot.kind,
        "window_min": int(window_min),
        "symbols": snapshot.hot_symbols[:8],
    }
    input_pack = {
        "snapshot": {
            "model_id": snapshot.model_id,
            "family": snapshot.family,
            "kind": snapshot.kind,
            "status": snapshot.status,
            "promotion_state": snapshot.promotion_state,
            "champion_flag": snapshot.champion_flag,
            "schema_ver": snapshot.schema_ver,
            "schema_hash": snapshot.schema_hash,
            "runtime_age_sec": snapshot.runtime_age_sec,
            "latency_p95_max_ms": snapshot.latency_p95_max_ms,
            "latency_p99_max_ms": snapshot.latency_p99_max_ms,
            "allow_rate_avg": snapshot.allow_rate_avg,
            "block_rate_avg": snapshot.block_rate_avg,
            "abstain_rate_avg": snapshot.abstain_rate_avg,
            "shadow_rate_avg": snapshot.shadow_rate_avg,
            "error_rate_max": snapshot.error_rate_max,
            "ece_max": snapshot.ece_max,
            "brier_max": snapshot.brier_max,
            "missing_critical_rate_max": snapshot.missing_critical_rate_max,
            "reason_codes": snapshot.reason_codes,
            "hot_symbols": snapshot.hot_symbols,
            "psi_top_json": snapshot.psi_top_json[:10],
            "ks_top_json": snapshot.ks_top_json[:10],
        },
        "training_run": training_run or {},
        "constraints": {
            "advisory_only": True,
            "no_auto_apply": True,
            "allowed_actions": [
                "require_shadow_retrain",
                "freeze_candidate",
                "unfreeze_candidate",
                "request_calibration_refresh",
                "propose_threshold_canary",
                "open_incident",
                "draft_postmortem",
            ],
        }
    }
    req_id_src = json.dumps(
        {
            "model_id": snapshot.model_id,
            "reason_codes": reasons,
            "runtime_ts_ms": snapshot.latest_runtime_ts_ms,
            "train_run_id": training_run.get("run_id", ""),
        },
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    )
    request_id = hashlib.sha1(req_id_src.encode("utf-8")).hexdigest()
    return {
        "schema_version": 1,
        "request_id": request_id,
        "ts_ms": _now_ms(),
        "task_type": task_type,
        "priority": priority,
        "scope_json": json.dumps(scope, ensure_ascii=False, separators=(",", ":")),
        "input_pack_json": json.dumps(input_pack, ensure_ascii=False, separators=(",", ":")),
        "reason_codes_json": json.dumps(reasons, ensure_ascii=False, separators=(",", ":")),
    }


async def _latest_stream_payload(r, stream: str, model_id: str) -> dict[str, Any]:
    rows = await r.xrevrange(stream, max="+", min="-", count=200)
    for _msg_id, fields in rows:
        data = {_s(k): _s(v) for k, v in fields.items()}
        if data.get("model_id") == model_id or data.get("family") == model_id:
            return data
    return {}


async def main() -> None:
    start_http_server(int(os.getenv("ML_ANALYSIS_PACK_BUILDER_METRICS_PORT", "9847")))
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    scan_sleep_sec = float(os.getenv("ML_ANALYSIS_PACK_BUILDER_EVERY_SEC", "60"))
    window_min = int(os.getenv("ML_ANALYSIS_PACK_BUILDER_WINDOW_MIN", "60"))
    max_stream_len = int(os.getenv("ML_ANALYSIS_REQUESTS_STREAM_MAXLEN", "100000"))
    import redis.asyncio as redis
    r = redis.from_url(redis_url, decode_responses=False)

    UP.set(1.0)
    while True:
        t0 = time.perf_counter()
        status = "ok"
        try:
            keys = await r.keys("metrics:ml:model_snapshot:*")
            now_ms = _now_ms()
            built = 0
            for raw_key in keys:
                key = _s(raw_key)
                if key.endswith(":last"):
                    continue
                payload = await r.hgetall(key)
                if not payload:
                    continue
                decoded = {_s(k): _s(v) for k, v in payload.items()}
                snapshot = normalize_snapshot(decoded)
                severity, reasons = severity_of_snapshot(snapshot)
                if not reasons and os.getenv("ML_ANALYSIS_PACK_BUILDER_INCLUDE_HEALTHY", "0") != "1":
                    continue
                training_run = await _latest_stream_payload(r, TRAINING_STREAM, snapshot.family)
                req = build_analysis_request(snapshot, training_run, window_min=window_min)
                await r.xadd(REQUESTS_STREAM, req, maxlen=max_stream_len, approximate=True)
                await r.hset(LAST_HASH, mapping={
                    "request_id": req["request_id"],
                    "model_id": snapshot.model_id,
                    "family": snapshot.family,
                    "priority": req["priority"],
                    "task_type": req["task_type"],
                    "ts_ms": now_ms,
                })
                await r.hset(STATE_HASH, mapping={
                    "last_request_id": req["request_id"],
                    "model_id": snapshot.model_id,
                    "family": snapshot.family,
                    "severity": severity,
                    "reason_codes_json": json.dumps(reasons, ensure_ascii=False),
                    "ts_ms": now_ms,
                })
                REQUESTS.labels(family=snapshot.family, priority=req["priority"]).inc()
                built += 1
            LAST_RUN_TS.set(time.time())
            RUNS.labels(status="ok").inc()
            await asyncio.sleep(scan_sleep_sec)
        except Exception:
            status = "err"
            RUNS.labels(status="err").inc()
            await asyncio.sleep(min(scan_sleep_sec, 10.0))
        finally:
            LOOP_LAT.observe(time.perf_counter() - t0)


if __name__ == "__main__":
    import asyncio
    asyncio.run(main())
