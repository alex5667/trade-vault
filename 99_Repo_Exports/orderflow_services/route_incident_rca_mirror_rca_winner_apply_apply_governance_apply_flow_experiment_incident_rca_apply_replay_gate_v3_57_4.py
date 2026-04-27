from __future__ import annotations

import asyncio
import json
import logging
import os
import time
from typing import Any, Dict, List

try:
    import redis.asyncio as redis
except Exception:
    redis = None

try:
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
except Exception:
    Counter = Gauge = Histogram = None
    def start_http_server(*args: Any, **kwargs: Any) -> None:
        return None

APP_NAME = "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_gate_v3_57_4"
PROM = APP_NAME

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
METRICS_PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_GATE_PORT", "10001"))
MAX_REPORT_AGE_SEC = int(os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_GATE_MAX_REPORT_AGE_SEC", "7200"))
REQUIRED_ALIASES = [x.strip() for x in os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_GATE_REQUIRED_ALIASES", "slo,retry,escalation").split(",") if x.strip()]
WINDOW_START_TS_MS = int(os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_GATE_WINDOW_START_TS_MS", "0"))
WINDOW_END_TS_MS = int(os.getenv("ML_ROUTE_INCIDENT_RCA_APPLY_REPLAY_GATE_WINDOW_END_TS_MS", "0"))

REPORT_STREAM = "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_validation_reports"
DECISION_STREAM = "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_gate_decisions"
AUDIT_STREAM = "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_gate_audit"
METRICS_HASH = "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_apply_flow_experiment_incident_rca_apply_replay_gate:last"

logging.basicConfig(level=getattr(logging, os.getenv("LOG_LEVEL", "INFO").upper(), logging.INFO))
log = logging.getLogger(APP_NAME)

RUNS = Counter(f"{PROM}_runs_total", "runs", ["status", "decision"]) if Counter else None
UP = Gauge(f"{PROM}_up", "up") if Gauge else None
LAST_RUN = Gauge(f"{PROM}_last_run_ts_seconds", "last run ts") if Gauge else None
ALIASES_OK = Gauge(f"{PROM}_aliases_ok", "aliases ok count") if Gauge else None
ALIASES_REQUIRED = Gauge(f"{PROM}_aliases_required", "aliases required count") if Gauge else None
GATE_PASS = Gauge(f"{PROM}_gate_pass", "gate pass 0/1") if Gauge else None
LAT = Histogram(f"{PROM}_loop_seconds", "loop seconds") if Histogram else None

def now_ms() -> int:
    return int(time.time() * 1000)

def as_dict(fields: Dict[Any, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    for k, v in fields.items():
        kk = k.decode() if isinstance(k, (bytes, bytearray)) else str(k)
        if isinstance(v, (bytes, bytearray)):
            try:
                out[kk] = v.decode()
            except Exception:
                out[kk] = v.hex()
        else:
            out[kk] = v
    return out

def maybe_json(v: Any) -> Dict[str, Any]:
    if isinstance(v, dict):
        return v
    if isinstance(v, (bytes, bytearray)):
        v = v.decode()
    if isinstance(v, str):
        try:
            x = json.loads(v)
            if isinstance(x, dict):
                return x
        except Exception:
            return {}
    return {}

def i64(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default

def f64(v: Any, default: float = 0.0) -> float:
    try:
        return float(v)
    except Exception:
        return default

def normalize(row: Dict[str, Any]) -> Dict[str, Any]:
    payload = maybe_json(row.get("payload"))
    return payload if payload else row

def evaluate_report(report: Dict[str, Any], now_ts_ms: int) -> List[str]:
    reasons: List[str] = []
    report_ts_ms = i64(report.get("ts_ms"))
    status = str(report.get("status") or "")
    key_cov = f64(report.get("key_coverage_ratio"), 0.0)
    hash_match = i64(report.get("hash_match"), 0)
    missing_in_pg_n = i64(report.get("missing_in_pg_n"), 0)
    extra_in_pg_n = i64(report.get("extra_in_pg_n"), 0)

    if report_ts_ms <= 0:
        reasons.append("BAD_REPORT_TS")
    elif now_ts_ms - report_ts_ms > MAX_REPORT_AGE_SEC * 1000:
        reasons.append("STALE_REPORT")

    if status != "PASS":
        reasons.append(f"STATUS_{status or 'UNKNOWN'}")
    if key_cov < 1.0:
        reasons.append("KEY_COVERAGE_LT_1")
    if hash_match != 1:
        reasons.append("HASH_MISMATCH")
    if missing_in_pg_n > 0:
        reasons.append("MISSING_IN_PG")
    if extra_in_pg_n > 0:
        reasons.append("EXTRA_IN_PG")
    return reasons

async def latest_reports_for_window(r: "redis.Redis", ts_min: int, ts_max: int) -> Dict[str, Dict[str, Any]]:
    rows = await r.xrevrange(REPORT_STREAM, "+", "-", count=500)
    latest: Dict[str, Dict[str, Any]] = {}
    for _, fields in rows:
        report = normalize(as_dict(fields))
        alias = str(report.get("alias") or "")
        if alias == "" or alias in latest:
            continue
        w0 = i64(report.get("window_start_ts_ms"))
        w1 = i64(report.get("window_end_ts_ms"))
        if w0 == ts_min and w1 == ts_max:
            latest[alias] = report
        if len(latest) >= len(REQUIRED_ALIASES):
            break
    return latest

async def run_gate() -> int:
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    if WINDOW_START_TS_MS <= 0 or WINDOW_END_TS_MS <= 0 or WINDOW_END_TS_MS < WINDOW_START_TS_MS:
        raise ValueError("invalid gate window")

    start_http_server(METRICS_PORT)
    if UP:
        UP.set(1)

    r = redis.from_url(REDIS_URL, decode_responses=False)
    status = "ok"
    decision = "BLOCK"
    t0 = time.time()
    try:
        now_ts_ms = now_ms()
        reports = await latest_reports_for_window(r, WINDOW_START_TS_MS, WINDOW_END_TS_MS)
        gate_reasons: List[str] = []
        alias_results: Dict[str, Dict[str, Any]] = {}
        ok_aliases = 0

        for alias in REQUIRED_ALIASES:
            rep = reports.get(alias)
            if rep is None:
                alias_results[alias] = {"decision": "BLOCK", "reasons": ["MISSING_REPORT"]}
                gate_reasons.append(f"{alias}:MISSING_REPORT")
                continue
            reasons = evaluate_report(rep, now_ts_ms)
            if not reasons:
                ok_aliases += 1
                alias_results[alias] = {"decision": "PASS", "reasons": []}
            else:
                alias_results[alias] = {"decision": "BLOCK", "reasons": reasons}
                gate_reasons.extend([f"{alias}:{x}" for x in reasons])

        decision = "PASS" if ok_aliases == len(REQUIRED_ALIASES) else "BLOCK"
        payload = {
            "schema_version": 1,
            "app_name": APP_NAME,
            "ts_ms": now_ts_ms,
            "window_start_ts_ms": WINDOW_START_TS_MS,
            "window_end_ts_ms": WINDOW_END_TS_MS,
            "required_aliases": REQUIRED_ALIASES,
            "aliases_ok": ok_aliases,
            "aliases_required": len(REQUIRED_ALIASES),
            "decision": decision,
            "gate_reasons": gate_reasons,
            "alias_results": alias_results,
        }

        blob = json.dumps(payload, ensure_ascii=False, sort_keys=True)
        await r.xadd(DECISION_STREAM, {"payload": blob}, maxlen=50000, approximate=True)
        await r.xadd(AUDIT_STREAM, {"payload": blob, "event": "replay_gate_complete"}, maxlen=50000, approximate=True)
        await r.hset(
            METRICS_HASH,
            mapping={k: json.dumps(v, ensure_ascii=False, sort_keys=True) if isinstance(v, (list, dict)) else str(v) for k, v in payload.items()},
        )

        if ALIASES_OK:
            ALIASES_OK.set(ok_aliases)
        if ALIASES_REQUIRED:
            ALIASES_REQUIRED.set(len(REQUIRED_ALIASES))
        if GATE_PASS:
            GATE_PASS.set(1 if decision == "PASS" else 0)
        if LAST_RUN:
            LAST_RUN.set(time.time())

        log.info("replay_gate decision=%s aliases_ok=%s aliases_required=%s reasons=%s",
                 decision, ok_aliases, len(REQUIRED_ALIASES), ",".join(gate_reasons))
        return 0 if decision == "PASS" else 2
    except Exception:
        status = "error"
        decision = "ERROR"
        log.exception("replay_gate failed")
        return 3
    finally:
        if RUNS:
            RUNS.labels(status=status, decision=decision).inc()
        if LAT:
            LAT.observe(max(0.0, time.time() - t0))
        if UP:
            UP.set(0)
        await r.close()

def main() -> None:
    raise SystemExit(asyncio.run(run_gate()))

if __name__ == "__main__":
    main()
