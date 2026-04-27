from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import json
import os
import time
from typing import Any, Dict, Tuple

try:  # pragma: no cover
    import redis.asyncio as redis
except Exception:  # pragma: no cover
    redis = None

try:  # pragma: no cover
    import psycopg
except Exception:  # pragma: no cover
    psycopg = None

try:  # pragma: no cover
    from prometheus_client import Counter, Gauge, Histogram, start_http_server
except Exception:  # pragma: no cover
    Counter = Gauge = Histogram = None
    def start_http_server(*args: Any, **kwargs: Any) -> None:
        return None


APP_NAME = "route_incident_rca_mirror_retry_controller_v3_10"
DECISIONS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_ROLLOUT_DECISIONS_STREAM",
    "stream:ml:route_incident_rca_mirror_rollout_decisions",
)
OUTPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RETRY_RESULTS_STREAM",
    "stream:ml:route_incident_rca_mirror_retry_results",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RETRY_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_retry:last",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RETRY_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_retry_audit",
)
STATE_PREFIX = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RETRY_STATE_PREFIX",
    "state:ml:route_incident_rca_mirror_retry:",
)
SHADOW_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_SHADOW_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_shadow_handoff:global",
)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RETRY_PORT", "9927"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RETRY_MAXLEN", "20000"))
RUN_EVERY_SEC = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RETRY_RUN_EVERY_SEC", "180"))
LOOKBACK_COUNT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RETRY_LOOKBACK_COUNT", "50"))
MAX_ATTEMPTS = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RETRY_MAX_ATTEMPTS", "2"))
BACKOFF_SEC = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RETRY_BACKOFF_SEC", "120"))
STATE_TTL_SEC = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RETRY_STATE_TTL_SEC", "86400"))


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter("ml_route_incident_rca_mirror_retry_runs_total", "Mirror retry runs", ("status", "decision"))
LAT = _hist("ml_route_incident_rca_mirror_retry_latency_seconds", "Mirror retry latency seconds")
UP = _gauge("ml_route_incident_rca_mirror_retry_up", "Mirror retry up")
LAST_RUN_TS = _gauge("ml_route_incident_rca_mirror_retry_last_run_ts_seconds", "Mirror retry last run ts")


def now_ms() -> int:
    return get_ny_time_millis()


def parse_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


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


def stable_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def maybe_json(v: Any, default: Any = None) -> Any:
    if v is None:
        return default
    if isinstance(v, (dict, list)):
        return v
    try:
        return json.loads(v)
    except Exception:
        return default


async def xr_recent(client: Any, stream_key: str, count: int) -> list[Dict[str, Any]]:
    try:
        rows = await client.xrevrange(stream_key, count=count)
    except Exception:
        return []
    out = []
    for entry_id, payload in rows:
        row = as_dict(payload)
        row["_stream_id"] = entry_id.decode() if isinstance(entry_id, (bytes, bytearray)) else str(entry_id)
        out.append(row)
    return out


def actionable_decision(row: Dict[str, Any]) -> bool:
    return str(row.get("controller_decision") or "") in {"PROMOTE", "ROLLBACK"}


def event_key(row: Dict[str, Any]) -> str:
    return f"{row.get('source','?')}:{row.get('transition_type','NONE')}:{row.get('ts_ms','0')}"


async def persist_if_configured(db_url: str, result: Dict[str, Any]) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO llm_route_incident_rca_mirror_retry_results (
                    ts_ms, event_key, decision, reason_code, attempts, target_mode, result_json
                ) VALUES (
                    %(ts_ms)s, %(event_key)s, %(decision)s, %(reason_code)s, %(attempts)s, %(target_mode)s, %(result_json)s
                )
                """,
                {
                    "ts_ms": now_ms(),
                    "event_key": result["event_key"],
                    "decision": result["decision"],
                    "reason_code": result["reason_code"],
                    "attempts": result["attempts"],
                    "target_mode": result["target_mode"],
                    "result_json": json.dumps(result),
                },
            )
            conn.commit()


async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PORT)
    if UP:
        UP.set(1)
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    db_url = os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL", "")

    while True:
        started = time.perf_counter()
        status = "ok"
        decision_label = "NOOP"
        try:
            rows = await xr_recent(r, DECISIONS_STREAM, LOOKBACK_COUNT)
            target = next((r0 for r0 in rows if actionable_decision(r0)), None)
            if not target:
                await asyncio.sleep(max(RUN_EVERY_SEC, 5))
                continue

            snapshot = maybe_json(target.get("snapshot_json"), {})
            policy = snapshot.get("policy", {}) if isinstance(snapshot, dict) else {}
            advisory_only = parse_int(policy.get("advisory_only"), 1)
            executor_mode = str(policy.get("executor_mode") or "DRY_RUN").upper()
            current_mode_live = str((await r.hget(SHADOW_POLICY_KEY, "mode")) or b"AUDIT_ONLY")
            if isinstance(current_mode_live, (bytes, bytearray)):
                current_mode_live = current_mode_live.decode()
            target_mode = str(target.get("target_mode") or target.get("current_mode") or "AUDIT_ONLY").upper()

            ek = event_key(target)
            state_key = f"{STATE_PREFIX}{ek}"
            st = as_dict(await r.hgetall(state_key))
            attempts = parse_int(st.get("attempts"), 0)
            last_attempt_ts = parse_int(st.get("last_attempt_ts_ms"), 0)
            resolved = parse_int(st.get("resolved"), 0)

            result = {
                "schema_version": 1,
                "event_key": ek,
                "target_mode": target_mode,
                "attempts": attempts,
                "decision": "NOOP",
                "reason_code": "NO_ACTION",
                "ts_ms": str(now_ms()),
            }

            if resolved == 1:
                result["reason_code"] = "ALREADY_RESOLVED"
            elif advisory_only == 1 or executor_mode != "COMMIT":
                result["reason_code"] = "ADVISORY_OR_NON_COMMIT"
            elif current_mode_live == target_mode:
                result["decision"] = "RESOLVED"
                result["reason_code"] = "TARGET_MODE_ALREADY_ACTIVE"
                await r.hset(state_key, mapping={"resolved": "1", "attempts": str(attempts), "last_attempt_ts_ms": str(last_attempt_ts)})
                await r.expire(state_key, STATE_TTL_SEC)
            elif attempts >= MAX_ATTEMPTS:
                result["decision"] = "EXHAUSTED"
                result["reason_code"] = "MAX_ATTEMPTS_REACHED"
            elif last_attempt_ts > 0 and (now_ms() - last_attempt_ts) < BACKOFF_SEC * 1000:
                result["reason_code"] = "BACKOFF_ACTIVE"
            else:
                await r.hset(
                    SHADOW_POLICY_KEY,
                    mapping={
                        "mode": target_mode,
                        "last_mode_switch_ts_ms": str(now_ms()),
                        "last_mode_switch_reason_code": "MIRROR_RETRY_REAPPLY",
                        "last_mode_switch_source": APP_NAME,
                    },
                )
                attempts += 1
                await r.hset(state_key, mapping={"attempts": str(attempts), "last_attempt_ts_ms": str(now_ms()), "resolved": "0"})
                await r.expire(state_key, STATE_TTL_SEC)
                result["attempts"] = attempts
                result["decision"] = "REAPPLIED"
                result["reason_code"] = "RETRY_REAPPLY"

            await persist_if_configured(db_url, result)
            await r.xadd(OUTPUT_STREAM, result, maxlen=MAXLEN, approximate=True)
            await r.xadd(
                AUDIT_STREAM,
                {"event_type": "ROUTE_INCIDENT_RCA_MIRROR_RETRY_EVALUATED", **result},
                maxlen=MAXLEN,
                approximate=True,
            )
            await r.hset(
                LAST_HASH,
                mapping={
                    "event_key": ek,
                    "decision": result["decision"],
                    "reason_code": result["reason_code"],
                    "attempts": str(result["attempts"]),
                    "target_mode": target_mode,
                    "ts_ms": str(now_ms()),
                },
            )
            decision_label = result["decision"]
            if LAST_RUN_TS:
                LAST_RUN_TS.set(time.time())
        except Exception as exc:
            status = "error"
            await r.xadd(AUDIT_STREAM, {"event_type": "ROUTE_INCIDENT_RCA_MIRROR_RETRY_FAILED", "error": str(exc), "ts_ms": str(now_ms())}, maxlen=MAXLEN, approximate=True)
        finally:
            if RUNS:
                RUNS.labels(status=status, decision=decision_label).inc()
            if LAT:
                LAT.observe(max(time.perf_counter() - started, 0.0))
            await asyncio.sleep(max(RUN_EVERY_SEC, 5))


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
