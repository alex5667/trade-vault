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


APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_governance_retry_controller_v3_42"
VERIFICATION_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERIFICATION_RESULTS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_verification_results",
)
OUTPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RETRY_RESULTS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_retry_results",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RETRY_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_retry:last",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RETRY_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_retry_audit",
)
STATE_PREFIX = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RETRY_STATE_PREFIX",
    "state:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_retry:",
)
EXPERIMENT_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_EXPERIMENT_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_experiment:global",
)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RETRY_PORT", "9971"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RETRY_MAXLEN", "20000"))
RUN_EVERY_SEC = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RETRY_RUN_EVERY_SEC", "180"))
LOOKBACK_COUNT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RETRY_LOOKBACK_COUNT", "50"))
MAX_ATTEMPTS = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RETRY_MAX_ATTEMPTS", "2"))
BACKOFF_SEC = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RETRY_BACKOFF_SEC", "120"))
STATE_TTL_SEC = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RETRY_STATE_TTL_SEC", "86400"))


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter("ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_retry_runs_total", "Governance retry runs", ("status", "decision"))
LAT = _hist("ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_retry_latency_seconds", "Governance retry latency seconds")
UP = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_retry_up", "Governance retry up")
LAST_RUN_TS = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_retry_last_run_ts_seconds", "Governance retry last run ts")


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


def actionable_verification(row: Dict[str, Any]) -> bool:
    return str(row.get("decision") or "") == "ROLLBACK_PREVIOUS_POLICY"


def event_key(row: Dict[str, Any]) -> str:
    return f"{row.get('reason_code','?')}:{row.get('rollback_mode','?')}:{row.get('rollback_primary_arm','?')}:{row.get('ts_ms','0')}"


async def persist_if_configured(db_url: str, result: Dict[str, Any]) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """,
                INSERT INTO llm_governance_retry_results (
                    ts_ms, event_key, decision, reason_code, attempts, rollback_mode, rollback_primary_arm, result_json
                ) VALUES (
                    %(ts_ms)s, %(event_key)s, %(decision)s, %(reason_code)s, %(attempts)s, %(rollback_mode)s, %(rollback_primary_arm)s, %(result_json)s
                )
                """,
                {
                    "ts_ms": now_ms(),
                    "event_key": result["event_key"],
                    "decision": result["decision"],
                    "reason_code": result["reason_code"],
                    "attempts": result["attempts"],
                    "rollback_mode": result["rollback_mode"],
                    "rollback_primary_arm": result["rollback_primary_arm"],
                    "result_json": json.dumps(result),
                },
            )
            conn.commit()


def reconstruct_shadow_arms(mode: str, primary_arm: str) -> str:
    if mode == "SINGLE_ARM":
        return "[]"
    return json.dumps([a for a in ("deterministic", "vertex_candidate", "local_fallback_candidate") if a != primary_arm])


async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PORT)
    if UP:
        UP.set(1)
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    db_url = os.getenv("DATABASE_URL", "")

    while True:
        started = time.perf_counter()
        status = "ok"
        decision_label = "NOOP"
        try:
            rows = await xr_recent(r, VERIFICATION_STREAM, LOOKBACK_COUNT)
            target = next((r0 for r0 in rows if actionable_verification(r0)), None)
            if not target:
                await asyncio.sleep(max(RUN_EVERY_SEC, 5))
                continue

            rollback_mode = str(target.get("rollback_mode") or "SHADOW")
            rollback_primary = str(target.get("rollback_primary_arm") or "deterministic")
            current_mode_raw = await r.hget(EXPERIMENT_POLICY_KEY, "mode")
            current_primary_raw = await r.hget(EXPERIMENT_POLICY_KEY, "primary_arm")
            current_mode = current_mode_raw.decode() if isinstance(current_mode_raw, (bytes, bytearray)) else str(current_mode_raw or "SHADOW")
            current_primary = current_primary_raw.decode() if isinstance(current_primary_raw, (bytes, bytearray)) else str(current_primary_raw or "deterministic")

            ek = event_key(target)
            state_key = f"{STATE_PREFIX}{ek}"
            st = as_dict(await r.hgetall(state_key))
            attempts = parse_int(st.get("attempts"), 0)
            last_attempt_ts = parse_int(st.get("last_attempt_ts_ms"), 0)
            resolved = parse_int(st.get("resolved"), 0)

            result = {
                "schema_version": 1,
                "event_key": ek,
                "rollback_mode": rollback_mode,
                "rollback_primary_arm": rollback_primary,
                "attempts": attempts,
                "decision": "NOOP",
                "reason_code": "NO_ACTION",
                "ts_ms": str(now_ms()),
            },

            if resolved == 1:
                result["reason_code"] = "ALREADY_RESOLVED"
            elif current_mode == rollback_mode and current_primary == rollback_primary:
                result["decision"] = "RESOLVED"
                result["reason_code"] = "ROLLBACK_TARGET_ALREADY_ACTIVE"
                await r.hset(state_key, mapping={"resolved": "1", "attempts": str(attempts), "last_attempt_ts_ms": str(last_attempt_ts)})
                await r.expire(state_key, STATE_TTL_SEC)
            elif attempts >= MAX_ATTEMPTS:
                result["decision"] = "EXHAUSTED"
                result["reason_code"] = "MAX_ATTEMPTS_REACHED"
            elif last_attempt_ts > 0 and (now_ms() - last_attempt_ts) < BACKOFF_SEC * 1000:
                result["reason_code"] = "BACKOFF_ACTIVE"
            else:
                await r.hset(
                    EXPERIMENT_POLICY_KEY,
                    mapping={
                        "mode": rollback_mode,
                        "primary_arm": rollback_primary,
                        "shadow_arms_json": reconstruct_shadow_arms(rollback_mode, rollback_primary),
                        "last_mode_switch_ts_ms": str(now_ms()),
                        "last_mode_switch_reason_code": "GOVERNANCE_RETRY_REAPPLY",
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
                {"event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RETRY_EVALUATED", **result},
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
                    "rollback_mode": rollback_mode,
                    "rollback_primary_arm": rollback_primary,
                    "ts_ms": str(now_ms()),
                },
            )
            decision_label = result["decision"]
            if LAST_RUN_TS:
                LAST_RUN_TS.set(time.time())
        except Exception as exc:
            status = "error"
            await r.xadd(AUDIT_STREAM, {"event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RETRY_FAILED", "error": str(exc), "ts_ms": str(now_ms())}, maxlen=MAXLEN, approximate=True)
        finally:
            if RUNS:
                RUNS.labels(status=status, decision=decision_label).inc()
            if LAT:
                LAT.observe(max(time.perf_counter() - started, 0.0))
            await asyncio.sleep(max(RUN_EVERY_SEC, 5))


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
