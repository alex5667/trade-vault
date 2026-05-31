from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any

from core.redis_keys import RedisKeyPrefixes as RK
from utils.time_utils import get_ny_time_millis

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


APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_feedback_governor_v3_37"
FEEDBACK_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_FEEDBACK_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_feedback",
)
ROLLUPS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_FEEDBACK_ROLLUPS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_feedback_rollups",
)
DECISIONS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_GOVERNANCE_DECISIONS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_governance_decisions",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_GOVERNANCE_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_governance_audit",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_GOVERNANCE_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_governance:last",
)
BRIDGE_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_RCA_BRIDGE_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_rca_bridge:global",
)
GLOBAL_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_GOVERNANCE_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_rca_governance:global",
)
GROUP = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_GOVERNANCE_GROUP", APP_NAME)
CONSUMER = os.getenv("HOSTNAME", APP_NAME)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_GOVERNANCE_PORT", "9965"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_GOVERNANCE_MAXLEN", "20000"))
LOOKBACK_COUNT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_GOVERNANCE_LOOKBACK_COUNT", "200"))
WINDOW_MIN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_GOVERNANCE_WINDOW_MIN", "10080"))

DEFAULT_MIN_SAMPLES = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_GOVERNANCE_MIN_SAMPLES", "10"))
DEFAULT_MIN_AVG_QUALITY = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_GOVERNANCE_MIN_AVG_QUALITY", "0.55"))
DEFAULT_MIN_AVG_USEFULNESS = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_GOVERNANCE_MIN_AVG_USEFULNESS", "0.60"))
DEFAULT_MIN_ACCEPTED_RATE = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_GOVERNANCE_MIN_ACCEPTED_RATE", "0.60"))
DEFAULT_MAX_LOW_QUALITY_RATE = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_GOVERNANCE_MAX_LOW_QUALITY_RATE", "0.35"))
DEFAULT_ADVISORY_ONLY = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_GOVERNANCE_ADVISORY_ONLY", "1"))
DEFAULT_EXECUTOR_MODE = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_RCA_GOVERNANCE_EXECUTOR_MODE", "DRY_RUN").upper()


def _counter(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_governance_runs_total",
    "Winner-apply apply governance vertex governance runs",
    ("status", "decision"),
)
LAT = _hist(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_governance_latency_seconds",
    "Winner-apply apply governance vertex governance latency seconds",
)
UP = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_governance_up",
    "Winner-apply apply governance vertex governance up",
)
LAST_RUN_TS = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_governance_last_run_ts_seconds",
    "Winner-apply apply governance vertex governance last run timestamp",
)
AVG_QUALITY = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_governance_avg_quality",
    "Winner-apply apply governance vertex governance avg quality",
)
AVG_USEFULNESS = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_governance_avg_usefulness",
    "Winner-apply apply governance vertex governance avg usefulness",
)
ACCEPTED_RATE = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_governance_vertex_governance_accepted_rate",
    "Winner-apply apply governance vertex governance accepted rate",
)


def now_ms() -> int:
    return get_ny_time_millis()


def parse_int(value: Any, default: int = 0) -> int:
    try:
        return int(float(value))
    except Exception:
        return default


def parse_float(value: Any, default: float = 0.0) -> float:
    try:
        return float(value)
    except Exception:
        return default


def stable_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


def as_dict(fields: dict[Any, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
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


def policy_from_hash(raw: dict[str, Any]) -> dict[str, Any]:
    return {
        "min_samples": parse_int(raw.get("min_samples"), DEFAULT_MIN_SAMPLES),
        "min_avg_quality": parse_float(raw.get("min_avg_quality"), DEFAULT_MIN_AVG_QUALITY),
        "min_avg_usefulness": parse_float(raw.get("min_avg_usefulness"), DEFAULT_MIN_AVG_USEFULNESS),
        "min_accepted_rate": parse_float(raw.get("min_accepted_rate"), DEFAULT_MIN_ACCEPTED_RATE),
        "max_low_quality_rate": parse_float(raw.get("max_low_quality_rate"), DEFAULT_MAX_LOW_QUALITY_RATE),
        "advisory_only": parse_int(raw.get("advisory_only"), DEFAULT_ADVISORY_ONLY),
        "executor_mode": str(raw.get("executor_mode") or DEFAULT_EXECUTOR_MODE).upper(),
    }


async def ensure_group(client: Any, stream_key: str, group: str) -> None:
    try:
        await client.xgroup_create(stream_key, group, id="$", mkstream=True)
    except Exception:
        return


async def xr_recent(client: Any, stream_key: str, count: int) -> list[dict[str, Any]]:
    try:
        rows = await client.xrevrange(stream_key, count=count)
    except Exception:
        return []
    out: list[dict[str, Any]] = []
    for entry_id, payload in rows:
        row = as_dict(payload)
        row["_stream_id"] = entry_id.decode() if isinstance(entry_id, (bytes, bytearray)) else str(entry_id)
        out.append(row)
    return out


def rollup_feedback(rows: list[dict[str, Any]]) -> dict[str, Any]:
    cutoff = now_ms() - WINDOW_MIN * 60 * 1000
    recent = [r for r in rows if parse_int(r.get("ts_ms"), 0) >= cutoff]
    n = len(recent)
    if n == 0:
        return {
            "n": 0,
            "avg_quality": 0.0,
            "avg_usefulness": 0.0,
            "accepted_rate": 0.0,
            "low_quality_rate": 0.0,
        }
    q = [parse_float(r.get("quality_score"), 0.0) for r in recent]
    u = [parse_float(r.get("usefulness_score"), 0.0) for r in recent]
    acc = [parse_int(r.get("accepted"), 0) for r in recent]
    low_q = [1 if parse_float(r.get("quality_score"), 0.0) < 0.5 else 0 for r in recent]
    return {
        "n": n,
        "avg_quality": round(sum(q) / n, 6),
        "avg_usefulness": round(sum(u) / n, 6),
        "accepted_rate": round(sum(acc) / n, 6),
        "low_quality_rate": round(sum(low_q) / n, 6),
    }


def evaluate_governance(rollup: dict[str, Any], policy: dict[str, Any]) -> dict[str, Any]:
    n = int(rollup["n"])
    if n < policy["min_samples"]:
        return {"decision": "HOLD", "reason_code": "INSUFFICIENT_SAMPLES", "target_bridge_mode": "AUTO"}
    if (
        rollup["avg_quality"] < policy["min_avg_quality"]
        or rollup["avg_usefulness"] < policy["min_avg_usefulness"]
        or rollup["accepted_rate"] < policy["min_accepted_rate"]
        or rollup["low_quality_rate"] > policy["max_low_quality_rate"]
    ):
        return {"decision": "PREFER_LOCAL_ONLY", "reason_code": "QUALITY_USEFULNESS_BELOW_THRESHOLD", "target_bridge_mode": "LOCAL_ONLY"}
    return {"decision": "KEEP_AUTO", "reason_code": "QUALITY_STABLE", "target_bridge_mode": "AUTO"}


async def persist_if_configured(db_url: str, feedback_row: dict[str, Any] | None, rollup: dict[str, Any], decision: dict[str, Any]) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            if feedback_row is not None:
                cur.execute(
                    """

                    INSERT INTO llm_governance_vertex_rca_feedback (
                        request_id, bundle_id, ts_ms, quality_score, usefulness_score, accepted, reason_code, feedback_json
                    ) VALUES (
                        %(request_id)s, %(bundle_id)s, %(ts_ms)s, %(quality_score)s, %(usefulness_score)s, %(accepted)s, %(reason_code)s, %(feedback_json)s
                    )
                    """,
                    {
                        "request_id": feedback_row.get("request_id", ""),
                        "bundle_id": feedback_row.get("bundle_id", ""),
                        "ts_ms": parse_int(feedback_row.get("ts_ms"), now_ms()),
                        "quality_score": parse_float(feedback_row.get("quality_score"), 0.0),
                        "usefulness_score": parse_float(feedback_row.get("usefulness_score"), 0.0),
                        "accepted": parse_int(feedback_row.get("accepted"), 0),
                        "reason_code": feedback_row.get("reason_code", ""),
                        "feedback_json": json.dumps(feedback_row),
                    }
                )
            cur.execute(
                """

                INSERT INTO llm_governance_vertex_rca_feedback_rollups (
                    ts_ms, window_min, n, avg_quality, avg_usefulness, accepted_rate, low_quality_rate, rollup_json
                ) VALUES (
                    %(ts_ms)s, %(window_min)s, %(n)s, %(avg_quality)s, %(avg_usefulness)s, %(accepted_rate)s, %(low_quality_rate)s, %(rollup_json)s
                )
                """,
                {
                    "ts_ms": now_ms(),
                    "window_min": WINDOW_MIN,
                    **rollup,
                    "rollup_json": json.dumps(rollup),
                }
            )
            cur.execute(
                """

                INSERT INTO llm_governance_vertex_rca_governance_decisions (
                    ts_ms, decision, reason_code, target_bridge_mode, decision_json
                ) VALUES (
                    %(ts_ms)s, %(decision)s, %(reason_code)s, %(target_bridge_mode)s, %(decision_json)s
                )
                """,
                {
                    "ts_ms": now_ms(),
                    "decision": decision["decision"],
                    "reason_code": decision["reason_code"],
                    "target_bridge_mode": decision["target_bridge_mode"],
                    "decision_json": json.dumps({"rollup": rollup, "decision": decision}),
                }
            )
            conn.commit()


async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PORT)
    if UP:
        UP.set(1)
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    await ensure_group(r, FEEDBACK_STREAM, GROUP)
    db_url = os.getenv("DATABASE_URL", "")

    while True:
        rows = await r.xreadgroup(GROUP, CONSUMER, {FEEDBACK_STREAM: ">"}, count=32, block=5000)
        if not rows:
            continue
        for _stream, messages in rows:
            for msg_id, payload in messages:
                started = time.perf_counter()
                status = "ok"
                decision_label = "HOLD"
                try:
                    feedback = as_dict(payload)
                    recent = await xr_recent(r, FEEDBACK_STREAM, LOOKBACK_COUNT)
                    rollup = rollup_feedback(recent + [feedback])
                    policy = policy_from_hash(as_dict(await r.hgetall(GLOBAL_POLICY_KEY)))
                    try:
                        exec_kill = await r.get(RK.EXEC_KILL_SWITCH)
                        if exec_kill and exec_kill.decode().strip() == '1':
                            policy['kill_switch'] = 1
                    except Exception: pass
                    decision = evaluate_governance(rollup, policy)
                    decision_label = decision["decision"]

                    if decision["decision"] == "PREFER_LOCAL_ONLY" and policy["advisory_only"] == 0 and policy["executor_mode"] == "COMMIT":
                        await r.hset(BRIDGE_POLICY_KEY, mapping={"mode": "LOCAL_ONLY", "last_mode_switch_source": APP_NAME, "last_mode_switch_reason_code": decision["reason_code"]})

                    await persist_if_configured(db_url, feedback, rollup, decision)
                    await r.xadd(
                        ROLLUPS_STREAM,
                        {"schema_version": 1, "rollup_json": stable_json(rollup), "ts_ms": str(now_ms())},
                        maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.xadd(
                        DECISIONS_STREAM,
                        {
                            "schema_version": 1,
                            "decision": decision["decision"],
                            "reason_code": decision["reason_code"],
                            "target_bridge_mode": decision["target_bridge_mode"],
                            "rollup_json": stable_json(rollup),
                            "ts_ms": str(now_ms()),
                        }, maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.hset(
                        LAST_HASH,
                        mapping={
                            "decision": decision["decision"],
                            "reason_code": decision["reason_code"],
                            "avg_quality": str(rollup["avg_quality"]),
                            "avg_usefulness": str(rollup["avg_usefulness"]),
                            "accepted_rate": str(rollup["accepted_rate"]),
                            "low_quality_rate": str(rollup["low_quality_rate"]),
                            "n": str(rollup["n"]),
                            "ts_ms": str(now_ms()),
                        }
                    )
                    await r.xadd(
                        AUDIT_STREAM,
                        {
                            "event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_GOVERNANCE_DECIDED",
                            "decision": decision["decision"],
                            "reason_code": decision["reason_code"],
                            "ts_ms": str(now_ms()),
                        }, maxlen=MAXLEN,
                        approximate=True,
                    )
                    if AVG_QUALITY:
                        AVG_QUALITY.set(rollup["avg_quality"])
                    if AVG_USEFULNESS:
                        AVG_USEFULNESS.set(rollup["avg_usefulness"])
                    if ACCEPTED_RATE:
                        ACCEPTED_RATE.set(rollup["accepted_rate"])
                    await r.xack(FEEDBACK_STREAM, GROUP, msg_id)
                    if LAST_RUN_TS:
                        LAST_RUN_TS.set(time.time())
                except Exception as exc:
                    status = "error"
                    await r.xadd(
                        AUDIT_STREAM,
                        {"event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_GOVERNANCE_VERTEX_GOVERNANCE_FAILED", "error": str(exc), "ts_ms": str(now_ms())},
                        maxlen=MAXLEN,
                        approximate=True,
                    )
                    await r.xack(FEEDBACK_STREAM, GROUP, msg_id)
                finally:
                    if RUNS:
                        RUNS.labels(status=status, decision=decision_label).inc()
                    if LAT:
                        LAT.observe(max(time.perf_counter() - started, 0.0))


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
