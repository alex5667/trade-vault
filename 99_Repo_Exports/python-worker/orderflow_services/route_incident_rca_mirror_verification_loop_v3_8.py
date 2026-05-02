from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import json
import os
import time
from typing import Any, Dict, List, Tuple

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


APP_NAME = "route_incident_rca_mirror_verification_loop_v3_8"
INPUT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_SHADOW_COMPARATOR_RESULTS_STREAM",
    "stream:ml:route_incident_rca_shadow_comparator_results",
)
RESULTS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_RESULTS_STREAM",
    "stream:ml:route_incident_rca_mirror_verification_results",
)
ROLLBACK_JOURNAL_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_ROLLBACK_JOURNAL_STREAM",
    "stream:ml:route_incident_rca_mirror_rollback_journal",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_verification_audit",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_verification:last",
)
COMPARATOR_LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_SHADOW_COMPARATOR_LAST_HASH",
    "metrics:ml:route_incident_rca_shadow_comparator:last",
)
SHADOW_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_SHADOW_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_shadow_handoff:global",
)
VERIFICATION_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_verification:global",
)
PENDING_PREFIX = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_SHADOW_COMPARATOR_PENDING_PREFIX",
    "state:ml:route_incident_rca_shadow_comparator:pending:",
)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_PORT", "9924"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_MAXLEN", "20000"))
RUN_EVERY_SEC = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_RUN_EVERY_SEC", "300"))
WINDOW_MIN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_WINDOW_MIN", "240"))
LOOKBACK_COUNT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_LOOKBACK_COUNT", "500"))

DEFAULT_ADVISORY_ONLY = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_ADVISORY_ONLY", "1"))
DEFAULT_EXECUTOR_MODE = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_EXECUTOR_MODE", "DRY_RUN").upper()
DEFAULT_MIN_SAMPLE = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_MIN_SAMPLE", "20"))
DEFAULT_MAX_MISMATCH_RATE = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_MAX_MISMATCH_RATE", "0.00"))
DEFAULT_MAX_DRIFT_RATE = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_MAX_DRIFT_RATE", "0.25"))
DEFAULT_MIN_MATCH_RATE = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_MIN_MATCH_RATE", "0.65"))
DEFAULT_MAX_PENDING_TOTAL = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_MAX_PENDING_TOTAL", "10"))
DEFAULT_MAX_COMPARATOR_AGE_MS = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_MAX_COMPARATOR_AGE_MS", "1800000"))
DEFAULT_ROLLBACK_COOLDOWN_SEC = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_ROLLBACK_COOLDOWN_SEC", "21600"))

# Database mapping convention
DB_URL = os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL", "")


def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_route_incident_rca_mirror_verification_runs_total",
    "Route incident RCA mirror verification loop runs",
    ("status", "decision"),
)
LAT = _hist(
    "ml_route_incident_rca_mirror_verification_latency_seconds",
    "Route incident RCA mirror verification loop latency seconds",
)
UP = _gauge(
    "ml_route_incident_rca_mirror_verification_up",
    "Route incident RCA mirror verification loop up",
)
LAST_RUN_TS = _gauge(
    "ml_route_incident_rca_mirror_verification_last_run_ts_seconds",
    "Route incident RCA mirror verification loop last run timestamp",
)
MATCH_RATE = _gauge(
    "ml_route_incident_rca_mirror_verification_match_rate",
    "Latest route incident RCA post-switch match rate",
)
DRIFT_RATE = _gauge(
    "ml_route_incident_rca_mirror_verification_drift_rate",
    "Latest route incident RCA post-switch drift rate",
)
MISMATCH_RATE = _gauge(
    "ml_route_incident_rca_mirror_verification_mismatch_rate",
    "Latest route incident RCA post-switch mismatch rate",
)
PENDING_TOTAL = _gauge(
    "ml_route_incident_rca_mirror_verification_pending_total",
    "Latest route incident RCA comparator pending total",
)
ROLLBACKS = _counter(
    "ml_route_incident_rca_mirror_rollbacks_total",
    "Route incident RCA mirror rollback executions",
    ("mode", "reason_code"),
)


def now_ms() -> int:
    return get_ny_time_millis()


def stable_json(obj: Any) -> str:
    return json.dumps(obj, ensure_ascii=False, sort_keys=True, separators=(",", ":"))


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


def policy_from_hash(raw: Dict[str, Any]) -> Dict[str, Any]:
    return {
        "advisory_only": parse_int(raw.get("advisory_only"), DEFAULT_ADVISORY_ONLY),
        "executor_mode": str(raw.get("executor_mode") or DEFAULT_EXECUTOR_MODE).upper(),
        "min_sample": parse_int(raw.get("min_sample"), DEFAULT_MIN_SAMPLE),
        "max_mismatch_rate": parse_float(raw.get("max_mismatch_rate"), DEFAULT_MAX_MISMATCH_RATE),
        "max_drift_rate": parse_float(raw.get("max_drift_rate"), DEFAULT_MAX_DRIFT_RATE),
        "min_match_rate": parse_float(raw.get("min_match_rate"), DEFAULT_MIN_MATCH_RATE),
        "max_pending_total": parse_int(raw.get("max_pending_total"), DEFAULT_MAX_PENDING_TOTAL),
        "max_comparator_age_ms": parse_int(raw.get("max_comparator_age_ms"), DEFAULT_MAX_COMPARATOR_AGE_MS),
        "rollback_cooldown_sec": parse_int(raw.get("rollback_cooldown_sec"), DEFAULT_ROLLBACK_COOLDOWN_SEC),
    }


def shadow_mode_from_hash(raw: Dict[str, Any]) -> str:
    return str(raw.get("mode") or "AUDIT_ONLY").upper()


async def xr_recent(client: Any, stream_key: str, count: int) -> List[Dict[str, Any]]:
    try:
        rows = await client.xrevrange(stream_key, count=count)
    except Exception:
        return []
    out: List[Dict[str, Any]] = []
    for entry_id, payload in rows:
        row = as_dict(payload)
        row["_stream_id"] = entry_id.decode() if isinstance(entry_id, (bytes, bytearray)) else str(entry_id)
        out.append(row)
    return out


async def pending_total_from_keys(r: Any, prefix: str) -> int:
    """Count pending keys using SCAN instead of KEYS to avoid blocking the event loop.

    KEYS on a large keyspace stalls Redis for 40+ ms.  async scan_iter
    with a small count drip-releases between batches.
    """,
    try:
        handoff_n = 0
        async for _ in r.scan_iter(f"{prefix}handoff:*", count=5000):
            handoff_n += 1
        legacy_n = 0
        async for _ in r.scan_iter(f"{prefix}legacy:*", count=5000):
            legacy_n += 1
        return handoff_n + legacy_n
    except Exception:
        return 0


def summarize_window(rows: List[Dict[str, Any]], window_min: int) -> Dict[str, Any]:
    cutoff = now_ms() - window_min * 60 * 1000
    selected = [r for r in rows if parse_int(r.get("ts_ms"), 0) >= cutoff]
    total = len(selected)
    match_n = sum(1 for r in selected if str(r.get("status") or "") == "MATCH")
    drift_n = sum(1 for r in selected if str(r.get("status") or "") == "DRIFT")
    mismatch_n = sum(1 for r in selected if str(r.get("status") or "") == "MISMATCH")
    return {
        "total": total,
        "match_n": match_n,
        "drift_n": drift_n,
        "mismatch_n": mismatch_n,
        "match_rate": (match_n / total) if total else 0.0,
        "drift_rate": (drift_n / total) if total else 0.0,
        "mismatch_rate": (mismatch_n / total) if total else 0.0,
    }


def evaluate_verification(
    *,
    current_mode: str,
    comparator_age_ms: int,
    pending_total: int,
    window_stats: Dict[str, Any],
    policy: Dict[str, Any],
    last_switch_ts_ms: int,
    now_ts_ms: int,
) -> Dict[str, Any]:
    total = int(window_stats["total"])
    match_rate = float(window_stats["match_rate"])
    drift_rate = float(window_stats["drift_rate"])
    mismatch_rate = float(window_stats["mismatch_rate"])
    rollback_cooldown_active = last_switch_ts_ms > 0 and (now_ts_ms - last_switch_ts_ms) < policy["rollback_cooldown_sec"] * 1000

    degraded = (
        comparator_age_ms > policy["max_comparator_age_ms"]
        or pending_total > policy["max_pending_total"]
        or total < policy["min_sample"]
        or mismatch_rate > policy["max_mismatch_rate"]
        or drift_rate > policy["max_drift_rate"]
        or match_rate < policy["min_match_rate"]
    )

    out = {
        "decision": "HOLD",
        "reason_code": "NO_CHANGE",
        "current_mode": current_mode,
        "target_mode": current_mode,
        "degraded": 1 if degraded else 0,
        "rollback_cooldown_active": 1 if rollback_cooldown_active else 0,
    }

    if current_mode != "MIRROR":
        out["decision"] = "HOLD"
        out["reason_code"] = "NOT_IN_MIRROR"
        return out

    if rollback_cooldown_active:
        out["decision"] = "KEEP_MIRROR"
        out["reason_code"] = "ROLLBACK_COOLDOWN_ACTIVE"
        return out

    if degraded:
        if comparator_age_ms > policy["max_comparator_age_ms"]:
            reason_code = "COMPARATOR_STALE"
        elif pending_total > policy["max_pending_total"]:
            reason_code = "PENDING_TOO_HIGH"
        elif total < policy["min_sample"]:
            reason_code = "INSUFFICIENT_POST_SWITCH_SAMPLE"
        elif mismatch_rate > policy["max_mismatch_rate"]:
            reason_code = "MISMATCH_RATE_TOO_HIGH"
        elif drift_rate > policy["max_drift_rate"]:
            reason_code = "DRIFT_RATE_TOO_HIGH"
        else:
            reason_code = "MATCH_RATE_TOO_LOW"
        out["decision"] = "ROLLBACK_TO_AUDIT"
        out["reason_code"] = reason_code
        out["target_mode"] = "AUDIT_ONLY"
        return out

    out["decision"] = "KEEP_MIRROR"
    out["reason_code"] = "POST_SWITCH_STABLE"
    return out


async def persist_if_configured(
    db_url: str,
    decision: Dict[str, Any],
    snapshot: Dict[str, Any],
) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """

                INSERT INTO llm_route_incident_rca_mirror_verification_results (
                    ts_ms,
                    current_mode,
                    target_mode,
                    decision,
                    reason_code,
                    advisory_only,
                    executor_mode,
                    snapshot_json
                ) VALUES (
                    %(ts_ms)s,
                    %(current_mode)s,
                    %(target_mode)s,
                    %(decision)s,
                    %(reason_code)s,
                    %(advisory_only)s,
                    %(executor_mode)s,
                    %(snapshot_json)s
                )
                """,
                {
                    "ts_ms": now_ms(),
                    "current_mode": decision["current_mode"],
                    "target_mode": decision["target_mode"],
                    "decision": decision["decision"],
                    "reason_code": decision["reason_code"],
                    "advisory_only": snapshot["advisory_only"],
                    "executor_mode": snapshot["executor_mode"],
                    "snapshot_json": json.dumps(snapshot),
                }
            )
            if decision["decision"] == "ROLLBACK_TO_AUDIT":
                cur.execute(
                    """

                    INSERT INTO llm_route_incident_rca_mirror_rollback_journal (
                        ts_ms,
                        reason_code,
                        mode_before,
                        mode_after,
                        snapshot_json
                    ) VALUES (
                        %(ts_ms)s,
                        %(reason_code)s,
                        %(mode_before)s,
                        %(mode_after)s,
                        %(snapshot_json)s
                    )
                    """,
                    {
                        "ts_ms": now_ms(),
                        "reason_code": decision["reason_code"],
                        "mode_before": decision["current_mode"],
                        "mode_after": decision["target_mode"],
                        "snapshot_json": json.dumps(snapshot),
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
    db_url = DB_URL

    while True:
        started = time.perf_counter()
        status = "ok"
        decision_label = "HOLD"
        try:
            policy = policy_from_hash(as_dict(await r.hgetall(VERIFICATION_POLICY_KEY)))
            try:
                exec_kill = await r.get('trade:exec_kill_switch')
                if exec_kill and exec_kill.decode().strip() == '1':
                    policy['kill_switch'] = 1
            except: pass
            shadow_policy = as_dict(await r.hgetall(SHADOW_POLICY_KEY))
            current_mode = shadow_mode_from_hash(shadow_policy)
            last_switch_ts_ms = parse_int(shadow_policy.get("last_mode_switch_ts_ms"), 0)

            comparator_last = as_dict(await r.hgetall(COMPARATOR_LAST_HASH))
            comparator_age_ms = now_ms() - parse_int(comparator_last.get("ts_ms"), 0) if comparator_last else 10**12
            pending_total = await pending_total_from_keys(r, PENDING_PREFIX)
            rows = await xr_recent(r, INPUT_STREAM, LOOKBACK_COUNT)
            window_stats = summarize_window(rows, WINDOW_MIN)

            decision = evaluate_verification(
                current_mode=current_mode,
                comparator_age_ms=comparator_age_ms,
                pending_total=pending_total,
                window_stats=window_stats,
                policy=policy,
                last_switch_ts_ms=last_switch_ts_ms,
                now_ts_ms=now_ms(),
            )
            decision_label = decision["decision"]

            snapshot = {
                "window_min": WINDOW_MIN,
                "current_mode": current_mode,
                "comparator_age_ms": comparator_age_ms,
                "pending_total": pending_total,
                "window_stats": window_stats,
                "advisory_only": policy["advisory_only"],
                "executor_mode": policy["executor_mode"],
                "policy": policy,
            }

            if (
                decision["decision"] == "ROLLBACK_TO_AUDIT"
                and policy["advisory_only"] == 0
                and policy["executor_mode"] == "COMMIT"
            ):
                await r.hset(
                    SHADOW_POLICY_KEY,
                    mapping={
                        "mode": "AUDIT_ONLY",
                        "last_mode_switch_ts_ms": str(now_ms()),
                        "last_mode_switch_reason_code": decision["reason_code"],
                        "last_mode_switch_source": APP_NAME,
                    }
                )
                await r.xadd(
                    ROLLBACK_JOURNAL_STREAM,
                    {
                        "schema_version": 1,
                        "decision": decision["decision"],
                        "reason_code": decision["reason_code"],
                        "mode_before": current_mode,
                        "mode_after": "AUDIT_ONLY",
                        "snapshot_json": stable_json(snapshot),
                        "ts_ms": str(now_ms()),
                    }, maxlen=MAXLEN,
                    approximate=True,
                )
                if ROLLBACKS:
                    ROLLBACKS.labels(mode=policy["executor_mode"], reason_code=decision["reason_code"]).inc()

            await persist_if_configured(db_url, decision, snapshot)

            out = {
                "schema_version": 1,
                "decision": decision["decision"],
                "reason_code": decision["reason_code"],
                "current_mode": decision["current_mode"],
                "target_mode": decision["target_mode"],
                "snapshot_json": stable_json(snapshot),
                "ts_ms": str(now_ms()),
            }
            await r.xadd(RESULTS_STREAM, out, maxlen=MAXLEN, approximate=True)
            await r.xadd(
                AUDIT_STREAM,
                {
                    "event_type": "ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_DECIDED",
                    **out,
                }, maxlen=MAXLEN,
                approximate=True,
            )
            await r.hset(
                LAST_HASH,
                mapping={
                    "decision": decision["decision"],
                    "reason_code": decision["reason_code"],
                    "current_mode": decision["current_mode"],
                    "target_mode": decision["target_mode"],
                    "match_rate": str(window_stats["match_rate"]),
                    "drift_rate": str(window_stats["drift_rate"]),
                    "mismatch_rate": str(window_stats["mismatch_rate"]),
                    "pending_total": str(pending_total),
                    "comparator_age_ms": str(comparator_age_ms),
                    "ts_ms": str(now_ms()),
                }
            )
            if MATCH_RATE:
                MATCH_RATE.set(window_stats["match_rate"])
            if DRIFT_RATE:
                DRIFT_RATE.set(window_stats["drift_rate"])
            if MISMATCH_RATE:
                MISMATCH_RATE.set(window_stats["mismatch_rate"])
            if PENDING_TOTAL:
                PENDING_TOTAL.set(pending_total)
            if LAST_RUN_TS:
                LAST_RUN_TS.set(time.time())
        except Exception as exc:
            status = "error"
            await r.xadd(
                AUDIT_STREAM,
                {
                    "event_type": "ROUTE_INCIDENT_RCA_MIRROR_VERIFICATION_FAILED",
                    "error": str(exc),
                    "ts_ms": str(now_ms()),
                }, maxlen=MAXLEN,
                approximate=True,
            )
        finally:
            if RUNS:
                RUNS.labels(status=status, decision=decision_label).inc()
            if LAT:
                LAT.observe(max(time.perf_counter() - started, 0.0))
            await asyncio.sleep(max(RUN_EVERY_SEC, 5))


if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
