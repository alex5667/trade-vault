from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any

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


APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_verification_loop_v3_33"
APPLY_JOURNAL_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_CONTROLLER_JOURNAL_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_controller_journal",
)
EXPOSURES_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EXPOSURES_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_experiment_exposures",
)
RESULTS_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERIFICATION_RESULTS_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_verification_results",
)
ROLLBACK_JOURNAL_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_ROLLBACK_JOURNAL_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_rollback_journal",
)
AUDIT_STREAM = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERIFICATION_AUDIT_STREAM",
    "stream:ml:route_incident_rca_mirror_rca_winner_apply_apply_verification_audit",
)
LAST_HASH = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERIFICATION_LAST_HASH",
    "metrics:ml:route_incident_rca_mirror_rca_winner_apply_apply_verification:last",
)
GLOBAL_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERIFICATION_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_verification:global",
)
EXPERIMENT_POLICY_KEY = os.getenv(
    "ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_EXPERIMENT_GLOBAL_POLICY_KEY",
    "cfg:ml:route_incident_rca_mirror_rca_winner_apply_apply_experiment:global",
)
PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERIFICATION_PORT", "9958"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERIFICATION_MAXLEN", "20000"))
RUN_EVERY_SEC = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERIFICATION_RUN_EVERY_SEC", "300"))
LOOKBACK_COUNT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERIFICATION_LOOKBACK_COUNT", "200"))
MAX_APPLY_AGE_SEC = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERIFICATION_MAX_APPLY_AGE_SEC", "86400"))

DEFAULT_ADVISORY_ONLY = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERIFICATION_ADVISORY_ONLY", "1"))
DEFAULT_EXECUTOR_MODE = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERIFICATION_EXECUTOR_MODE", "DRY_RUN").upper()
DEFAULT_MIN_EXPOSURES = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERIFICATION_MIN_EXPOSURES", "5"))
DEFAULT_MIN_PRIMARY_MATCH_RATE = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERIFICATION_MIN_PRIMARY_MATCH_RATE", "0.80"))
DEFAULT_MAX_UNEXPECTED_PRIMARY_RATE = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERIFICATION_MAX_UNEXPECTED_PRIMARY_RATE", "0.20"))
DEFAULT_MAX_SHADOW_RATE_SINGLE_ARM = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERIFICATION_MAX_SHADOW_RATE_SINGLE_ARM", "0.05"))
DEFAULT_REQUIRE_POLICY_MATCH = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERIFICATION_REQUIRE_POLICY_MATCH", "1"))
DEFAULT_ROLLBACK_COOLDOWN_SEC = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERIFICATION_ROLLBACK_COOLDOWN_SEC", "21600"))

ACTIONABLE_DECISIONS = {"APPLY_PRIMARY_ARM_SHADOW", "APPLY_SINGLE_ARM"}
ALL_ARMS = ("deterministic", "vertex_candidate", "local_fallback_candidate")


def _counter(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None


def _gauge(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None


def _hist(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None


RUNS = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_verification_runs_total",
    "Winner-apply apply verification runs",
    ("status", "decision"),
)
LAT = _hist(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_verification_latency_seconds",
    "Winner-apply apply verification latency seconds",
)
UP = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_verification_up",
    "Winner-apply apply verification up",
)
LAST_RUN_TS = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_verification_last_run_ts_seconds",
    "Winner-apply apply verification last run ts",
)
PRIMARY_MATCH_RATE = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_primary_match_rate",
    "Winner-apply apply verification primary match rate",
)
UNEXPECTED_PRIMARY_RATE = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_unexpected_primary_rate",
    "Winner-apply apply verification unexpected primary rate",
)
SHADOW_RATE = _gauge(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_shadow_rate",
    "Winner-apply apply verification shadow rate",
)
ROLLBACKS = _counter(
    "ml_route_incident_rca_mirror_rca_winner_apply_apply_rollbacks_total",
    "Winner-apply apply rollbacks",
    ("reason_code", "target_mode"),
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
        "enabled": parse_int(raw.get("enabled"), 1),
        "kill_switch": parse_int(raw.get("kill_switch"), 0),
        "advisory_only": parse_int(raw.get("advisory_only"), DEFAULT_ADVISORY_ONLY),
        "executor_mode": str(raw.get("executor_mode") or DEFAULT_EXECUTOR_MODE).upper(),
        "min_exposures": parse_int(raw.get("min_exposures"), DEFAULT_MIN_EXPOSURES),
        "min_primary_match_rate": parse_float(raw.get("min_primary_match_rate"), DEFAULT_MIN_PRIMARY_MATCH_RATE),
        "max_unexpected_primary_rate": parse_float(raw.get("max_unexpected_primary_rate"), DEFAULT_MAX_UNEXPECTED_PRIMARY_RATE),
        "max_shadow_rate_single_arm": parse_float(raw.get("max_shadow_rate_single_arm"), DEFAULT_MAX_SHADOW_RATE_SINGLE_ARM),
        "require_policy_match": parse_int(raw.get("require_policy_match"), DEFAULT_REQUIRE_POLICY_MATCH),
        "rollback_cooldown_sec": parse_int(raw.get("rollback_cooldown_sec"), DEFAULT_ROLLBACK_COOLDOWN_SEC),
    }


def experiment_policy_from_hash(raw: dict[str, Any]) -> dict[str, Any]:
    mode = (raw.get("mode") or "SHADOW").upper()
    primary_arm = (raw.get("primary_arm") or "deterministic")
    return {
        "mode": mode,
        "primary_arm": primary_arm if primary_arm in ALL_ARMS else "deterministic",
        "shadow_arms_json": (raw.get("shadow_arms_json") or "[]"),
        "last_mode_switch_ts_ms": parse_int(raw.get("last_mode_switch_ts_ms"), 0),
    }


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


def latest_actionable_apply(rows: list[dict[str, Any]]) -> dict[str, Any] | None:
    cutoff = now_ms() - MAX_APPLY_AGE_SEC * 1000
    for row in rows:
        decision = (row.get("decision") or "")
        ts_ms = parse_int(row.get("ts_ms"), 0)
        if decision in ACTIONABLE_DECISIONS and ts_ms >= cutoff:
            return row
    return None


def filter_exposures_after(rows: list[dict[str, Any]], ts_ms: int) -> list[dict[str, Any]]:
    return [r for r in rows if parse_int(r.get("ts_ms"), 0) >= ts_ms]


def compute_exposure_stats(exposures: list[dict[str, Any]], target_primary_arm: str) -> dict[str, Any]:
    total = len(exposures)
    primary_rows = [r for r in exposures if parse_int(r.get("is_primary"), 0) == 1]
    primary_total = len(primary_rows)
    target_primary_n = sum(1 for r in primary_rows if (r.get("arm") or "") == target_primary_arm)
    unexpected_primary_n = sum(1 for r in primary_rows if (r.get("arm") or "") != target_primary_arm)
    shadow_n = sum(1 for r in exposures if parse_int(r.get("is_primary"), 0) == 0)
    primary_match_rate = (target_primary_n / primary_total) if primary_total > 0 else 0.0
    unexpected_primary_rate = (unexpected_primary_n / primary_total) if primary_total > 0 else 0.0
    shadow_rate = (shadow_n / total) if total > 0 else 0.0
    return {
        "total": total,
        "primary_total": primary_total,
        "target_primary_n": target_primary_n,
        "unexpected_primary_n": unexpected_primary_n,
        "shadow_n": shadow_n,
        "primary_match_rate": round(primary_match_rate, 6),
        "unexpected_primary_rate": round(unexpected_primary_rate, 6),
        "shadow_rate": round(shadow_rate, 6),
    }


def reconstruct_shadow_arms(mode: str, primary_arm: str) -> list[str]:
    if mode == "SINGLE_ARM":
        return []
    return [arm for arm in ALL_ARMS if arm != primary_arm]


def evaluate_verification(
    *,
    apply_event: dict[str, Any] | None,
    current_policy: dict[str, Any],
    exposure_stats: dict[str, Any],
    verify_policy: dict[str, Any],
    now_ts_ms: int,
) -> dict[str, Any]:
    if apply_event is None:
        return {
            "decision": "HOLD",
            "reason_code": "NO_RECENT_APPLY",
            "current_mode": current_policy["mode"],
            "current_primary_arm": current_policy["primary_arm"],
            "target_mode": current_policy["mode"],
            "target_primary_arm": current_policy["primary_arm"],
            "rollback_mode": current_policy["mode"],
            "rollback_primary_arm": current_policy["primary_arm"],
        }

    target_mode = str(apply_event.get("mode_after") or current_policy["mode"]).upper()
    target_primary_arm = str(apply_event.get("primary_arm_after") or current_policy["primary_arm"])
    rollback_mode = (apply_event.get("mode_before") or "SHADOW").upper()
    rollback_primary_arm = (apply_event.get("primary_arm_before") or "deterministic")

    rollback_cooldown_active = (
        current_policy["last_mode_switch_ts_ms"] > 0
        and (now_ts_ms - current_policy["last_mode_switch_ts_ms"]) < verify_policy["rollback_cooldown_sec"] * 1000
    )

    base = {
        "decision": "HOLD",
        "reason_code": "NO_CHANGE",
        "current_mode": current_policy["mode"],
        "current_primary_arm": current_policy["primary_arm"],
        "target_mode": target_mode,
        "target_primary_arm": target_primary_arm,
        "rollback_mode": rollback_mode,
        "rollback_primary_arm": rollback_primary_arm,
        "rollback_cooldown_active": 1 if rollback_cooldown_active else 0,
    }

    if verify_policy["kill_switch"] == 1:
        base["reason_code"] = "KILL_SWITCH"
        return base
    if verify_policy["enabled"] != 1:
        base["reason_code"] = "DISABLED"
        return base
    if rollback_cooldown_active:
        base["reason_code"] = "ROLLBACK_COOLDOWN_ACTIVE"
        return base

    if verify_policy["require_policy_match"] == 1:
        if current_policy["mode"] != target_mode or current_policy["primary_arm"] != target_primary_arm:
            base["decision"] = "ROLLBACK_PREVIOUS_POLICY"
            base["reason_code"] = "POLICY_MISMATCH_AFTER_APPLY"
            return base

    if exposure_stats["total"] < verify_policy["min_exposures"]:
        base["reason_code"] = "INSUFFICIENT_POST_APPLY_EXPOSURES"
        return base

    if exposure_stats["primary_total"] <= 0:
        base["decision"] = "ROLLBACK_PREVIOUS_POLICY"
        base["reason_code"] = "NO_PRIMARY_EXPOSURES"
        return base

    if exposure_stats["primary_match_rate"] < verify_policy["min_primary_match_rate"]:
        base["decision"] = "ROLLBACK_PREVIOUS_POLICY"
        base["reason_code"] = "PRIMARY_MATCH_RATE_TOO_LOW"
        return base

    if exposure_stats["unexpected_primary_rate"] > verify_policy["max_unexpected_primary_rate"]:
        base["decision"] = "ROLLBACK_PREVIOUS_POLICY"
        base["reason_code"] = "UNEXPECTED_PRIMARY_RATE_TOO_HIGH"
        return base

    if target_mode == "SINGLE_ARM" and exposure_stats["shadow_rate"] > verify_policy["max_shadow_rate_single_arm"]:
        base["decision"] = "ROLLBACK_PREVIOUS_POLICY"
        base["reason_code"] = "SHADOW_EXPOSURES_PRESENT_IN_SINGLE_ARM"
        return base

    base["decision"] = "KEEP_APPLIED"
    base["reason_code"] = "POST_APPLY_VERIFIED"
    return base


async def persist_if_configured(
    db_url: str,
    apply_event: dict[str, Any] | None,
    exposure_stats: dict[str, Any],
    evaluation: dict[str, Any],
) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """

                INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_apply_verification_results (
                    ts_ms,
                    decision,
                    reason_code,
                    current_mode,
                    current_primary_arm,
                    target_mode,
                    target_primary_arm,
                    rollback_mode,
                    rollback_primary_arm,
                    exposure_stats_json,
                    apply_event_json,
                    evaluation_json
                ) VALUES (
                    %(ts_ms)s,
                    %(decision)s,
                    %(reason_code)s,
                    %(current_mode)s,
                    %(current_primary_arm)s,
                    %(target_mode)s,
                    %(target_primary_arm)s,
                    %(rollback_mode)s,
                    %(rollback_primary_arm)s,
                    %(exposure_stats_json)s,
                    %(apply_event_json)s,
                    %(evaluation_json)s
                )
                """,
                {
                    "ts_ms": now_ms(),
                    "decision": evaluation["decision"],
                    "reason_code": evaluation["reason_code"],
                    "current_mode": evaluation["current_mode"],
                    "current_primary_arm": evaluation["current_primary_arm"],
                    "target_mode": evaluation["target_mode"],
                    "target_primary_arm": evaluation["target_primary_arm"],
                    "rollback_mode": evaluation["rollback_mode"],
                    "rollback_primary_arm": evaluation["rollback_primary_arm"],
                    "exposure_stats_json": json.dumps(exposure_stats),
                    "apply_event_json": json.dumps(apply_event or {}),
                    "evaluation_json": json.dumps(evaluation),
                }
            )
            if evaluation["decision"] == "ROLLBACK_PREVIOUS_POLICY":
                cur.execute(
                    """

                    INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_apply_rollback_journal (
                        ts_ms,
                        reason_code,
                        mode_before,
                        primary_arm_before,
                        mode_after,
                        primary_arm_after,
                        rollback_json
                    ) VALUES (
                        %(ts_ms)s,
                        %(reason_code)s,
                        %(mode_before)s,
                        %(primary_arm_before)s,
                        %(mode_after)s,
                        %(primary_arm_after)s,
                        %(rollback_json)s
                    )
                    """,
                    {
                        "ts_ms": now_ms(),
                        "reason_code": evaluation["reason_code"],
                        "mode_before": evaluation["current_mode"],
                        "primary_arm_before": evaluation["current_primary_arm"],
                        "mode_after": evaluation["rollback_mode"],
                        "primary_arm_after": evaluation["rollback_primary_arm"],
                        "rollback_json": json.dumps({"apply_event": apply_event or {}, "evaluation": evaluation, "exposure_stats": exposure_stats}),
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
    db_url = os.getenv("DATABASE_URL", "")

    while True:
        started = time.perf_counter()
        status = "ok"
        decision_label = "HOLD"
        try:
            verify_policy = policy_from_hash(as_dict(await r.hgetall(GLOBAL_POLICY_KEY)))
            journal_rows = await xr_recent(r, APPLY_JOURNAL_STREAM, LOOKBACK_COUNT)
            apply_event = latest_actionable_apply(journal_rows)
            current_policy = experiment_policy_from_hash(as_dict(await r.hgetall(EXPERIMENT_POLICY_KEY)))

            apply_ts_ms = parse_int((apply_event or {}).get("ts_ms"), 0)
            exposure_rows = await xr_recent(r, EXPOSURES_STREAM, LOOKBACK_COUNT)
            post_apply_exposures = filter_exposures_after(exposure_rows, apply_ts_ms) if apply_ts_ms > 0 else []
            target_primary = str((apply_event or {}).get("primary_arm_after") or current_policy["primary_arm"])
            exposure_stats = compute_exposure_stats(post_apply_exposures, target_primary)

            evaluation = evaluate_verification(
                apply_event=apply_event,
                current_policy=current_policy,
                exposure_stats=exposure_stats,
                verify_policy=verify_policy,
                now_ts_ms=now_ms(),
            )
            decision_label = evaluation["decision"]

            if (
                evaluation["decision"] == "ROLLBACK_PREVIOUS_POLICY"
                and verify_policy["advisory_only"] == 0
                and verify_policy["executor_mode"] == "COMMIT"
            ):
                rollback_mode = evaluation["rollback_mode"]
                rollback_primary = evaluation["rollback_primary_arm"]
                await r.hset(
                    EXPERIMENT_POLICY_KEY,
                    mapping={
                        "mode": rollback_mode,
                        "primary_arm": rollback_primary,
                        "shadow_arms_json": stable_json(reconstruct_shadow_arms(rollback_mode, rollback_primary)),
                        "last_mode_switch_ts_ms": str(now_ms()),
                        "last_mode_switch_source": APP_NAME,
                        "last_mode_switch_reason_code": evaluation["reason_code"],
                    }
                )
                await r.xadd(
                    ROLLBACK_JOURNAL_STREAM,
                    {
                        "schema_version": 1,
                        "decision": evaluation["decision"],
                        "reason_code": evaluation["reason_code"],
                        "mode_before": evaluation["current_mode"],
                        "primary_arm_before": evaluation["current_primary_arm"],
                        "mode_after": rollback_mode,
                        "primary_arm_after": rollback_primary,
                        "ts_ms": str(now_ms()),
                    }, maxlen=MAXLEN,
                    approximate=True,
                )
                if ROLLBACKS:
                    ROLLBACKS.labels(reason_code=evaluation["reason_code"], target_mode=rollback_mode).inc()

            await persist_if_configured(db_url, apply_event, exposure_stats, evaluation)

            out = {
                "schema_version": 1,
                "decision": evaluation["decision"],
                "reason_code": evaluation["reason_code"],
                "current_mode": evaluation["current_mode"],
                "current_primary_arm": evaluation["current_primary_arm"],
                "target_mode": evaluation["target_mode"],
                "target_primary_arm": evaluation["target_primary_arm"],
                "rollback_mode": evaluation["rollback_mode"],
                "rollback_primary_arm": evaluation["rollback_primary_arm"],
                "primary_match_rate": str(exposure_stats["primary_match_rate"]),
                "unexpected_primary_rate": str(exposure_stats["unexpected_primary_rate"]),
                "shadow_rate": str(exposure_stats["shadow_rate"]),
                "exposure_total": str(exposure_stats["total"]),
                "ts_ms": str(now_ms()),
            }
            await r.xadd(RESULTS_STREAM, out, maxlen=MAXLEN, approximate=True)
            await r.xadd(
                AUDIT_STREAM,
                {"event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERIFICATION_DECIDED", **out},
                maxlen=MAXLEN,
                approximate=True,
            )
            await r.hset(
                LAST_HASH,
                mapping={
                    "decision": evaluation["decision"],
                    "reason_code": evaluation["reason_code"],
                    "current_mode": evaluation["current_mode"],
                    "current_primary_arm": evaluation["current_primary_arm"],
                    "target_mode": evaluation["target_mode"],
                    "target_primary_arm": evaluation["target_primary_arm"],
                    "primary_match_rate": str(exposure_stats["primary_match_rate"]),
                    "unexpected_primary_rate": str(exposure_stats["unexpected_primary_rate"]),
                    "shadow_rate": str(exposure_stats["shadow_rate"]),
                    "exposure_total": str(exposure_stats["total"]),
                    "ts_ms": str(now_ms()),
                }
            )
            if PRIMARY_MATCH_RATE:
                PRIMARY_MATCH_RATE.set(exposure_stats["primary_match_rate"])
            if UNEXPECTED_PRIMARY_RATE:
                UNEXPECTED_PRIMARY_RATE.set(exposure_stats["unexpected_primary_rate"])
            if SHADOW_RATE:
                SHADOW_RATE.set(exposure_stats["shadow_rate"])
            if LAST_RUN_TS:
                LAST_RUN_TS.set(time.time())
        except Exception as exc:
            status = "error"
            await r.xadd(
                AUDIT_STREAM,
                {
                    "event_type": "ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_APPLY_VERIFICATION_FAILED",
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
