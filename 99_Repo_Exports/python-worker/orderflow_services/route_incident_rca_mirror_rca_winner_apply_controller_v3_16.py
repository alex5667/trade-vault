from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any

from utils.time_utils import get_ny_time_millis
import contextlib

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

APP_NAME = "route_incident_rca_mirror_rca_winner_apply_controller_v3_16"
EVALUATOR_DECISIONS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EVALUATOR_DECISIONS_STREAM", "stream:ml:route_incident_rca_mirror_rca_evaluator_decisions")

CFG_EXPERIMENT_GLOBAL = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EXPERIMENT_CFG", "cfg:ml:route_incident_rca_mirror_rca_experiment:global")
CFG_APPLY_GLOBAL = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_CFG", "cfg:ml:route_incident_rca_mirror_rca_winner_apply:global")

DECISIONS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_DECISIONS_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_decisions")
JOURNAL_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_JOURNAL_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_journal")
AUDIT_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_AUDIT_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_audit")
LAST_HASH = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_LAST_HASH", "metrics:ml:route_incident_rca_mirror_rca_winner_apply:last")

PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_PORT", "9935"))

ADVISORY_ONLY = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_ADVISORY_ONLY", "1"))
EXECUTOR_MODE = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXECUTOR_MODE", "DRY_RUN") # DRY_RUN, COMMIT
STRATEGY = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_STRATEGY", "SHADOW_PRIMARY") # SHADOW_PRIMARY, SINGLE_ARM

COOLDOWN_SEC = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_COOLDOWN_SEC", "21600")) # 6h
ALLOW_ARMS_JSON = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_ALLOW_ARMS_JSON", '["vertex_candidate","local_fallback_candidate"]')

MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_MAXLEN", "1000"))
POLL_INTERVAL_SEC = 5.0

def _counter(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None

def _gauge(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None

def _hist(name: str, doc: str, labels: tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_route_incident_rca_mirror_rca_winner_apply_runs_total", "Runs", ("status", "decision"))
LAT = _hist("ml_route_incident_rca_mirror_rca_winner_apply_latency_seconds", "Latency")
UP = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_up", "Up")
LAST_RUN = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_last_run_ts_seconds", "Last run")

TRANSITIONS = _counter("ml_route_incident_rca_mirror_rca_winner_apply_transitions_total", "Transitions", ("apply_strategy", "winner_arm"))
CURRENT_MODE = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_current_mode", "Current Mode", ("mode",))
CURRENT_PRIMARY_ARM = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_current_primary_arm", "Current Primary Arm", ("arm",))

def now_ms() -> int:
    return get_ny_time_millis()

def decode_dict(d: dict[Any, Any]) -> dict[str, Any]:
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in d.items()
    }

async def load_current_experiment_config(r: Any) -> dict[str, Any]:
    if not r:
        return {}
    res = await r.hgetall(CFG_EXPERIMENT_GLOBAL)
    if res:
        return decode_dict(res)
    return {}

async def load_last_apply_ts(r: Any) -> int:
    if not r:
        return 0
    res = await r.hget(CFG_APPLY_GLOBAL, "last_apply_ts_ms")
    if res:
        return int(res.decode() if isinstance(res, bytes) else res)
    return 0

def build_new_config(current_cfg: dict[str, Any], winner: str, strategy: str, allow_arms: list[str]) -> tuple[bool, dict[str, Any], str]:
    if winner not in allow_arms:
        return False, {}, f"winner arm {winner} not allowed"

    current_mode = current_cfg.get("mode", "SHADOW")
    current_primary = current_cfg.get("primary_arm", "deterministic")

    if current_primary == winner:
        return False, {}, f"winner {winner} is already primary"

    new_cfg = dict(current_cfg)

    if strategy == "SHADOW_PRIMARY":
        if current_mode != "SHADOW":
            return False, {}, f"SHADOW_PRIMARY strategy requires SHADOW mode, current is {current_mode}"

        new_cfg["primary_arm"] = winner
        try:
            shadows = json.loads(current_cfg.get("shadow_arms", "[]"))
        except Exception:
            shadows = []

        new_shadows = []
        for x in shadows:
            if x != winner:
                new_shadows.append(x)
        if current_primary != winner:
            new_shadows.append(current_primary)

        new_cfg["shadow_arms"] = json.dumps(new_shadows)

    elif strategy == "SINGLE_ARM":
        new_cfg["mode"] = "SINGLE_ARM"
        new_cfg["primary_arm"] = winner
        new_cfg["shadow_arms"] = "[]"
    else:
        return False, {}, f"unknown strategy: {strategy}"

    return True, new_cfg, "success"

async def apply_config(r: Any, db_url: str, new_cfg: dict[str, Any], winner: str, strategy: str, executor_mode: str) -> None:
    curr_ms = now_ms()

    if executor_mode == "COMMIT":
        await r.hset(CFG_EXPERIMENT_GLOBAL, mapping=new_cfg)
        await r.hset(CFG_APPLY_GLOBAL, mapping={"last_apply_ts_ms": str(curr_ms), "last_winner": winner})

        await r.xadd(JOURNAL_STREAM, {
            "winner": winner,
            "strategy": strategy,
            "executor_mode": executor_mode,
            "new_config": json.dumps(new_cfg),
            "ts_ms": str(curr_ms)
        }, maxlen=MAXLEN, approximate=True)

        if TRANSITIONS:
            TRANSITIONS.labels(apply_strategy=strategy, winner_arm=winner).inc()

        if not db_url or psycopg is None:
            return
        with psycopg.connect(db_url) as conn:  # pragma: no cover
            with conn.cursor() as cur:
                cur.execute(
                    """

                    INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_journal (
                        winner, strategy, executor_mode, new_config_json, ts_ms
                    ) VALUES (
                        %(winner)s, %(strategy)s, %(executor_mode)s, %(new_config_json)s, %(ts_ms)s
                    )
                    """,
                    {
                        "winner": winner,
                        "strategy": strategy,
                        "executor_mode": executor_mode,
                        "new_config_json": json.dumps(new_cfg),
                        "ts_ms": curr_ms,
                    }
                )
                conn.commit()

async def persist_decision(db_url: str, decision: str, recommendation: str, reason: str) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """

                INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_decisions (
                    decision, recommendation, reason, ts_ms
                ) VALUES (
                    %(decision)s, %(recommendation)s, %(reason)s, %(ts_ms)s
                )
                """,
                {
                    "decision": decision,
                    "recommendation": recommendation,
                    "reason": reason,
                    "ts_ms": now_ms(),
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
    db_url = os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL", "")

    allow_arms = []
    with contextlib.suppress(Exception):
        allow_arms = json.loads(ALLOW_ARMS_JSON)

    last_eval_id = "$"

    # Initialize exporter labels with known good defaults to zero
    if CURRENT_MODE:
        CURRENT_MODE.labels(mode="SHADOW").set(1)
        CURRENT_MODE.labels(mode="SINGLE_ARM").set(0)
    if CURRENT_PRIMARY_ARM:
        CURRENT_PRIMARY_ARM.labels(arm="deterministic").set(1)
        for arm in allow_arms:
            CURRENT_PRIMARY_ARM.labels(arm=arm).set(0)

    while True:
        started = time.perf_counter()
        status = "ok"
        decision_label = "none"

        try:
            streams = {EVALUATOR_DECISIONS_STREAM: last_eval_id}
            results = await r.xread(streams, count=1, block=int(POLL_INTERVAL_SEC*1000))
            if results:
                for stream_name, events in results:
                    for msg_id, fields in events:
                        m_id = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
                        decoded = decode_dict(fields)

                        recommendation = decoded.get("decision", "")
                        winner = decoded.get("winner", "")

                        if recommendation.startswith("PROMOTE_") and winner:
                            if ADVISORY_ONLY:
                                decision_label = "HOLD_ADVISORY"
                                await persist_decision(db_url, "HOLD", recommendation, "advisory mode active")
                            else:
                                curr_ms = now_ms()
                                last_apply = await load_last_apply_ts(r)

                                if (curr_ms - last_apply) < COOLDOWN_SEC * 1000:
                                    decision_label = "HOLD_COOLDOWN"
                                    await persist_decision(db_url, "HOLD", recommendation, "in cooldown")
                                else:
                                    current_cfg = await load_current_experiment_config(r)
                                    can_apply, new_cfg, reason = build_new_config(current_cfg, winner, STRATEGY, allow_arms)

                                    if can_apply:
                                        await apply_config(r, db_url, new_cfg, winner, STRATEGY, EXECUTOR_MODE)
                                        decision_label = f"APPLY_{STRATEGY}"
                                        await persist_decision(db_url, "APPLY", recommendation, f"applied in {EXECUTOR_MODE}")

                                        if CURRENT_MODE:
                                            CURRENT_MODE.labels(mode=current_cfg.get("mode", "SHADOW")).set(0)
                                            CURRENT_MODE.labels(mode=new_cfg.get("mode", "SHADOW")).set(1)
                                        if CURRENT_PRIMARY_ARM:
                                            CURRENT_PRIMARY_ARM.labels(arm=current_cfg.get("primary_arm", "deterministic")).set(0)
                                            CURRENT_PRIMARY_ARM.labels(arm=new_cfg.get("primary_arm", "deterministic")).set(1)
                                    else:
                                        decision_label = "HOLD_INVALID"
                                        await persist_decision(db_url, "HOLD", recommendation, reason)
                        else:
                            decision_label = "IGNORED"

                        last_eval_id = m_id

            if LAST_RUN:
                LAST_RUN.set(time.time())

        except Exception as exc:
            status = "error"
            await r.xadd(AUDIT_STREAM, {"event_type": "APPLY_FAILED", "error": str(exc), "ts_ms": str(now_ms())}, maxlen=MAXLEN, approximate=True)
            await asyncio.sleep(POLL_INTERVAL_SEC)
        finally:
            if RUNS:
                RUNS.labels(status=status, decision=decision_label).inc()
            if LAT:
                LAT.observe(max(time.perf_counter() - started, 0.0))

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
