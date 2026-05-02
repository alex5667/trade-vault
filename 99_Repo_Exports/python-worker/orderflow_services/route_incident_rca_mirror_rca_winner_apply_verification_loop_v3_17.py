from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import asyncio
import json
import os
import time
from typing import Any, Dict, Tuple, List, Optional

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

APP_NAME = "route_incident_rca_mirror_rca_winner_apply_verification_loop_v3_17"
JOURNAL_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_JOURNAL_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_journal")
EXPOSURES_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EXPERIMENT_EXPOSURES_STREAM", "stream:ml:route_incident_rca_mirror_rca_experiment_exposures")
CFG_EXPERIMENT_GLOBAL = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_EXPERIMENT_CFG", "cfg:ml:route_incident_rca_mirror_rca_experiment:global")

RESULTS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_RESULTS_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_verification_results")
ROLLBACK_JOURNAL_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_ROLLBACK_JOURNAL_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_rollback_journal")
AUDIT_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_AUDIT_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_verification_audit")

PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_PORT", "9936"))

ADVISORY_ONLY = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_ADVISORY_ONLY", "1"))
EXECUTOR_MODE = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_EXECUTOR_MODE", "DRY_RUN") # DRY_RUN, COMMIT

MIN_EXPOSURES = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_MIN_EXPOSURES", "5"))
MIN_PRIMARY_MATCH_RATE = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_MIN_PRIMARY_MATCH_RATE", "0.80"))
MAX_UNEXPECTED_PRIMARY_RATE = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_MAX_UNEXPECTED_PRIMARY_RATE", "0.20"))
MAX_SHADOW_RATE_SINGLE_ARM = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_MAX_SHADOW_RATE_SINGLE_ARM", "0.05"))

MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_MAXLEN", "1000"))
POLL_INTERVAL_SEC = 10.0

def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None

def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None

def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_route_incident_rca_mirror_rca_winner_apply_verification_runs_total", "Runs", ("status", "decision"))
LAT = _hist("ml_route_incident_rca_mirror_rca_winner_apply_verification_latency_seconds", "Latency")
UP = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_verification_up", "Up")
LAST_RUN = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_verification_last_run_ts_seconds", "Last run")

PRIMARY_MATCH_RATE = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_primary_match_rate", "Primary Match Rate")
UNEXPECTED_PRIMARY_RATE = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_unexpected_primary_rate", "Unexpected Primary Rate")
SHADOW_RATE = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_shadow_rate", "Shadow Rate")
ROLLBACKS = _counter("ml_route_incident_rca_mirror_rca_winner_apply_rollbacks_total", "Rollbacks", ("reason_code", "target_mode"))

def now_ms() -> int:
    return get_ny_time_millis()

def decode_dict(d: Dict[Any, Any]) -> Dict[str, Any]:
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in d.items()
    }

async def read_last_apply(r: Any) -> Optional[Dict[str, Any]]:
    # Get last applied transition from journal
    res = await r.xrevrange(JOURNAL_STREAM, max="+", min="-", count=1)
    if not res:
        return None
    msg_id, fields = res[0]
    return decode_dict(fields)

async def read_active_policy(r: Any) -> Dict[str, Any]:
    res = await r.hgetall(CFG_EXPERIMENT_GLOBAL)
    if not res:
        return {}
    return decode_dict(res)

async def read_post_apply_exposures(r: Any, ts_ms: int) -> List[Dict[str, Any]]:
    # Read exposures after the apply ts
    # ts is standard unix ms string from redis XADD
    res = await r.xrange(EXPOSURES_STREAM, min=f"{ts_ms}-0", max="+", count=100)
    exposures = []
    if res:
        for msg_id, fields in res:
            exposures.append(decode_dict(fields))
    return exposures

def verify_exposures(exposures: List[Dict[str, Any]], target_mode: str, target_primary: str) -> Tuple[str, str, Dict[str, float]]:
    if len(exposures) < MIN_EXPOSURES:
        return "HOLD", "insufficient_exposures", {}

    primary_count = 0
    unexpected_primary_count = 0
    shadow_count = 0
    
    for ex in exposures:
        role = ex.get("role", "")
        arm = ex.get("arm", "")
        
        if role == "primary":
            if arm == target_primary:
                primary_count += 1
            else:
                unexpected_primary_count += 1
        elif role == "shadow":
            shadow_count += 1

    total = len(exposures)
    primary_match_rate = primary_count / total if total > 0 else 0
    unexpected_primary_rate = unexpected_primary_count / total if total > 0 else 0
    shadow_rate = shadow_count / total if total > 0 else 0

    rates = {
        "primary_match_rate": primary_match_rate,
        "unexpected_primary_rate": unexpected_primary_rate,
        "shadow_rate": shadow_rate
    }
    
    if unexpected_primary_rate > MAX_UNEXPECTED_PRIMARY_RATE:
        return "ROLLBACK_PREVIOUS_POLICY", "HIGH_UNEXPECTED_PRIMARY_RATE", rates
        
    if primary_match_rate < MIN_PRIMARY_MATCH_RATE:
        return "ROLLBACK_PREVIOUS_POLICY", "LOW_PRIMARY_MATCH_RATE", rates
        
    if target_mode == "SINGLE_ARM" and shadow_rate > MAX_SHADOW_RATE_SINGLE_ARM:
        return "ROLLBACK_PREVIOUS_POLICY", "HIGH_SHADOW_RATE_FOR_SINGLE_ARM", rates

    return "KEEP_APPLIED", "verification_passed", rates

async def execute_rollback(r: Any, db_url: str, apply_record: Dict[str, Any], live_policy: Dict[str, Any], reason: str, executor_mode: str) -> None:
    # Need to rollback to the fallback or the state before apply
    # Since we don't have a full snapshot before apply in this design (just the new_config)
    # We will fallback to safe deterministic state
    
    fallback_arm = "deterministic"
    # Construct a safe shadow state without the failing winner
    failed_winner = apply_record.get("winner", "")
    
    target_mode = "SHADOW"
    
    current_shadows = []
    try:
        current_shadows = json.loads(live_policy.get("shadow_arms", "[]"))
    except Exception:
        pass
        
    safe_shadows = []
    for s in current_shadows:
        if s != failed_winner and s != fallback_arm:
            safe_shadows.append(s)
            
    if live_policy.get("primary_arm") != failed_winner and live_policy.get("primary_arm") != fallback_arm:
        if live_policy.get("primary_arm"):
             safe_shadows.append(live_policy["primary_arm"])
             
    new_cfg = {
        "mode": target_mode,
        "primary_arm": fallback_arm,
        "shadow_arms": json.dumps(safe_shadows)
    }

    curr_ms = now_ms()
    
    if executor_mode == "COMMIT":
        await r.hset(CFG_EXPERIMENT_GLOBAL, mapping=new_cfg)
        
        await r.xadd(ROLLBACK_JOURNAL_STREAM, {
            "failed_winner": failed_winner, 
            "reason": reason, 
            "executor_mode": executor_mode,
            "new_config": json.dumps(new_cfg), 
            "ts_ms": str(curr_ms)
        }, maxlen=MAXLEN, approximate=True)
        
        if ROLLBACKS:
            ROLLBACKS.labels(reason_code=reason, target_mode=target_mode).inc()
            
        if not db_url or psycopg is None:
            return
        with psycopg.connect(db_url) as conn:  # pragma: no cover
            with conn.cursor() as cur:
                cur.execute(
                    """

                    INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_rollback_journal (
                        failed_winner, reason, executor_mode, new_config_json, ts_ms
                    ) VALUES (
                        %(failed_winner)s, %(reason)s, %(executor_mode)s, %(new_config_json)s, %(ts_ms)s
                    )
                    """,
                    {
                        "failed_winner": failed_winner,
                        "reason": reason,
                        "executor_mode": executor_mode,
                        "new_config_json": json.dumps(new_cfg),
                        "ts_ms": curr_ms,
                    }
                )
                conn.commit()

async def persist_result(db_url: str, decision: str, reason: str, rates: str) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """

                INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_verification_results (
                    decision, reason, metrics_json, ts_ms
                ) VALUES (
                    %(decision)s, %(reason)s, %(metrics_json)s, %(ts_ms)s
                )
                """,
                {
                    "decision": decision,
                    "reason": reason,
                    "metrics_json": rates,
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
    
    while True:
        started = time.perf_counter()
        status = "ok"
        decision_label = "none"
        
        try:
            apply_record = await read_last_apply(r)
            if apply_record:
                apply_ts_ms = int(apply_record.get("ts_ms", "0"))
                target_cfg = json.loads(apply_record.get("new_config", "{}"))
                target_mode = target_cfg.get("mode", "")
                target_primary = target_cfg.get("primary_arm", "")
                
                live_policy = await read_active_policy(r)
                live_mode = live_policy.get("mode", "")
                live_primary = live_policy.get("primary_arm", "")
                
                if live_mode != target_mode or live_primary != target_primary:
                    decision_label = "ROLLBACK_POLICY_MISMATCH"
                    
                    if ADVISORY_ONLY:
                        await persist_result(db_url, "HOLD_ADVISORY", "POLICY_MISMATCH_AFTER_APPLY", "{}")
                    else:
                        await execute_rollback(r, db_url, apply_record, live_policy, "POLICY_MISMATCH_AFTER_APPLY", EXECUTOR_MODE)
                        await persist_result(db_url, "ROLLBACK_PREVIOUS_POLICY", "POLICY_MISMATCH_AFTER_APPLY", "{}")
                else:
                    exposures = await read_post_apply_exposures(r, apply_ts_ms)
                    decision, reason, rates = verify_exposures(exposures, target_mode, target_primary)
                    
                    if PRIMARY_MATCH_RATE:
                        PRIMARY_MATCH_RATE.set(rates.get("primary_match_rate", 0))
                    if UNEXPECTED_PRIMARY_RATE:
                        UNEXPECTED_PRIMARY_RATE.set(rates.get("unexpected_primary_rate", 0))
                    if SHADOW_RATE:
                        SHADOW_RATE.set(rates.get("shadow_rate", 0))
                        
                    if decision == "ROLLBACK_PREVIOUS_POLICY":
                        decision_label = "ROLLBACK_" + reason
                        if ADVISORY_ONLY:
                             await persist_result(db_url, "HOLD_ADVISORY", reason, json.dumps(rates))
                        else:
                             await execute_rollback(r, db_url, apply_record, live_policy, reason, EXECUTOR_MODE)
                             await persist_result(db_url, decision, reason, json.dumps(rates))
                    else:
                        decision_label = decision
                        await persist_result(db_url, decision, reason, json.dumps(rates))
                    
            if LAST_RUN:
                LAST_RUN.set(time.time())
                
        except Exception as exc:
            status = "error"
            await r.xadd(AUDIT_STREAM, {"event_type": "VERIFICATION_FAILED", "error": str(exc), "ts_ms": str(now_ms())}, maxlen=MAXLEN, approximate=True)
        finally:
            if RUNS:
                RUNS.labels(status=status, decision=decision_label).inc()
            if LAT:
                LAT.observe(max(time.perf_counter() - started, 0.0))
                
            await asyncio.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
