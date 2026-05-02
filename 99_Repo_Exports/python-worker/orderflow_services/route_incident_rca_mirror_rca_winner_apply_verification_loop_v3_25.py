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

APP_NAME = "route_incident_rca_mirror_rca_winner_apply_verification_loop_v3_25"

CTRL_JOURNAL_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_CONTROLLER_JOURNAL_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_controller_journal")
CTRL_DECISIONS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_CONTROLLER_DECISIONS_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_controller_decisions")
EXPOSURES_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_EXPOSURES_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_experiment_exposures")

VERIFY_RESULTS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_RESULTS", "stream:ml:route_incident_rca_mirror_rca_winner_apply_verification_results")
ROLLBACK_JOURNAL_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_ROLLBACK_JOURNAL", "stream:ml:route_incident_rca_mirror_rca_winner_apply_rollback_journal")
VERIFY_AUDIT_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_AUDIT", "stream:ml:route_incident_rca_mirror_rca_winner_apply_verification_audit")

LAST_METRIC = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_LAST", "metrics:ml:route_incident_rca_mirror_rca_winner_apply_verification:last")

CFG_HARNESS = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_CFG", "cfg:ml:route_incident_rca_mirror_rca_winner_apply_experiment:global")

PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_PORT", "9947"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_MAXLEN", "2000"))

ADVISORY_ONLY = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_ADVISORY_ONLY", "1"))
EXECUTOR_MODE = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_EXECUTOR_MODE", "DRY_RUN")

MIN_EXPOSURES = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_MIN_EXPOSURES", "5"))
MIN_PRIMARY_MATCH_RATE = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_MIN_PRIMARY_MATCH_RATE", "0.80"))
MAX_UNEXPECTED_PRIMARY_RATE = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_MAX_UNEXPECTED_PRIMARY_RATE", "0.20"))
MAX_SHADOW_RATE_SINGLE_ARM = float(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_VERIFICATION_MAX_SHADOW_RATE_SINGLE_ARM", "0.05"))

# All known arms to reconstruct logic
ALL_KNOWN_ARMS = ["deterministic", "vertex_candidate", "local_fallback_candidate"]

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

PRIMARY_MATCH_RATE = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_primary_match_rate", "Pri Match")
UNEX_PRIMARY_RATE = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_unexpected_primary_rate", "Unex Pri")
SHADOW_RATE = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_shadow_rate", "Shadow Rate")

ROLLBACKS = _counter("ml_route_incident_rca_mirror_rca_winner_apply_rollbacks_total", "Rollbacks", ("reason_code", "target_mode"))

def now_ms() -> int:
    return get_ny_time_millis()

def decode_dict(d: Dict[Any, Any]) -> Dict[str, Any]:
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in d.items()
    }

async def persist_verification(db_url: str, apply_id: str, decision: str, 
                               primary_match_rate: float, unexpected_primary_rate: float, shadow_rate: float, 
                               target_mode: str, target_primary_arm: str) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """

                INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_verification_results (
                    apply_id, decision, primary_match_rate, unexpected_primary_rate, shadow_rate,
                    target_mode, target_primary_arm, ts_ms
                ) VALUES (
                    %(apply_id)s, %(decision)s, %(primary_match_rate)s, %(unexpected_primary_rate)s, %(shadow_rate)s,
                    %(target_mode)s, %(target_primary_arm)s, %(ts_ms)s
                )
                """,
                {
                    "apply_id": apply_id,
                    "decision": decision,
                    "primary_match_rate": primary_match_rate,
                    "unexpected_primary_rate": unexpected_primary_rate,
                    "shadow_rate": shadow_rate,
                    "target_mode": target_mode,
                    "target_primary_arm": target_primary_arm,
                    "ts_ms": now_ms(),
                }
            )
            conn.commit()

async def persist_rollback(db_url: str, apply_id: str, reason_code: str, harness_state_restored_json: str) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """

                INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_rollback_journal (
                    apply_id, reason_code, harness_state_restored_json, ts_ms
                ) VALUES (
                    %(apply_id)s, %(reason_code)s, %(harness_state_restored_json)s, %(ts_ms)s
                )
                """,
                {
                    "apply_id": apply_id,
                    "reason_code": reason_code,
                    "harness_state_restored_json": harness_state_restored_json,
                    "ts_ms": now_ms(),
                }
            )
            conn.commit()

def calculate_rates(exposures: List[Dict[str, Any]], target_mode: str, target_primary_arm: str) -> Tuple[int, float, float, float]:
    if not exposures:
        return 0, 0.0, 0.0, 0.0
        
    n_total = len(exposures)
    n_target_primary = sum(1 for e in exposures if e.get("mode") == target_mode and e.get("arm") == target_primary_arm)
    n_unexpected_primary = sum(1 for e in exposures if e.get("mode") == target_mode and e.get("arm") != target_primary_arm and "shadow" not in e.get("arm", "").lower())
    n_shadows = sum(1 for e in exposures if e.get("mode") == "SHADOW" and e.get("arm") != target_primary_arm)
    
    primary_match_rate = n_target_primary / n_total
    unexpected_primary_rate = n_unexpected_primary / n_total
    shadow_rate = n_shadows / n_total
    
    return n_total, primary_match_rate, unexpected_primary_rate, shadow_rate

def evaluate_verification(n_total: int, target_mode: str, primary_match_rate: float, unexpected_primary_rate: float, shadow_rate: float) -> Tuple[str, str]:
    if n_total < MIN_EXPOSURES:
        return "HOLD", "INSUFFICIENT_DATA"
        
    if primary_match_rate < MIN_PRIMARY_MATCH_RATE:
        return "ROLLBACK_PREVIOUS_POLICY", "LOW_PRIMARY_MATCH_RATE"
        
    if unexpected_primary_rate > MAX_UNEXPECTED_PRIMARY_RATE:
        return "ROLLBACK_PREVIOUS_POLICY", "HIGH_UNEXPECTED_PRIMARY"
        
    if target_mode == "SINGLE_ARM" and shadow_rate > MAX_SHADOW_RATE_SINGLE_ARM:
        return "ROLLBACK_PREVIOUS_POLICY", "HIGH_SHADOW_RATE_IN_SINGLE_ARM"
        
    return "KEEP_APPLIED", "OK"

def build_rollback_state(current_harness: Dict[str, str], restore_mode: str, restore_primary_arm: str) -> Dict[str, str]:
    new_harness = dict(current_harness)
    new_harness["ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_MODE"] = restore_mode
    new_harness["ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_PRIMARY_ARM"] = restore_primary_arm
    
    if restore_mode == "SHADOW":
        # we reconstruct the shadows array from all known MINUS the restored primary
        shadows = [a for a in ALL_KNOWN_ARMS if a != restore_primary_arm]
        new_harness["ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_SHADOW_ARMS_JSON"] = json.dumps(shadows)
    elif restore_mode == "SINGLE_ARM":
        new_harness["ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_SHADOW_ARMS_JSON"] = "[]"
        
    return new_harness

async def fetch_recent(r: Any, stream: str, count: int) -> List[Dict[str, Any]]:
    try:
        hist = await r.xrevrange(stream, max="+", min="-", count=count)
        return [{"msg_id": msg_id.decode() if isinstance(msg_id, bytes) else msg_id, **decode_dict(fields)} for msg_id, fields in hist] if hist else []
    except Exception:
        return []

async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PORT)
    if UP:
        UP.set(1)
        
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    db_url = os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL", "")
    
    last_processed_apply_id = ""

    while True:
        started = time.perf_counter()
        status = "ok"
        decision_code = "none"
        
        try:
            # Reconstruct recent apply to know what to verify against
            decisions = await fetch_recent(r, CTRL_DECISIONS_STREAM, 10)
            
            target_apply_id = ""
            target_mode = "UNKNOWN"
            target_primary_arm = "UNKNOWN"
            strategy = "UNKNOWN"
            ts_ms_str = "0"
            
            # Find the latest actual APPLY (not HOLD)
            for d in decisions:
                if d.get("decision", "").startswith("APPLY"):
                    target_apply_id = d.get("apply_id", "")
                    target_primary_arm = d.get("winner_arm", "UNKNOWN")
                    strategy = d.get("strategy", "UNKNOWN")
                    ts_ms_str = d.get("ts_ms", "0")
                    # strategy mapped to target mode approx
                    if strategy == "SINGLE_ARM":
                        target_mode = "SINGLE_ARM"
                    elif strategy == "SHADOW_PRIMARY":
                        target_mode = "SHADOW"
                    break
                    
            if target_apply_id:
                # read post-apply exposures
                all_exposures = await fetch_recent(r, EXPOSURES_STREAM, 2000)
                post_apply_exposures = [e for e in all_exposures if int(e.get("ts_ms", "0")) >= int(ts_ms_str)]
                
                n_total, pmr, upr, sr = calculate_rates(post_apply_exposures, target_mode, target_primary_arm)
                
                if PRIMARY_MATCH_RATE: PRIMARY_MATCH_RATE.set(pmr)
                if UNEX_PRIMARY_RATE: UNEX_PRIMARY_RATE.set(upr)
                if SHADOW_RATE: SHADOW_RATE.set(sr)
                
                decision_code, reason_code = evaluate_verification(n_total, target_mode, pmr, upr, sr)
                
                harness = decode_dict(await r.hgetall(CFG_HARNESS) or {})
                live_mode = harness.get("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_MODE", "UNKNOWN")
                live_arm = harness.get("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_PRIMARY_ARM", "UNKNOWN")
                
                if live_mode != target_mode or live_arm != target_primary_arm:
                    decision_code = "ROLLBACK_PREVIOUS_POLICY"
                    reason_code = "POLICY_MISMATCH_AFTER_APPLY"
                    
                if decision_code == "ROLLBACK_PREVIOUS_POLICY" and target_apply_id != last_processed_apply_id:
                    last_processed_apply_id = target_apply_id
                    
                    if ROLLBACKS:
                        ROLLBACKS.labels(reason_code=reason_code, target_mode=target_mode).inc()
                        
                    if ADVISORY_ONLY == 1 or EXECUTOR_MODE == "DRY_RUN":
                        decision_code = "ROLLBACK_DRY_RUN"
                    else:
                        # Find the state before the apply via looking back.
                        # Simple bound: we always go back to SHADOW / deterministic.
                        rb_mode = "SHADOW"
                        rb_arm = "deterministic"
                        
                        rb_harness = build_rollback_state(harness, rb_mode, rb_arm)
                        
                        await r.hset(CFG_HARNESS, mapping=rb_harness)
                        await persist_rollback(db_url, target_apply_id, reason_code, json.dumps(rb_harness))
                        
                        await r.xadd(ROLLBACK_JOURNAL_STREAM, {
                            "apply_id": target_apply_id,
                            "reason_code": reason_code,
                            "target_mode": target_mode,
                            "target_primary_arm": target_primary_arm,
                            "restored_mode": rb_mode,
                            "restored_primary_arm": rb_arm,
                            "ts_ms": str(now_ms())
                        }, maxlen=MAXLEN, approximate=True)

                await r.xadd(VERIFY_RESULTS_STREAM, {
                    "apply_id": target_apply_id,
                    "decision": decision_code,
                    "reason_code": reason_code,
                    "pmr": str(pmr),
                    "upr": str(upr),
                    "sr": str(sr),
                    "ts_ms": str(now_ms())
                }, maxlen=MAXLEN, approximate=True)
                
                await persist_verification(db_url, target_apply_id, decision_code, pmr, upr, sr, target_mode, target_primary_arm)
                
                await r.hset(LAST_METRIC, "apply_id", target_apply_id)
                await r.hset(LAST_METRIC, "decision", decision_code)
                await r.hset(LAST_METRIC, "reason", reason_code)

            if LAST_RUN:
                LAST_RUN.set(time.time())
                
        except Exception as exc:
            status = "error"
        finally:
            if RUNS:
                RUNS.labels(status=status, decision=decision_code).inc()
            if LAT:
                LAT.observe(max(time.perf_counter() - started, 0.0))
                
            await asyncio.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
