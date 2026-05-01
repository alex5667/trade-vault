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

APP_NAME = "route_incident_rca_mirror_rca_winner_apply_apply_controller_v3_24"

EVAL_DECISIONS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EVALUATOR_DECISIONS_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_evaluator_decisions")

CTRL_DECISIONS_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_CONTROLLER_DECISIONS_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_controller_decisions")
CTRL_JOURNAL_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_CONTROLLER_JOURNAL_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_controller_journal")
CTRL_AUDIT_STREAM = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_CONTROLLER_AUDIT_STREAM", "stream:ml:route_incident_rca_mirror_rca_winner_apply_controller_audit")

LAST_METRIC = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_CONTROLLER_LAST", "metrics:ml:route_incident_rca_mirror_rca_winner_apply_controller:last")

CFG_HARNESS = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_CFG", "cfg:ml:route_incident_rca_mirror_rca_winner_apply_experiment:global")
CFG_CONTROLLER = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_CONTROLLER_CFG", "cfg:ml:route_incident_rca_mirror_rca_winner_apply_controller:global")

PORT = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_CONTROLLER_PORT", "9946"))
MAXLEN = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_CONTROLLER_MAXLEN", "2000"))

# Dry-run vs real commit
ADVISORY_ONLY = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_CONTROLLER_ADVISORY_ONLY", "1"))
EXECUTOR_MODE = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_CONTROLLER_EXECUTOR_MODE", "DRY_RUN")

# Options: SHADOW_PRIMARY, SINGLE_ARM
DEFAULT_STRATEGY = os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_CONTROLLER_STRATEGY", "SHADOW_PRIMARY")
COOLDOWN_SEC = int(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_CONTROLLER_COOLDOWN_SEC", "21600")) # 6h

# Bounded surface
ALLOW_ARMS = set(os.getenv("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_CONTROLLER_ALLOW_ARMS_JSON", '["vertex_candidate","local_fallback_candidate"]').strip('[]"').split('","'))

POLL_INTERVAL_SEC = 5.0

def _counter(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Counter(name, doc, labels) if Counter else None

def _gauge(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Gauge(name, doc, labels) if Gauge else None

def _hist(name: str, doc: str, labels: Tuple[str, ...] = ()) -> Any:
    return Histogram(name, doc, labels) if Histogram else None

RUNS = _counter("ml_route_incident_rca_mirror_rca_winner_apply_controller_runs_total", "Runs", ("status", "decision"))
TRANSITIONS = _counter("ml_route_incident_rca_mirror_rca_winner_apply_controller_transitions_total", "Trans", ("apply_strategy", "winner_arm"))
LAT = _hist("ml_route_incident_rca_mirror_rca_winner_apply_controller_latency_seconds", "Latency")
UP = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_controller_up", "Up")
LAST_RUN = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_controller_last_run_ts_seconds", "Last run")

CUR_MODE = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_controller_current_mode", "Cur mode", ("mode",))
CUR_ARM = _gauge("ml_route_incident_rca_mirror_rca_winner_apply_controller_current_primary_arm", "Cur arm", ("arm",))

def now_ms() -> int:
    return get_ny_time_millis()

def decode_dict(d: Dict[Any, Any]) -> Dict[str, Any]:
    return {
        (k.decode() if isinstance(k, bytes) else k): (v.decode() if isinstance(v, bytes) else v)
        for k, v in d.items()
    },

async def persist_decision(db_url: str, apply_id: str, decision: str, winner_arm: str, strategy: str, harness_state_json: str) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """,
                INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_controller_decisions (
                    apply_id, decision, winner_arm, strategy, harness_state_json, ts_ms
                ) VALUES (
                    %(apply_id)s, %(decision)s, %(winner_arm)s, %(strategy)s, %(harness_state_json)s, %(ts_ms)s
                )
                """,
                {
                    "apply_id": apply_id,
                    "decision": decision,
                    "winner_arm": winner_arm,
                    "strategy": strategy,
                    "harness_state_json": harness_state_json,
                    "ts_ms": now_ms(),
                },
            )
            conn.commit()

async def persist_journal(db_url: str, apply_id: str, log_type: str, message: str) -> None:
    if not db_url or psycopg is None:
        return
    with psycopg.connect(db_url) as conn:  # pragma: no cover
        with conn.cursor() as cur:
            cur.execute(
                """,
                INSERT INTO llm_route_incident_rca_mirror_rca_winner_apply_controller_journal (
                    apply_id, log_type, message, ts_ms
                ) VALUES (
                    %(apply_id)s, %(log_type)s, %(message)s, %(ts_ms)s
                )
                """,
                {
                    "apply_id": apply_id,
                    "log_type": log_type,
                    "message": message,
                    "ts_ms": now_ms(),
                },
            )
            conn.commit()

def calculate_new_harness_state(winner_arm: str, strategy: str, current_harness: Dict[str, str], bounded_arms: List[str]) -> Tuple[bool, Dict[str, str], str]:
    if winner_arm not in bounded_arms:
        return False, {}, f"Winner {winner_arm} not in bounded arms {bounded_arms}"
        
    old_primary = current_harness.get("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_PRIMARY_ARM", "deterministic")
    old_mode = current_harness.get("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_MODE", "SHADOW")
    
    if winner_arm == old_primary and (
        (strategy == "SINGLE_ARM" and old_mode == "SINGLE_ARM") or 
        (strategy == "SHADOW_PRIMARY" and old_mode == "SHADOW")
    ):
        return False, {}, f"Already in desired state ({winner_arm}, {strategy})"
        
    try:
        old_shadows = json.loads(current_harness.get("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_SHADOW_ARMS_JSON", '[]'))
    except Exception:
        old_shadows = []
        
    new_harness = dict(current_harness)
    
    if strategy == "SINGLE_ARM":
        new_harness["ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_MODE"] = "SINGLE_ARM"
        new_harness["ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_PRIMARY_ARM"] = winner_arm
        new_harness["ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_SHADOW_ARMS_JSON"] = '[]'
        return True, new_harness, "Switched to SINGLE_ARM"
        
    elif strategy == "SHADOW_PRIMARY":
        new_harness["ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_MODE"] = "SHADOW"
        new_harness["ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_PRIMARY_ARM"] = winner_arm
        
        # Build new shadows: old primary becomes a shadow, winner is removed from shadows if present
        new_shadows = set(old_shadows)
        new_shadows.add(old_primary)
        if winner_arm in new_shadows:
            new_shadows.remove(winner_arm)
            
        new_harness["ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_SHADOW_ARMS_JSON"] = json.dumps(list(new_shadows))
        return True, new_harness, "Swapped Primary in SHADOW"
        
    return False, {}, f"Unknown strategy {strategy}"

async def main() -> None:  # pragma: no cover
    if redis is None:
        raise RuntimeError("redis.asyncio is required")
    start_http_server(PORT)
    if UP:
        UP.set(1)
        
    r = redis.from_url(os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    db_url = os.getenv("ANALYTICS_DB_DSN") or os.getenv("DATABASE_URL", "")
    
    last_id = "0-0"
    try:
        last_metric = await r.hgetall(LAST_METRIC)
        if last_metric:
            last_id = "$"
    except Exception:
        pass

    while True:
        started = time.perf_counter()
        status = "ok"
        decision_code = "none"
        
        try:
            cfg = await r.hgetall(CFG_CONTROLLER)
            cfg = decode_dict(cfg) if cfg else {}
            
            advisory = int(cfg.get("ADVISORY_ONLY", str(ADVISORY_ONLY)))
            strategy = cfg.get("STRATEGY", DEFAULT_STRATEGY)
            cooldown = int(cfg.get("COOLDOWN_SEC", str(COOLDOWN_SEC)))
            
            last_apply_ts = int(await r.hget(LAST_METRIC, "last_apply_ts_ms") or 0)
            
            # Update metrics based on current harness
            harness = decode_dict(await r.hgetall(CFG_HARNESS) or {})
            cur_mode = harness.get("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_MODE", "UNKNOWN")
            cur_arm = harness.get("ML_ROUTE_INCIDENT_RCA_MIRROR_RCA_WINNER_APPLY_EXPERIMENT_PRIMARY_ARM", "UNKNOWN")
            
            # Clear old labels
            if CUR_MODE:
                for m in ["SHADOW", "SINGLE_ARM", "MULTI_ARM", "DISABLED", "UNKNOWN"]:
                    CUR_MODE.labels(mode=m).set(1 if m == cur_mode else 0)
            if CUR_ARM:
                for a in ["deterministic", "vertex_candidate", "local_fallback_candidate", "UNKNOWN"]:
                    CUR_ARM.labels(arm=a).set(1 if a == cur_arm else 0)

            res_stream = await r.xread({EVAL_DECISIONS_STREAM: last_id}, count=10, block=int(POLL_INTERVAL_SEC * 1000))
            if res_stream:
                for stream_name, messages in res_stream:
                    for msg_id, fields in messages:
                        last_id = msg_id.decode() if isinstance(msg_id, bytes) else msg_id
                        decoded = decode_dict(fields)
                        
                        rec = decoded.get("recommendation", "")
                        winner_arm = decoded.get("winner_arm", "")
                        
                        apply_id = f"apply_{int(time.time())}"
                        
                        if not rec.startswith("PROMOTE_"):
                            decision_code = "HOLD_NOT_PROMOTE"
                        elif (now_ms() - last_apply_ts) < (cooldown * 1000):
                            decision_code = "HOLD_COOLDOWN"
                            await persist_journal(db_url, apply_id, "INFO", f"Skipping {rec} due to cooldown")
                        else:
                            should_apply, new_state, msg = calculate_new_harness_state(winner_arm, strategy, harness, list(ALLOW_ARMS))
                            
                            if not should_apply:
                                decision_code = "HOLD_REJECTED"
                                await persist_journal(db_url, apply_id, "WARN", msg)
                            else:
                                decision_code = "APPLY"
                                if advisory == 1 or EXECUTOR_MODE == "DRY_RUN":
                                    decision_code = "APPLY_DRY_RUN"
                                    await persist_journal(db_url, apply_id, "INFO", f"[DRY_RUN] Would apply: {msg}")
                                else:
                                    # Perform the switch in Redis
                                    await r.reset() # Close pipelines just in case
                                    
                                    # Set new config
                                    await r.hset(CFG_HARNESS, mapping=new_state)
                                    await r.hset(LAST_METRIC, "last_apply_ts_ms", str(now_ms()))
                                    
                                    await persist_journal(db_url, apply_id, "INFO", f"Successfully applied: {msg}")
                                    
                                    if TRANSITIONS:
                                        TRANSITIONS.labels(apply_strategy=strategy, winner_arm=winner_arm).inc()
                                
                                await r.xadd(CTRL_DECISIONS_STREAM, {
                                    "apply_id": apply_id,
                                    "decision": decision_code,
                                    "winner_arm": winner_arm,
                                    "strategy": strategy,
                                    "advisory": str(advisory),
                                    "ts_ms": str(now_ms())
                                }, maxlen=MAXLEN, approximate=True)
                                
                                await persist_decision(db_url, apply_id, decision_code, winner_arm, strategy, json.dumps(new_state))
                                
                                await r.hset(LAST_METRIC, "apply_id", apply_id)
                                await r.hset(LAST_METRIC, "decision", decision_code)
                                await r.hset(LAST_METRIC, "winner_arm", winner_arm)

                
            if LAST_RUN:
                LAST_RUN.set(time.time())
                
        except Exception as exc:
            status = "error"
        finally:
            if RUNS:
                RUNS.labels(status=status, decision=decision_code).inc()
            if LAT:
                LAT.observe(max(time.perf_counter() - started, 0.0))
                
            if not res_stream:
                await asyncio.sleep(POLL_INTERVAL_SEC)

if __name__ == "__main__":  # pragma: no cover
    asyncio.run(main())
