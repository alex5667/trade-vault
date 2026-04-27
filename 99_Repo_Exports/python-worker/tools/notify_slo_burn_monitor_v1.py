#!/usr/bin/env python3
"""
notify_slo_burn_monitor_v1.py

Calculates SLO burn rates for Notification delivery based on Redis rolling counters.
Implements Multi-Window Burn Rate Monitoring (Google SRE workbook style).
P6.9: Adds automated Emergency Suggestions & Trade Pause/Unpause on SLO violation.

Redis keys used:
    notify:win1m:<bucket> hash with fields: ok:<SEV>, err:<SEV>
    notify:last_ok_ts_ms:<SEV>
    notify:last_queue_lag_ms
    
    # P6.9 New keys
    sre:notify_slo:cooldown:<action>:<scope>   (Ex: trade_pause:ALL)
    sre:notify_slo:trade_pause_sid:<scope>     (Stores active pause SID)
    cfg:suggestions:entry_policy:meta:{sid}    (Where suggestions are pushed)
    cfg:suggestions:{prefix}:applied:{sid}     (Checked for unpause)

Logic:
1. Scan last N 1-minute buckets from Redis.
2. Sum OK and ERR counts for Fast Window (e.g. 5m) and Slow Window (e.g. 60m).
3. valid_requests = ok + err
4. error_ratio = err / valid_requests
5. Burn Rate = error_ratio / (1 - SLO_TARGET)
   e.g. SLO=99.9% => budget=0.1% => 0.001
   If error_ratio = 1% => 0.01
   Burn Rate = 0.01 / 0.001 = 10x

Alert Logic:
- PAGE if (FastBurn > 14.4 && SlowBurn > 6)   [~2% budget consumed in 1h]
- TICKET if (FastBurn > ... && SlowBurn > ...) [Implementation specific, here we use CRIT]

Actions (P6.9):
- If PAGE/CRIT -> Emit Emergency Suggestion (deduped).
- If PAGE -> Emit Trade Pause Suggestion (deduped, once per incident).
- If OK -> Check if Paused -> Emit Trade Unpause Suggestion (deduped).

Severities:
    WARN:  SLO 99.5%
    CRIT:  SLO 99.9%
    PAGE:  SLO 99.95% (Extreme)

Output:
- Prints JSON status (for SRE monitor wrapper)
- Auto-escalates (if enabled and PAGE triggered)
"""

import os
import sys
import time
import json
import logging
import uuid
import redis
import argparse

from common.redis_errors import retry_redis_operation

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S"
)
logger = logging.getLogger("NotifySLOBurnMonitor")

# Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")

# SLO Definitions (99.9% = 0.999)
SLO_TARGET_WARN = float(os.getenv("NOTIFY_SLO_WARN", "0.995"))
SLO_TARGET_CRIT = float(os.getenv("NOTIFY_SLO_CRIT", "0.999"))
SLO_TARGET_PAGE = float(os.getenv("NOTIFY_SLO_PAGE", "0.9995"))

# Burn Thresholds (Google SRE defaults often 14.4 for 1h/5m PAGE)
BURN_THRESHOLD_FAST_HI = float(os.getenv("NOTIFY_SLO_FAST_BURN_HI", "14.4"))
BURN_THRESHOLD_SLOW_HI = float(os.getenv("NOTIFY_SLO_SLOW_BURN_HI", "6.0"))

BURN_THRESHOLD_FAST_LO = float(os.getenv("NOTIFY_SLO_FAST_BURN_LO", "6.0"))
BURN_THRESHOLD_SLOW_LO = float(os.getenv("NOTIFY_SLO_SLOW_BURN_LO", "3.0"))

# Window sizes in minutes
WINDOW_FAST_MIN = int(os.getenv("NOTIFY_SLO_WINDOW_FAST_MIN", "5"))
WINDOW_SLOW_MIN = int(os.getenv("NOTIFY_SLO_WINDOW_SLOW_MIN", "60"))

# Latency/Freshness thresholds
MAX_STALE_MS = int(os.getenv("NOTIFY_SLO_MAX_STALE_MS", "300000")) # 5 min no OK
MAX_QUEUE_LAG_MS = int(os.getenv("NOTIFY_SLO_MAX_QUEUE_LAG_MS", "600000")) # 10 min lag

# Minimum samples to be statistically significant
MIN_SAMPLES = int(os.getenv("NOTIFY_SLO_MIN_SAMPLES", "20"))

# P6.9 Configuration
NOTIFY_SLO_EMIT_SUGGESTIONS = int(os.getenv("NOTIFY_SLO_EMIT_SUGGESTIONS", "0"))
CFG_SUGGESTIONS_PREFIX = os.getenv("CFG_SUGGESTIONS_PREFIX", "cfg:suggestions:entry_policy") # Where applied status lives
NOTIFY_SLO_SUGGESTIONS_SCOPE = os.getenv("NOTIFY_SLO_SUGGESTIONS_SCOPE", "ALL")
NOTIFY_SLO_SUGGESTION_TTL_SEC = int(os.getenv("NOTIFY_SLO_SUGGESTION_TTL_SEC", "86400"))
NOTIFY_SLO_ALLOW_NOOP_SUGGESTIONS = int(os.getenv("NOTIFY_SLO_ALLOW_NOOP_SUGGESTIONS", "0"))

# Emergency
NOTIFY_SLO_EMERGENCY_KIND = os.getenv("NOTIFY_SLO_EMERGENCY_KIND", "emergency_notify_delivery_degraded")
NOTIFY_SLO_EMERGENCY_COOLDOWN_SEC = int(os.getenv("NOTIFY_SLO_EMERGENCY_COOLDOWN_SEC", "1800"))
NOTIFY_SLO_EMERGENCY_OPS_JSON = os.getenv("NOTIFY_SLO_EMERGENCY_OPS_JSON", "[]")

# Trade Pause
NOTIFY_SLO_EMIT_TRADE_PAUSE = int(os.getenv("NOTIFY_SLO_EMIT_TRADE_PAUSE", "0"))
NOTIFY_SLO_TRADE_PAUSE_KIND = os.getenv("NOTIFY_SLO_TRADE_PAUSE_KIND", "trade_pause")
NOTIFY_SLO_TRADE_PAUSE_SCOPE = os.getenv("NOTIFY_SLO_TRADE_PAUSE_SCOPE", "ALL") # Usually same as global scope
NOTIFY_SLO_TRADE_PAUSE_COOLDOWN_SEC = int(os.getenv("NOTIFY_SLO_TRADE_PAUSE_COOLDOWN_SEC", "3600"))
NOTIFY_SLO_TRADE_PAUSE_OPS_JSON = os.getenv("NOTIFY_SLO_TRADE_PAUSE_OPS_JSON", "[]")

# Trade Unpause
NOTIFY_SLO_TRADE_UNPAUSE_KIND = os.getenv("NOTIFY_SLO_TRADE_UNPAUSE_KIND", "trade_unpause")
NOTIFY_SLO_TRADE_UNPAUSE_COOLDOWN_SEC = int(os.getenv("NOTIFY_SLO_TRADE_UNPAUSE_COOLDOWN_SEC", "3600"))
NOTIFY_SLO_UNPAUSE_ON_OK = int(os.getenv("NOTIFY_SLO_UNPAUSE_ON_OK", "1"))
NOTIFY_SLO_TRADE_UNPAUSE_OPS_JSON = os.getenv("NOTIFY_SLO_TRADE_UNPAUSE_OPS_JSON", "[]")

# Apply Freeze/Unfreeze (P6.10)
NOTIFY_SLO_EMIT_APPLY_FREEZE = int(os.getenv("NOTIFY_SLO_EMIT_APPLY_FREEZE", "0"))
NOTIFY_SLO_APPLY_FREEZE_KIND = os.getenv("NOTIFY_SLO_APPLY_FREEZE_KIND", "apply_freeze")
NOTIFY_SLO_APPLY_FREEZE_SCOPE = os.getenv("NOTIFY_SLO_APPLY_FREEZE_SCOPE", "ALL")
NOTIFY_SLO_APPLY_FREEZE_COOLDOWN_SEC = int(os.getenv("NOTIFY_SLO_APPLY_FREEZE_COOLDOWN_SEC", "1800"))
NOTIFY_SLO_APPLY_FREEZE_OPS_JSON = os.getenv("NOTIFY_SLO_APPLY_FREEZE_OPS_JSON", "[]")

NOTIFY_SLO_APPLY_UNFREEZE_ON_OK = int(os.getenv("NOTIFY_SLO_APPLY_UNFREEZE_ON_OK", "1"))
NOTIFY_SLO_APPLY_UNFREEZE_KIND = os.getenv("NOTIFY_SLO_APPLY_UNFREEZE_KIND", "apply_unfreeze")
NOTIFY_SLO_APPLY_UNFREEZE_COOLDOWN_SEC = int(os.getenv("NOTIFY_SLO_APPLY_UNFREEZE_COOLDOWN_SEC", "1800"))
NOTIFY_SLO_APPLY_UNFREEZE_OPS_JSON = os.getenv("NOTIFY_SLO_APPLY_UNFREEZE_OPS_JSON", "[]")


def get_redis_client():
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)

def fetch_window_stats(r, now_ts, window_min):
    """
    Sum counts from last `window_min` buckets.
    Returns: {
        "INFO": {"ok": 0, "err": 0},
        "CRIT": {"ok": 0, "err": 0},
        "PAGE": {"ok": 0, "err": 0},
        "TOTAL": {"ok": 0, "err": 0}
    }
    """
    stats = {
        "INFO": {"ok": 0, "err": 0},
        "CRIT": {"ok": 0, "err": 0},
        "PAGE": {"ok": 0, "err": 0},
        "TOTAL": {"ok": 0, "err": 0}
    }
    
    current_bucket = int(now_ts / 60)
    
    # We scan back `window_min` buckets
    # Note: current bucket might be partial, but usually included in rolling windows
    for i in range(window_min):
        bucket = current_bucket - i
        key = f"notify:win1m:{bucket}"
        
        try:
            data = retry_redis_operation(
                lambda: r.hgetall(key),
                operation_name=f"hgetall:{key}",
                max_retries=3
            )
        except Exception:
            data = {}
        
        if not data:
            continue
            
        for field, val_str in data.items():
            # field format: "ok:INFO", "err:CRIT"
            try:
                parts = field.split(":")
                kind = parts[0] # ok/err
                sev = parts[1] # INFO/CRIT/PAGE
                val = int(val_str)
                
                if sev in stats:
                    stats[sev][kind] += val
                
                stats["TOTAL"][kind] += val
                
            except Exception:
                pass
                
    return stats

def calculate_burn_rate(stats, slo_target):
    ok = stats["ok"]
    err = stats["err"]
    total = ok + err
    
    if total < MIN_SAMPLES:
        return 0.0, total
        
    error_ratio = err / total
    error_budget = 1.0 - slo_target
    
    if error_budget <= 0:
        return 0.0, total # Should not happen with valid SLO < 1.0
        
    burn_rate = error_ratio / error_budget
    return burn_rate, total

def check_staleness(r, now_ms):
    # Check global and per-severity last OK
    stale_status = {}
    
    keys = ["notify:last_ok_ts_ms", "notify:last_ok_ts_ms:CRIT", "notify:last_ok_ts_ms:PAGE"]
    
    for k in keys:
        try:
            last_ts = retry_redis_operation(lambda: r.get(k), operation_name=f"get:{k}", max_retries=3)
        except Exception:
            last_ts = None

        if last_ts:
            ago = now_ms - int(last_ts)
            stale_status[k] = ago
        else:
            stale_status[k] = -1 # Never?
            
    return stale_status

def emit_suggestion(r, kind, scope, ops_json_str, meta, cooldown_sec, dedup_key_suffix, dry_run=False):
    """
    Emits a configuration suggestion if not in cooldown.
    """
    if not NOTIFY_SLO_EMIT_SUGGESTIONS and not dry_run:
        return None

    # Check ops
    try:
        ops = json.loads(ops_json_str)
    except json.JSONDecodeError:
        logger.error(f"Invalid OPS JSON for {kind}: {ops_json_str}")
        return None

    if not ops and not NOTIFY_SLO_ALLOW_NOOP_SUGGESTIONS:
        logger.info(f"Skipping NOOP suggestion {kind} (empty ops)")
        return None

    # Check cooldown
    cooldown_key = f"sre:notify_slo:cooldown:{dedup_key_suffix}:{scope}"
    if not dry_run:
        try:
            if retry_redis_operation(lambda: r.exists(cooldown_key), operation_name=f"exists:{cooldown_key}", max_retries=3):
                logger.info(f"Cooldown active for {kind}:{scope}, skipping.")
                return None
        except Exception:
            return None # Fail safe

    # Create suggestion
    sid = str(uuid.uuid4())
    suggestion = {
        "sid": sid,
        "kind": kind,
        "scope": scope,
        "ts": time.time(),
        "ops": ops,
        "meta": meta,
        "author": "notify_slo_burn_monitor_v1"
    }

    # Helper for pretty print or log
    log_msg = f"Emitting suggestion {kind} sid={sid} scope={scope} ops={len(ops)}"
    
    if dry_run:
        print(f"[DRY-RUN] {log_msg}")
        return sid

    # Write to Redis
    try:
        def _write_suggestion():
            # 1. Set Cooldown
            r.setex(cooldown_key, cooldown_sec, "1")
            
            # 2. Push Suggestion
            key = f"cfg:suggestions:entry_policy:meta:{sid}"
            r.setex(key, NOTIFY_SLO_SUGGESTION_TTL_SEC, json.dumps(suggestion))
            return key
            
        key = retry_redis_operation(_write_suggestion, operation_name="emit_suggestion_tx", max_retries=3)
        logger.info(f"{log_msg} -> {key}")
        return sid
    except Exception as e:
        logger.error(f"Failed to emit suggestion {kind}: {e}")
        return None

def handle_trade_pause(r, scope, meta, dry_run=False):
    """
    Handles logic for Trade Pause.
    1. Emit trade_pause suggestion.
    2. Store SID in sre:notify_slo:trade_pause_sid:<scope> for later unpause tracking.
    """
    if not NOTIFY_SLO_EMIT_TRADE_PAUSE:
        return

    sid = emit_suggestion(
        r, 
        NOTIFY_SLO_TRADE_PAUSE_KIND, 
        scope, 
        NOTIFY_SLO_TRADE_PAUSE_OPS_JSON, 
        meta, 
        NOTIFY_SLO_TRADE_PAUSE_COOLDOWN_SEC, 
        "trade_pause",
        dry_run=dry_run
    )

    if sid and not dry_run:
        # Mark as paused state by this tool
        state_key = f"sre:notify_slo:trade_pause_sid:{scope}"
        try:
            retry_redis_operation(lambda: r.set(state_key, sid), operation_name=f"set:{state_key}", max_retries=3)
        except Exception:
            pass

def handle_trade_unpause(r, scope, meta, dry_run=False):
    """
    Handles logic for Trade Unpause.
    1. Check if we have an active pause from this tool.
    2. Check if that pause (SID) was actually APPLIED.
    3. Emit unpause suggestion.
    4. Clear pause state.
    """
    if not NOTIFY_SLO_EMIT_TRADE_PAUSE or not NOTIFY_SLO_UNPAUSE_ON_OK:
        return

    state_key = f"sre:notify_slo:trade_pause_sid:{scope}"
    try:
        pause_sid = retry_redis_operation(lambda: r.get(state_key), operation_name=f"get:{state_key}", max_retries=3)
    except Exception:
        pause_sid = None
    
    if not pause_sid:
        return # No active pause initiated by us
        
    # Check if applied
    # applied key: CFG_SUGGESTIONS_PREFIX:applied:{sid} 
    # e.g. "cfg:suggestions:entry_policy:applied:<sid>"
    applied_key = f"{CFG_SUGGESTIONS_PREFIX}:applied:{pause_sid}"
    
    if not dry_run:
        try:
            if not retry_redis_operation(lambda: r.exists(applied_key), operation_name=f"exists:{applied_key}", max_retries=3):
                return
        except Exception:
            return

    # Emit Unpause
    sid = emit_suggestion(
        r, 
        NOTIFY_SLO_TRADE_UNPAUSE_KIND, 
        scope, 
        NOTIFY_SLO_TRADE_UNPAUSE_OPS_JSON, 
        meta, 
        NOTIFY_SLO_TRADE_UNPAUSE_COOLDOWN_SEC, 
        "trade_unpause",
        dry_run=dry_run
    )
    
    if sid and not dry_run:
        # Clear the state so we don't unpause again
        try:
            retry_redis_operation(lambda: r.delete(state_key), operation_name=f"del:{state_key}", max_retries=3)
        except Exception:
            pass


def handle_apply_freeze(r, scope, meta, dry_run=False):
    """
    Handles logic for Apply Freeze (P6.10).
    1. Emit apply_freeze suggestion.
    2. Store SID in sre:notify_slo:apply_freeze_sid:<scope>.
    """
    if not NOTIFY_SLO_EMIT_APPLY_FREEZE:
        return

    # Check if already frozen by us
    state_key = f"sre:notify_slo:apply_freeze_sid:{scope}"
    if not dry_run:
        try:
            if retry_redis_operation(lambda: r.exists(state_key), operation_name=f"exists:{state_key}", max_retries=3):
                return
        except Exception:
            return

    sid = emit_suggestion(
        r,
        NOTIFY_SLO_APPLY_FREEZE_KIND,
        scope,
        NOTIFY_SLO_APPLY_FREEZE_OPS_JSON,
        meta,
        NOTIFY_SLO_APPLY_FREEZE_COOLDOWN_SEC,
        "apply_freeze",
        dry_run=dry_run
    )

    if sid and not dry_run:
        try:
            retry_redis_operation(lambda: r.set(state_key, sid), operation_name=f"set:{state_key}", max_retries=3)
        except Exception:
            pass

def handle_apply_unfreeze(r, scope, meta, dry_run=False):
    """
    Handles logic for Apply Unfreeze (P6.10).
    1. Check if frozen by us.
    2. Check if freeze was APPLIED.
    3. Emit apply_unfreeze suggestion.
    4. Clear freeze state.
    """
    if not NOTIFY_SLO_EMIT_APPLY_FREEZE or not NOTIFY_SLO_APPLY_UNFREEZE_ON_OK:
        return

    state_key = f"sre:notify_slo:apply_freeze_sid:{scope}"
    try:
        freeze_sid = retry_redis_operation(lambda: r.get(state_key), operation_name=f"get:{state_key}", max_retries=3)
    except Exception:
        freeze_sid = None

    if not freeze_sid:
        return

    # Check if applied
    applied_key = f"{CFG_SUGGESTIONS_PREFIX}:applied:{freeze_sid}"
    if not dry_run:
        try:
            if not retry_redis_operation(lambda: r.exists(applied_key), operation_name=f"exists:{applied_key}", max_retries=3):
                return
        except Exception:
            return

    sid = emit_suggestion(
        r,
        NOTIFY_SLO_APPLY_UNFREEZE_KIND,
        scope,
        NOTIFY_SLO_APPLY_UNFREEZE_OPS_JSON,
        meta,
        NOTIFY_SLO_APPLY_UNFREEZE_COOLDOWN_SEC,
        "apply_unfreeze",
        dry_run=dry_run
    )

    if sid and not dry_run:
        try:
            retry_redis_operation(lambda: r.delete(state_key), operation_name=f"del:{state_key}", max_retries=3)
        except Exception:
            pass


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--notify", action="store_true", help="Enable auto-escalation/notification (legacy flag)")
    parser.add_argument("--emit-suggestions", action="store_true", help="Enable P6.9 Suggestion emission")
    parser.add_argument("--print-json", "--print_json", dest="print_json", action="store_true", help="Print JSON report to stdout")
    parser.add_argument("--dry-run", "--dry_run", dest="dry_run", action="store_true", help="Do not write to Redis")
    parser.add_argument("--emit-metrics", action="store_true", help="Emit Prometheus metrics (ignored for now)")
    args = parser.parse_args()

    # Override env if generic flag is present? 
    # The flag `--emit_suggestions` explicitly enables it even if ENV is 0?
    # Let's trust ENV but use flag as gatekeeper if needed.
    # Actually, logic: if args.emit_suggestions is True, we proceed.
    
    # We update global var based on args if verified
    global NOTIFY_SLO_EMIT_SUGGESTIONS
    if args.emit_suggestions:
        NOTIFY_SLO_EMIT_SUGGESTIONS = 1
        
    r = get_redis_client()
    
    # Verify Redis is UP and responsive before proceeding
    if not args.dry_run:
        try:
            retry_redis_operation(
                lambda: r.ping(),
                operation_name="ping_redis",
                max_retries=5
            )
        except Exception as e:
            logger.error(f"Cannot connect to Redis: {e}")
            sys.exit(1)

    now_ts = time.time()
    now_ms = int(now_ts * 1000)

    # 1. Fetch Windows
    stats_fast = fetch_window_stats(r, now_ts, WINDOW_FAST_MIN)
    stats_slow = fetch_window_stats(r, now_ts, WINDOW_SLOW_MIN)
    
    # 2. Check Staleness & Lag
    stale_map = check_staleness(r, now_ms)
    try:
        queue_lag_str = retry_redis_operation(lambda: r.get("notify:last_queue_lag_ms"), operation_name="get:queue_lag", max_retries=3)
    except Exception:
        queue_lag_str = None
        
    queue_lag = int(queue_lag_str) if queue_lag_str else 0
    
    # 3. Analyze Burn Rates
    
    report = {
        "timestamp": now_ts,
        "status": "OK",
        "alerts": [],
        "burn_rates": {},
        "decision": {"actions": []}
    }
    
    rules = [
        ("WARN", SLO_TARGET_WARN, "INFO"), 
        ("CRIT", SLO_TARGET_CRIT, "CRIT"),
        ("PAGE", SLO_TARGET_PAGE, "PAGE")
    ]
    
    max_severity_level = 0 # 0=OK, 1=WARN, 2=CRIT, 3=PAGE
    
    for rule_name, slo, lookup in rules:
        # Fast Window
        br_fast, sample_fast = calculate_burn_rate(stats_fast[lookup], slo)
        # Slow Window
        br_slow, sample_slow = calculate_burn_rate(stats_slow[lookup], slo)
        
        report["burn_rates"][rule_name] = {
            "fast": br_fast,
            "slow": br_slow,
            "samples_fast": sample_fast,
            "samples_slow": sample_slow,
            "slo": slo
        }
        
        # Evaluation
        is_page = (br_fast > BURN_THRESHOLD_FAST_HI) and (br_slow > BURN_THRESHOLD_SLOW_HI)
        is_ticket = (br_fast > BURN_THRESHOLD_FAST_LO) and (br_slow > BURN_THRESHOLD_SLOW_LO)
        
        if is_page:
            report["alerts"].append(f"HighBurnRate:{rule_name} (Fast={br_fast:.1f}x, Slow={br_slow:.1f}x)")
            if rule_name in ["CRIT", "PAGE"]:
                max_severity_level = max(max_severity_level, 3)
            else:
                max_severity_level = max(max_severity_level, 2)
                
        elif is_ticket:
             report["alerts"].append(f"ElevatedBurnRate:{rule_name} (Fast={br_fast:.1f}x, Slow={br_slow:.1f}x)")
             if rule_name in ["CRIT", "PAGE"]:
                 max_severity_level = max(max_severity_level, 2)
             else:
                 max_severity_level = max(max_severity_level, 1)

    # 4. Analyze Staleness/Lag
    stale_crit = stale_map.get("notify:last_ok_ts_ms:CRIT", 0)
    # CRIT alerts are sparse, so they can legitimately go over MAX_STALE_MS without updates.
    # We rely on stale_global (notify:last_ok_ts_ms) to ensure the notifier is healthy.
    # if stale_crit > MAX_STALE_MS:
    #     report["alerts"].append(f"Stale:CRIT ({stale_crit/1000:.0f}s > {MAX_STALE_MS/1000}s)")
    #     max_severity_level = max(max_severity_level, 3)
    
    stale_global = stale_map.get("notify:last_ok_ts_ms", 0)
    if stale_global > MAX_STALE_MS:
         report["alerts"].append(f"Stale:Global ({stale_global/1000:.0f}s)")
         max_severity_level = max(max_severity_level, 1)

    if queue_lag > MAX_QUEUE_LAG_MS:
        report["alerts"].append(f"QueueLagHigh ({queue_lag/1000:.0f}s)")
        max_severity_level = max(max_severity_level, 2)

    # 5. Final Status
    if max_severity_level == 3:
        report["status"] = "PAGE"
    elif max_severity_level == 2:
        report["status"] = "CRIT"
    elif max_severity_level == 1:
        report["status"] = "WARN"

    # P6.9 Actions
    if NOTIFY_SLO_EMIT_SUGGESTIONS:
        meta_data = {
           "reason": f"NotifySLO Status: {report['status']}",
           "report": report
        }

        # Action 1: Emergency Suggestion (CRIT or PAGE)
        if max_severity_level >= 2:
            sid = emit_suggestion(
                r, 
                NOTIFY_SLO_EMERGENCY_KIND, 
                NOTIFY_SLO_SUGGESTIONS_SCOPE, 
                NOTIFY_SLO_EMERGENCY_OPS_JSON, 
                meta_data, 
                NOTIFY_SLO_EMERGENCY_COOLDOWN_SEC, 
                "emergency_notify",
                dry_run=args.dry_run
            )
            if sid: 
                report["decision"]["actions"].append(f"emergency:{sid}")

        # Action 2: Trade Pause (PAGE only)
        if max_severity_level >= 3:
             # Use predefined scope for pause
             handle_trade_pause(r, NOTIFY_SLO_TRADE_PAUSE_SCOPE, meta_data, dry_run=args.dry_run)
             report["decision"]["actions"].append(f"check_pause:{NOTIFY_SLO_TRADE_PAUSE_SCOPE}")
        
        # Action 3: Apply Freeze (CRIT or PAGE) (P6.10)
        if max_severity_level >= 2:
            handle_apply_freeze(r, NOTIFY_SLO_APPLY_FREEZE_SCOPE, meta_data, dry_run=args.dry_run)
            report["decision"]["actions"].append(f"check_apply_freeze:{NOTIFY_SLO_APPLY_FREEZE_SCOPE}")

        # Action 4: Trade Unpause & Apply Unfreeze (Recovery to OK)
        # Note: We classify WARN (1) as OK-ish for unpause? Or strictly OK (0)?
        # Usually Unpause should happen when we are confident. WARN might be shaky.
        # Let's stick to max_severity_level == 0 (OK)
        if max_severity_level == 0:
            handle_trade_unpause(r, NOTIFY_SLO_TRADE_PAUSE_SCOPE, meta_data, dry_run=args.dry_run)
            report["decision"]["actions"].append(f"check_unpause:{NOTIFY_SLO_TRADE_PAUSE_SCOPE}")
            
            handle_apply_unfreeze(r, NOTIFY_SLO_APPLY_FREEZE_SCOPE, meta_data, dry_run=args.dry_run)
            report["decision"]["actions"].append(f"check_apply_unfreeze:{NOTIFY_SLO_APPLY_FREEZE_SCOPE}")

        
    if args.print_json:
        print(json.dumps(report, indent=2))
        
    if max_severity_level >= 3:
        sys.exit(2)
    elif max_severity_level == 2:
        sys.exit(1)
    else:
        sys.exit(0)

if __name__ == "__main__":
    main()
