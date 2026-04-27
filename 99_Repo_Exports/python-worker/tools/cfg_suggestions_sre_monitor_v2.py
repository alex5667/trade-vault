from utils.time_utils import get_ny_time_millis
#!/usr/bin/env python3
"""
cfg_suggestions_sre_monitor_v2.py

Lifecycle monitoring for cfg:suggestions:*
- Detects stuck suggestions (pending too long, approved but not applied).
- Flapping detection (too many freeze/unfreeze cycles).
- Auto-escalation of alerts.
- Metrics emission to Redis Stream.
- [P6.5] Auto Trade Pause on PAGE severity.
- [P6.5] Auto Trade Unpause on recovery.
- [P6.5] ACK mechanism for alert suppression.
- [P6.5] Delivery Receipt (retry) for PAGE alerts.
"""
import os
import sys
import json
import time
import argparse
import logging
import hashlib
from typing import Dict, List, Optional, Any
from redis import Redis

# Default Settings
DEFAULT_PREFIX = "cfg:suggestions:entry_policy"
DEFAULT_KINDS = ["meta_freeze"]
DEFAULT_SCOPES = ["ALL"]

# Thresholds
PENDING_MAX_MS = int(os.getenv("CFG_SUGGESTIONS_PENDING_MAX_MS", 3600000))  # 1h
APPROVED_MAX_MS = int(os.getenv("CFG_SUGGESTIONS_APPROVED_MAX_MS", 600000))    # 10m
FLAP_THRESHOLD_24H = int(os.getenv("CFG_SUGGESTIONS_FLAP_THRESHOLD_24H", 4))
FLAP_TTL_SEC = int(os.getenv("CFG_SUGGESTIONS_FLAP_TTL_SEC", 86400))
ESCALATE_PENDING_MS = int(os.getenv("CFG_SUGGESTIONS_ESCALATE_PENDING_MS", 7200000)) # 2h
ESCALATION_COOLDOWN_SEC = int(os.getenv("CFG_SUGGESTIONS_ESCALATION_COOLDOWN_SEC", 900)) # 15m

# Keys & Streams
NOTIFY_STREAM = os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram")
METRICS_STREAM = "metrics:cfg_suggestions"

logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(message)s')
logger = logging.getLogger("cfg_suggestions_monitor")

def _loads_ops_json(s: str) -> List[Dict[str, Any]]:
    if not s:
        return []
    try:
        v = json.loads(s)
    except Exception:
        return []
    if isinstance(v, list):
        return [x for x in v if isinstance(x, dict)]
    if isinstance(v, dict):
        return [v]
    return []

def _emergency_sid(emergency_kind: str, scope: str, ref_sid: str, now_ts_ms: int) -> str:
    h = hashlib.sha256(f"{emergency_kind}|{scope}|{ref_sid}".encode("utf-8", "ignore")).hexdigest()[:12]
    return f"emg:{emergency_kind}:{scope}:{now_ts_ms}:{h}"

class SugSREMonitor:
    def __init__(self, redis_url: str, dry_run: bool = False, 
                 emergency_enable: bool = False,
                 emergency_kind: str = "emergency_apply_stuck",
                 emergency_min_ms: int = 1800000,
                 emergency_cooldown_sec: int = 3600,
                 emergency_ttl_sec: int = 86400,
                 emergency_ops_json: str = "",
                 trade_pause_enable: bool = False,
                 trade_pause_kind: str = "trade_pause",
                 trade_unpause_kind: str = "trade_unpause",
                 trade_pause_ops_json: str = "",
                 trade_unpause_ops_json: str = "",
                 trade_pause_ttl_sec: int = 86400,
                 notify_require_receipt_page: bool = False,
                 notify_receipt_resend_sec: int = 300,
                 notify_receipt_key_prefix: str = "notify:receipt:"):
        self.redis = Redis.from_url(redis_url, decode_responses=True)
        self.dry_run = dry_run
        self.now_ms = get_ny_time_millis()
        
        # Emergency settings
        self.emergency_enable = emergency_enable
        self.emergency_kind = emergency_kind
        self.emergency_min_ms = emergency_min_ms
        self.emergency_cooldown_sec = emergency_cooldown_sec
        self.emergency_ttl_sec = emergency_ttl_sec
        self.emergency_ops_json = emergency_ops_json

        # Trade Pause settings [P6.5]
        self.trade_pause_enable = trade_pause_enable
        self.trade_pause_kind = trade_pause_kind
        self.trade_unpause_kind = trade_unpause_kind
        self.trade_pause_ops_json = trade_pause_ops_json
        self.trade_unpause_ops_json = trade_unpause_ops_json
        self.trade_pause_ttl_sec = trade_pause_ttl_sec
        
        # Receipt settings [P6.5]
        self.notify_require_receipt_page = notify_require_receipt_page
        self.notify_receipt_resend_sec = notify_receipt_resend_sec
        self.notify_receipt_key_prefix = notify_receipt_key_prefix

    def get_latest_sid(self, kind: str, scope: str) -> Optional[str]:
        key = f"latest:{kind}:{scope}"
        return self.redis.get(key)

    def get_suggestion(self, kind: str, scope: str, sid: str) -> Optional[Dict]:
        key = f"cfg:suggestions:{kind}:{scope}:{sid}"
        data = self.redis.get(key)
        if not data:
            return None
        try:
            return json.loads(data)
        except Exception as e:
            logger.error(f"Failed to parse suggestion {key}: {e}")
            return None

    def check_flapping(self, kind: str, scope: str) -> int:
        # Counter for switches in 24h
        flap_key = f"flap:cnt:{kind}:{scope}"
        cnt = self.redis.get(flap_key)
        return int(cnt) if cnt else 0

    def emit_metric(self, data: Dict):
        if self.dry_run:
            logger.info(f"[DRY-RUN] Metric: {data}")
            return
        try:
            self.redis.xadd(METRICS_STREAM, data, maxlen=10000)
        except Exception as e:
            logger.error(f"Failed to emit metric: {e}")

    def notify(self, message: str, severity: str = "WARN", receipt_id: str = None):
        """
        Send notification to Redis Stream.
        If strict receipt is required (PAGE), include receipt_id/require_receipt.
        """
        if self.dry_run:
            logger.info(f"[DRY-RUN] [Notify {severity}] {message} (rcpt={receipt_id})")
            return
        
        base = os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram")
        warn_stream = os.getenv("NOTIFY_TELEGRAM_STREAM_WARN", base)
        crit_stream = os.getenv("NOTIFY_TELEGRAM_STREAM_CRIT", base)
        page_stream = os.getenv("NOTIFY_TELEGRAM_STREAM_PAGE", base)

        sev = (severity or "WARN").upper()
        stream = warn_stream if sev in ("INFO", "WARN") else (crit_stream if sev == "CRIT" else page_stream)

        payload = {
            "type": "report",
            "text": message,  # Use 'text' for compatibility with standard schema
            "ts": str(self.now_ms),
            "severity": sev,
            "source": "cfg_suggestions_sre",
        }
        
        if receipt_id:
            payload["receipt_id"] = receipt_id
            payload["require_receipt"] = "1"

        # Chat ID override
        chat_id = os.getenv(f"NOTIFY_TELEGRAM_CHAT_ID_{sev}") or os.getenv("NOTIFY_TELEGRAM_CHAT_ID")
        if chat_id:
            payload["chat_id"] = str(chat_id)

        try:
            # Main stream
            self.redis.xadd(stream, payload, maxlen=50000)
            
            # Mirror to base if different
            mirror_base = int(os.getenv("NOTIFY_TELEGRAM_MIRROR_BASE", "1"))
            if mirror_base and stream != base:
                self.redis.xadd(base, payload, maxlen=50000)
                
            logger.info(f"Notification sent: [{severity}] {message[:100]}... (rcpt={receipt_id})")
        except Exception as e:
            logger.error(f"Failed to send notification: {e}")

    def maybe_emit_trade_pause(self, prefix: str, scope: str, cause_kind: str, cause_sid: str) -> bool:
        """ [P6.5] Auto-emit trade_pause suggestion if PAGE. """
        if not self.trade_pause_enable:
            return False

        req_key = f"sre:trade_pause:requested:{scope}"
        # If we already requested pause for this scope and it hasn't expired, skip
        if self.redis.exists(req_key):
             return False

        # Generate SID
        h = hashlib.sha256(f"{self.trade_pause_kind}|{scope}|{cause_sid}".encode("utf-8")).hexdigest()[:8]
        pause_sid = f"autopause:{scope}:{self.now_ms}:{h}"
        
        ops = _loads_ops_json(self.trade_pause_ops_json)
        
        meta = {
            "kind": self.trade_pause_kind,
            "scope": self.trade_pause_kind_scope(scope), # Use specific scope logic if needed
            "ts_ms": self.now_ms,
            "severity": "PAGE",
            "refs": {"cause_kind": cause_kind, "cause_sid": cause_sid},
            "ops": ops,
            "hint": "Auto Trade Pause triggered by SRE Monitor (PAGE severity)"
        }
        
        if self.dry_run:
            logger.info(f"[DRY-RUN] Auto Trade Pause: {meta}")
            return True
            
        try:
             # Standard suggestion write
             self.redis.setex(f"{prefix}:meta:{pause_sid}", self.trade_pause_ttl_sec, json.dumps(meta))
             self.redis.setex(f"latest:{self.trade_pause_kind}:{meta['scope']}", self.trade_pause_ttl_sec, pause_sid)
             
             # Mark 'requested' so we don't spam it. 
             # Also used by unpause logic to know we paused it.
             self.redis.setex(req_key, self.trade_pause_ttl_sec, pause_sid)
             
             logger.warning(f"AUTO TRADE PAUSE emitted: {pause_sid}")
             return True
        except Exception as e:
            logger.error(f"Failed to emit trade pause: {e}")
            return False
            
    def trade_pause_kind_scope(self, issue_scope: str) -> str:
        # If env var CFG_SUGGESTIONS_TRADE_PAUSE_SCOPE is set, use it (e.g. ALL)
        # Otherwise use issue_scope
        override = os.getenv("CFG_SUGGESTIONS_TRADE_PAUSE_SCOPE", "")
        return override if override else issue_scope

    def maybe_emit_trade_unpause(self, prefix: str, scope: str):
        """ [P6.5] Auto-emit trade_unpause if we paused it and issues are cleared. """
        if not self.trade_pause_enable:
            return

        # Check if we paused this scope
        req_key = f"sre:trade_pause:requested:{scope}"
        pause_sid = self.redis.get(req_key)
        if not pause_sid:
            return # We didn't pause it (or TTL expired), so we don't unpause automatically
        
        # Determine effective scope for Unpause
        unpause_scope = self.trade_pause_kind_scope(scope)
        
        # Check if pause was applied
        if not self.redis.exists(f"{prefix}:applied:{pause_sid}"):
            # Not applied yet, so no need to unpause (cancel?) - Keep simple, just wait.
            return

        # Safety: Ensure Unpause isn't spammed. 
        # But here we only call this if NO PAGE incidents remain for this scope.
        
        h = hashlib.sha256(f"{self.trade_unpause_kind}|{scope}|{pause_sid}".encode("utf-8")).hexdigest()[:8]
        unpause_sid = f"autoclear:{scope}:{self.now_ms}:{h}"
        ops = _loads_ops_json(self.trade_unpause_ops_json)
        
        meta = {
            "kind": self.trade_unpause_kind,
            "scope": unpause_scope,
            "ts_ms": self.now_ms,
            "severity": "INFO",
            "refs": {"pause_sid": pause_sid},
            "ops": ops,
            "hint": "Auto Trade Unpause - issues cleared"
        }
        
        if self.dry_run:
            logger.info(f"[DRY-RUN] Auto Trade Unpause: {meta}")
            return

        try:
             self.redis.setex(f"{prefix}:meta:{unpause_sid}", self.trade_pause_ttl_sec, json.dumps(meta))
             self.redis.setex(f"latest:{self.trade_unpause_kind}:{unpause_scope}", self.trade_pause_ttl_sec, unpause_sid)
             
             # Clear the request key so we don't try to unpause again
             self.redis.delete(req_key)
             logger.info(f"AUTO TRADE UNPAUSE emitted: {unpause_sid}")
        except Exception as e:
            logger.error(f"Failed to emit trade unpause: {e}")

    def maybe_emit_emergency(self, prefix: str, kind: str, scope: str, sid: str, age_ms: int, severity: str, alerts: List[str]) -> bool:
        """Emit emergency suggestion if configured."""
        if not self.emergency_enable:
            return False
            
        # Check cooldown
        st_key = f"sre:cfg_sugg:emergency:last_ms:{kind}:{scope}"
        last_ms = int(float(self.redis.get(st_key) or 0))
        if last_ms > 0 and self.now_ms - last_ms < self.emergency_cooldown_sec * 1000:
            return False

        # Dedup: check if latest emergency is still pending
        em_latest_key = f"{prefix}:latest:{self.emergency_kind}:{scope}"
        existing_sid = self.redis.get(em_latest_key)
        if existing_sid:
            if not self.redis.exists(f"{prefix}:applied:{existing_sid}"):
                 return False

        ops = _loads_ops_json(self.emergency_ops_json)
        hint = os.getenv(
            "CFG_SUGGESTIONS_EMERGENCY_HINT",
            "Investigate ApplyRunner and apply or rollback the referenced proposal; consider unlocking apply contour if stuck.",
        )

        em_sid = _emergency_sid(self.emergency_kind, scope, sid, self.now_ms)
        
        meta = {
            "kind": self.emergency_kind,
            "scope": scope,
            "ts_ms": self.now_ms,
            "severity": (severity or "CRIT").upper(),
            "refs": {"kind": kind, "scope": scope, "sid": sid},
            "age_ms": int(age_ms),
            "alerts": list(alerts)[:10],
            "hint": hint,
            "ops": ops,
        }

        if self.dry_run:
            logger.info(f"[DRY-RUN] Would create EMERGENCY suggestion {em_sid}: {meta}")
            return True

        # Write meta and approvals
        try:
            self.redis.setex(f"{prefix}:meta:{em_sid}", self.emergency_ttl_sec, json.dumps(meta))
            
            # Optional approvals hash
            self.redis.hset(f"{prefix}:approvals:{em_sid}", mapping={"ts_ms": str(self.now_ms), "status": "pending"})
            self.redis.expire(f"{prefix}:approvals:{em_sid}", self.emergency_ttl_sec)
            
            # Update latest pointer
            self.redis.setex(em_latest_key, self.emergency_ttl_sec, em_sid)
            
            # Update cooldown
            self.redis.setex(st_key, max(self.emergency_ttl_sec, self.emergency_cooldown_sec * 2), str(self.now_ms))
            
            logger.warning(f"EMERGENCY suggestion created: {em_sid}")
            return True
        except Exception as e:
            logger.error(f"Failed to emit emergency suggestion: {e}")
            return False

    def run(self, kinds: List[str], scopes: List[str], emit_metrics: bool = True, do_notify: bool = True):
        summary = {
            "pending_n": 0,
            "approved_n": 0,
            "applied_n": 0,
            "stuck_pending_n": 0,
            "stuck_approved_n": 0,
            "alerts_n": 0,
            "max_sev": "OK"
        }

        prefix = DEFAULT_PREFIX # Use default prefix for emergency construction
        page_incident_in_scope = {s: False for s in scopes} # Track PAGE incidents per scope

        for kind in kinds:
            for scope in scopes:
                sid = self.get_latest_sid(kind, scope)
                if not sid:
                    continue
                
                sug = self.get_suggestion(kind, scope, sid)
                if not sug:
                    continue

                state = sug.get("state", "pending")
                created_at = sug.get("created_at", self.now_ms)
                approved_at = sug.get("approved_at")
                
                age_pending = self.now_ms - created_at
                
                # Logic
                alert_msg = None
                sev = "OK"
                emergency_emitted = 0

                if state == "pending":
                    summary["pending_n"] += 1
                    if age_pending > PENDING_MAX_MS:
                        summary["stuck_pending_n"] += 1
                        sev = "WARN"
                        if age_pending > ESCALATE_PENDING_MS:
                            sev = "CRIT"
                        alert_msg = f"Suggestion {kind}:{scope}:{sid} PENDING for {age_pending//1000}s"
                
                elif state == "approved":
                    summary["approved_n"] += 1
                    age_approved = self.now_ms - approved_at if approved_at else 0
                    if age_approved > APPROVED_MAX_MS:
                        summary["stuck_approved_n"] += 1
                        sev = "CRIT"
                        
                        # Escalation to PAGE
                        if age_approved >= self.emergency_min_ms: # Using emergency threshold for PAGE too
                             sev = "PAGE"
                        
                        # Emergency Proposal Logic
                        if self.emergency_enable and age_approved >= self.emergency_min_ms:
                            if self.maybe_emit_emergency(prefix, kind, scope, sid, age_approved, sev, ["stuck_approved"]):
                                emergency_emitted = 1
                                alert_msg = f"Suggestion {kind}:{scope}:{sid} APPROVED but NOT APPLIED for {age_approved//1000}s (Emergency Emitted)"
                            else:
                                alert_msg = f"Suggestion {kind}:{scope}:{sid} APPROVED but NOT APPLIED for {age_approved//1000}s"
                        else:
                            alert_msg = f"Suggestion {kind}:{scope}:{sid} APPROVED but NOT APPLIED for {age_approved//1000}s"

                elif state == "applied":
                    summary["applied_n"] += 1

                # Flapping check
                flap_cnt = self.check_flapping(kind, scope)
                if flap_cnt >= FLAP_THRESHOLD_24H:
                    sev = "CRIT"
                    alert_msg = f"FLAPPING DETECTED for {kind}:{scope}: {flap_cnt} toggles in 24h"

                metric_node = {
                    "kind": kind,
                    "scope": scope,
                    "sid": sid,
                    "state": state,
                    "age_ms": age_pending,
                    "flap_cnt_24h": flap_cnt,
                    "emergency_emitted": emergency_emitted
                }
                
                # Handle Alert
                if alert_msg:
                    summary["alerts_n"] += 1

                    # [P6.5] Trade Pause Trigger
                    if sev == "PAGE":
                        page_incident_in_scope[scope] = True
                        self.maybe_emit_trade_pause(prefix, scope, kind, sid)

                    # Update max severity
                    current_sev_rank = {"OK":0, "WARN":1, "CRIT":2, "PAGE":3}
                    if current_sev_rank.get(sev, 0) > current_sev_rank.get(summary["max_sev"], 0):
                        summary["max_sev"] = sev
                        
                    if do_notify:
                        # [P6.5] Check ACK
                        # We check distinct ACK keys: by Kind+Scope or Generic
                        # 1. Specific Kind+Scope
                        ack_key = f"sre:ack:cfg_sugg:{kind}:{scope}"
                        is_acked = self.redis.exists(ack_key)
                        
                        if not is_acked:
                            # 2. Check Cooldown / Lock
                            # Logic changed for Receipts:
                            # If PAGE and Require Receipt, logic handled below
                            
                            lock_key = f"sre:alert:lock:{kind}:{scope}:{sid}:{sev}"
                            cooldown = ESCALATION_COOLDOWN_SEC
                            
                            receipt_id = None
                            
                            # [P6.5] Receipt Handling
                            if sev == "PAGE" and self.notify_require_receipt_page:
                                # Generate deterministic receipt ID for this incident
                                r_hash = hashlib.md5(f"{kind}:{scope}:{sid}".encode()).hexdigest()
                                receipt_id = f"rcpt:{r_hash}"
                                
                                # Check if receipt exists
                                r_key = f"{self.notify_receipt_key_prefix}{receipt_id}"
                                if self.redis.exists(r_key):
                                    # Receipt exists -> Considered ACKed/Handled -> Stop notifying
                                    logger.info(f"PAGE alert suppressed by receipt {receipt_id}")
                                    is_acked = True
                                else:
                                    # Receipt missing -> Resend faster
                                    cooldown = self.notify_receipt_resend_sec
                            
                            if not is_acked:
                                # Try to take lock
                                # Note: For receipts, lock expires faster (cooldown) so we retry
                                if self.redis.set(lock_key, "1", nx=True, ex=cooldown):
                                    self.notify(alert_msg, severity=sev, receipt_id=receipt_id)
                        else:
                            logger.info(f"Alert suppressed by ACK: {ack_key}")

                if emit_metrics:
                    self.emit_metric(metric_node)
        
        # [P6.5] Trade Unpause Check
        for scope in scopes:
            if not page_incident_in_scope[scope]:
                # No PAGE incidents in this scope during this run -> Try Unpause
                self.maybe_emit_trade_unpause(prefix, scope)

        # Emit aggregate summary
        if emit_metrics:
            self.emit_metric(summary)
            
        return 2 if summary["alerts_n"] > 0 else 0

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--kinds", default=",".join(DEFAULT_KINDS))
    parser.add_argument("--scopes", default=",".join(DEFAULT_SCOPES))
    parser.add_argument("--emit-metrics", action="store_true")
    parser.add_argument("--notify", action="store_true")
    
    # Emergency args
    parser.add_argument("--emergency_enable", type=int, default=int(os.getenv("CFG_SUGGESTIONS_EMERGENCY_ENABLE", "1")))
    parser.add_argument("--emergency_kind", default=os.getenv("CFG_SUGGESTIONS_EMERGENCY_KIND", "emergency_apply_stuck"))
    parser.add_argument("--emergency_min_ms", type=int, default=int(os.getenv("CFG_SUGGESTIONS_EMERGENCY_MIN_MS", "1800000")))
    parser.add_argument("--emergency_cooldown_sec", type=int, default=int(os.getenv("CFG_SUGGESTIONS_EMERGENCY_COOLDOWN_SEC", "3600")))
    parser.add_argument("--emergency_ttl_sec", type=int, default=int(os.getenv("CFG_SUGGESTIONS_EMERGENCY_TTL_SEC", "86400")))
    parser.add_argument("--emergency_ops_json", default=os.getenv("CFG_SUGGESTIONS_EMERGENCY_OPS_JSON", ""))
    
    # [P6.5] Trade Pause Params
    parser.add_argument("--trade_pause_enable", type=int, default=int(os.getenv("CFG_SUGGESTIONS_TRADE_PAUSE_ENABLE", "0")))
    parser.add_argument("--trade_pause_kind", default=os.getenv("CFG_SUGGESTIONS_TRADE_PAUSE_KIND", "trade_pause"))
    parser.add_argument("--trade_unpause_kind", default=os.getenv("CFG_SUGGESTIONS_TRADE_UNPAUSE_KIND", "trade_unpause"))
    parser.add_argument("--trade_pause_ops_json", default=os.getenv("CFG_SUGGESTIONS_TRADE_PAUSE_OPS_JSON", ""))
    parser.add_argument("--trade_unpause_ops_json", default=os.getenv("CFG_SUGGESTIONS_TRADE_UNPAUSE_OPS_JSON", ""))
    
    # [P6.5] Receipt Params
    parser.add_argument("--notify_require_receipt_page", type=int, default=int(os.getenv("NOTIFY_REQUIRE_RECEIPT_PAGE", "0")))
    parser.add_argument("--notify_receipt_resend_sec", type=int, default=int(os.getenv("NOTIFY_RECEIPT_RESEND_SEC", "300")))

    args = parser.parse_args()

    kinds = args.kinds.split(",")
    scopes = args.scopes.split(",")

    monitor = SugSREMonitor(
        args.redis_url, 
        dry_run=args.dry_run,
        emergency_enable=bool(args.emergency_enable),
        emergency_kind=args.emergency_kind,
        emergency_min_ms=args.emergency_min_ms,
        emergency_cooldown_sec=args.emergency_cooldown_sec,
        emergency_ttl_sec=args.emergency_ttl_sec,
        emergency_ops_json=args.emergency_ops_json,
        trade_pause_enable=bool(args.trade_pause_enable),
        trade_pause_kind=args.trade_pause_kind,
        trade_unpause_kind=args.trade_unpause_kind,
        trade_pause_ops_json=args.trade_pause_ops_json,
        trade_unpause_ops_json=args.trade_unpause_ops_json,
        notify_require_receipt_page=bool(args.notify_require_receipt_page),
        notify_receipt_resend_sec=args.notify_receipt_resend_sec
    )
    rc = monitor.run(kinds, scopes, emit_metrics=args.emit_metrics, do_notify=args.notify)
    
    if rc != 0:
        logger.warning(f"Monitor finished with alerts (rc={rc})")
    else:
        logger.info("Monitor finished: OK")
        
    sys.exit(rc)

if __name__ == "__main__":
    main()
