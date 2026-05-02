from utils.time_utils import get_ny_time_millis
#!/usr/bin/env python3,
"""meta_cov_quarantine_monitor_v1.py

P35: SRE Monitor for Meta Coverage Quarantine.
Checks:
- Active quarantine (cfg2.meta_cov_quarantine_<b>==1) with expired TTL.
- Non-zero share (cfg2.meta_enforce_share_cov_<b> > 0) while quarantined.
- Recent outcome apply (meta_cov_outcome_last_apply_ms freshness).

Actions:
- Emits metrics to redis (metrics:meta_cov_quarantine)
- Sends notifications (Telegram/Slack via redis:notify:telegram)
- Optional: --auto-recover (force share=0 or clear expired quarantine) - NOT IMPLEMENTED fully yet, just logs.

Usage:
  python3 -m tools.meta_cov_quarantine_monitor_v1 --emit-metrics --notify
  python3 -m tools.meta_cov_quarantine_monitor_v1 --dry-run,
""",
import argparse
import json
import logging
import os
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import redis

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    datefmt="%Y-%m-%d %H:%M:%S",
)
logger = logging.getLogger("QuarantineMon")

REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
DYN_CFG_KEY = os.getenv("DYN_CFG_KEY", "settings:dynamic_cfg")
NOTIFY_STREAM = os.getenv("CRYPTO_NOTIFY_STREAM", "notify:telegram")

# Cooldown for notifications (3600s default)
NOTIFY_COOLDOWN_SEC = int(os.getenv("META_COV_QUAR_NOTIFY_COOLDOWN_SEC", "3600"))
NOTIFY_COOLDOWN_KEY = "monitor:meta_cov_quarantine:notify_cooldown"


def get_redis_client() -> redis.Redis:
    return redis.Redis.from_url(REDIS_URL, decode_responses=False)


def _load_cfg2(r: redis.Redis) -> Dict[str, Any]:
    try:
        d = r.hgetall(DYN_CFG_KEY) or {}
        decoded = {}
        for k, v in d.items():
            ks = k.decode() if isinstance(k, bytes) else str(k)
            try:
                # Try JSON or float/int
                vs = v.decode() if isinstance(v, bytes) else str(v)
                if (vs.startswith("{") and vs.endswith("}")) or (vs.startswith("[") and vs.endswith("]")):
                    decoded[ks] = json.loads(vs)
                else:
                    # try int/float
                    try:
                        if "." in vs:
                            decoded[ks] = float(vs)
                        else:
                            decoded[ks] = int(vs)
                    except ValueError:
                        decoded[ks] = vs
            except Exception:
                decoded[ks] = v
        return decoded
    except Exception as e:
        logger.error(f"Failed to load cfg2: {e}")
        return {}


def _notify(r: redis.Redis, msg: str) -> None:
    """Send notification with cooldown check."""
    # Check cooldown
    last_sent = r.get(NOTIFY_COOLDOWN_KEY)
    now = time.time()
    if last_sent:
        if now - float(last_sent) < NOTIFY_COOLDOWN_SEC:
            logger.info(f"Notification suppressed (cooldown). Msg: {msg}")
            return

    payload = {"channel": "alerts", "message": f"[MetaCovQuarantine] {msg}", "level": "error"}
    r.xadd(NOTIFY_STREAM, {"payload": json.dumps(payload)}, maxlen=50000)
    r.set(NOTIFY_COOLDOWN_KEY, now)
    logger.info(f"Notification sent: {msg}")


def main() -> None:
    parser = argparse.ArgumentParser(description="Meta Cov Quarantine Monitor")
    parser.add_argument("--emit-metrics", action="store_true", help="Emit metrics to Redis stream")
    parser.add_argument("--notify", action="store_true", help="Send notifications on violations")
    parser.add_argument("--auto-recover", action="store_true", help="Attempt auto-recovery (experimental)")
    parser.add_argument("--dry-run", action="store_true", help="Dry run mode")

    args = parser.parse_args()

    r = get_redis_client()
    cfg2 = _load_cfg2(r)
    now_ms = get_ny_time_millis()

    alerts: List[str] = []
    metrics: List[Dict[str, Any]] = []

    # 1. Check invariants per bucket
    for b in ["a", "b", "c", "d"]:
        q_active = int(cfg2.get(f"meta_cov_quarantine_{b}", 0))
        q_until = int(cfg2.get(f"meta_cov_quarantine_until_ms_{b}", 0))
        share = float(cfg2.get(f"meta_enforce_share_cov_{b}", cfg2.get("meta_enforce_share", 0.0)))
        
        # Check: Quarantined but share > 0
        if q_active == 1:
            if share > 0:
                alerts.append(f"Bucket {b}: QUARANTINED but share={share:.2f} > 0!")
            
            # Check: Expired but still active
            if q_until > 0 and q_until < now_ms:
                # Expired
                alerts.append(f"Bucket {b}: Quarantine EXPIRED (until {q_until}) but active=1.")
                
            metrics.append({
                "bucket": b,
                "quarantine_active": 1,
                "share": share,
                "ttl_ms": max(0, q_until - now_ms) if q_until > 0 else 0
            })
        else:
            metrics.append({
                "bucket": b,
                "quarantine_active": 0,
                "share": share,
                "ttl_ms": 0
            })

    # 2. Check outcome freshness
    last_apply = int(cfg2.get("meta_cov_outcome_last_apply_ms", 0))
    hours_since = (now_ms - last_apply) / 1000.0 / 3600.0
    if last_apply > 0 and hours_since > 25: # Warning if > 25h (missed nightly?)
        alerts.append(f"Outcome not applied for {hours_since:.1f} hours (last: {last_apply})")

    # 3. Report
    if alerts:
        msg = " | ".join(alerts)
        logger.error(msg)
        if args.notify and not args.dry_run:
            _notify(r, msg)
    else:
        logger.info("No quarantine violations found.")

    # 4. Emit Metrics
    if args.emit_metrics and not args.dry_run:
        stream_key = "metrics:meta_cov_quarantine"
        for m in metrics:
            r.xadd(stream_key, {"json": json.dumps(m)}, maxlen=50000)
        logger.info(f"Emitted {len(metrics)} metrics to {stream_key}")

    if args.dry_run:
        print(json.dumps({"alerts": alerts, "metrics": metrics}, indent=2))

if __name__ == "__main__":
    main()
