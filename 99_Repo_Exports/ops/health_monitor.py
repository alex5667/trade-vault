#!/usr/bin/env python3
"""
Application Health Monitor
==========================
Monitors Redis consumer groups and stream activity to detect stalled workers.

Features:
- Checks consumer group lag and pending messages
- Monitors last-delivered-id timestamps
- Detects workers that haven't processed messages in N minutes
- Sends alerts to Telegram via notify:telegram stream

Usage:
    python3 ops/health_monitor.py
"""

import redis
import os
import sys
import time
import json
import logging
from datetime import datetime, timezone
from typing import Dict, Any, Optional, List

# Add python-worker to path to import core models
SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
PROJECT_ROOT = os.path.dirname(SCRIPT_DIR)
PYTHON_WORKER_DIR = os.path.join(PROJECT_ROOT, "python-worker")
if PYTHON_WORKER_DIR not in sys.path:
    sys.path.insert(0, PYTHON_WORKER_DIR)

try:
    from core.entry_policy_overrides_v1 import EntryPolicyOverridesV1
except ImportError:
    EntryPolicyOverridesV1 = None

# Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
NOTIFY_STREAM = os.getenv("NOTIFY_STREAM", "notify:telegram")
CHECK_INTERVAL_SEC = int(os.getenv("HEALTH_CHECK_INTERVAL", "60"))
STALL_THRESHOLD_SEC = int(os.getenv("HEALTH_STALL_THRESHOLD", "600"))  # 10 minutes
STARTUP_GRACE_SEC = int(os.getenv("HEALTH_STARTUP_GRACE_SEC", "300"))  # 5 minutes

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] health_monitor: %(message)s"
)
logger = logging.getLogger("health_monitor")

# Monitored streams and their consumer groups
MONITORED_STREAMS = {
    "stream:signals:outbox": {
        "group": "signals-outbox-group",
        "name": "signal-dispatcher",
        "description": "Signal Dispatcher",
        "stall_threshold": 600,  # 10 minutes
    },
    "notify:telegram": {
        "group": "notify-group",
        "name": "notify-worker",
        "description": "Telegram Notifier",
        "stall_threshold": 600,
    },
}

# State tracking
last_alert_time: Dict[str, float] = {}
ALERT_COOLDOWN_SEC = 300  # 5 minutes between repeated alerts

def get_redis_client() -> redis.Redis:
    """Create a new Redis client and verify connectivity with ping."""
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    r.ping()
    return r


def _reconnect_with_backoff() -> redis.Redis:
    """Block until Redis is reachable, then return a fresh client."""
    delay = 1.0
    attempt = 0
    while True:
        try:
            r = get_redis_client()
            if attempt > 0:
                logger.info(f"Redis reconnected after {attempt} attempt(s)")
            return r
        except Exception as e:
            attempt += 1
            logger.warning(f"Redis not ready (attempt {attempt}): {e}. Retry in {delay:.0f}s...")
            time.sleep(delay)
            delay = min(delay * 2, 30.0)

def send_alert(r: redis.Redis, title: str, message: str, level: str = "WARNING"):
    """Publish alert to notify:telegram stream."""
    try:
        icon = "🚨" if level == "ERROR" else "⚠️"
        if level == "INFO":
            icon = "✅"
            
        text = (
            f"{icon} <b>Health Alert: {title}</b>\n\n"
            f"{message}\n"
            f"<i>Time: {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}</i>"
        )
        
        payload = {
            "type": "report",
            "text": text,
            "source": "HealthMonitor",
            "level": level
        }
        
        r.xadd(NOTIFY_STREAM, payload, maxlen=1000)
        logger.info(f"Sent alert: {title}")
        
    except Exception as e:
        logger.error(f"Failed to send alert to Redis: {e}")

def _toggle_shadow_mode(r: redis.Redis, enable: bool, reason: str):
    """Write to Redis entry policy override to force shadow mode globally."""
    if EntryPolicyOverridesV1 is None:
        logger.error("Cannot toggle shadow mode: EntryPolicyOverridesV1 not imported!")
        return

    now_ms = int(time.time() * 1000)
    overrides = EntryPolicyOverridesV1(
        updated_ts_ms=now_ms,
        enabled=1,
        freeze_active=1 if enable else 0,
        freeze_mode="shadow",
        freeze_reason=reason[:100],  # truncate just in case
    )
    
    key = "cfg:entry_policy:overrides:v1"
    try:
        r.set(key, overrides.to_json())
        status = "ENABLED" if enable else "DISABLED"
        
        # Also send a Telegram alert specifically about shadow mode transition
        send_alert(
            r,
            f"Auto-Shadow Mode {status}",
            f"Reason: {reason}\nSystem trading is now in {'SHADOW' if enable else 'LIVE'} mode.",
            level="WARNING" if enable else "INFO"
        )
        logger.info(f"Global override set: shadow mode {status} (Reason: {reason})")
    except Exception as e:
        logger.error(f"Failed to set global shadow mode override: {e}")


def parse_stream_id(stream_id: str) -> Optional[int]:
    """Extract timestamp (ms) from Redis stream ID (format: timestamp-sequence)."""
    try:
        return int(stream_id.split("-")[0])
    except (ValueError, IndexError):
        return None

def _min_consumer_idle_sec(r: redis.Redis, stream_key: str, group_name: str) -> Optional[float]:
    """Return min idle time (seconds) across consumers; None if no consumers."""
    try:
        consumers = r.xinfo_consumers(stream_key, group_name)
    except Exception:
        return None
    if not consumers:
        return None
    min_idle_ms = min((c.get("idle", 0) for c in consumers), default=None)
    if min_idle_ms is None:
        return None
    return min_idle_ms / 1000.0


def check_consumer_group(r: redis.Redis, stream_key: str, config: Dict[str, Any]) -> Optional[Dict[str, Any]]:
    """Check health of a consumer group. Returns issue dict if unhealthy, None if healthy."""
    try:
        # Get consumer group info
        groups = r.xinfo_groups(stream_key)
        
        # Find our group
        group_info = None
        for g in groups:
            if g.get("name") == config["group"]:
                group_info = g
                break
        
        if not group_info:
            logger.warning(f"Consumer group '{config['group']}' not found for stream '{stream_key}'")
            return None
        
        # Extract metrics
        lag = group_info.get("lag", 0)
        pending = group_info.get("pending", 0)
        last_delivered_id = group_info.get("last-delivered-id", "0-0")
        
        # Parse timestamp from last-delivered-id
        last_ts_ms = parse_stream_id(last_delivered_id)
        if not last_ts_ms:
            logger.warning(f"Could not parse timestamp from ID: {last_delivered_id}")
            return None
        
        # Calculate time since last activity
        now_ms = int(time.time() * 1000)
        idle_ms = now_ms - last_ts_ms
        idle_sec = idle_ms / 1000

        # Check consumer activity
        min_consumer_idle_sec = _min_consumer_idle_sec(r, stream_key, config["group"])
        consumer_count = group_info.get("consumers", 0)
        
        # Check if stalled
        threshold = config.get("stall_threshold", STALL_THRESHOLD_SEC)
        if lag <= 0:
            return None
        if idle_sec <= threshold:
            return None
        if consumer_count and min_consumer_idle_sec is not None and min_consumer_idle_sec <= threshold:
            # Consumers are active; allow some time to catch up.
            return None
        if idle_sec > threshold:
            return {
                "stream": stream_key,
                "service": config["description"],
                "idle_sec": idle_sec,
                "idle_minutes": idle_sec / 60,
                "lag": lag,
                "pending": pending,
                "last_id": last_delivered_id,
                "threshold": threshold,
                "consumer_idle_sec": min_consumer_idle_sec,
                "consumers": consumer_count,
            }
        
        # Log healthy status periodically
        if int(time.time()) % 300 == 0:  # Every 5 minutes
            logger.info(
                f"✓ {config['description']}: lag={lag}, pending={pending}, "
                f"idle={idle_sec:.0f}s"
            )
        
        return None
        
    except redis.exceptions.ResponseError as e:
        if "no such key" in str(e).lower():
            logger.debug(f"Stream '{stream_key}' does not exist yet")
        else:
            logger.error(f"Redis error checking {stream_key}: {e}")
        return None
    except Exception as e:
        logger.error(f"Error checking consumer group for {stream_key}: {e}")
        return None

def should_send_alert(service_name: str) -> bool:
    """Check if enough time has passed since last alert for this service."""
    now = time.time()
    last_alert = last_alert_time.get(service_name, 0)
    
    if now - last_alert >= ALERT_COOLDOWN_SEC:
        last_alert_time[service_name] = now
        return True
    
    return False

def main():
    logger.info("Starting Application Health Monitor...")
    startup_time = time.time()
    
    # Connect to Redis
    while True:
        try:
            r = get_redis_client()
            r.ping()
            logger.info(f"Connected to Redis at {REDIS_URL}")
            break
        except redis.exceptions.ResponseError as e:
            if "LOADING" in str(e):
                logger.warning("Redis is loading dataset... waiting 5s")
            else:
                logger.warning(f"Redis response error: {e}... waiting 5s")
            time.sleep(5)
        except redis.exceptions.ConnectionError as e:
            logger.warning(f"Redis connection failed: {e}... waiting 5s")
            time.sleep(5)
        except Exception as e:
            logger.critical(f"Unexpected error connecting to Redis: {e}")
            logger.info("Retrying in 5s...")
            time.sleep(5)
    
    # Send startup notification
    send_alert(
        r,
        "Health Monitor Started",
        f"Monitoring {len(MONITORED_STREAMS)} services for stalls.\n"
        f"Check interval: {CHECK_INTERVAL_SEC}s\n"
        f"Stall threshold: {STALL_THRESHOLD_SEC}s",
        level="INFO"
    )
    
    logger.info(f"Monitoring {len(MONITORED_STREAMS)} streams...")
    logger.info(f"Check interval: {CHECK_INTERVAL_SEC}s, Stall threshold: {STALL_THRESHOLD_SEC}s")
    
    # Main monitoring loop
    system_shadow_forced = False

    while True:
        try:
            stalled_services = []
            
            for stream_key, config in MONITORED_STREAMS.items():
                issue = check_consumer_group(r, stream_key, config)
                
                if issue:
                    if time.time() - startup_time < STARTUP_GRACE_SEC:
                        logger.info(
                            "Startup grace period active (%ss) - suppressing alert for %s",
                            STARTUP_GRACE_SEC,
                            config["description"],
                        )
                        continue
                        
                    stalled_services.append(config["description"])
                    
                    if should_send_alert(config["name"]):
                        # Service is stalled - send alert
                        message = (
                            f"Service has not processed messages for <b>{issue['idle_minutes']:.1f} minutes</b>\n\n"
                            f"Stream: <code>{issue['stream']}</code>\n"
                            f"Consumer Group: <code>{config['group']}</code>\n"
                            f"Lag: <b>{issue['lag']}</b> messages\n"
                            f"Pending: <b>{issue['pending']}</b> messages\n"
                            f"Consumers: <b>{issue.get('consumers', 0)}</b>\n"
                            f"Min consumer idle: <b>{(issue.get('consumer_idle_sec') or 0):.0f}s</b>\n"
                            f"Last ID: <code>{issue['last_id']}</code>\n\n"
                            f"⚠️ Service may be stuck or crashed silently."
                        )
                        
                        send_alert(
                            r,
                            f"{issue['service']} Stalled",
                            message,
                            level="ERROR"
                        )
            
            # --- Auto-Shadow Fallback Logic ---
            if stalled_services:
                if not system_shadow_forced:
                    logger.warning(f"Services stalled ({', '.join(stalled_services)}). Forcing Global Shadow Mode!")
                    _toggle_shadow_mode(r, enable=True, reason=f"Stalled: {', '.join(stalled_services)}")
                    system_shadow_forced = True
            else:
                if system_shadow_forced:
                    logger.info("All scanned services recovered. Restoring standard trading mode.")
                    _toggle_shadow_mode(r, enable=False, reason="Services recovered")
                    system_shadow_forced = False
            
            time.sleep(CHECK_INTERVAL_SEC)
            
        except KeyboardInterrupt:
            logger.info("Shutting down health monitor...")
            break
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError, OSError) as e:
            # redis-worker-1 restarted with a new IP — reconnect
            logger.warning(f"Redis connection lost in monitoring loop: {e}. Reconnecting...")
            r = _reconnect_with_backoff()
        except Exception as e:
            logger.error(f"Error in monitoring loop: {e}")
            time.sleep(CHECK_INTERVAL_SEC)

if __name__ == "__main__":
    main()
