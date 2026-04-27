#!/usr/bin/env python3
"""
Docker Watchdog Service
=======================
Monitors Docker events for container crashes (die/oom) and sends alerts 
to Telegram via the 'scanner-notify-worker'.

Features:
- Connects to local Docker socket.
- Filters for 'die' events with non-zero exit codes.
- Filters for 'oom' (Out of Memory) events.
- Ignores manual stops (exit code 137 without OOM) unless configured strictly.
- Publishes structured alerts to Redis stream `notify:telegram`.

Usage:
    python3 ops/docker_watchdog.py
"""

import docker
import redis
import os
import sys
import time
import json
import logging
import socket
from datetime import datetime, timezone
from typing import Dict, Any, Optional

# Add python-worker to path for imports
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'python-worker'))

from core.redis_client import get_redis, wait_for_redis

# Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
NOTIFY_STREAM = os.getenv("NOTIFY_STREAM", "notify:telegram")
HOSTNAME = socket.gethostname()
WATCH_PREFIXES = ["scanner-", "redis-", "trade-"]  # Filter relevant containers

# Logging setup
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] watchdog: %(message)s"
)
logger = logging.getLogger("watchdog")

def is_watched_container(name: str) -> bool:
    """Check if container name matches our watch list."""
    # Ensure strict string type
    if not isinstance(name, str):
        return False
        
    name = name.strip()
    # Remove leading slash if present (docker API quirk)
    if name.startswith("/"):
        name = name[1:]
        
    for prefix in WATCH_PREFIXES:
        if name.startswith(prefix) or prefix in name:
            return True
            
    # Also watch specific criticals
    if name in ["redis", "postgres", "timescaledb"]:
        return True
        
    return False

def send_alert(r: redis.Redis, title: str, message: str, level: str = "ERROR"):
    """Publish alert to notify:telegram stream."""
    try:
        # Emojis based on level
        icon = "🚨" if level == "ERROR" else "⚠️"
        
        # HTML formatted text for Telegram
        text = (
            f"{icon} <b>Service Alert: {title}</b>\n\n"
            f"{message}\n"
            f"<i>Host: {HOSTNAME}</i>\n"
            f"<i>Time: {datetime.now(timezone.utc).strftime('%H:%M:%S UTC')}</i>"
        )
        
        payload = {
            "type": "report",  # Use 'report' type to bypass signal parsing logic
            "text": text,
            "source": "DockerWatchdog",
            "level": level
        }
        
        r.xadd(NOTIFY_STREAM, payload, maxlen=1000)
        logger.info(f"Sent alert: {title}")
        
    except Exception as e:
        logger.error(f"Failed to send alert to Redis: {e}")

def process_event(r: redis.Redis, event: Dict[str, Any]):
    """Process a single docker event."""
    try:
        status = event.get("status")
        actor = event.get("Actor", {})
        attrs = actor.get("Attributes", {})
        config_name = attrs.get("com.docker.compose.service", attrs.get("name", "unknown"))
        container_id = actor.get("ID", "")[:12]
        
        # Filter unrelated containers
        if not is_watched_container(config_name):
            return

        # 1. Handle OOM (Out of Memory)
        if status == "oom":
            msg = (
                f"🔥 <b>OOM KILL DETECTED</b>\n"
                f"Service: <code>{config_name}</code>\n"
                f"Container ID: <code>{container_id}</code>\n"
                f"System killed the process to save memory."
            )
            send_alert(r, "OOM Kill", msg, level="ERROR")
            return

        # 2. Handle Crash (die)
        if status == "die":
            exit_code = attrs.get("exitCode")
            
            # Convert to int safely
            try:
                code = int(exit_code)
            except (ValueError, TypeError):
                code = 0
            
            # Check for generic clean exit
            if code == 0:
                logger.debug(f"Ignored clean exit (0) for {config_name}")
                return
                
            # Check for Manual Stop (137) or SIGTERM (143)
            # Usually 137 is SIGKILL (often manual 'docker kill' or 'docker stop' timeout)
            # We alert on 137 ONLY if we suspected something wrong, but usually it's noise during deploys.
            # Let's filter frequent deploy noise: 137 and 143.
            if code in [0, 143]: 
                return

            # Note: code 1 is generic error, 139 is segfault, 137 is SIGKILL
            # If 137 happens WITHOUT 'oom' event preceding, it might be manual kill.
            # We will alert on 137 just in case, but label it.
            
            reason = "Crash"
            if code == 137: reason = "SIGKILL (Manual or Timeout)"
            if code == 139: reason = "Segmentation Fault"
            if code == 1: reason = "Runtime Error"
            
            msg = (
                f"💀 <b>Container Died</b>\n"
                f"Service: <code>{config_name}</code>\n"
                f"Exit Code: <code>{code}</code>\n"
                f"Reason: {reason}\n"
                f"Image: {attrs.get('image', 'unknown')}"
            )
            
            send_alert(r, f"Service Crash: {config_name}", msg, level="ERROR")

    except Exception as e:
        logger.error(f"Error processing event: {e}")

def main():
    logger.info("Starting Docker Watchdog...")
    
    # 1. Connect to Docker
    try:
        client = docker.from_env()
        logger.info("Connected to Docker daemon.")
    except Exception as e:
        logger.critical(f"Failed to connect to Docker: {e}")
        return

    # 2. Connect to Redis (with retry for 'loading' state)
    try:
        logger.info(f"Connecting to Redis at {REDIS_URL}...")
        r = get_redis(retry_attempts=20, retry_delay=2)
        # Wait for Redis to be fully ready (handles BusyLoading)
        logger.info("Waiting for Redis to be ready...")
        if not wait_for_redis(r, max_retries=30, delay=10.0):
            logger.critical("Redis is still loading after maximum wait time")
            return
        logger.info(f"Connected to Redis at {REDIS_URL}")
    except Exception as e:
        logger.critical(f"Failed to connect to Redis: {e}")
        return

    # Send startup message
    send_alert(r, "Watchdog Monitoring Started", "Docker events monitor is active.", level="INFO")

    # 3. Event Loop
    try:
        filters = {"type": "container", "event": ["die", "oom"]}
        logger.info(f"Listening for events: {filters}")
        
        for event in client.events(decode=True, filters=filters):
            process_event(r, event)
            
    except Exception as e:
        logger.critical(f"Watchdog loop failed: {e}")
        # Try to send one last gasp alert if possible
        try:
            send_alert(r, "Watchdog Failed", f"Monitor process crashed: {e}", level="ERROR")
        except:
            pass
        time.sleep(5) # Prevent tight loop restart spam

if __name__ == "__main__":
    main()
