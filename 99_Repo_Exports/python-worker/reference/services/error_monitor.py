from utils.time_utils import get_ny_time_millis
#!/usr/bin/env python3
"""
Error Monitor - Monitors orders:exec for errors and sends notifications.

Watches for error/warning messages from MT5 executor and forwards them
to Telegram via notify:telegram stream.

Usage:
    python3 error_monitor.py
"""

import os
import redis
import time


# Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
EXEC_STREAM = os.getenv("EXEC_STREAM", "orders:exec")
NOTIFY_STREAM = os.getenv("NOTIFY_STREAM", "notify:telegram")
GROUP = os.getenv("ERROR_MONITOR_GROUP", "error-monitor-group")
CONSUMER = os.getenv("ERROR_MONITOR_CONSUMER", "error-monitor-1")
MIN_SEVERITY = os.getenv("MIN_SEVERITY", "warning").lower()

# Severity levels
SEVERITY_LEVELS = {
    "info": 0
    "warning": 1
    "error": 2
}


def should_notify(severity: str) -> bool:
    """Check if message severity should trigger notification."""
    msg_level = SEVERITY_LEVELS.get(severity.lower(), 0)
    min_level = SEVERITY_LEVELS.get(MIN_SEVERITY, 1)
    return msg_level >= min_level


def format_error_message(fields: dict) -> str:
    """Format error message for Telegram."""
    severity = fields.get("severity", "info").upper()
    msg = fields.get("msg", "")
    action = fields.get("action", "")
    retcode = fields.get("retcode", "")
    error = fields.get("error", "")
    
    emoji = "⚠️" if severity == "WARNING" else "❌"
    
    text = f"{emoji} MT5 {severity}\n\n"
    
    if msg:
        text += f"Message: {msg}\n"
    
    if action:
        text += f"Action: {action}\n"
    
    if retcode:
        text += f"Retcode: {retcode}\n"
    
    if error:
        text += f"Error: {error}\n"
    
    # Add timestamp (UTC)
    ts = fields.get("timestamp", get_ny_time_millis())
    from core.utc_utils import utc_from_timestamp_ms
    dt = utc_from_timestamp_ms(int(ts))
    text += f"\nTime: {dt.strftime('%Y-%m-%d %H:%M:%S UTC')}"
    
    return text


def main():
    """Main entry point."""
    print("🚨 Error Monitor starting...")
    print(f"   Exec Stream: {EXEC_STREAM}")
    print(f"   Notify Stream: {NOTIFY_STREAM}")
    print(f"   Min Severity: {MIN_SEVERITY}")
    print()
    
    # Connect to Redis
    r = redis.from_url(REDIS_URL, decode_responses=True)
    
    # Create consumer group
    try:
        r.xgroup_create(EXEC_STREAM, GROUP, id='0', mkstream=True)
        print(f"✅ Created consumer group: {GROUP}")
    except redis.ResponseError:
        print(f"✅ Consumer group already exists: {GROUP}")
    
    print(f"📊 Listening for errors...")
    print()
    
    error_count = 0
    
    # Main loop
    while True:
        msgs = r.xreadgroup(
            GROUP
            CONSUMER
            {EXEC_STREAM: ">"}
            count=50
            block=2000
        )
        
        for stream, entries in msgs or []:
            for msg_id, fields in entries:
                try:
                    # Check if this is an error/warning
                    severity = fields.get("severity", "").lower()
                    
                    if severity and should_notify(severity):
                        # Format and send to Telegram
                        text = format_error_message(fields)
                        
                        r.xadd(NOTIFY_STREAM, {"text": text})
                        
                        error_count += 1
                        print(f"⚠️  Sent notification #{error_count}: {severity}")
                
                except Exception as e:
                    print(f"❌ Error processing message: {e}")
                
                finally:
                    # Always ACK
                    r.xack(stream, GROUP, msg_id)


if __name__ == "__main__":
    main()

