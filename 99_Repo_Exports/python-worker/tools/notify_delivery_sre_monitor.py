import os
import sys
import time
import json
import redis
import subprocess

# Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
NOTIFY_TELEGRAM_MIRROR_BASE = os.getenv("NOTIFY_TELEGRAM_MIRROR_BASE", "0")
NOTIFY_DELIVERY_ALERT_COOLDOWN_SEC = int(os.getenv("NOTIFY_DELIVERY_ALERT_COOLDOWN_SEC", "300"))

ALERT_STREAM = "notify:telegram:crit"

def get_redis_client():
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)

def send_alert(r, message):
    # Cooldown check
    cooldown_key = "notify:delivery:sre_alert:cooldown"
    if r.get(cooldown_key):
        print(f"Skipping alert (cooldown): {message}")
        return

    payload = {
        "message": f"🚨 <b>Delivery Degraded</b>\n\n{message}",
        "severity": "critical",
        "timestamp": time.time()
    }
    
    # Send to critical stream
    r.xadd(ALERT_STREAM, {"payload": json.dumps(payload)}, maxlen=50000)
    print(f"Sent alert to {ALERT_STREAM}: {message}")

    # Set cooldown
    r.setex(cooldown_key, NOTIFY_DELIVERY_ALERT_COOLDOWN_SEC, "1")

def main():
    # Run the check tool
    cmd = [sys.executable, "python-worker/tools/check_notify_delivery_health.py"]
    try:
        result = subprocess.run(cmd, capture_output=True, text=True)
    except Exception as e:
        print(f"Error running check tool: {e}")
        return

    try:
        data = json.loads(result.stdout)
    except:
        data = {}

    if result.returncode != 0:
        # Failure detected
        issues = data.get("issues", [])
        if not issues and data.get("error"):
            issues = [data.get("error")]
        
        if not issues:
            if result.stderr:
                 issues = [f"Check tool process failed (code {result.returncode})", f"Diagnostic output: {result.stderr.strip()}"]
            else:
                 issues = ["Unknown error (check tool failed)"]
            
        issue_str = "\n".join(issues)
        print(f"DETECTED FAILURE (exit {result.returncode}): {issue_str}")
        
        if "loading the dataset" in issue_str or "BusyLoadingError" in issue_str or "ConnectionError" in issue_str:
            print("Redis is loading or unavailable, skipping alert.")
            sys.exit(0)
            
        try:
            r = get_redis_client()
            send_alert(r, issue_str)
        except Exception as e:
            print(f"Failed to send alert (Redis unavailable): {e}")
        sys.exit(2)
    else:
        print("Health OK")
        sys.exit(0)

if __name__ == "__main__":
    main()
