import os
import sys
import json
import time
import subprocess
import argparse
import redis
import logging

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] PAGE-RECEIPT-MONITOR: %(message)s"
)
logger = logging.getLogger("PageReceiptsSREMonitor")

# Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
CHECK_TOOL_PATH = os.path.join(os.path.dirname(__file__), "check_page_receipts_health.py")
ALERT_STREAM = "notify:telegram:crit" 
ALERT_COOLDOWN_SEC = 3600 # 1 hour cooldown for alerts
ALERT_STATE_KEY = "sre:alert:page_receipts:last_ts"

def get_redis_client():
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)

def run_check():
    try:
        result = subprocess.run(
            [sys.executable, CHECK_TOOL_PATH, "--print-json"], 
            capture_output=True, 
            text=True
        )
        return result.returncode, result.stdout, result.stderr
    except Exception as e:
        logger.error(f"Failed to run check tool: {e}")
        return 1, "", str(e)

def send_alert(r, report):
    try:
        last_alert = r.get(ALERT_STATE_KEY)
        now = time.time()
        
        if last_alert and (now - float(last_alert) < ALERT_COOLDOWN_SEC):
            logger.info("Alert suppressed due to cooldown")
            return

        issues_count = report.get('issues_count', 'Unknown')
        issues_summary = f"Found {issues_count} missing receipts."
        
        # If we have diagnostic info from process crash
        if 'diagnostic' in report:
            message = f"🚨 <b>PAGE Receipt Process Failure</b>\n{report['diagnostic']}\n\nReview python-worker environment or PYTHONPATH."
        else:
            def _fmt_issue(i):
                mid = i.get('message_id', 'N/A')
                age = i.get('age_sec')
                reason = i.get('reason', '')
                age_str = f" (Age: {age:.1f}s)" if age is not None else ""
                reason_str = f" \u2014 {reason}" if reason else ""
                return f"- ID {mid}{age_str}{reason_str}"

            details = "\n".join([_fmt_issue(i) for i in report.get("issues", [])[:5]])
            message = f"🚨 <b>PAGE Receipt Failure</b>\n{issues_summary}\n\n{details}\n\nReview python-worker logic or Telegram bot health."
        
        payload = {
            "message": message,
            "severity": "CRITICAL",
             "source": "page_receipts_sre_monitor"
        }
        
        r.xadd(ALERT_STREAM, {"payload": json.dumps(payload)}, maxlen=50000)
        r.set(ALERT_STATE_KEY, now)
        logger.info("Alert sent to Redis stream")
        
    except Exception as e:
        logger.error(f"Failed to send alert: {e}")

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--notify", action="store_true", help="Send alert on failure")
    args = parser.parse_args()
    
    r = get_redis_client()
    
    logger.info("Running health check...")
    ret_code, output, stderr = run_check()
    
    if ret_code != 0:
        logger.error(f"Health check FAILED (code {ret_code})")
        try:
            report = json.loads(output)
        except:
            diagnostic = f"Process failed with code {ret_code}."
            if stderr:
                diagnostic += f" Diagnostic: {stderr.strip()}"
            report = {"issues_count": "Unknown", "issues": [], "diagnostic": diagnostic}
            
        if args.notify:
            error_str = str(report.get("error", ""))
            issues_str = str(report.get("issues", ""))
            if "loading the dataset" in error_str or "BusyLoadingError" in error_str or "ConnectionError" in error_str or \
               "loading the dataset" in issues_str or "BusyLoadingError" in issues_str or "ConnectionError" in issues_str:
                logger.warning("Redis is loading or unavailable, skipping alert.")
            else:
                send_alert(r, report)
        
        sys.exit(ret_code)
    else:
        logger.info("Health check OK")
        sys.exit(0)

if __name__ == "__main__":
    main()
