import argparse
import json
import os
import sys
import time

import redis
from core.redis_keys import RedisStreams as RS

# Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://redis:6379/0")
NOTIFY_RECEIPT_KEY_PREFIX = os.getenv("NOTIFY_RECEIPT_KEY_PREFIX", "notify:receipt:")
STREAM_KEY = RS.NOTIFY_TELEGRAM_PAGE
LOOKBACK_COUNT = 50
NOTIFY_RECEIPT_RESEND_SEC = int(os.getenv("NOTIFY_RECEIPT_RESEND_SEC", "300"))

def get_redis_client():
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)

def check_health(r, print_json=False):
    # Read last N messages from stream
    # Using XREVRANGE to get latest
    messages = r.xrevrange(STREAM_KEY, max="+", min="-", count=LOOKBACK_COUNT)

    issues = []

    now = time.time()

    for message_id, data in messages:
        payload_str = data.get("payload", "{}")
        try:
            payload = json.loads(payload_str)
        except Exception:
            continue

        require_receipt = payload.get("require_receipt")
        receipt_id = payload.get("receipt_id")

        if str(require_receipt) == "1" and receipt_id:
            # Check if receipt exists
            receipt_key = f"{NOTIFY_RECEIPT_KEY_PREFIX}{receipt_id}"
            if not r.exists(receipt_key):
                # Check timestamp of message to see if it's "expired"/late
                # message_id is like "1638383838383-0"
                try:
                    ts_ms = int(message_id.split("-")[0])
                    ts_sec = ts_ms / 1000.0
                    age = now - ts_sec

                    if age > NOTIFY_RECEIPT_RESEND_SEC:
                        issues.append({
                            "message_id": message_id,
                            "receipt_id": receipt_id,
                            "age_sec": age,
                            "reason": "Missing receipt for PAGE message > TTL"
                        })
                except Exception:
                    pass

    report = {
        "status": "OK" if not issues else "FAIL",
        "issues_count": len(issues),
        "issues": issues,
        "checked_count": len(messages),
        "timestamp": now
    }

    if print_json:
        print(json.dumps(report, indent=2))

    return 0 if not issues else 2

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--print-json", action="store_true", help="Print JSON report to stdout")
    args = parser.parse_args()

    r = get_redis_client()
    try:
        exit_code = check_health(r, args.print_json)
        sys.exit(exit_code)
    except Exception as e:
        report = {
            "status": "FAIL",
            "issues_count": 1,
            "issues": [{"reason": str(e)}],
            "checked_count": 0,
            "timestamp": time.time(),
            "error": str(e)
        }
        if args.print_json:
            print(json.dumps(report, indent=2))
        sys.exit(1)

if __name__ == "__main__":
    main()
