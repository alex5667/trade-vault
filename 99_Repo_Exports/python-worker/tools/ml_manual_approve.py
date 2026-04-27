from utils.time_utils import get_ny_time_millis
import sys
import os
import time
import json
import redis
import hmac
import hashlib

# Add project root to sys.path
sys.path.append(os.getcwd())

from core.recs_contract import sign_bundle_id

import argparse

def ml_manual_approve():
    parser = argparse.ArgumentParser()
    parser.add_argument("--bundle", help="Explicit bundle ID to approve")
    parser.add_argument("--force", action="store_true", help="Force preview before confirm (bypasses restriction)")
    args = parser.parse_args()

    # 1. Setup Redis
    redis_url = os.getenv("REDIS_URL", "redis://localhost:6379/0")
    r = redis.Redis.from_url(redis_url, decode_responses=True)
    
    # 2. Identify the bundle to approve
    bundle_id = args.bundle
    if not bundle_id:
        bundle_keys = r.keys("recs:bundle:*")
        if not bundle_keys:
            print("❌ No recommendation bundles found in Redis.")
            return

        pending_bundles = []
        for k in bundle_keys:
            bid = k.split(":")[-1]
            status = r.get(f"recs:status:{bid}")
            if status in ("PENDING", "PREVIEWED"):
                pending_bundles.append(bid)
                    
        if not pending_bundles:
            print("❌ No PENDING or PREVIEWED bundles found.")
            return

        bundle_id = pending_bundles[0]
    
    print(f"📦 Selected bundle: {bundle_id}")

    # 3. Sign the bundle
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    sig = sign_bundle_id(bundle_id, secret)
    
    # 4. Push events
    stream = os.getenv("BOT_CALLBACKS_STREAM", "bot:callbacks")
    who = {
        "timestamp": str(get_ny_time_millis()),
        "chat_id": os.getenv("TELEGRAM_CHAT_ID", "manual_approval"),
        "user_id": "0",
        "username": "cli_admin"
    }

    if args.force:
        print(f"🔄 Forcing preview for {bundle_id}...")
        who["callback"] = f"recs:preview:{bundle_id}:{sig}"
        r.xadd(stream, who, maxlen=50000)
        time.sleep(1) # Wait for worker to update status

    print(f"✅ Sending confirm for {bundle_id}...")
    who["callback"] = f"recs:confirm:{bundle_id}:{sig}"
    who["timestamp"] = str(get_ny_time_millis())
    r.xadd(stream, who, maxlen=50000)
    
    print("🚀 Triggered manual activation.")

if __name__ == "__main__":
    ml_manual_approve()
