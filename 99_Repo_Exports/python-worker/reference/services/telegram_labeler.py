from utils.time_utils import get_ny_time_millis

#!/usr/bin/env python3
"""
Telegram Callback Labeler.

Consumes callback events from Telegram bot (stream: bot:callbacks) and writes
labeled trade decisions to labels:trades stream, keyed by sid (signal ID).

Expected callback format:
  - open:LONG:0.10:<sid>    → opened trade
  - cancel::<sid>            → canceled trade
  - size:0.5:<sid>           → adjusted position size
  
This enables offline analysis and calibration based on actual trader decisions.
"""

import os

import redis

# Configuration
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
CALLBACKS_STREAM = os.getenv("CALLBACKS_STREAM", "bot:callbacks")
LABELS_STREAM = os.getenv("LABELS_STREAM", "labels:trades")
GROUP = os.getenv("LABELS_GROUP", "labels-group")
CONSUMER = os.getenv("LABELS_CONSUMER", "labels-consumer-1")


def parse_callback(cb: str, ts_action: int) -> dict:
    """
    Parse callback string into structured record.
    
    Args:
        cb: Callback string (e.g., "open:LONG:0.10:<sid>")
        ts_action: Timestamp of action
        
    Returns:
        Dictionary with parsed fields
    """
    parts = cb.split(":")
    action = parts[0] if parts else "unknown"

    record = {
        "action": action,
        "ts_action": ts_action,
        "raw": cb
    }

    sid = None

    if action == "open" and len(parts) >= 4:
        # Format: open:SIDE:LOT:SID
        side = parts[1]
        lot = parts[2]
        sid = parts[3]
        record.update({
            "side": side,
            "lot": lot,
            "sid": sid,
            "status": "opened"
        })

    elif action == "cancel" and len(parts) >= 2:
        # Format: cancel::SID or cancel:<anything>:SID
        sid = parts[-1]  # Last part is SID
        record.update({
            "sid": sid,
            "status": "canceled"
        })

    elif action == "size" and len(parts) >= 3:
        # Format: size:MULTIPLIER:SID
        mult = parts[1]
        sid = parts[2]
        record.update({
            "sid": sid,
            "size_mult": mult,
            "status": "sized"
        })

    elif action == "approve" and len(parts) >= 2:
        # Format: approve:SID
        sid = parts[1]
        record.update({
            "sid": sid,
            "status": "approved"
        })

    return record


def main():
    """Main entry point."""
    print("🏷️  Telegram Callback Labeler starting...")
    print(f"   Callbacks: {CALLBACKS_STREAM}")
    print(f"   Labels: {LABELS_STREAM}")
    print(f"   Group: {GROUP}")
    print(f"   Consumer: {CONSUMER}")
    print()

    # Connect to Redis
    r = redis.from_url(REDIS_URL, decode_responses=True)

    # Create consumer group
    try:
        r.xgroup_create(CALLBACKS_STREAM, GROUP, id='0', mkstream=True)
        print(f"✅ Created consumer group: {GROUP}")
    except redis.ResponseError:
        print(f"✅ Consumer group already exists: {GROUP}")

    print("📊 Listening for callbacks...")
    print()

    labeled_count = 0

    # Main loop
    while True:
        msgs = r.xreadgroup(
            GROUP,
            CONSUMER,
            {CALLBACKS_STREAM: ">"},
            count=100,
            block=2000
        )

        for stream, entries in msgs or []:
            for msg_id, fields in entries:
                try:
                    # Extract callback data
                    cb = fields.get("callback") or fields.get("data") or ""
                    ts_action = int(fields.get("timestamp") or get_ny_time_millis())

                    # Parse callback
                    record = parse_callback(cb, ts_action)

                    # Add metadata
                    record["msg_id"] = msg_id
                    user_id = (fields.get("user_id") or "telegram_unknown")
                    record["user_id"] = user_id
                    if "chat_id" in fields:
                        record["chat_id"] = fields["chat_id"]

                    # Special logic for 'approve' action
                    if record.get("status") == "approved" and record.get("sid"):
                        sid = record["sid"]
                        appr_key = f"cfg:suggestions:entry_policy:approvals:{sid}"
                        r.sadd(appr_key, user_id)
                        # expire for cleanup
                        ttl = int(os.getenv("ENTRY_POLICY_APPROVALS_TTL_SEC", "1209600"))
                        if ttl > 0:
                            r.expire(appr_key, ttl)

                    # Write to labels stream
                    r.xadd(LABELS_STREAM, record)

                    labeled_count += 1

                    # Log
                    sid = record.get("sid", "N/A")
                    action = record.get("action")
                    status = record.get("status", "unknown")
                    print(f"✅ Labeled {action} → {status} (sid={sid[:20]}...)")

                    if labeled_count % 10 == 0:
                        print(f"📊 Total labeled: {labeled_count}")

                except Exception as e:
                    print(f"❌ Error processing callback: {e}")

                finally:
                    # Always ACK
                    r.xack(stream, GROUP, msg_id)


if __name__ == "__main__":
    main()

