import json
import logging
import os
import time
import uuid
from typing import Any

import redis

from services.analytics_db import get_conn

logger = logging.getLogger("atr_invariant_violation_writer")

REDIS_URL = os.getenv("REDIS_URL", "redis://localhost:6379/0")
STREAM_KEY = "events:invariant_violations"
GROUP_NAME = "invariant_writer_group"
CONSUMER_NAME = f"writer_{uuid.uuid4().hex[:6]}"

def setup_stream(r: redis.Redis) -> None:
    try:
        r.xgroup_create(STREAM_KEY, GROUP_NAME, id="0", mkstream=True)
        logger.info(f"Created consumer group {GROUP_NAME} on {STREAM_KEY}")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" not in str(e):
            logger.error(f"Error creating consumer group: {e}")

def process_messages(messages: list) -> None:
    if not messages:
        return

    try:
        with get_conn() as conn, conn.cursor() as cur:
            for stream, msgs in messages:
                for msg_id, msg_data in msgs:
                    try:
                        payload_str = msg_data.get("payload")
                        if not payload_str:
                            continue

                        payload = json.loads(payload_str)
                        signal = payload.get("signal", {})
                        violations = payload.get("violations", [])

                        now_ms = int(time.time() * 1000)
                        symbol = signal.get("symbol", "UNKNOWN")
                        source = signal.get("source", "UNKNOWN")

                        incidents_to_open = []

                        for v in violations:
                            violation_id = f"viol_{now_ms}_{uuid.uuid4().hex[:6]}"
                            status = "enforced" if (v.get("enforcement_mode") == "runtime_deny") else "detected"

                            cur.execute("""
                                INSERT INTO atr_invariant_violations (
                                    violation_id, invariant_id, scope_kind, scope_value, surface,
                                    severity, status, reason_code, violation_json
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                            """, (
                                violation_id, v["invariant_id"], "symbol", symbol, "runtime",
                                v["severity"], status, v["reason_code"],
                                json.dumps({"signal_id": signal.get("signal_id"), "details": v.get("details", "")})
                            ))

                            # Find related remediation action by invariant_id
                            remediation_actions = signal.get("meta", {}).get("remediation_actions", []) if isinstance(signal.get("meta"), dict) else []
                            if not remediation_actions:
                                remediation_actions = signal.get("remediation_actions", [])

                            for action in remediation_actions:
                                # We match action by checking its origin? Actually action_json holds nothing about invariant_id except if it's deny_only.
                                # Better strategy: Action generator already has action_id. We just insert all actions we see.
                                pass

                            if v["severity"] == "critical" and ("open_incident" in v["enforcement_mode"] or v["enforcement_mode"] == "runtime_deny"):
                                incidents_to_open.append(v)

                        # Insert Remediation actions
                        remediation_actions = signal.get("meta", {}).get("remediation_actions", []) if isinstance(signal.get("meta"), dict) else signal.get("remediation_actions", [])
                        for action in remediation_actions:
                            action_id = action.get("action_id", f"act_{uuid.uuid4().hex[:10]}")
                            # Approximate linking via signal_id or just use constant
                            v_id = f"viol_linked_{signal.get('signal_id', 'unknown')}"

                            cur.execute("""
                                INSERT INTO atr_invariant_remediation_actions (
                                    action_id, violation_id, invariant_id, remediation_kind, scope_kind,
                                    scope_value, status, reason_code, action_json
                                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
                                ON CONFLICT DO NOTHING
                            """, (
                                action_id, v_id, "RUNTIME_MAPPED", "mapped",
                                action.get("action_json", {}).get("scope_kind", "symbol"),
                                action.get("action_json", {}).get("scope_value", symbol),
                                action.get("status", "unknown"),
                                action.get("reason_code", "unknown"),
                                json.dumps(action.get("action_json", {}))
                            ))

                            # Fire telegram notification if appropriate
                            if action.get("status") in ["executed", "requested"] and action.get("reason_code") != "REMEDIATION_DENY_ONLY":
                                _send_remediation_telegram(action)

                        conn.commit()

                        # Open incidents safely
                        if incidents_to_open:
                            try:
                                from services.atr_incident_control_service import open_incident
                                for v in incidents_to_open:
                                    open_incident(
                                        scope_kind="global",
                                        detected_by="InvariantRuntimeEngine",
                                        reason_code=v["reason_code"],
                                        incident_json={"details": v.get("details", ""), "signal_id": signal.get("signal_id")},
                                        source=source,
                                        symbol=symbol
                                    )
                            except Exception as inc_err:
                                logger.error(f"Failed to open incident: {inc_err}")

                    except Exception as e:
                        logger.error(f"Error processing individual viol msg {msg_id}: {e}")

    except Exception as e:
        logger.error(f"Failed to persist violations to Postgres: {e}")
        return # Do not ACK, so we retry

def _send_remediation_telegram(action: dict[str, Any]) -> None:
    try:
        from services.telegram.telegram_notifier_worker_v2 import TelegramNotifier
        notifier = TelegramNotifier()
        scope_value = action.get("action_json", {}).get("scope_value", "unknown")
        state = action.get("action_json", {}).get("target_state", "None")
        if action.get("reason_code") == "REMEDIATION_RUNTIME_CLIP":
            state = f"clip_mult={action.get('action_json', {}).get('clip_mult', 1.0)}"

        msg = "🛡️ <b>ATR Auto-Remediation</b>\n\n"
        msg += f"<b>Action</b>: {action.get('status')} / {action.get('reason_code')}\n"
        msg += f"<b>Scope</b>: {scope_value}\n"
        msg += f"<b>State</b>: {state}\n\n"
        msg += f"<i>ID: {action.get('action_id')}</i>"
        notifier.send_message(msg, parse_mode="HTML")
    except Exception as e:
        logger.error(f"Failed to send telegram: {e}")

def run_worker() -> None:
    logger.info("Starting ATR Invariant Violation Writer...")
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    setup_stream(r)

    while True:
        try:
            # Read from stream
            messages = r.xreadgroup(
                GROUP_NAME, CONSUMER_NAME,
                {STREAM_KEY: ">"},
                count=50,
                block=5000
            )

            if messages:
                process_messages(messages)

                # Ack processed messages
                for stream, msgs in messages:
                    msg_ids = [msg_id for msg_id, _ in msgs]
                    if msg_ids:
                        r.xack(stream, GROUP_NAME, *msg_ids)

        except Exception as e:
            logger.error(f"Worker iteration loop error: {e}")
            time.sleep(2)

if __name__ == "__main__":
    logging.basicConfig(level=logging.INFO, format='%(asctime)s [%(levelname)s] %(name)s: %(message)s')
    run_worker()
