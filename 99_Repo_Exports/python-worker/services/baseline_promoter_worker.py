from __future__ import annotations

"""Baseline promoter worker: handles baseline:* callbacks from Telegram.

Listens to bot:callbacks stream for baseline:preview/confirm/rollback/reject/cancel actions.
Performs atomic file promotion (copy→replace) with backup for rollback.

Flow:
  1. propose_baseline_update creates bundle and sends Telegram with baseline:preview button
  2. User clicks preview → shows diff summary
  3. User clicks confirm → atomic promote (candidate → baseline) + backup
  4. User clicks rollback → restore from backup

Usage:
  python -m services.baseline_promoter_worker
  (reads ENV vars for Redis, auth, baseline paths)
"""

import hashlib
import hmac
import json
import os
import shutil
import time

import redis

from common.log import setup_logger
from common.redis_errors import is_redis_connection_error, is_redis_stream_error, is_redis_timeout_error
from core.redis_client import get_redis, wait_for_redis
from core.redis_keys import RedisStreams as RS
from utils.time_utils import get_ny_time_millis

logger = setup_logger("BaselinePromoterWorker")


REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
BOT_CALLBACKS_STREAM = os.getenv("BOT_CALLBACKS_STREAM", RS.BOT_CALLBACKS)
GROUP = os.getenv("BASELINE_CALLBACKS_GROUP", "baseline-callbacks")
CONSUMER = os.getenv("BASELINE_CALLBACKS_CONSUMER", "baseline-promoter-1")
NOTIFY_TELEGRAM_STREAM = os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM)

RECS_HMAC_SECRET = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
TTL = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)

BASELINE_DIR = os.getenv("BASELINE_DIR", "/app/of_reports_baselines")

RECS_ALLOWED_USER_IDS = os.getenv("RECS_ALLOWED_USER_IDS", "")
RECS_ALLOWED_CHAT_IDS = os.getenv("RECS_ALLOWED_CHAT_IDS", "")


def _now_ms() -> int:
    """Returns current timestamp in milliseconds (epoch)."""
    return get_ny_time_millis()


def _csv_set(s: str) -> set[str]:
    """Parses CSV string into set of strings."""
    return {x.strip() for x in (s or "").split(",") if x.strip()}


_ALLOWED_USERS = _csv_set(RECS_ALLOWED_USER_IDS)
_ALLOWED_CHATS = _csv_set(RECS_ALLOWED_CHAT_IDS)


def _allowed(who: dict[str, str]) -> bool:
    """
    Checks if user is allowed to approve baseline updates.
    
    Args:
        who: Dictionary with user info {timestamp, chat_id, user_id, username}
        
    Returns:
        True if user is allowed, False otherwise
    """
    uid = (who.get("user_id", "") or "")
    cid = (who.get("chat_id", "") or "")
    if _ALLOWED_USERS and uid not in _ALLOWED_USERS:
        return False
    if _ALLOWED_CHATS and cid not in _ALLOWED_CHATS:
        return False
    return True


def _sign(bid: str) -> str:
    """Generates short HMAC signature for bundle_id (8 hex characters)."""
    d = hmac.new(RECS_HMAC_SECRET.encode("utf-8"), bid.encode("utf-8"), hashlib.sha256).hexdigest()
    return d[:8]


def _verify(bid: str, sig: str) -> bool:
    """Verifies bundle_id signature using hmac.compare_digest."""
    return hmac.compare_digest(_sign(bid), (sig or ""))


def _ensure_group(r: redis.Redis) -> None:
    """Creates consumer group for stream bot:callbacks (if not exists)."""
    try:
        r.xgroup_create(BOT_CALLBACKS_STREAM, GROUP, id="0-0", mkstream=True)
    except Exception:
        # Group already exists - this is normal
        pass


def _notify(r: redis.Redis, text: str, buttons: list | None = None) -> None:
    """Sends notification to notify:telegram stream."""
    fields = {"type": "report", "text": text, "ts": str(_now_ms())}
    if buttons is not None:
        fields["buttons"] = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    r.xadd(NOTIFY_TELEGRAM_STREAM, fields, maxlen=200000, approximate=True)


def _get_bundle(r: redis.Redis, bid: str) -> dict | None:
    """Reads baseline bundle from Redis."""
    raw = r.get(f"baseline:bundle:{bid}")
    if not raw:
        return None
    return json.loads(raw)


def _set_status(r: redis.Redis, bid: str, st: str) -> None:
    """Sets baseline bundle status in Redis."""
    r.set(f"baseline:status:{bid}", st, ex=TTL)


def _read_status(r: redis.Redis, bid: str) -> str:
    """Reads baseline bundle status from Redis."""
    v = r.get(f"baseline:status:{bid}")
    return str(v) if v is not None else "MISSING"


def _atomic_copy_replace(src: str, dst: str) -> None:
    """
    Atomically replaces dst with src using copy→rename pattern.
    
    Args:
        src: Source file path
        dst: Destination file path
    """
    tmp = dst + ".tmp"
    shutil.copy2(src, tmp)
    os.replace(tmp, dst)


def _preview(r: redis.Redis, bid: str, who: dict) -> None:
    """Shows preview of baseline diff."""
    b = _get_bundle(r, bid)
    if not b:
        _notify(r, f"baseline preview: <code>{bid}</code> -> <b>missing bundle</b>")
        return

    diff_path = b.get("diff_path", "")
    try:
        rep = json.loads(open(diff_path, encoding="utf-8").read())
    except Exception:
        rep = {}

    msg = (
        "<b>Baseline preview</b>\n"
        f"id=<code>{bid}</code>\n"
        f"mismatches=<code>{rep.get('mismatches',0)}</code> overlap_n=<code>{rep.get('n',0)}</code>\n"
        f"by_field=<code>{rep.get('mismatch_by_field',{})}</code>\n"
        f"top_scn=<code>{rep.get('mismatch_by_scenario_v4_top',[])}</code>"
    )

    sig = _sign(bid)
    buttons = [[
        {"text": "✅✅ Confirm promote", "callback": f"baseline:confirm:{bid}:{sig}"},
        {"text": "❌ Cancel", "callback": f"baseline:cancel:{bid}:{sig}"},
    ]]
    _set_status(r, bid, "PREVIEWED")
    _notify(r, msg, buttons=buttons)


def _confirm(r: redis.Redis, bid: str, who: dict) -> None:
    """Promotes candidate baseline to production (atomic replace with backup)."""
    b = _get_bundle(r, bid)
    if not b:
        _notify(r, f"baseline confirm: <code>{bid}</code> -> <b>missing bundle</b>")
        return

    st = _read_status(r, bid)
    if st in ("PROMOTED", "ROLLED_BACK"):
        _notify(r, f"baseline confirm: <code>{bid}</code> -> <b>not allowed</b> status={st}")
        return

    os.makedirs(f"{BASELINE_DIR}/backups", exist_ok=True)

    cand_in = b["candidate_inputs"]
    cand_out = b["candidate_output"]
    base_in = b["baseline_inputs"]
    base_out = b["baseline_output"]

    backup_in = f"{BASELINE_DIR}/backups/{bid}_inputs_{int(time.time())}.ndjson"
    backup_out = f"{BASELINE_DIR}/backups/{bid}_output_{int(time.time())}.ndjson"

    # backup current if exists
    if os.path.exists(base_in):
        shutil.copy2(base_in, backup_in)
    if os.path.exists(base_out):
        shutil.copy2(base_out, backup_out)

    # promote (atomic replace)
    _atomic_copy_replace(cand_in, base_in)
    _atomic_copy_replace(cand_out, base_out)

    # store backup paths for rollback
    r.set(
        f"baseline:backup:{bid}",
        json.dumps({"backup_inputs": backup_in, "backup_output": backup_out}, ensure_ascii=False),
        ex=TTL
    )
    _set_status(r, bid, "PROMOTED")

    sig = _sign(bid)
    buttons = [[{"text": "↩ Rollback", "callback": f"baseline:rollback:{bid}:{sig}"}]]

    _notify(
        r,
        "<b>Baseline promoted</b>\n"
        f"id=<code>{bid}</code>\n"
        f"base_inputs=<code>{base_in}</code>\n"
        f"base_output=<code>{base_out}</code>",
        buttons=buttons,
    )

    logger.info("Baseline promoted: bundle_id=%s", bid)


def _rollback(r: redis.Redis, bid: str, who: dict) -> None:
    """Rolls back baseline promotion (restores from backup)."""
    st = _read_status(r, bid)
    if st != "PROMOTED":
        _notify(r, f"baseline rollback: <code>{bid}</code> -> <b>not promoted</b> status={st}")
        return

    b = _get_bundle(r, bid)
    if not b:
        _notify(r, f"baseline rollback: <code>{bid}</code> -> <b>missing bundle</b>")
        return

    backup_raw = r.get(f"baseline:backup:{bid}")
    if not backup_raw:
        _notify(r, f"baseline rollback: <code>{bid}</code> -> <b>missing backup</b>")
        return

    bk = json.loads(backup_raw)
    backup_in = bk.get("backup_inputs", "")
    backup_out = bk.get("backup_output", "")
    base_in = b["baseline_inputs"]
    base_out = b["baseline_output"]

    if backup_in and os.path.exists(backup_in):
        _atomic_copy_replace(backup_in, base_in)
    if backup_out and os.path.exists(backup_out):
        _atomic_copy_replace(backup_out, base_out)

    _set_status(r, bid, "ROLLED_BACK")
    _notify(r, f"<b>Baseline rolled back</b>\nid=<code>{bid}</code>")

    logger.info("Baseline rolled back: bundle_id=%s", bid)


def _reject(r: redis.Redis, bid: str) -> None:
    """Rejects baseline proposal."""
    _set_status(r, bid, "REJECTED")
    _notify(r, f"baseline rejected: <code>{bid}</code>")


def _cancel(r: redis.Redis, bid: str) -> None:
    """Cancels baseline proposal (returns to PENDING)."""
    _set_status(r, bid, "PENDING")
    _notify(r, f"baseline cancelled: <code>{bid}</code> -> PENDING")


def main() -> None:
    """Main worker loop: reads bot:callbacks, processes baseline:* actions."""
    # Connect to Redis with proper retry and readiness check
    try:
        logger.info("Connecting to Redis...")
        r = get_redis(retry_attempts=20, retry_delay=2)
        # Wait for Redis to be fully ready (handles BusyLoading)
        logger.info("Waiting for Redis to be ready...")
        if not wait_for_redis(r, max_retries=30, delay=10.0):
            logger.error("Redis is still loading after maximum wait time")
            raise RuntimeError("Redis is not ready after waiting")
        logger.info("Redis connected and ready")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        raise

    _ensure_group(r)

    logger.info("Starting baseline promoter worker: stream=%s, group=%s, consumer=%s", BOT_CALLBACKS_STREAM, GROUP, CONSUMER)
    if RECS_ALLOWED_USER_IDS or RECS_ALLOWED_CHAT_IDS:
        logger.info("Security: allowlist enabled (users=%s, chats=%s)", RECS_ALLOWED_USER_IDS, RECS_ALLOWED_CHAT_IDS)

    while True:
        try:
            # Read from stream with NOGROUP and BusyLoadingError handling
            resp = None
            try:
                resp = r.xreadgroup(GROUP, CONSUMER, {BOT_CALLBACKS_STREAM: ">"}, count=50, block=5000)
            except redis.exceptions.ResponseError as e:
                # Handle NOGROUP errors by recreating consumer group
                error_msg = str(e).upper()
                if "NOGROUP" in error_msg or is_redis_stream_error(e):
                    logger.warning("Consumer group %s missing or stream error, recreating... Error: %s", GROUP, e)
                    try:
                        _ensure_group(r)
                        time.sleep(0.5)  # Brief pause before retrying
                        # Retry the read after recreating group
                        resp = r.xreadgroup(GROUP, CONSUMER, {BOT_CALLBACKS_STREAM: ">"}, count=50, block=5000)
                    except Exception as group_err:
                        logger.error("Failed to recreate consumer group: %s", group_err)
                        time.sleep(2)
                        continue
                else:
                    # Re-raise non-NOGROUP ResponseErrors
                    raise
            except redis.exceptions.BusyLoadingError:
                # Redis is loading dataset, retry with backoff
                logger.warning("Redis is loading dataset, retrying in 2s...")
                time.sleep(2)
                continue

            if not resp:
                continue
            for _stream, msgs in resp:
                for msg_id, fields in msgs:
                    try:
                        cb = (fields.get("callback", "") or "")
                        who = {
                            "timestamp": (fields.get("timestamp", "") or ""),
                            "chat_id": (fields.get("chat_id", "") or ""),
                            "user_id": (fields.get("user_id", "") or ""),
                            "username": (fields.get("username", "") or ""),
                        }
                        if not _allowed(who):
                            _notify(r, "baseline: <b>denied</b> (not allowed)")
                            logger.warning("Access denied for user_id=%s, chat_id=%s", who.get("user_id"), who.get("chat_id"))
                            continue

                        parts = cb.split(":")
                        if len(parts) != 4 or parts[0] != "baseline":
                            continue
                        action, bid, sig = parts[1], parts[2], parts[3]
                        if action not in ("preview", "confirm", "rollback", "reject", "cancel"):
                            continue
                        if not _verify(bid, sig):
                            _notify(r, f"baseline {action}: <code>{bid}</code> -> <b>invalid signature</b>")
                            continue

                        if action == "preview":
                            _preview(r, bid, who)
                        elif action == "confirm":
                            _confirm(r, bid, who)
                        elif action == "rollback":
                            _rollback(r, bid, who)
                        elif action == "reject":
                            _reject(r, bid)
                        else:
                            _cancel(r, bid)

                        logger.info("Processed %s for baseline bundle %s", action, bid)

                    except Exception as e:
                        logger.error("Error processing message %s: %s", msg_id, e, exc_info=True)
                    finally:
                        try:
                            r.xack(BOT_CALLBACKS_STREAM, GROUP, msg_id)
                        except Exception as ack_err:
                            logger.error("Error ACKing message %s: %s", msg_id, ack_err)
        except redis.exceptions.ResponseError as e:
            # Handle NOGROUP and other stream errors
            error_msg = str(e).upper()
            if "NOGROUP" in error_msg or is_redis_stream_error(e):
                logger.warning("Consumer group %s missing or stream error, recreating... Error: %s", GROUP, e)
                try:
                    _ensure_group(r)
                    time.sleep(0.5)  # Brief pause before retrying
                except Exception as group_err:
                    logger.error("Failed to recreate consumer group: %s", group_err)
                    time.sleep(2)
            else:
                logger.error("Redis ResponseError in main loop: %s", e, exc_info=True)
                time.sleep(1)
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
            # Handle connection/timeout errors with retry
            if is_redis_connection_error(e) or is_redis_timeout_error(e):
                logger.warning("Redis connection/timeout error, retrying in 2s... Error: %s", e)
                time.sleep(2)
            else:
                logger.error("Redis connection/timeout error: %s", e, exc_info=True)
                time.sleep(1)
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            break
        except Exception as e:
            logger.error("Error in main loop: %s", e, exc_info=True)
            time.sleep(1)


if __name__ == "__main__":
    main()

