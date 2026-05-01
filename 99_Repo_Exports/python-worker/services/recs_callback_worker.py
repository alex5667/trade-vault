# python-worker/services/recs_callback_worker.py
# ------------------------------------------------------------
# Two-phase approve for config recommendations via Telegram bot-nest callbacks.
#
# Flow:
#   1) cron_of_reports creates recs bundle in Redis:
#        - recs:bundle:<id>  JSON {id, ttl_sec, ops:[{op:"HSET", key, field, value}], meta...}
#        - recs:status:<id>  "PENDING"
#   2) cron_of_reports sends Telegram message via notify:telegram with buttons:
#        callback="recs:preview:<id>:<sig>"
#   3) bot-nest writes callback clicks into Redis stream bot:callbacks:
#        {callback, timestamp, chat_id, user_id, username}
#   4) This worker reads bot:callbacks, verifies signature, shows preview diff.
#   5) On confirm, applies HSET ops, stores audit (old->new), sets status APPLIED.
#   6) Provides rollback button: callback="recs:rollback:<id>:<sig>"
#
# Security:
#   - HMAC signature in callback_data: sig = HMAC_SHA256(secret, bundle_id)[:8]
#   - allowlist user_ids/chat_ids
#   - idempotency lock per bundle
#   - TTL for bundles/status/audit
#
# Telegram sending:
#   - via notify:telegram stream using fields:
#       text: string (HTML allowed if your bot sends parse_mode)
#       buttons: JSON string 2D array of {text, callback}
#
# ------------------------------------------------------------

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
from typing import Dict, List, Optional, Tuple

import redis

from common.log import setup_logger
from common.redis_errors import is_redis_stream_error
from core.recs_contract import sign_bundle_id, verify_sig, RecBundle
from services.recs_store import (
    get_bundle as store_get_bundle,
    get_status as store_get_status,
    set_status as store_set_status,
    append_audit as store_append_audit,
    AUDIT_KEY,
)
from core.redis_keys import RedisStreams as RS

logger = setup_logger("RecsCallbackWorker")

# -----------------------------
# ENV Configuration
# -----------------------------
REDIS_URL = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")

BOT_CALLBACKS_STREAM = os.getenv("BOT_CALLBACKS_STREAM", RS.BOT_CALLBACKS)
BOT_CALLBACKS_GROUP = os.getenv("BOT_CALLBACKS_GROUP", "recs-callbacks")
BOT_CALLBACKS_CONSUMER = os.getenv("BOT_CALLBACKS_CONSUMER", "recs-cb-1")

RECS_HMAC_SECRET = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
RECS_TTL_SEC = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)

# Allowlist (recommended to set)
RECS_ALLOWED_USER_IDS = os.getenv("RECS_ALLOWED_USER_IDS", "")  # CSV
RECS_ALLOWED_CHAT_IDS = os.getenv("RECS_ALLOWED_CHAT_IDS", "")  # CSV

# Where to send feedback messages
NOTIFY_STREAM = os.getenv("NOTIFY_STREAM", RS.NOTIFY_TELEGRAM)

# Optional: strict 2-phase (only allow confirm after preview)
STRICT_CONFIRM_REQUIRES_PREVIEW = int(os.getenv("STRICT_CONFIRM_REQUIRES_PREVIEW", "1") or 1)

# Preview rendering limits
PREVIEW_MAX_OPS = int(os.getenv("RECS_PREVIEW_MAX_OPS", "40") or 40)

# -----------------------------
# Redis keys
# -----------------------------
LOCK_KEY = "recs:lock:"


# -----------------------------
# Helper functions
# -----------------------------
def _now_ms() -> int:
    """Returns current timestamp in milliseconds (epoch)."""
    return get_ny_time_millis()


def _csv_set(s: str) -> set[str]:
    """Parses CSV string into set of strings."""
    out = set()
    for x in (s or "").split(","):
        x = x.strip()
        if x:
            out.add(x)
    return out


_ALLOWED_USERS = _csv_set(RECS_ALLOWED_USER_IDS)
_ALLOWED_CHATS = _csv_set(RECS_ALLOWED_CHAT_IDS)


def _allowed(who: Dict[str, str]) -> bool:
    """
    Checks if user is allowed to approve recommendations.
    
    If allowlist is provided -> enforce.
    If allowlist empty -> allow all (not recommended).
    
    Args:
        who: Dictionary with user info {timestamp, chat_id, user_id, username}
        
    Returns:
        True if user is allowed, False otherwise
    """
    uid = str(who.get("user_id", "") or "")
    cid = str(who.get("chat_id", "") or "")
    if _ALLOWED_USERS and uid not in _ALLOWED_USERS:
        return False
    if _ALLOWED_CHATS and cid not in _ALLOWED_CHATS:
        return False
    return True


def _sign(bundle_id: str) -> str:
    """
    Generates short HMAC signature for bundle_id (8 hex characters).
    
    Uses core.recs_contract.sign_bundle_id() with RECS_HMAC_SECRET.
    
    Args:
        bundle_id: Bundle identifier
        
    Returns:
        8 hex characters signature
    """
    return sign_bundle_id(bundle_id, RECS_HMAC_SECRET)


def _verify(bundle_id: str, sig: str) -> bool:
    """
    Verifies bundle_id signature.
    
    Uses hmac.compare_digest for timing attack protection.
    
    Args:
        bundle_id: Bundle identifier
        sig: Signature to verify (8 hex characters)
        
    Returns:
        True if signature is valid, False otherwise
    """
    return verify_sig(bundle_id, sig, RECS_HMAC_SECRET)


def _ensure_group(r: redis.Redis) -> None:
    """
    Creates consumer group for stream bot:callbacks (if not exists).
    
    Args:
        r: Redis client
    """
    try:
        r.xgroup_create(BOT_CALLBACKS_STREAM, BOT_CALLBACKS_GROUP, id="0-0", mkstream=True)
    except Exception:
        # Group already exists - this is normal
        pass




def _format_preview(bundle_id: str, who: dict, changes: List[Tuple[str, Optional[str], str, str]], total_ops: int) -> str:
    """
    Renders HTML-safe-ish preview (bot uses HTML parsing in reports).
    
    Args:
        bundle_id: Bundle identifier
        who: User info dictionary
        changes: List of (key, field_or_none, old_value, new_value) tuples
                 field_or_none is None for SET operations, str for HSET operations
        total_ops: Total number of operations
        
    Returns:
        HTML-formatted preview text
    """
    lines: List[str] = []
    lines.append(f"<b>Recommendations preview</b>  id=<code>{bundle_id}</code>")
    uname = who.get("username", "") or ""
    lines.append(f"user=<code>{uname}</code> uid=<code>{who.get('user_id','')}</code> chat=<code>{who.get('chat_id','')}</code>")
    lines.append("")
    lines.append("<b>Changes</b> (old → new)")
    for key, field, oldv, newv in changes:
        if field is None:
            # SET operation
            lines.append(f"- <code>SET {key}</code>: <code>{oldv}</code> → <code>{newv}</code>")
        else:
            # HSET operation
            lines.append(f"- <code>HSET {key}</code> <code>{field}</code>: <code>{oldv}</code> → <code>{newv}</code>")
    if total_ops > len(changes):
        lines.append(f"... and {total_ops - len(changes)} more")
    return "\n".join(lines)


def _preview_bundle(r: redis.Redis, bundle_id: str, who: dict) -> str:
    """
    Shows preview bundle (diff old→new for each HSET) and Confirm/Cancel buttons.
    
    Process:
    1. Reads bundle from Redis
    2. Checks status (should not be REJECTED/ROLLED_BACK)
    3. Collects old values (without writing)
    4. Formats diff old→new
    5. Sets status PREVIEWED
    6. Sends message with diff and Confirm/Cancel buttons
    
    Args:
        r: Redis client
        bundle_id: Bundle identifier
        who: Dictionary with user info {timestamp, chat_id, user_id, username}
        
    Returns:
        Operation status: "previewed", "missing_bundle", "not_available", "empty_ops", "already_applied"
    """
    b = store_get_bundle(r, bundle_id)
    if not b:
        _notify(r, f"recs preview: <code>{bundle_id}</code> -> <b>missing bundle</b>")
        return "missing_bundle"

    st = store_get_status(r, bundle_id)
    if st in ("REJECTED", "ROLLED_BACK"):
        _notify(r, f"recs preview: <code>{bundle_id}</code> -> <b>not available</b> (status={st})")
        return f"not_available({st})"
    if st == "APPLIED":
        # already applied -> show rollback button
        rb = [[{"text": "↩ Rollback", "callback": f"recs:rollback:{bundle_id}:{_sign(bundle_id)}"}]]
        _notify(r, f"recs preview: <code>{bundle_id}</code> -> <b>already applied</b>", buttons=rb)
        return "already_applied"

    # Extract HSET and SET operations
    ops = []
    if isinstance(b, RecBundle):
        ops = [
            {"op": op.op, "key": op.key, "field": getattr(op, "field", None), "value": op.value}
            for op in (b.ops or [])
            if op.op in ("HSET", "SET")
        ]
    else:
        # Fallback for dict format
        ops = [op for op in (b.get("ops") or []) if isinstance(op, dict) and op.get("op") in ("HSET", "SET")]
    
    if not ops:
        _notify(r, f"recs preview: <code>{bundle_id}</code> -> <b>empty ops</b>")
        return "empty_ops"

    # Read old values
    changes: List[Tuple[str, Optional[str], str, str]] = []
    for op in ops[:PREVIEW_MAX_OPS]:
        op_type = str(op.get("op", ""))
        key = str(op.get("key", ""))
        newv = str(op.get("value", ""))
        
        if op_type == "HSET":
            field = str(op.get("field", ""))
            old = r.hget(key, field)
            oldv = "" if old is None else str(old)
            changes.append((key, field, oldv, newv))
        elif op_type == "SET":
            old = r.get(key)
            oldv = "" if old is None else str(old)
            changes.append((key, None, oldv, newv))

    # Mark PREVIEWED
    store_set_status(r, bundle_id, "PREVIEWED", RECS_TTL_SEC)
    store_append_audit(r, bundle_id, {"ts_ms": _now_ms(), "previewed": True, "who": who}, RECS_TTL_SEC)

    text = _format_preview(bundle_id, who, changes, total_ops=len(ops))

    sig = _sign(bundle_id)
    buttons = [[
        {"text": "✅✅ Confirm apply", "callback": f"recs:confirm:{bundle_id}:{sig}"},
        {"text": "❌ Cancel",         "callback": f"recs:cancel:{bundle_id}:{sig}"},
    ]]
    _notify(r, text, buttons=buttons)
    return "previewed"


def _notify(r: redis.Redis, text: str, buttons: Optional[list] = None) -> None:
    """
    Sends notification to notify:telegram stream.
    
    Args:
        r: Redis client
        text: Message text (HTML)
        buttons: Optional list of buttons (2D array of {text, callback})
    """
    fields = {
        "type": "report",
        "text": text,
        "ts": str(_now_ms()),
    }
    if buttons is not None:
        fields["buttons"] = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    r.xadd(NOTIFY_STREAM, fields, maxlen=200000, approximate=True)


def _apply_bundle(r: redis.Redis, bundle_id: str, who: dict) -> str:
    """
    Applies bundle (executes all HSET operations).
    
    Process:
    1. Reads bundle from Redis
    2. Checks status (should not be APPLIED/REJECTED/ROLLED_BACK)
    3. Takes distributed lock (recs:lock:<id>)
    4. Reads old values before changing
    5. Executes all HSET operations in pipeline
    6. Writes audit log (old values for rollback)
    7. Sets status APPLIED
    
    Args:
        r: Redis client
        bundle_id: Bundle identifier
        who: Dictionary with user info {timestamp, chat_id, user_id, username}
        
    Returns:
        Operation status: "applied", "missing_bundle", "already_applied", "already_rejected", "already_rolled_back", "locked", "not_previewed", "empty_ops"
    """
    b = store_get_bundle(r, bundle_id)
    if not b:
        return "missing_bundle"

    st = store_get_status(r, bundle_id)
    if st in ("APPLIED", "REJECTED", "ROLLED_BACK"):
        return f"already_{st.lower()}"

    if STRICT_CONFIRM_REQUIRES_PREVIEW and st != "PREVIEWED":
        return f"not_previewed({st})"

    # Idempotency lock (distributed lock via Redis SET NX)
    if not r.set(LOCK_KEY + bundle_id, "1", nx=True, ex=RECS_TTL_SEC):
        return "locked"

    try:
        # Extract HSET and SET operations
        ops = []
        if isinstance(b, RecBundle):
            ops = [
                {"op": op.op, "key": op.key, "field": getattr(op, "field", None), "value": op.value}
                for op in (b.ops or [])
                if op.op in ("HSET", "SET")
            ]
        else:
            # Fallback for dict format
            ops = [op for op in (b.get("ops") or []) if isinstance(op, dict) and op.get("op") in ("HSET", "SET")]
        
        if not ops:
            return "empty_ops"

        # Audit old values and prepare operations
        audit_rows = []
        pipe = r.pipeline()
        
        for op in ops:
            op_type = str(op.get("op", ""))
            key = str(op.get("key", ""))
            newv = str(op.get("value", ""))
            
            if op_type == "HSET":
                field = str(op.get("field", ""))
                old = r.hget(key, field)
                old_null = 1 if old is None else 0
                oldv = "" if old is None else str(old)
                audit_rows.append({"op": "HSET", "key": key, "field": field, "old": oldv, "old_null": old_null, "new": newv})
                pipe.hset(key, field, newv)
            elif op_type == "SET":
                old = r.get(key)
                old_null = 1 if old is None else 0
                oldv = "" if old is None else str(old)
                audit_rows.append({"op": "SET", "key": key, "old": oldv, "old_null": old_null, "new": newv})
                pipe.set(key, newv)
        
        pipe.execute()

        ts_ms = _now_ms()
        for a in audit_rows:
            a["ts_ms"] = ts_ms
            a["who"] = who
            store_append_audit(r, bundle_id, a, RECS_TTL_SEC)

        store_set_status(r, bundle_id, "APPLIED", RECS_TTL_SEC)
        
        # --- record last meta ramp apply (for DiD windows) ---
        try:
            # Handle both RecBundle object and dict format
            if isinstance(b, RecBundle):
                meta = getattr(b, "meta", None)
            else:
                meta = b.get("meta") if isinstance(b, dict) else None
            
            if isinstance(meta, dict) and meta.get("kind") == "meta_enforce_ramp":
                # ts_ms is apply timestamp we already computed
                r.set(os.getenv("META_RAMP_LAST_APPLIED_MS_KEY", "meta:ramp:last_applied_ms"), str(ts_ms), ex=RECS_TTL_SEC)
                to_share = meta.get("to_share", "")
                # общий
                r.set(os.getenv("META_RAMP_LAST_SHARE_KEY", "meta:ramp:last_share"), str(to_share), ex=RECS_TTL_SEC)
                # если per-regime — считаем, что trend/range одинаково
                r.set("meta:ramp:last_share_trend", str(to_share), ex=RECS_TTL_SEC)
                r.set("meta:ramp:last_share_range", str(to_share), ex=RECS_TTL_SEC)
                r.set(os.getenv("META_RAMP_LAST_BUNDLE_ID_KEY", "meta:ramp:last_bundle_id"), str(bundle_id), ex=RECS_TTL_SEC)
        except Exception:
            pass
        
        # --- record freeze/unfreeze state for meta cells ---
        try:
            # Handle both RecBundle object and dict format
            if isinstance(b, RecBundle):
                meta = getattr(b, "meta", None)
            else:
                meta = b.get("meta") if isinstance(b, dict) else None
            
            if isinstance(meta, dict) and meta.get("kind") == "meta_enforce_freeze_cells":
                # Store per cell in Redis hash meta:freeze:cells  (cell -> json)
                hkey = os.getenv("META_FREEZE_REGISTRY_KEY", "meta:freeze:cells")
                cells = meta.get("cells") or []
                freeze_to = meta.get("freeze_to")
                use_per_regime = int(meta.get("use_per_regime", 1) or 1)

                # Build lookup from audit_rows to capture previous values
                # audit_rows exists in _apply_bundle scope (we created it before pipe.execute)
                prev = {}
                for a in audit_rows:
                    if a.get("op") == "HSET":
                        prev[(a.get("key", ""), a.get("field", ""))] = a.get("old", "")

                for ck in cells:
                    if not isinstance(ck, str) or "|" not in ck:
                        continue
                    sym, bucket = ck.split("|", 1)
                    sym = sym.upper().strip()
                    bucket = bucket.lower().strip()
                    key = f"{os.getenv('CFG_HASH_PREFIX', 'config:orderflow:')}{sym}"

                    if use_per_regime == 1:
                        field = f"meta_enforce_share_{bucket}"
                    else:
                        field = "meta_enforce_share"

                    oldv = prev.get((key, field), "")
                    rec = {
                        "cell": f"{sym}|{bucket}",
                        "symbol": sym,
                        "bucket": bucket,
                        "applied_ms": ts_ms,
                        "freeze_to": freeze_to,
                        "prev_share": oldv,
                        "field": field,
                        "cfg_key": key,
                        "bundle_id": bundle_id,
                    }
                    r.hset(hkey, f"{sym}|{bucket}", json.dumps(rec, ensure_ascii=False, separators=(",", ":")))
                r.expire(hkey, RECS_TTL_SEC)

            if isinstance(meta, dict) and meta.get("kind") == "meta_enforce_unfreeze_cells":
                hkey = os.getenv("META_FREEZE_REGISTRY_KEY", "meta:freeze:cells")
                cells = meta.get("cells") or []
                for ck in cells:
                    if isinstance(ck, str):
                        r.hdel(hkey, ck)
            
            # --- staged unfreeze progress registry ---
            if isinstance(meta, dict) and meta.get("kind") == "meta_enforce_unfreeze_stage":
                reg_freeze = os.getenv("META_FREEZE_REGISTRY_KEY", "meta:freeze:cells")
                reg_unf = os.getenv("META_UNFREEZE_REGISTRY_KEY", "meta:unfreeze:cells")
                prefix = os.getenv("CFG_HASH_PREFIX", "config:orderflow:")

                stage = int(meta.get("stage", 1) or 1)
                cells = meta.get("cells") or []
                restore_map = meta.get("restore_map") or {}
                restore_final = meta.get("restore_final_map") or {}

                # build lookup old values from audit_rows (already in scope)
                prev = {}
                for a in audit_rows:
                    if a.get("op") == "HSET":
                        prev[(a.get("key", ""), a.get("field", ""))] = a.get("old", "")

                for ck in cells:
                    if not isinstance(ck, str) or "|" not in ck:
                        continue
                    sym, bucket = ck.split("|", 1)
                    sym = sym.upper().strip()
                    bucket = bucket.lower().strip()
                    cfg_key = f"{prefix}{sym}"
                    field = f"meta_enforce_share_{bucket}"

                    rec = {
                        "cell": f"{sym}|{bucket}",
                        "symbol": sym,
                        "bucket": bucket,
                        "stage": stage,
                        "applied_ms": ts_ms,
                        "target_share": str(restore_map.get(ck, "")),
                        "restore_final": str(restore_final.get(ck, "")),
                        "prev_share": prev.get((cfg_key, field), ""),
                        "field": field,
                        "cfg_key": cfg_key,
                        "bundle_id": bundle_id,
                    }

                    # stage1: mark as unfreeze-in-progress, remove from freeze registry
                    if stage == 1:
                        r.hset(reg_unf, f"{sym}|{bucket}", json.dumps(rec, ensure_ascii=False, separators=(",", ":")))
                        r.hdel(reg_freeze, f"{sym}|{bucket}")
                    # stage2 (final): remove progress entry
                    else:
                        r.hdel(reg_unf, f"{sym}|{bucket}")

                r.expire(reg_unf, RECS_TTL_SEC)
                r.expire(reg_freeze, RECS_TTL_SEC)
        except Exception:
            pass
        
        return "applied"
    finally:
        # Release lock
        r.delete(LOCK_KEY + bundle_id)


def _cancel_bundle(r: redis.Redis, bundle_id: str, who: dict) -> str:
    """
    Cancels bundle (returns to PENDING status).
    
    Args:
        r: Redis client
        bundle_id: Bundle identifier
        who: Dictionary with user info
        
    Returns:
        Operation status: "pending", "not_cancelable"
    """
    st = store_get_status(r, bundle_id)
    if st in ("APPLIED", "REJECTED", "ROLLED_BACK"):
        return f"not_cancelable({st})"
    store_set_status(r, bundle_id, "PENDING", RECS_TTL_SEC)
    store_append_audit(r, bundle_id, {"ts_ms": _now_ms(), "cancelled": True, "who": who}, RECS_TTL_SEC)
    return "pending"


def _reject_bundle(r: redis.Redis, bundle_id: str, who: dict) -> str:
    """
    Rejects bundle (sets REJECTED status).
    
    Args:
        r: Redis client
        bundle_id: Bundle identifier
        who: Dictionary with user info
        
    Returns:
        Operation status: "rejected", "not_rejectable"
    """
    st = store_get_status(r, bundle_id)
    if st in ("APPLIED", "ROLLED_BACK"):
        return f"not_rejectable({st})"
    store_set_status(r, bundle_id, "REJECTED", RECS_TTL_SEC)
    store_append_audit(r, bundle_id, {"ts_ms": _now_ms(), "rejected": True, "who": who}, RECS_TTL_SEC)
    return "rejected"


def _rollback_bundle(r: redis.Redis, bundle_id: str, who: dict) -> str:
    """
    Rolls back bundle (restores old values from audit log).
    
    Process:
    1. Reads bundle and checks status (must be APPLIED)
    2. Reads audit log (list of entries {key, field, old, new})
    3. Restores old values via HSET
    4. Sets status ROLLED_BACK
    
    Args:
        r: Redis client
        bundle_id: Bundle identifier
        who: Dictionary with user info {timestamp, chat_id, user_id, username}
        
    Returns:
        Operation status: "rolled_back", "missing_bundle", "not_applied", "no_audit"
    """
    b = store_get_bundle(r, bundle_id)
    if not b:
        return "missing_bundle"

    st = store_get_status(r, bundle_id)
    if st != "APPLIED":
        return f"not_applied({st})"

    # Read audit log (Redis List with JSON strings)
    entries = r.lrange(AUDIT_KEY + bundle_id, 0, -1)
    if not entries:
        return "no_audit"

    # Restore old values from audit entries
    pipe = r.pipeline()
    restored = 0
    for e in entries:
        d = json.loads(e)
        op = d.get("op")
        if op == "HSET":
            key = d.get("key")
            field = d.get("field")
            if key and field is not None:
                if int(d.get("old_null", 0)) == 1:
                    pipe.hdel(key, field)
                else:
                    pipe.hset(key, field, d.get("old", ""))
                restored += 1
        elif op == "SET":
            key = d.get("key")
            if key:
                if int(d.get("old_null", 0)) == 1:
                    pipe.delete(key)
                else:
                    pipe.set(key, d.get("old", ""))
                restored += 1
        else:
            # Backward compat: old audit shape {key,field,old,new}
            if "key" in d and "field" in d and "old" in d:
                pipe.hset(d["key"], d["field"], d.get("old", ""))
                restored += 1
    pipe.execute()

    store_set_status(r, bundle_id, "ROLLED_BACK", RECS_TTL_SEC)
    store_append_audit(r, bundle_id, {"ts_ms": _now_ms(), "rollback": True, "restored": restored, "who": who}, RECS_TTL_SEC)
    return "rolled_back"


def _send_action_result(r: redis.Redis, bundle_id: str, action: str, res: str) -> None:
    """
    Sends action result notification.
    Enriched with context-specific messages for known bundle kinds.
    
    Args:
        r: Redis client
        bundle_id: Bundle identifier
        action: Action name (confirm, cancel, reject, rollback)
        res: Result status
    """
    # Attach rollback button only if applied
    buttons = None
    if res == "applied":
        buttons = [[{"text": "↩ Rollback", "callback": f"recs:rollback:{bundle_id}:{_sign(bundle_id)}"}]]

    # Try to enrich notification for known bundle kinds
    rich_text = _build_rich_action_text(r, bundle_id, action, res)
    if rich_text:
        _notify(r, rich_text, buttons=buttons)
    else:
        _notify(r, f"recs {action}: <code>{bundle_id}</code> -> <b>{res}</b>", buttons=buttons)


def _build_rich_action_text(r: redis.Redis, bundle_id: str, action: str, res: str) -> Optional[str]:
    """
    Build a rich notification text for known bundle kinds.
    Returns None if bundle is unknown or generic.
    """
    try:
        b = store_get_bundle(r, bundle_id)
        if not b:
            return None

        if isinstance(b, RecBundle):
            meta = getattr(b, "meta", None)
        else:
            meta = b.get("meta") if isinstance(b, dict) else None

        if not isinstance(meta, dict):
            return None

        kind = meta.get("kind", "")

        # ML Scorer V2 mode promotion
        if kind == "ml_scorer_mode_promote":
            return _rich_ml_scorer_action(bundle_id, action, res)

        # OF Gate config recommendations
        if kind == "of_gate_recs":
            return _rich_of_gate_action(r, bundle_id, action, res, meta, b)

        return None
    except Exception:
        return None


def _rich_ml_scorer_action(bundle_id: str, action: str, res: str) -> str:
    """Rich notification for ML Scorer V2 mode promotion actions."""
    if action == "confirm" and res == "applied":
        return (
            f"<b>✅ ML Scorer V2 — режим обновлён</b>\n\n"
            f"<code>ML_SCORER_MODE</code>: <b>shadow → enforce</b>\n"
            f"ML Scorer V2 теперь влияет на confidence scoring.\n\n"
            f"Calibrator следующий раз проверит стабильность через ≥72ч.\n\n"
            f"bundle: <code>{bundle_id}</code>"
        )
    elif action == "reject" and res == "rejected":
        return (
            f"<b>❌ ML Scorer V2 — предложение отклонено</b>\n\n"
            f"<code>ML_SCORER_MODE</code> остаётся <b>shadow</b>\n"
            f"Calibrator предложит снова в следующем цикле\n"
            f"(если пороги по-прежнему выполнены).\n\n"
            f"bundle: <code>{bundle_id}</code>"
        )
    elif action == "rollback" and res == "rolled_back":
        return (
            f"<b>↩ ML Scorer V2 — откат выполнен</b>\n\n"
            f"<code>ML_SCORER_MODE</code>: <b>enforce → shadow</b>\n"
            f"ML Scorer V2 откатился к shadow режиму.\n\n"
            f"bundle: <code>{bundle_id}</code>"
        )
    elif action == "cancel":
        return (
            f"<b>⏸ ML Scorer V2 — отменено</b>\n\n"
            f"Предложение промоции <code>shadow → enforce</code> отменено.\n"
            f"Статус вернулся в PENDING.\n\n"
            f"bundle: <code>{bundle_id}</code>"
        )
    return None

def _rich_of_gate_action(
    r: redis.Redis,
    bundle_id: str,
    action: str,
    res: str,
    meta: dict,
    bundle,
) -> Optional[str]:
    """Rich notification for OF Gate config recommendation actions."""
    mode = meta.get("mode", "?")
    ts = meta.get("ts", "?")

    # Build ops summary from bundle
    ops_lines: List[str] = []
    ops_list = []
    if isinstance(bundle, RecBundle):
        ops_list = [
            {"op": op.op, "key": op.key, "field": getattr(op, "field", None), "value": op.value}
            for op in (bundle.ops or [])
        ]
    elif isinstance(bundle, dict):
        ops_list = bundle.get("ops") or []

    for op in ops_list[:10]:
        op_type = str(op.get("op", ""))
        key = str(op.get("key", ""))
        field = op.get("field") or ""
        newv = str(op.get("value", ""))
        if op_type == "HSET" and field:
            try:
                old = r.hget(key, field)
                oldv = str(old) if old is not None else "∅"
            except Exception:
                oldv = "?"
            # Show only the short key suffix for readability
            short_key = key.rsplit(":", 1)[-1] if ":" in key else key
            ops_lines.append(f"  • <code>{short_key}</code> <code>{field}</code>: <code>{oldv}</code> → <code>{newv}</code>")

    n_ops = len(ops_list)
    if n_ops > 10:
        ops_lines.append(f"  … и ещё {n_ops - 10}")

    ops_block = "\n".join(ops_lines) if ops_lines else "  (нет операций)"

    if action == "confirm" and res == "applied":
        return (
            f"<b>✅ OF Gate Recs — применено</b>\n"
            f"mode=<code>{mode}</code>  ts=<code>{ts}</code>\n"
            f"Изменено: <code>{n_ops}</code> ops\n"
            f"{ops_block}\n\n"
            f"bundle: <code>{bundle_id}</code>"
        )
    elif action == "reject" and res == "rejected":
        return (
            f"<b>❌ OF Gate Recs — отклонено</b>\n"
            f"mode=<code>{mode}</code>  ts=<code>{ts}</code>\n"
            f"Рекомендации <b>не применены</b>. "
            f"Следующий отчёт предложит заново (если условия сохранятся).\n\n"
            f"bundle: <code>{bundle_id}</code>"
        )
    elif action == "rollback" and res == "rolled_back":
        return (
            f"<b>↩ OF Gate Recs — откат выполнен</b>\n"
            f"mode=<code>{mode}</code>  ts=<code>{ts}</code>\n"
            f"Откачено: <code>{n_ops}</code> ops — значения восстановлены.\n\n"
            f"bundle: <code>{bundle_id}</code>"
        )
    elif action == "cancel":
        return (
            f"<b>⏸ OF Gate Recs — отменено</b>\n"
            f"mode=<code>{mode}</code>  ts=<code>{ts}</code>\n"
            f"Предложение отменено, статус вернулся в PENDING.\n\n"
            f"bundle: <code>{bundle_id}</code>"
        )
    return None



def main() -> None:
    """
    Main worker loop.
    
    Reads events from Redis stream bot:callbacks via consumer group,
    processes two-phase approve for bundle recommendations.
    
    Two-phase process:
    1. Preview: shows diff old→new, Confirm/Cancel buttons
    2. Confirm: applies bundle (HSET), shows Rollback
    3. Cancel: returns to PENDING
    4. Reject: single-phase, sets REJECTED status
    5. Rollback: rolls back applied bundle
    
    Callback format from bot-nest:
    - callback: "recs:<action>:<bundle_id>:<sig>" (action: preview/confirm/cancel/reject/rollback)
    - timestamp: string (epoch ms)
    - chat_id: string
    - user_id: string
    - username: string
    """
    # Use decode_responses=True for automatic string decoding
    r = redis.Redis.from_url(REDIS_URL, decode_responses=True)
    _ensure_group(r)

    logger.info(
        "Starting recommendations callback worker: stream=%s, group=%s, consumer=%s",
        BOT_CALLBACKS_STREAM, BOT_CALLBACKS_GROUP, BOT_CALLBACKS_CONSUMER
    )
    if RECS_ALLOWED_USER_IDS or RECS_ALLOWED_CHAT_IDS:
        logger.info(
            "Security: allowlist enabled (users=%s, chats=%s)",
            RECS_ALLOWED_USER_IDS, RECS_ALLOWED_CHAT_IDS
        )

    while True:
        try:
            # Read events from stream (blocking read, timeout 5 seconds)
            resp = None
            try:
                resp = r.xreadgroup(
                    BOT_CALLBACKS_GROUP,
                    BOT_CALLBACKS_CONSUMER,
                    {BOT_CALLBACKS_STREAM: ">"},
                    count=50,
                    block=5000,
                )
            except redis.exceptions.ResponseError as e:
                # Handle NOGROUP errors by recreating consumer group
                error_msg = str(e).upper()
                if "NOGROUP" in error_msg or is_redis_stream_error(e):
                    logger.warning("Consumer group %s missing or stream error, recreating... Error: %s", BOT_CALLBACKS_GROUP, e)
                    try:
                        _ensure_group(r)
                        time.sleep(0.5)  # Brief pause before retrying
                        # Retry the read after recreating group
                        resp = r.xreadgroup(
                            BOT_CALLBACKS_GROUP,
                            BOT_CALLBACKS_CONSUMER,
                            {BOT_CALLBACKS_STREAM: ">"},
                            count=50,
                            block=5000,
                        )
                    except Exception as group_err:
                        logger.error("Failed to recreate consumer group: %s", group_err)
                        time.sleep(2)
                        continue
                else:
                    # Re-raise non-NOGROUP ResponseErrors
                    raise
            
            if not resp:
                continue

            # Process messages
            for _stream, msgs in resp:
                for msg_id, fields in msgs:
                    try:
                        # bot-nest writes all fields as strings
                        cb = str(fields.get("callback", "") or "")
                        ts = str(fields.get("timestamp", "") or "")
                        chat_id = str(fields.get("chat_id", "") or "")
                        user_id = str(fields.get("user_id", "") or "")
                        username = str(fields.get("username", "") or "")

                        who = {"timestamp": ts, "chat_id": chat_id, "user_id": user_id, "username": username}

                        # Allowlist check (security)
                        if not _allowed(who):
                            _notify(r, "recs: <b>denied</b> (not allowed)")
                            logger.warning("Access denied for user_id=%s, chat_id=%s", user_id, chat_id)
                            continue

                        # Parse callback: expect format recs:<action>:<bundle_id>:<sig>
                        parts = cb.split(":")
                        if len(parts) != 4 or parts[0] != "recs":
                            continue

                        action = parts[1]
                        bundle_id = parts[2]
                        sig = parts[3]

                        if action not in ("preview", "confirm", "cancel", "reject", "rollback"):
                            continue

                        # Verify HMAC signature
                        if not _verify(bundle_id, sig):
                            _notify(r, f"recs {action}: <code>{bundle_id}</code> -> <b>invalid signature</b>")
                            continue

                        # Process action
                        if action == "preview":
                            res = _preview_bundle(r, bundle_id, who)
                            # preview already sends its own message
                        elif action == "confirm":
                            res = _apply_bundle(r, bundle_id, who)
                            _send_action_result(r, bundle_id, "confirm", res)
                        elif action == "cancel":
                            res = _cancel_bundle(r, bundle_id, who)
                            _send_action_result(r, bundle_id, "cancel", res)
                        elif action == "reject":
                            res = _reject_bundle(r, bundle_id, who)
                            _send_action_result(r, bundle_id, "reject", res)
                        else:
                            res = _rollback_bundle(r, bundle_id, who)
                            _send_action_result(r, bundle_id, "rollback", res)

                        logger.info("Processed %s for bundle %s: %s", action, bundle_id, res)

                    except Exception as e:
                        logger.error("Error processing message %s: %s", msg_id, e, exc_info=True)
                    finally:
                        # ACK message (remove from pending)
                        try:
                            r.xack(BOT_CALLBACKS_STREAM, BOT_CALLBACKS_GROUP, msg_id)
                        except Exception as ack_err:
                            logger.error("Error ACKing message %s: %s", msg_id, ack_err)
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as e:
            # Handle connection/timeout errors with retry
            logger.warning("Redis connection/timeout error, retrying in 2s... Error: %s", e)
            time.sleep(2)
        except KeyboardInterrupt:
            logger.info("Shutting down...")
            break
        except Exception as e:
            logger.error("Error in main loop: %s", e, exc_info=True)
            time.sleep(1)  # Small pause before retry


if __name__ == "__main__":
    main()
