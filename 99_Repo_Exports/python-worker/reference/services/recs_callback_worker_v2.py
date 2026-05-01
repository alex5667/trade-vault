from __future__ import annotations
"""Two-phase callback worker v2 for recs bundles (preview2 -> confirm).

This worker handles:
- recs:preview2:<id>:<sig> -> shows diff, sets PREVIEWED
- recs:confirm:<id>:<sig> -> applies ops, writes audit, sets APPLIED (requires PREVIEWED)
- recs:reject:<id>:<sig> -> sets REJECTED
- recs:rollback:<id>:<sig> -> rolls back by audit

Reads from bot:callbacks stream, writes to notify:telegram.
"""

from utils.time_utils import get_ny_time_millis

import json
import os
import time
import hmac
import hashlib
from typing import Any, Dict, List, Tuple, Optional

import redis


def now_ms() -> int:
    """Returns current timestamp in milliseconds (epoch)."""
    return get_ny_time_millis()


def sign(bundle_id: str, secret: str) -> str:
    """Generates short HMAC signature for bundle_id (8 hex characters)."""
    d = hmac.new(secret.encode("utf-8"), bundle_id.encode("utf-8"), hashlib.sha256).hexdigest()
    return d[:8]


def notify(r: redis.Redis, text: str, buttons: Optional[List[List[Dict[str, str]]]] = None) -> None:
    """Sends notification to notify:telegram stream."""
    fields = {"type": "report", "text": text, "ts": str(now_ms())}
    if buttons is not None:
        fields["buttons"] = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"), fields, maxlen=200000, approximate=True)


def parse_cb(cb: str) -> Tuple[str, str, str]:
    """Parse callback: recs:<action>:<bundle_id>:<sig>"""
    parts = cb.split(":")
    if len(parts) < 4:
        return "", "", ""
    if parts[0] != "recs":
        return "", "", ""
    action = parts[1]
    bundle_id = parts[2]
    sig = parts[3]
    return action, bundle_id, sig


def read_bundle(r: redis.Redis, bundle_id: str) -> Optional[Dict[str, Any]]:
    """Read bundle from recs:bundle:<id>."""
    raw = r.get(f"recs:bundle:{bundle_id}")
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None


def set_status(r: redis.Redis, bundle_id: str, status: str, ttl: int) -> None:
    """Set recs:status:<id>."""
    r.set(f"recs:status:{bundle_id}", status, ex=ttl)


def status(r: redis.Redis, bundle_id: str) -> str:
    """Get recs:status:<id>."""
    return (r.get(f"recs:status:{bundle_id}") or "").strip().upper()


def audit_push(r: redis.Redis, bundle_id: str, entry: Dict[str, Any], ttl: int) -> None:
    """Append entry to recs:audit:<id> list."""
    r.rpush(f"recs:audit:{bundle_id}", json.dumps(entry, ensure_ascii=False, separators=(",", ":")))
    r.expire(f"recs:audit:{bundle_id}", ttl)


def get_audit(r: redis.Redis, bundle_id: str) -> List[Dict[str, Any]]:
    """Read all entries from recs:audit:<id> list."""
    key = f"recs:audit:{bundle_id}"
    n = r.llen(key)
    out = []
    for i in range(n):
        s = r.lindex(key, i)
        if not s:
            continue
        try:
            out.append(json.loads(s))
        except Exception:
            pass
    return out


def op_preview_diff(r: redis.Redis, bundle: Dict[str, Any], max_lines: int = 60) -> str:
    """Format preview diff for bundle ops."""
    ops = bundle.get("ops") or []
    lines = []
    for op in ops:
        typ = op.get("op")
        key = op.get("key")
        field = op.get("field")
        
        # Validations: HSET/HDEL need field; SET does not
        if not typ or not key:
            continue
        if typ in ("HSET", "HDEL") and not field:
            continue

        cur = r.hget(key, field)
        cur_s = "" if cur is None else str(cur)

        if typ == "HSET":
            newv = str(op.get("value", ""))
            lines.append(f"{key} {field}: {cur_s} -> {newv}")
        elif typ == "HDEL":
            lines.append(f"{key} {field}: {cur_s} -> <DEL>")
        elif typ == "SET":
            # For SET, field is usually empty/ignored, we get key directly
            cur_val = r.get(key)
            cur_val_s = "" if cur_val is None else str(cur_val)
            # Truncate for preview
            if len(cur_val_s) > 50: cur_val_s = cur_val_s[:47] + "..."
            newv = str(op.get("value", ""))
            if len(newv) > 50: newv = newv[:47] + "..."
            lines.append(f"SET {key}: {cur_val_s} -> {newv}")


        if len(lines) >= max_lines:
            lines.append("... (truncated)")
            break

    head = f"id={bundle.get('id')} who={bundle.get('who')} ops={len(ops)}"
    body = "\n".join(lines) if lines else "(no ops)"
    return f"<b>RECS PREVIEW</b>\n<code>{head}</code>\n<pre>{body}</pre>"


def apply_ops(r: redis.Redis, bundle: Dict[str, Any], ttl: int, actor: Dict[str, str]) -> int:
    """
    Apply bundle ops, write recs:audit:<id>.
    Returns number of applied ops.
    """
    bundle_id = str(bundle.get("id", ""))
    ops = bundle.get("ops") or []
    ts = now_ms()
    applied = 0

    pipe = r.pipeline()
    for op in ops:
        typ = op.get("op")
        key = op.get("key")
        field = op.get("field")
        
        # Validations: HSET/HDEL need field; SET does not
        if not typ or not key:
            continue
        if typ in ("HSET", "HDEL") and not field:
            continue

        old = r.hget(key, field)
        old_null = 1 if old is None else 0

        if typ == "HSET":
            val = str(op.get("value", ""))
            pipe.hset(key, field, val)
            audit_push(r, bundle_id, {
                "op": "HSET",
                "key": key,
                "field": field,
                "old": "" if old is None else str(old),
                "old_null": old_null,
                "new": val,
                "ts_ms": ts,
                "who": "recs_callback_worker_v2",
                "actor": actor,
            }, ttl)
            applied += 1

        elif typ == "HDEL":
            pipe.hdel(key, field)
            audit_push(r, bundle_id, {
                "op": "HDEL",
                "key": key,
                "field": field,
                "old": "" if old is None else str(old),
                "old_null": old_null,
                "new": "",
                "ts_ms": ts,
                "who": "recs_callback_worker_v2",
                "actor": actor,
            }, ttl)
            applied += 1

        elif typ == "SET":
            # SET operation (no field)
            val = str(op.get("value", ""))
            
            # For SET, we need to know the old value to rollback.
            # Unlike HSET which we can read mostly cheaply, SET might be large?
            # We must read it to support rollback.
            old_val = r.get(key)
            old_null = 1 if old_val is None else 0
            
            pipe.set(key, val)
            audit_push(r, bundle_id, {
                "op": "SET",
                "key": key,
                "field": "",  # no field for SET
                "old": "" if old_val is None else str(old_val),
                "old_null": old_null,
                "new": val,
                "ts_ms": ts,
                "who": "recs_callback_worker_v2",
                "actor": actor,
            }, ttl)
            applied += 1

    pipe.execute()
    return applied


def rollback_ops(r: redis.Redis, bundle_id: str, ttl: int, actor: Dict[str, str]) -> int:
    """Rollback bundle by reversing audit entries."""
    aud = get_audit(r, bundle_id)
    if not aud:
        return 0

    ts = now_ms()
    applied = 0
    pipe = r.pipeline()

    # reverse order
    for a in reversed(aud):
        key = a.get("key")
        field = a.get("field")
        # For rollback, key is mandatory. Field is mandatory only for hash ops, but audit doesn't store op type explicitly in accessible way easily?
        # Actually audit stores 'op'. Let's check op if possible, or just relax check if key is present.
        # But wait, audit entries are just dicts. simpler: 
        if not key:
            continue
        old_null = int(a.get("old_null", 0) or 0)
        old = "" if a.get("old") is None else str(a.get("old", ""))

        if old_null == 1:
            if field:
                pipe.hdel(key, field)
            else:
                # If field is empty, it was a SET (or DEL) on a key
                pipe.delete(key)
        else:
            if field:
                pipe.hset(key, field, old)
            else:
                # SET rollback
                pipe.set(key, old)
        applied += 1

    pipe.execute()
    r.set(f"recs:status:{bundle_id}", "ROLLED_BACK", ex=ttl)
    notify(r, f"<b>Rollback done</b>\nid=<code>{bundle_id}</code>\nops=<code>{applied}</code>\nactor=<code>{actor}</code>")
    return applied


def _ensure_group(r: redis.Redis, stream: str, group: str) -> None:
    """Create consumer group if it doesn't exist."""
    try:
        r.xgroup_create(stream, group, id="0", mkstream=True)
    except Exception:
        # Group already exists - this is normal
        pass


def main() -> None:
    """Main worker loop."""
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = redis.Redis.from_url(redis_url, decode_responses=True)

    ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")

    stream = os.getenv("BOT_CALLBACKS_STREAM", "bot:callbacks")
    group = os.getenv("BOT_CALLBACKS_GROUP", "recs-callback-worker-v2")
    consumer = os.getenv("BOT_CALLBACKS_CONSUMER", "c1")

    # Create group if needed
    _ensure_group(r, stream, group)

    while True:
        resp = None
        try:
            resp = r.xreadgroup(group, consumer, {stream: ">"}, count=10, block=5000)
        except redis.exceptions.ResponseError as e:
            # Handle NOGROUP errors by recreating consumer group
            error_msg = str(e).upper()
            if "NOGROUP" in error_msg:
                print(f"⚠️ NOGROUP error detected, recreating consumer group: {e}")
                try:
                    _ensure_group(r, stream, group)
                    time.sleep(0.5)  # Brief pause before retrying
                    # Retry the read after recreating group
                    resp = r.xreadgroup(group, consumer, {stream: ">"}, count=10, block=5000)
                except Exception as group_err:
                    print(f"❌ Failed to recreate consumer group: {group_err}")
                    time.sleep(2)
                    continue
            else:
                # Re-raise non-NOGROUP ResponseErrors
                print(f"⚠️ Redis ResponseError in xreadgroup: {e}")
                time.sleep(1)
                continue
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError, OSError) as e:
            # Connection errors - retry with backoff
            print(f"⚠️ Redis connection error: {e}")
            time.sleep(2)
            continue
        except Exception as e:
            # Other errors - log and retry
            print(f"⚠️ Unexpected error reading from stream: {e}")
            time.sleep(2)
            continue

        if not resp:
            continue

        for _stream, msgs in resp:
            for msg_id, fields in msgs:
                cb = str(fields.get("callback", "") or "")
                action, bundle_id, sig = parse_cb(cb)

                # ack by default at end
                try:
                    if not action or not bundle_id or not sig:
                        r.xack(stream, group, msg_id)
                        continue

                    if sign(bundle_id, secret) != sig:
                        notify(r, f"<b>Bad signature</b>\ncb=<code>{cb}</code>")
                        r.xack(stream, group, msg_id)
                        continue

                    bundle = read_bundle(r, bundle_id)
                    if bundle is None:
                        notify(r, f"<b>Bundle not found</b>\nid=<code>{bundle_id}</code>")
                        r.xack(stream, group, msg_id)
                        continue

                    actor = {
                        "chat_id": str(fields.get("chat_id", "")),
                        "user_id": str(fields.get("user_id", "")),
                        "username": str(fields.get("username", "")),
                        "timestamp": str(fields.get("timestamp", "")),
                    }

                    st = status(r, bundle_id)

                    if action == "preview2":
                        if st in ("APPLIED", "REJECTED", "ROLLED_BACK"):
                            notify(r, f"<b>Preview ignored</b>\nid=<code>{bundle_id}</code> status=<code>{st}</code>")
                        else:
                            txt = op_preview_diff(r, bundle)
                            set_status(r, bundle_id, "PREVIEWED", ttl)
                            notify(r, txt)
                        r.xack(stream, group, msg_id)
                        continue

                    if action == "confirm":
                        if st not in ("PREVIEWED",):
                            notify(r, f"<b>Confirm blocked</b>\nid=<code>{bundle_id}</code>\nstatus=<code>{st}</code>\nneed=<code>PREVIEWED</code>")
                            r.xack(stream, group, msg_id)
                            continue

                        n = apply_ops(r, bundle, ttl, actor)
                        set_status(r, bundle_id, "APPLIED", ttl)
                        notify(r, f"<b>Applied</b>\nid=<code>{bundle_id}</code>\nops=<code>{n}</code>\nactor=<code>{actor}</code>")
                        r.xack(stream, group, msg_id)
                        continue

                    if action == "reject":
                        if st in ("APPLIED", "ROLLED_BACK"):
                            notify(r, f"<b>Reject ignored</b>\nid=<code>{bundle_id}</code> status=<code>{st}</code>")
                        else:
                            set_status(r, bundle_id, "REJECTED", ttl)
                            notify(r, f"<b>Rejected</b>\nid=<code>{bundle_id}</code>\nactor=<code>{actor}</code>")
                        r.xack(stream, group, msg_id)
                        continue

                    if action == "rollback":
                        rollback_ops(r, bundle_id, ttl, actor)
                        r.xack(stream, group, msg_id)
                        continue

                    # unknown recs action
                    r.xack(stream, group, msg_id)

                except Exception as e:
                    notify(r, f"<b>Callback worker error</b>\ncb=<code>{cb}</code>\nerr=<code>{e}</code>")
                    # ack to avoid poison-loop
                    r.xack(stream, group, msg_id)


if __name__ == "__main__":
    main()

