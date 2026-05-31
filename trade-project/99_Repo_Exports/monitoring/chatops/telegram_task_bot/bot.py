"""Telegram ChatOps bot — Antigravity task bridge.

Accepts /task commands via Telegram, stores them in a Redis LIST
(``antigravity:inbox``), and provides /tasks + /done for queue management.

Architecture follows chatops-telegram-freeze-bot (raw ``requests`` polling,
no python-telegram-bot framework).
"""

from __future__ import annotations

import json
import os
import sys
import time
import uuid
from typing import Any, Dict, List, Optional, Tuple


import requests
import redis


# ── ENV helpers ──────────────────────────────────────────────────────────

def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


# ── Config ───────────────────────────────────────────────────────────────

BOT_TOKEN: str = _env("TELEGRAM_BOT_TOKEN", "")
REDIS_URL: str = _env("REDIS_URL", "redis://redis-worker-1:6379/0")
INBOX_KEY: str = _env("ANTIGRAVITY_INBOX_KEY", "antigravity:inbox")
DONE_KEY: str = _env("ANTIGRAVITY_DONE_KEY", "antigravity:done")
OPS_EVENT_STREAM: str = _env("OPS_EVENT_STREAM", "ops:eventlog")

# Security
ALLOWED_CHAT_ID: str = _env("TELEGRAM_ALLOWED_CHAT_ID", _env("TELEGRAM_CHAT_ID", ""))
ADMIN_USER_IDS_RAW: str = _env("TELEGRAM_ADMIN_USER_IDS", "")
ADMIN_USER_IDS: List[str] = [x.strip() for x in ADMIN_USER_IDS_RAW.split(",") if x.strip()]

# Polling
OFFSET_KEY: str = _env("TELEGRAM_BOT_OFFSET_KEY", "ops:telegram_task_bot:offset")
POLL_INTERVAL_S: float = float(_env("TELEGRAM_BOT_POLL_INTERVAL_S", "2.0"))
TIMEOUT_S: int = int(_env("TELEGRAM_BOT_LONGPOLL_TIMEOUT_S", "25"))

# Limits
MAX_TASK_LEN: int = 2000
MAX_INBOX_SIZE: int = 100


# ── Telegram API ─────────────────────────────────────────────────────────

def _tg_api(method: str) -> str:
    return f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"


class _TelegramConflict(Exception):
    """409 — another getUpdates session is active."""


def _tg_get_updates(offset: int) -> List[Dict[str, Any]]:
    params = {"timeout": TIMEOUT_S, "offset": offset, "allowed_updates": ["message", "callback_query"]}
    r = requests.get(_tg_api("getUpdates"), params=params, timeout=TIMEOUT_S + 5)
    if r.status_code == 409:
        raise _TelegramConflict(f"409 Conflict: {r.text[:200]}")
    if r.status_code >= 300:
        raise RuntimeError(f"telegram_http:{r.status_code}:{r.text[:200]}")
    js = r.json()
    if not js.get("ok"):
        raise RuntimeError(f"telegram_api_error:{js}")
    return js.get("result") or []


def _tg_send(chat_id: str, text: str, reply_to: Optional[int] = None) -> None:
    payload: Dict[str, Any] = {
        "chat_id": chat_id,
        "text": text,
        "disable_web_page_preview": True,
        "parse_mode": "Markdown",
    }
    if reply_to is not None:
        payload["reply_to_message_id"] = reply_to
    resp = requests.post(_tg_api("sendMessage"), json=payload, timeout=10)
    if resp.status_code >= 300:
        # Fallback without markdown if parse fails
        payload.pop("parse_mode", None)
        requests.post(_tg_api("sendMessage"), json=payload, timeout=10)


# ── Redis helpers ────────────────────────────────────────────────────────

def _redis() -> redis.Redis:
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)


def _write_ops_event(event: Dict[str, Any]) -> None:
    try:
        r = _redis()
        r.xadd(
            OPS_EVENT_STREAM,
            {"ts_ms": str(int(time.time() * 1000)), "event": json.dumps(event, ensure_ascii=False)[:4000]},
            maxlen=5000,
            approximate=True,
        )
    except Exception:
        pass


# ── Security ─────────────────────────────────────────────────────────────

def _chat_allowed(chat_id: Optional[int]) -> bool:
    if not ALLOWED_CHAT_ID:
        return True  # No restriction configured
    if chat_id is None:
        return False
    return str(chat_id) == ALLOWED_CHAT_ID


def _is_admin(user_id: Optional[int]) -> bool:
    if not ADMIN_USER_IDS:
        return True  # No restriction — all users allowed
    if user_id is None:
        return False
    return str(user_id) in ADMIN_USER_IDS


# ── Task helpers ─────────────────────────────────────────────────────────

def _now_ms() -> int:
    return int(time.time() * 1000)


def _make_task_id() -> str:
    """Short 6-char hex ID."""
    return uuid.uuid4().hex[:6]


def _task_to_json(task: Dict[str, Any]) -> str:
    return json.dumps(task, ensure_ascii=False)


def _json_to_task(raw: str) -> Dict[str, Any]:
    try:
        return json.loads(raw)
    except Exception:
        return {"text": raw, "id": "?", "ts": 0}


# ── Commands ─────────────────────────────────────────────────────────────

HELP_TEXT = """🤖 *Antigravity Task Bot*

Commands:
  `/task <описание>` — добавить задачу в очередь
  `/tasks` — показать pending задачи
  `/done <id>` — пометить задачу выполненной
  `/clear` — очистить всю очередь
  `/help` — эта справка

Задачи из очереди автоматически попадают в `tasks/inbox.md` через watcher.
"""


def _parse_command(text: str) -> Optional[Tuple[str, str]]:
    """Parse /command <args> → (command, args_str) or None."""
    t = (text or "").strip()
    if not t.startswith("/"):
        return None
    # Strip bot mention: /task@mybotname → /task
    parts = t.split(None, 1)
    cmd = parts[0].split("@")[0].lower()
    args = parts[1] if len(parts) > 1 else ""
    return (cmd, args.strip())


def cmd_task(args: str, actor: Dict[str, Any], r: redis.Redis) -> str:
    """Add a new task to the inbox."""
    if not args:
        return "❌ Usage: `/task <описание задачи>`"
    if len(args) > MAX_TASK_LEN:
        return f"❌ Task too long (max {MAX_TASK_LEN} chars)"

    # Check queue size
    current_size = r.llen(INBOX_KEY)
    if current_size >= MAX_INBOX_SIZE:
        return f"❌ Queue full ({MAX_INBOX_SIZE} tasks). Use `/done <id>` or `/clear`."

    task_id = _make_task_id()
    task = {
        "id": task_id,
        "text": args,
        "from_user": actor.get("username", ""),
        "from_user_id": actor.get("actor", ""),
        "from_name": actor.get("name", ""),
        "ts": _now_ms(),
        "status": "pending",
    }
    r.rpush(INBOX_KEY, _task_to_json(task))
    _write_ops_event({"type": "antigravity_task_added", "task_id": task_id, **actor})
    return f"✅ Task `#{task_id}` queued:\n_{args}_"


def cmd_tasks(r: redis.Redis) -> str:
    """List pending tasks."""
    raw_list = r.lrange(INBOX_KEY, 0, -1)
    if not raw_list:
        return "📭 No pending tasks."
    lines = ["📋 *Pending Tasks:*\n"]
    for i, raw in enumerate(raw_list, 1):
        t = _json_to_task(raw)
        ts_str = ""
        if t.get("ts"):
            ts_str = time.strftime("%H:%M", time.gmtime(t["ts"] / 1000))
        tid = t.get("id", "?")
        text_preview = (t.get("text", "?"))[:80]
        from_user = t.get("from_user", "")
        from_part = f" (@{from_user})" if from_user else ""
        lines.append(f"{i}. `#{tid}` [{ts_str}]{from_part}\n   {text_preview}")
    lines.append(f"\n_Total: {len(raw_list)}_")
    return "\n".join(lines)


def cmd_done(args: str, actor: Dict[str, Any], r: redis.Redis) -> str:
    """Mark a task as done and remove from inbox."""
    task_id = args.strip().lstrip("#")
    if not task_id:
        return "❌ Usage: `/done <task_id>`"

    raw_list = r.lrange(INBOX_KEY, 0, -1)
    found = False
    for raw in raw_list:
        t = _json_to_task(raw)
        if t.get("id") == task_id:
            # Move to done list
            t["status"] = "done"
            t["done_ts"] = _now_ms()
            t["done_by"] = actor.get("actor", "")
            r.rpush(DONE_KEY, _task_to_json(t))
            r.lrem(INBOX_KEY, 1, raw)
            found = True
            _write_ops_event({"type": "antigravity_task_done", "task_id": task_id, **actor})
            break
    if not found:
        return f"❌ Task `#{task_id}` not found in queue."
    return f"✅ Task `#{task_id}` marked as done."


def cmd_clear(actor: Dict[str, Any], r: redis.Redis) -> str:
    """Clear all pending tasks."""
    count = r.llen(INBOX_KEY)
    if count == 0:
        return "📭 Queue is already empty."
    r.delete(INBOX_KEY)
    _write_ops_event({"type": "antigravity_task_clear", "count": count, **actor})
    return f"🗑 Cleared {count} task(s)."


# ── Offset persistence ──────────────────────────────────────────────────

def _load_offset(r: redis.Redis) -> int:
    try:
        v = r.get(OFFSET_KEY)
        return int(v) if v else 0
    except Exception:
        return 0


def _save_offset(r: redis.Redis, offset: int) -> None:
    try:
        r.set(OFFSET_KEY, str(offset))
    except Exception:
        pass


# ── Main loop ────────────────────────────────────────────────────────────

def main() -> int:
    if not BOT_TOKEN:
        print("TELEGRAM_BOT_TOKEN is required", file=sys.stderr)
        return 2

    if _env("BOT_POLLING_ENABLED", "true").lower() in {"0", "false", "no"}:
        print("chatops-telegram-task-bot: polling disabled. Sleeping forever.")
        while True:
            time.sleep(3600)

    r = _redis()
    offset = _load_offset(r)
    print(json.dumps({
        "ok": True,
        "bot": "chatops-telegram-task-bot",
        "offset": offset,
        "allowed_chat_id": ALLOWED_CHAT_ID or "(any)",
        "admin_user_ids": ADMIN_USER_IDS or ["(any)"],
        "inbox_key": INBOX_KEY,
    }, ensure_ascii=False))

    _conflict_backoff = 0

    while True:
        try:
            updates = _tg_get_updates(offset=offset + 1 if offset else 0)
            _conflict_backoff = 0

            for u in updates:
                upd_id = int(u.get("update_id", 0))

                cb = u.get("callback_query")
                if cb:
                    try:
                        r.publish("telegram_callbacks", json.dumps(cb))
                    except Exception:
                        pass
                    offset = max(offset, upd_id)
                    _save_offset(r, offset)
                    continue

                msg = u.get("message") or {}
                chat = msg.get("chat") or {}
                from_ = msg.get("from") or {}
                chat_id = chat.get("id")
                user_id = from_.get("id")
                text = msg.get("text") or ""
                msg_id = msg.get("message_id")

                offset = max(offset, upd_id)
                _save_offset(r, offset)

                # Security checks
                if not _chat_allowed(chat_id):
                    continue
                if not _is_admin(user_id):
                    try:
                        _tg_send(str(chat_id), "🔒 Unauthorized", reply_to=msg_id)
                    except Exception:
                        pass
                    _write_ops_event({"type": "task_bot_unauthorized", "user_id": user_id})
                    continue

                parsed = _parse_command(text)
                if not parsed:
                    continue

                cmd, args = parsed
                actor = {
                    "actor": str(user_id),
                    "chat_id": str(chat_id),
                    "username": str(from_.get("username") or ""),
                    "name": f"{from_.get('first_name', '')}".strip(),
                }

                # Dispatch
                if cmd == "/task":
                    resp = cmd_task(args, actor, r)
                elif cmd == "/tasks":
                    resp = cmd_tasks(r)
                elif cmd == "/done":
                    resp = cmd_done(args, actor, r)
                elif cmd == "/clear":
                    resp = cmd_clear(actor, r)
                elif cmd in ("/help", "/start"):
                    resp = HELP_TEXT
                else:
                    continue  # Ignore unknown commands

                try:
                    _tg_send(str(chat_id), resp, reply_to=msg_id)
                except Exception as e:
                    _write_ops_event({"type": "task_bot_send_failed", "err": str(e)[:200], **actor})

        except _TelegramConflict as e:
            _conflict_backoff = min(_conflict_backoff + 60, 120)
            wait = _conflict_backoff + (hash(str(time.time())) % 15)
            print(
                f"⚠️ chatops-task-bot: {e}. "
                f"Backing off {wait}s. Use a dedicated bot token."
            )
            _write_ops_event({"type": "task_bot_conflict", "err": str(e)[:200]})
            time.sleep(wait)
            continue

        except Exception as e:
            _write_ops_event({"type": "task_bot_loop_error", "err": str(e)[:200]})
            time.sleep(2.0)
            continue

        time.sleep(POLL_INTERVAL_S)


if __name__ == "__main__":
    raise SystemExit(main())
