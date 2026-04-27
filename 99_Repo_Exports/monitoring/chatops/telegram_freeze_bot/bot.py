from __future__ import annotations

import json
import os
import re
import sys
import time
from typing import Any, Dict, List, Optional, Tuple

import requests
import redis

import sys
sys.path.insert(0, "/app/python-worker")

from core.promote_freeze import read_freeze, set_freeze, clear_freeze


def _env(name: str, default: str = "") -> str:
    return (os.getenv(name) or default).strip()


BOT_TOKEN = _env("TELEGRAM_BOT_TOKEN", "")
REDIS_URL = _env("REDIS_URL", "redis://redis-worker-1:6379/0")
OPS_EVENT_STREAM = _env("OPS_EVENT_STREAM", "ops:eventlog")

# Config keys
CFG_ALLOWED_CHAT_ID_KEY = _env("CHATOPS_CFG_ALLOWED_CHAT_ID_KEY", "cfg:chatops:allowed_chat_id")
CFG_ADMINS_SET_KEY = _env("CHATOPS_CFG_ADMINS_SET_KEY", "cfg:chatops:admins")
CFG_TWO_PERSON_CLEAR_KEY = _env("CHATOPS_CFG_TWO_PERSON_CLEAR_KEY", "cfg:chatops:two_person_clear")
CFG_TWO_PERSON_WINDOW_S_KEY = _env("CHATOPS_CFG_TWO_PERSON_WINDOW_S_KEY", "cfg:chatops:two_person_window_s")
CFG_RATE_LIMIT_PER_MIN_KEY = _env("CHATOPS_CFG_RATE_LIMIT_PER_MIN_KEY", "cfg:chatops:rate_limit_per_min")

# Bot state
OFFSET_KEY = _env("TELEGRAM_BOT_OFFSET_KEY", "ops:telegram_freeze_bot:offset")
PENDING_CLEAR_KEY = _env("TELEGRAM_BOT_PENDING_CLEAR_KEY", "ops:telegram_freeze_bot:pending_clear")
PENDING_CLEAR_APPROVERS_KEY = _env("TELEGRAM_BOT_PENDING_CLEAR_APPROVERS_KEY", "ops:telegram_freeze_bot:pending_clear_approvers")
POLL_INTERVAL_S = float(_env("TELEGRAM_BOT_POLL_INTERVAL_S", "2.0"))
TIMEOUT_S = int(_env("TELEGRAM_BOT_LONGPOLL_TIMEOUT_S", "25"))

# Operational knobs
MAX_REASON_LEN = 240
CFG_CACHE_TTL_S = float(_env("CHATOPS_CFG_CACHE_TTL_S", "10.0"))

_cfg_cache: Dict[str, Any] = {"ts": 0.0}

def _now_ms() -> int:
    return int(time.time() * 1000)

def _now_s() -> float:
    return time.time()

def _redis() -> redis.Redis:
    return redis.Redis.from_url(REDIS_URL, decode_responses=True)

def _tg_api(method: str) -> str:
    return f"https://api.telegram.org/bot{BOT_TOKEN}/{method}"

def _tg_post(method: str, payload: Dict[str, Any]) -> Dict[str, Any]:
    r = requests.post(_tg_api(method), json=payload, timeout=10)
    if r.status_code >= 300:
        raise RuntimeError(f"telegram_http:{r.status_code}:{r.text[:200]}")
    js = r.json()
    if not js.get("ok"):
        raise RuntimeError(f"telegram_api_error:{js}")
    return js

class _TelegramConflict(Exception):
    """Raised on HTTP 409 – another getUpdates session is active."""

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
    payload: Dict[str, Any] = {"chat_id": chat_id, "text": text, "disable_web_page_preview": True}
    if reply_to is not None:
        payload["reply_to_message_id"] = reply_to
    _tg_post("sendMessage", payload)

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

def _metrics_incr(r: redis.Redis, key: str, amount: int = 1) -> None:
    try:
        r.incrby(key, int(amount))
    except Exception:
        pass

def _metrics_set(r: redis.Redis, key: str, value: str) -> None:
    try:
        r.set(key, value)
    except Exception:
        pass

def _metrics_cmd_incr(r: redis.Redis, cmd: str) -> None:
    _metrics_incr(r, f"metrics:chatops:cmd_total:{cmd}", 1)

def _metrics_unauthorized(r: redis.Redis) -> None:
    _metrics_incr(r, "metrics:chatops:unauthorized_total", 1)
    _metrics_set(r, "metrics:chatops:last_unauthorized_ts_ms", str(_now_ms()))

def _metrics_rate_limited(r: redis.Redis) -> None:
    _metrics_incr(r, "metrics:chatops:rate_limited_total", 1)
    _metrics_set(r, "metrics:chatops:last_rate_limited_ts_ms", str(_now_ms()))

def _rate_limit_allow(r: redis.Redis, *, chat_id: str, user_id: str, per_min: int) -> bool:
    if per_min <= 0:
        return True
    bucket = int(_now_s() // 60)
    key = f"ops:chatops:rl:{chat_id}:{user_id}:{bucket}"
    try:
        n = int(r.incr(key))
        if n == 1:
            r.expire(key, 70)
        return n <= per_min
    except Exception:
        return False

def _load_cfg(r: redis.Redis) -> Dict[str, Any]:
    now = time.time()
    if now - _cfg_cache["ts"] < CFG_CACHE_TTL_S and "cfg" in _cfg_cache:
        return _cfg_cache["cfg"]
    cfg: Dict[str, Any] = {}
    try:
        cfg["allowed_chat_id"] = str(r.get(CFG_ALLOWED_CHAT_ID_KEY) or "").strip()
    except Exception:
        pass
    try:
        cfg["admins"] = set(r.smembers(CFG_ADMINS_SET_KEY) or [])
    except Exception:
        cfg["admins"] = set()
    try:
        v = str(r.get(CFG_TWO_PERSON_CLEAR_KEY) or "").strip()
        cfg["two_person_clear"] = v in ("1", "true", "yes") if v else False
    except Exception:
        cfg["two_person_clear"] = False
    try:
        v2 = str(r.get(CFG_TWO_PERSON_WINDOW_S_KEY) or "").strip()
        cfg["two_person_window_s"] = int(v2) if v2 else 300
    except Exception:
        cfg["two_person_window_s"] = 300
    rl = ""
    try:
        rl = str(r.get(CFG_RATE_LIMIT_PER_MIN_KEY) or "").strip()
    except Exception:
        rl = ""
    if not rl:
        rl = _env("CHATOPS_RATE_LIMIT_PER_MIN", "10")
    try:
        cfg["rate_limit_per_min"] = max(1, int(rl))
    except Exception:
        cfg["rate_limit_per_min"] = 10

    if not cfg.get("allowed_chat_id"):
        cfg["allowed_chat_id"] = _env("TELEGRAM_ALLOWED_CHAT_ID", _env("TELEGRAM_CHAT_ID", ""))
    if not cfg.get("admins"):
        cfg["admins"] = set(x for x in re.split(r"[,\s]+", _env("TELEGRAM_ADMIN_USER_IDS", "")) if x)

    _cfg_cache["ts"] = now
    _cfg_cache["cfg"] = dict(cfg)
    return cfg

def _is_admin(user_id: Optional[int], cfg: Dict[str, Any]) -> bool:
    if user_id is None:
        return False
    admins = cfg.get("admins")
    if not admins:
        return False
    return str(user_id) in admins

def _chat_allowed(chat_id: Optional[int], cfg: Dict[str, Any]) -> bool:
    allowed = cfg.get("allowed_chat_id")
    if not allowed:
        return False
    if chat_id is None:
        return False
    return str(chat_id) == str(allowed)

HELP = """ChatOps: Edge Stack promote-freeze

Commands:
  /freeze status
  /freeze set <duration_s> <reason...>
  /freeze clear <reason...>

Examples:
  /freeze set 3600 manual investigation
  /freeze status

Security:
  - only TELEGRAM_ALLOWED_CHAT_ID is accepted
  - only TELEGRAM_ADMIN_USER_IDS are allowed (fail-closed if empty)

Two-person rule (recommended):
  - /freeze clear requires 2 different admin confirmations within a time window.
  - reason is required on the first /freeze clear (to start the pending request)

Rate limit:
  - per admin: rate_limit_per_min (default 10/min)
"""

def _parse_freeze_cmd(text: str) -> Optional[Tuple[str, List[str]]]:
    t = (text or "").strip()
    if not t:
        return None
    if t.startswith("/freeze"):
        rest = t[len("/freeze") :].strip()
    elif t.startswith("freeze"):
        rest = t[len("freeze") :].strip()
    else:
        return None
    if not rest:
        return ("help", [])
    parts = rest.split()
    return (parts[0].lower(), parts[1:])

def _format_status() -> str:
    st = read_freeze(REDIS_URL)
    now_ms = _now_ms()
    left_s = 0
    if st.until_ts_ms and st.until_ts_ms > now_ms:
        left_s = int((st.until_ts_ms - now_ms) / 1000)
    return json.dumps(
        {"active": st.active, "until_ts_ms": st.until_ts_ms, "left_s": left_s, "reason": st.reason, "source": st.source},
        ensure_ascii=False,
    )

def _pending_clear_get(r: redis.Redis) -> Dict[str, str]:
    try:
        return r.hgetall(PENDING_CLEAR_KEY)
    except Exception:
        return {}

def _pending_clear_reset(r: redis.Redis) -> None:
    try:
        r.delete(PENDING_CLEAR_KEY, PENDING_CLEAR_APPROVERS_KEY)
    except Exception:
        pass

def _pending_clear_start(r: redis.Redis, *, window_s: int, initiator: Dict[str, Any]) -> None:
    now_ms = _now_ms()
    try:
        r.hset(PENDING_CLEAR_KEY, mapping={
            "started_ts_ms": str(now_ms),
            "expires_ts_ms": str(now_ms + window_s * 1000),
            "initiator_id": str(initiator.get("actor", "")),
        })
        r.expire(PENDING_CLEAR_KEY, window_s + 10)
        r.sadd(PENDING_CLEAR_APPROVERS_KEY, str(initiator.get("actor", "")))
        r.expire(PENDING_CLEAR_APPROVERS_KEY, window_s + 10)
    except Exception:
        pass

def _handle_cmd(cmd: str, args: List[str], actor: Dict[str, Any], cfg: Dict[str, Any], r: redis.Redis) -> Tuple[bool, str]:
    if cmd in ("help", "h", "?"):
        return True, HELP
    if cmd == "status":
        _metrics_cmd_incr(r, "status")
        return True, _format_status()
    if cmd == "clear":
        _metrics_cmd_incr(r, "clear")
        if bool(cfg.get("two_person_clear", True)):
            window_s = int(cfg.get("two_person_window_s", 300))
            pend = _pending_clear_get(r)
            now_ms = _now_ms()
            if not pend:
                if not args:
                    return False, "Usage: /freeze clear <reason...>  (two-person rule enabled)"
                reason = " ".join(args).strip()[:MAX_REASON_LEN]
                _pending_clear_start(r, window_s=window_s, initiator=actor)
                try:
                    r.hset(PENDING_CLEAR_KEY, mapping={"reason": reason})
                except Exception:
                    pass
                _write_ops_event({"type": "chatops_clear_pending_started", "window_s": window_s, **actor})
                _metrics_incr(r, "metrics:chatops:clear_pending_started_total", 1)
                return True, f"Clear pending started. Need 2nd admin confirm within {window_s}s: send /freeze clear"
            try:
                exp_ms = int(float(pend.get("expires_ts_ms", "0") or 0))
            except Exception:
                exp_ms = 0
            if exp_ms and exp_ms < now_ms:
                _pending_clear_reset(r)
                if not args:
                    return False, "Pending expired. Usage: /freeze clear <reason...>"
                reason = " ".join(args).strip()[:MAX_REASON_LEN]
                _pending_clear_start(r, window_s=window_s, initiator=actor)
                try:
                    r.hset(PENDING_CLEAR_KEY, mapping={"reason": reason})
                except Exception:
                    pass
                _write_ops_event({"type": "chatops_clear_pending_restarted", "window_s": window_s, **actor})
                _metrics_incr(r, "metrics:chatops:clear_pending_started_total", 1)
                return True, f"Previous pending expired. Restarted. Need 2nd admin within {window_s}s: send /freeze clear"
            
            actor_id = str(actor.get("actor", ""))
            try:
                r.sadd(PENDING_CLEAR_APPROVERS_KEY, actor_id)
                approver_count = int(r.scard(PENDING_CLEAR_APPROVERS_KEY) or 0)
            except Exception:
                approver_count = 1
            if approver_count < 2:
                return True, f"Added to approvers. Need {2 - approver_count} more admin(s)."

            ok = clear_freeze(REDIS_URL)
            approvers = []
            try:
                approvers = sorted([str(x) for x in (r.smembers(PENDING_CLEAR_APPROVERS_KEY) or []) if str(x)])
            except Exception:
                approvers = [actor_id]
            reason = str(pend.get("reason", "") or "")
            _pending_clear_reset(r)
            _write_ops_event({"type": "promote_freeze_clear", "ok": ok, "approvers": approvers, "reason": reason, **actor})
            return ok, json.dumps({"ok": ok, "approvers": approvers, "reason": reason}, ensure_ascii=False)

        ok = clear_freeze(REDIS_URL)
        _write_ops_event({"type": "promote_freeze_clear", "ok": ok, **actor})
        return ok, json.dumps({"ok": ok}, ensure_ascii=False)
    
    if cmd == "set":
        _metrics_cmd_incr(r, "set")
        if len(args) < 2:
            return False, "Usage: /freeze set <duration_s> <reason...>"
        try:
            duration_s = int(args[0])
        except Exception:
            return False, "duration_s must be integer seconds"
        reason = " ".join(args[1:]).strip()[:MAX_REASON_LEN]
        if duration_s <= 0:
            return False, "duration_s must be > 0"
        ok = set_freeze(
            REDIS_URL,
            duration_s=duration_s,
            reason=reason,
            source="chatops",
            extra={"actor": actor.get("actor", "unknown")},
        )
        _write_ops_event({"type": "promote_freeze_set", "ok": ok, "duration_s": duration_s, "reason": reason, **actor})
        return ok, json.dumps({"ok": ok, "duration_s": duration_s}, ensure_ascii=False)
    return False, f"Unknown subcommand: {cmd}\n\n{HELP}"


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


def main() -> int:
    if not BOT_TOKEN:
        print("TELEGRAM_BOT_TOKEN is required", file=sys.stderr)
        return 2

    # Allow disabling polling via ENV (useful when a sibling service polls the same token)
    if _env("BOT_POLLING_ENABLED", "true").lower() in {"0", "false", "no"}:
        print("chatops-telegram-freeze-bot: polling disabled via BOT_POLLING_ENABLED=false. Sleeping forever.")
        while True:
            time.sleep(3600)

    r = _redis()
    offset = _load_offset(r)
    cfg = _load_cfg(r)
    print(json.dumps({"ok": True, "offset": offset, "allowed_chat_id": cfg.get("allowed_chat_id"), "admin_user_ids": list(cfg.get("admins", []))}, ensure_ascii=False))

    _conflict_backoff = 0  # exponential back-off (seconds) on 409 Conflict

    while True:
        try:
            updates = _tg_get_updates(offset=offset + 1 if offset else 0)
            _conflict_backoff = 0  # reset on success
            for u in updates:
                upd_id = int(u.get("update_id", 0))
                
                offset = max(offset, upd_id)
                _save_offset(r, offset)
                
                cb = u.get("callback_query")
                if cb:
                    try:
                        r.publish("telegram_callbacks", json.dumps(cb))
                    except Exception:
                        pass
                    continue
                
                msg = (u.get("message") or {})
                chat = (msg.get("chat") or {})
                from_ = (msg.get("from") or {})
                chat_id = chat.get("id")
                user_id = from_.get("id")
                text = msg.get("text") or ""
                msg_id = msg.get("message_id")

                offset = max(offset, upd_id)
                _save_offset(r, offset)

                cfg = _load_cfg(r)
                if not _chat_allowed(chat_id, cfg):
                    continue
                per_min = int(cfg.get("rate_limit_per_min", 10))

                if not _is_admin(user_id, cfg):
                    _metrics_unauthorized(r)
                    try:
                        _tg_send(str(chat_id), "Unauthorized", reply_to=msg_id)
                    except Exception:
                        pass
                    _write_ops_event({"type": "chatops_unauthorized", "user_id": user_id, "chat_id": chat_id, "text": text[:200]})
                    continue

                parsed = _parse_freeze_cmd(text)
                if not parsed:
                    continue
                subcmd, args = parsed
                actor = {
                    "actor": str(user_id),
                    "chat_id": str(chat_id),
                    "username": str(from_.get("username") or ""),
                    "name": f"{from_.get('first_name','')}".strip(),
                }
                if not _rate_limit_allow(r, chat_id=str(chat_id), user_id=str(user_id), per_min=per_min):
                    _metrics_rate_limited(r)
                    _write_ops_event({"type": "chatops_rate_limited", "per_min": per_min, **actor})
                    try:
                        _tg_send(str(chat_id), f"Rate limited ({per_min}/min). Try later.", reply_to=msg_id)
                    except Exception:
                        pass
                    continue
                ok, resp = _handle_cmd(subcmd, args, actor, cfg, r)
                try:
                    _tg_send(str(chat_id), resp, reply_to=msg_id)
                except Exception as e:
                    _write_ops_event({"type": "chatops_send_failed", "err": str(e)[:200], **actor})
                else:
                    _write_ops_event({"type": "chatops_freeze_cmd", "cmd": subcmd, "ok": ok, "args": args[:10], **actor})

        except _TelegramConflict as e:
            # 409: another getUpdates session active on same token.
            # Telegram expires stale sessions in ~60s; back off longer each retry.
            _conflict_backoff = min(_conflict_backoff + 60, 120)
            wait = _conflict_backoff + (hash(str(time.time())) % 15)  # simple jitter
            print(
                f"⚠️ chatops-freeze-bot: {e}. "
                f"Backing off {wait}s. Fix: set BOT_POLLING_ENABLED=false or use a dedicated bot token."
            )
            _write_ops_event({"type": "chatops_loop_error", "err": str(e)[:200]})
            time.sleep(wait)
            continue

        except Exception as e:
            _write_ops_event({"type": "chatops_loop_error", "err": str(e)[:200]})
            time.sleep(2.0)
            continue

        time.sleep(POLL_INTERVAL_S)


if __name__ == "__main__":
    raise SystemExit(main())
