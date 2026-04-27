from __future__ import annotations
from utils.time_utils import get_ny_time_millis

"""Operator acknowledgement layer for Binance dust cleanup admin reminders.

Responsibilities
----------------
Allows operators to explicitly ACK (acknowledge) a denylist or cooldown-loop
reminder so that the notifier is suppressed until the ACK expires or is revoked.
Supports:
  - ack_reminder       : create a new ACK for a kind/symbol pair
  - renew_reminder_ack : extend the TTL of an existing ACK
  - revoke_reminder_ack: delete an ACK (re-enables reminder)
  - should_suppress_reminder: decide at notify-time whether to silently skip
  - ack_dashboard      : enumerate all active ACK states
  - dashboard_with_unacked: merge ack state with live denylist/cooldown scan

All state is stored in Redis with configurable TTL. An audit trail is appended
to a Redis stream so every ACK/renew/revoke is permanently recorded.

ENV vars
--------
BINANCE_DUST_ADMIN_ACK_PREFIX              default: orders:dust_cleanup:ack:
BINANCE_DUST_ADMIN_ACK_RENEW_REMINDER_SEC  default: 900  (15 min before expiry)
BINANCE_DUST_ADMIN_AUDIT_STREAM            default: orders:dust_cleanup:audit
BINANCE_DUST_ADMIN_AUDIT_STREAM_MAXLEN     default: 10000
"""

import json
import os
import time
from dataclasses import dataclass
from typing import Any, Dict, List, Optional


def _now_ms() -> int:
    """Return current epoch time in milliseconds."""
    return get_ny_time_millis()


def _json_dumps(value: Any) -> str:
    """Serialize value to JSON with stable key order."""
    return json.dumps(value, ensure_ascii=False, sort_keys=True)


def _safe_json_loads(value: Any) -> Dict[str, Any]:
    """Safely parse bytes/str to dict; returns {} on any failure."""
    if value is None:
        return {}
    if isinstance(value, dict):
        return value
    if isinstance(value, bytes):
        value = value.decode("utf-8", errors="replace")
    if not value:
        return {}
    try:
        payload = json.loads(value)
        return payload if isinstance(payload, dict) else {}
    except Exception:
        return {}


def _bool_env(name: str, default: bool) -> bool:
    """Read a boolean environment variable."""
    raw = os.getenv(name)
    if raw is None:
        return default
    return str(raw).strip().lower() in {"1", "true", "yes", "on"}


@dataclass
class DustReminderAckKeys:
    """Centralised ENV-driven key config for the ACK layer."""
    ack_prefix: str
    audit_stream: str
    stream_maxlen: int


def ack_keys_from_env() -> DustReminderAckKeys:
    """Build key config from environment variables."""
    return DustReminderAckKeys(
        ack_prefix=os.getenv(
            "BINANCE_DUST_ADMIN_ACK_PREFIX",
            "orders:dust_cleanup:ack:",
        ),
        audit_stream=os.getenv(
            "BINANCE_DUST_ADMIN_AUDIT_STREAM",
            "orders:dust_cleanup:audit",
        ),
        stream_maxlen=int(os.getenv("BINANCE_DUST_ADMIN_AUDIT_STREAM_MAXLEN", "10000")),
    )


def ack_key(kind: str, symbol: str) -> str:
    """Build the Redis key for a specific kind/symbol ACK."""
    return f"{ack_keys_from_env().ack_prefix}{kind}:{symbol.upper()}"


def _xadd_audit(redis_client: Any, payload: Dict[str, Any]) -> None:
    """Append an audit event to the configured Redis stream.

    Errors are silently swallowed — audit must never block the control path.
    """
    keys = ack_keys_from_env()
    try:
        redis_client.xadd(
            keys.audit_stream,
            {k: _json_dumps(v) if isinstance(v, (dict, list)) else str(v) for k, v in payload.items()},
            maxlen=keys.stream_maxlen,
            approximate=True,
        )
    except Exception:
        # audit must not break the control path; caller already gets explicit response
        pass


def _set_json(redis_client: Any, key: str, value: Dict[str, Any], ttl_sec: int) -> None:
    """Persist a dict as JSON to Redis with optional TTL."""
    payload = _json_dumps(value)
    if ttl_sec > 0:
        redis_client.set(key, payload, ex=ttl_sec)
    else:
        redis_client.set(key, payload)


def _ttl_seconds(redis_client: Any, key: str) -> int:
    """Return TTL in seconds; -1 = no expiry, -2 = key missing."""
    try:
        ttl = int(redis_client.ttl(key))
    except Exception:
        return -1
    return ttl


def reminder_ack_state(redis_client: Any, kind: str, symbol: str) -> Dict[str, Any]:
    """Read the current ACK state for a given kind/symbol pair.

    Returns an empty dict if no ACK is present. Augments with live ttl_sec
    and ack_key so callers don't need to re-derive the Redis key.
    """
    key = ack_key(kind, symbol)
    state = _safe_json_loads(redis_client.get(key))
    if not state:
        return {}
    state["ttl_sec"] = _ttl_seconds(redis_client, key)
    state["ack_key"] = key
    return state


def ack_reminder(
    redis_client: Any,
    *,
    kind: str,
    symbol: str,
    operator: str,
    reason: str,
    ticket: str,
    ttl_sec: int,
    fingerprint: str = "",
) -> Dict[str, Any]:
    """Create or overwrite an ACK for a dust reminder.

    Args:
        redis_client: Redis connection.
        kind:        Reminder category (e.g. "old_denylist", "cooldown_loop").
        symbol:      Trading symbol (normalised to upper-case internally).
        operator:    Operator identifier who is acknowledging.
        reason:      Free-text reason for the ACK.
        ticket:      Incident/ticket reference number.
        ttl_sec:     Seconds before the ACK automatically expires (0 = no expiry).
        fingerprint: Optional content hash; enables fingerprint-mismatch detection.

    Returns:
        Dict with the full ACK document plus live ttl_sec and ack_key.
    """
    symbol_u = symbol.upper()
    key = ack_key(kind, symbol_u)
    now_ms = _now_ms()
    doc = {
        "kind": kind,
        "symbol": symbol_u,
        "operator": operator,
        "reason": reason,
        "ticket": ticket,
        "fingerprint": fingerprint,
        "acked_at_ms": now_ms,
        "renewed_at_ms": now_ms,
        "expires_at_ms": now_ms + max(ttl_sec, 0) * 1000,
        "ack_version": 1,
    }
    _set_json(redis_client, key, doc, ttl_sec)
    _xadd_audit(
        redis_client,
        {
            "ts_ms": now_ms,
            "area": "dust_cleanup_ack",
            "action": "ack_reminder",
            "kind": kind,
            "symbol": symbol_u,
            "operator": operator,
            "reason": reason,
            "ticket": ticket,
            "fingerprint": fingerprint,
            "ttl_sec": ttl_sec,
            "result": "ok",
        },
    )
    out = dict(doc)
    out["ttl_sec"] = _ttl_seconds(redis_client, key)
    out["ack_key"] = key
    return out


def renew_reminder_ack(
    redis_client: Any,
    *,
    kind: str,
    symbol: str,
    operator: str,
    reason: str,
    ticket: str,
    ttl_sec: int,
) -> Dict[str, Any]:
    """Extend the TTL of an existing ACK document.

    Fails with ok=False if no ACK is currently present for the kind/symbol.
    Increments ack_version on each renewal so callers can detect concurrent
    changes.
    """
    symbol_u = symbol.upper()
    key = ack_key(kind, symbol_u)
    current = _safe_json_loads(redis_client.get(key))
    now_ms = _now_ms()
    if not current:
        return {
            "ok": False,
            "reason": "ack_not_found",
            "kind": kind,
            "symbol": symbol_u,
            "ack_key": key,
        }
    current["renewed_at_ms"] = now_ms
    current["expires_at_ms"] = now_ms + max(ttl_sec, 0) * 1000
    current["renew_operator"] = operator
    current["renew_reason"] = reason
    current["renew_ticket"] = ticket
    current["ack_version"] = int(current.get("ack_version", 1)) + 1
    _set_json(redis_client, key, current, ttl_sec)
    _xadd_audit(
        redis_client,
        {
            "ts_ms": now_ms,
            "area": "dust_cleanup_ack",
            "action": "renew_ack",
            "kind": kind,
            "symbol": symbol_u,
            "operator": operator,
            "reason": reason,
            "ticket": ticket,
            "ttl_sec": ttl_sec,
            "result": "ok",
        },
    )
    out = dict(current)
    out["ttl_sec"] = _ttl_seconds(redis_client, key)
    out["ack_key"] = key
    out["ok"] = True
    return out


def revoke_reminder_ack(
    redis_client: Any,
    *,
    kind: str,
    symbol: str,
    operator: str,
    reason: str,
    ticket: str,
) -> Dict[str, Any]:
    """Delete an ACK, re-enabling reminder notifications for the symbol.

    Idempotent: if no ACK exists, result="noop" is returned (not an error).
    """
    symbol_u = symbol.upper()
    key = ack_key(kind, symbol_u)
    existed = redis_client.get(key)
    redis_client.delete(key)
    now_ms = _now_ms()
    _xadd_audit(
        redis_client,
        {
            "ts_ms": now_ms,
            "area": "dust_cleanup_ack",
            "action": "revoke_ack",
            "kind": kind,
            "symbol": symbol_u,
            "operator": operator,
            "reason": reason,
            "ticket": ticket,
            "result": "ok" if existed else "noop",
        },
    )
    return {
        "ok": True,
        "kind": kind,
        "symbol": symbol_u,
        "ack_key": key,
        "result": "ok" if existed else "noop",
    }


def should_suppress_reminder(
    redis_client: Any,
    *,
    kind: str,
    symbol: str,
    fingerprint: str = "",
) -> Dict[str, Any]:
    """Decide whether a reminder notification should be suppressed.

    Decision logic:
    1. No ACK present            → not suppressed ("no_ack")
    2. ACK fingerprint mismatch  → not suppressed ("fingerprint_mismatch")
    3. ACK key is missing        → not suppressed ("ack_missing", key deleted externally)
    4. ACK has positive TTL or
       no TTL (permanent)        → suppressed ("acked")
    5. ACK TTL expired           → not suppressed ("ack_expired")

    Returns a dict with at least {"suppressed": bool, "reason": str}.
    When suppressed, "ack_state" is included for downstream renew-reminder logic.
    """
    state = reminder_ack_state(redis_client, kind, symbol)
    if not state:
        return {"suppressed": False, "reason": "no_ack"}
    if fingerprint and state.get("fingerprint") and state.get("fingerprint") != fingerprint:
        return {"suppressed": False, "reason": "fingerprint_mismatch", "ack_state": state}
    ttl = int(state.get("ttl_sec", -1))
    if ttl == -2:
        return {"suppressed": False, "reason": "ack_missing"}
    if ttl == -1 or ttl > 0:
        return {"suppressed": True, "reason": "acked", "ack_state": state}
    return {"suppressed": False, "reason": "ack_expired", "ack_state": state}


def recent_ack_audit(redis_client: Any, *, symbol: str = "", limit: int = 50) -> List[Dict[str, Any]]:
    """Read the most recent ACK audit events from the audit stream.

    Args:
        redis_client: Redis connection.
        symbol:  If non-empty, filter to events for this symbol only.
        limit:   Maximum number of events to return.

    Returns:
        List of audit event dicts in reverse-chronological order.
    """
    stream = ack_keys_from_env().audit_stream
    out: List[Dict[str, Any]] = []
    try:
        rows = redis_client.xrevrange(stream, count=max(limit, 1))
    except Exception:
        return out
    symbol_u = symbol.upper() if symbol else ""
    for row_id, fields in rows:
        event: Dict[str, Any] = {"id": row_id.decode() if isinstance(row_id, bytes) else str(row_id)}
        for k, v in fields.items():
            key = k.decode() if isinstance(k, bytes) else str(k)
            raw = v.decode() if isinstance(v, bytes) else v
            parsed = _safe_json_loads(raw)
            event[key] = parsed if parsed else raw
        if symbol_u and str(event.get("symbol", "")).upper() != symbol_u:
            continue
        if event.get("area") != "dust_cleanup_ack":
            continue
        out.append(event)
        if len(out) >= limit:
            break
    return out


def ack_dashboard(redis_client: Any, *, limit: int = 200) -> Dict[str, Any]:
    """Return all active ACK states scanned from Redis.

    Walks the ack key namespace with SCAN so it is safe on large keyspaces.
    Returns summary counts per kind and per-item details including age_sec and ttl_sec.
    """
    now_ms = _now_ms()
    prefix = ack_keys_from_env().ack_prefix
    rows: List[Dict[str, Any]] = []
    for key in redis_client.scan_iter(match=f"{prefix}*", count=max(limit, 50)):
        key_s = key.decode() if isinstance(key, bytes) else str(key)
        doc = _safe_json_loads(redis_client.get(key_s))
        if not doc:
            continue
        ttl = _ttl_seconds(redis_client, key_s)
        doc["ttl_sec"] = ttl
        doc["ack_key"] = key_s
        doc["age_sec"] = max(0, (now_ms - int(doc.get("acked_at_ms", now_ms))) // 1000)
        rows.append(doc)
    rows.sort(key=lambda x: (str(x.get("kind", "")), str(x.get("symbol", ""))))
    without_ack = {
        "denylist_old_without_ack": 0,
        "cooldown_loop_without_ack": 0,
    }
    for row in rows:
        if row.get("kind") == "old_denylist":
            without_ack["denylist_old_without_ack"] += 1
        if row.get("kind") == "cooldown_loop":
            without_ack["cooldown_loop_without_ack"] += 1
    return {
        "ok": True,
        "generated_at_ms": now_ms,
        "items": rows,
        "counts": {
            "acks": len(rows),
            **without_ack,
        },
    }


def dashboard_with_unacked(
    redis_client: Any,
    *,
    stale_denylist: List[Dict[str, Any]],
    cooldown_loops: List[Dict[str, Any]],
) -> Dict[str, Any]:
    """Merge live scan results with ACK state to highlight unacknowledged items.

    Args:
        redis_client:   Redis connection.
        stale_denylist: Items from the denylist reminder scan (each needs "symbol" key).
        cooldown_loops: Items from the cooldown-loop reminder scan (each needs "symbol" key).

    Returns a dict with filtered lists of items that have NO active operator ACK.
    """
    now_ms = _now_ms()
    acked_deny = set()
    acked_cd = set()
    for item in ack_dashboard(redis_client).get("items", []):
        symbol = str(item.get("symbol", "")).upper()
        kind = str(item.get("kind", ""))
        if kind == "old_denylist":
            acked_deny.add(symbol)
        elif kind == "cooldown_loop":
            acked_cd.add(symbol)

    deny_without_ack = [x for x in stale_denylist if str(x.get("symbol", "")).upper() not in acked_deny]
    cooldown_without_ack = [x for x in cooldown_loops if str(x.get("symbol", "")).upper() not in acked_cd]
    return {
        "ok": True,
        "generated_at_ms": now_ms,
        "stale_denylist_without_ack": deny_without_ack,
        "cooldown_loops_without_ack": cooldown_without_ack,
        "counts": {
            "stale_denylist_without_ack": len(deny_without_ack),
            "cooldown_loops_without_ack": len(cooldown_without_ack),
        },
    }


__all__ = [
    "ack_reminder",
    "renew_reminder_ack",
    "revoke_reminder_ack",
    "should_suppress_reminder",
    "reminder_ack_state",
    "recent_ack_audit",
    "ack_dashboard",
    "dashboard_with_unacked",
]
