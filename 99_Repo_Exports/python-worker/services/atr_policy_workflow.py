from __future__ import annotations

import hashlib
import json
import os
import time
from typing import Any, Dict, Optional

import psycopg2
import psycopg2.extras
import redis
from services.atr_policy_state_store import get_conn, upsert_proposal, insert_decision, update_proposal_status


def _redis():
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)


def _dsn() -> str:
    return (
        os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("TRADES_DB_DSN")
        or "postgresql://postgres:12345@postgres:5432/scanner_analytics"
    )


def _proposal_id(obj: Dict[str, Any]) -> str:
    base = "|".join([
        str(obj.get("source") or ""),
        str(obj.get("symbol") or ""),
        str(obj.get("scenario") or ""),
        str(obj.get("regime") or ""),
        str(obj.get("risk_horizon_bucket") or ""),
        str(obj.get("stop_ttl_mode") or ""),
        str(obj.get("trailing_mode") or ""),
        str(obj.get("updated_at_ms") or 0),
    ])
    return hashlib.sha1(base.encode("utf-8")).hexdigest()


def proposal_key(proposal_id: str) -> str:
    return f"cfg:proposals:atr_policy:{proposal_id}"


def decision_key(proposal_id: str) -> str:
    return f"cfg:decisions:atr_policy:{proposal_id}"


def active_key(obj: Dict[str, Any]) -> str:
    return (
        f"cfg:atr_policy:active:{obj['source']}:{obj['symbol']}:"
        f"{obj['scenario']}:{obj['regime']}:{obj['risk_horizon_bucket']}"
    )


def active_prev_key(obj: Dict[str, Any]) -> str:
    return (
        f"cfg:atr_policy:active_prev:{obj['source']}:{obj['symbol']}:"
        f"{obj['scenario']}:{obj['regime']}:{obj['risk_horizon_bucket']}"
    )


def submit_proposal(payload: Dict[str, Any]) -> str:
    r = _redis()
    now_ms = int(time.time() * 1000)
    obj = dict(payload)
    obj["proposal_id"] = _proposal_id(obj)
    obj["status"] = "SUBMITTED"
    obj["approved"] = False
    obj.setdefault("created_at_ms", now_ms)
    obj["updated_at_ms"] = now_ms

    pid = obj["proposal_id"]
    policy_ver = int(obj.get("policy_ver", 0))

    # 1. PostgreSQL write-through
    with get_conn() as conn:
        try:
            upsert_proposal(conn, obj)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    # 2. Redis logic
    r.set(proposal_key(pid), json.dumps(obj, ensure_ascii=False, sort_keys=True))
    r.sadd("queue:atr_policy:pending", pid)
    return pid


def record_decision(proposal_id: str, *, action: str, actor: str, note: str = "") -> bool:
    r = _redis()
    raw = r.get(proposal_key(proposal_id))
    if not raw:
        return False
    obj = json.loads(raw)
    action_u = str(action or "").upper()
    if action_u not in {"APPROVE", "REJECT", "REVOKE"}:
        return False

    now_ms = int(time.time() * 1000)
    decision = {
        "proposal_id": proposal_id,
        "action": action_u,
        "actor": actor,
        "note": note,
        "ts_ms": now_ms,
    }

    if action_u == "APPROVE":
        obj["status"] = "APPROVED"
        obj["approved"] = True
    elif action_u == "REJECT":
        obj["status"] = "REJECTED"
        obj["approved"] = False
    elif action_u == "REVOKE":
        obj["status"] = "REVOKE_REQUESTED"
        obj["approved"] = False
    obj["updated_at_ms"] = now_ms

    # 1. PostgreSQL write-through
    with get_conn() as conn:
        try:
            insert_decision(conn, proposal_id, decision)
            update_proposal_status(conn, proposal_id, status=obj["status"], approved=bool(obj["approved"]), updated_at_ms=now_ms)
            conn.commit()
        except Exception:
            conn.rollback()
            return False

    # 2. Redis logic
    r.set(decision_key(proposal_id), json.dumps(decision, ensure_ascii=False, sort_keys=True))
    r.sadd("queue:atr_policy:decided", proposal_id)
    r.set(proposal_key(proposal_id), json.dumps(obj, ensure_ascii=False, sort_keys=True))
    return True
