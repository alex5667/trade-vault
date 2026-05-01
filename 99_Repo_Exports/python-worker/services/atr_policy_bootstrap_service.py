from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Tuple

import psycopg2
import psycopg2.extras
import redis
try:
    from core.redis_client import get_atr_redis
except Exception:
    get_atr_redis = None


def _dsn() -> str:
    return (
        os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("TRADES_DB_DSN")
        or "postgresql://postgres:12345@postgres:5432/scanner_analytics"
    )


def _redis():
    if get_atr_redis is not None:
        return get_atr_redis()
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)


def _mode() -> str:
    return str(os.getenv("ATR_POLICY_BOOTSTRAP_MODE", "restore_if_missing") or "restore_if_missing").strip().lower()


def _now_ms() -> int:
    return int(time.time() * 1000)


def _active_key(obj: Dict[str, Any]) -> str:
    return (
        f"cfg:atr_policy:active:{obj['source']}:{obj['symbol']}:"
        f"{obj['scenario']}:{obj['regime']}:{obj['risk_horizon_bucket']}"
    )


def _last_good_key(obj: Dict[str, Any]) -> str:
    return (
        f"cfg:atr_policy:last_good:{obj['source']}:{obj['symbol']}:"
        f"{obj['scenario']}:{obj['regime']}:{obj['risk_horizon_bucket']}"
    )


def _proposal_key(proposal_id: str) -> str:
    return f"cfg:proposals:atr_policy:{proposal_id}"


def _decision_key(proposal_id: str) -> str:
    return f"cfg:decisions:atr_policy:{proposal_id}"


def _load_current_snapshots(conn, snapshot_kind: str) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """,
            SELECT snapshot_json
            FROM atr_policy_snapshots
            WHERE snapshot_kind = %s
              AND is_current = true
            """,
            (snapshot_kind,),
        )
        return [dict(r["snapshot_json"]) for r in cur.fetchall()]


def _load_pending_proposals(conn) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """,
            SELECT proposal_json
            FROM atr_policy_proposals
            WHERE status = 'SUBMITTED'
            ORDER BY created_at_ms ASC
            """,
        )
        return [dict(r["proposal_json"]) for r in cur.fetchall()]


def _load_decided_but_not_applied(conn) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
    """,
    Deterministic rebuild for decided queue:
      - APPROVED but not APPLIED
      - REVOKE_REQUESTED but not REVOKED_APPLIED
      - legacy REVOKED handled by snapshot comparison
    """,
    out: List[Tuple[Dict[str, Any], Dict[str, Any]]] = []
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """,
            WITH last_decision AS (
              SELECT DISTINCT ON (proposal_id)
                     proposal_id, decision_json, ts_ms
              FROM atr_policy_decisions
              ORDER BY proposal_id, ts_ms DESC
            )
            SELECT p.proposal_json, d.decision_json, p.status
            FROM atr_policy_proposals p
            JOIN last_decision d
              ON d.proposal_id = p.proposal_id
            WHERE p.status IN ('APPROVED','REVOKE_REQUESTED','REVOKED')
            ORDER BY p.updated_at_ms ASC
            """,
        )
        for row in cur.fetchall():
            proposal = dict(row["proposal_json"])
            decision = dict(row["decision_json"])
            out.append((proposal, decision))
    return out


def _restore_key(r, key: str, obj: Dict[str, Any], mode: str) -> bool:
    raw = r.get(key)
    if mode == "audit_only":
        return raw is not None
    
    # Optional diagnostics tracing:
    obj["recovered_from_sql"] = True
    obj["bootstrap_restored_at_ms"] = _now_ms()

    if mode == "restore_if_missing":
        if raw:
            return True
        r.set(key, json.dumps(obj, ensure_ascii=False, sort_keys=True))
        return True
    
    if mode == "force_sql_over_redis":
        r.set(key, json.dumps(obj, ensure_ascii=False, sort_keys=True))
        return True
        
    return False


def _insert_recovery_event(conn, *, event_type: str, obj: Dict[str, Any], status: str, reason_code: str, payload: Dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """,
            INSERT INTO atr_policy_recovery_events (
              event_type, source, symbol, scenario, regime, risk_horizon_bucket,
              status, reason_code, payload
            ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
            """,
            (
                event_type,
                str(obj.get("source", "")),
                str(obj.get("symbol", "")).upper(),
                str(obj.get("scenario", "")).lower(),
                str(obj.get("regime", "")).lower(),
                str(obj.get("risk_horizon_bucket", "")).lower(),
                status,
                reason_code,
                json.dumps(payload, ensure_ascii=False, sort_keys=True),
            ),
        )


def run_once() -> Dict[str, Any]:
    mode = _mode()
    r = _redis()
    conn = psycopg2.connect(_dsn(), connect_timeout=5, application_name="atr_policy_bootstrap_service")
    restored_active = 0
    restored_last_good = 0
    rebuilt_pending = 0
    rebuilt_decided = 0
    try:
        # 1) active snapshots
        for obj in _load_current_snapshots(conn, "active"):
            ok = _restore_key(r, _active_key(obj), obj, mode)
            if ok and mode != "audit_only":
                restored_active += 1
            _insert_recovery_event(
                conn,
                event_type="boot_restore_active",
                obj=obj,
                status="ok" if ok else "failed",
                reason_code="BOOTSTRAP_ACTIVE_RESTORED" if ok else "BOOTSTRAP_ACTIVE_RESTORE_FAILED",
                payload={"mode": mode},
            )

        # 2) last_good snapshots
        for obj in _load_current_snapshots(conn, "last_good"):
            ok = _restore_key(r, _last_good_key(obj), obj, mode)
            if ok and mode != "audit_only":
                restored_last_good += 1
            _insert_recovery_event(
                conn,
                event_type="boot_restore_last_good",
                obj=obj,
                status="ok" if ok else "failed",
                reason_code="BOOTSTRAP_LAST_GOOD_RESTORED" if ok else "BOOTSTRAP_LAST_GOOD_RESTORE_FAILED",
                payload={"mode": mode},
            )

        # 3) rebuild pending proposals queue
        pending = _load_pending_proposals(conn)
        if mode != "audit_only":
            # clear + rebuild deterministic set
            if pending:
                for obj in pending:
                    pid = str(obj["proposal_id"])
                    r.set(_proposal_key(pid), json.dumps(obj, ensure_ascii=False, sort_keys=True))
                    r.sadd("queue:atr_policy:pending", pid)
                    rebuilt_pending += 1

        # 4) rebuild decided queue
        decided = _load_decided_but_not_applied(conn)
        if mode != "audit_only":
            for proposal, decision in decided:
                pid = str(proposal["proposal_id"])
                r.set(_proposal_key(pid), json.dumps(proposal, ensure_ascii=False, sort_keys=True))
                r.set(_decision_key(pid), json.dumps(decision, ensure_ascii=False, sort_keys=True))
                r.sadd("queue:atr_policy:decided", pid)
                rebuilt_decided += 1

        conn.commit()
        if mode != "audit_only":
            r.set("atr_policy:bootstrap:last_run_ts_ms", str(_now_ms()))
            
        return {
            "mode": mode,
            "restored_active": restored_active,
            "restored_last_good": restored_last_good,
            "rebuilt_pending": rebuilt_pending,
            "rebuilt_decided": rebuilt_decided,
        }
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    print(json.dumps(run_once(), ensure_ascii=False, sort_keys=True))
