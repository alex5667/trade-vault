from __future__ import annotations

import hashlib
import json
import os
import time
from typing import Any

import psycopg2
import psycopg2.extras
import redis


def _dsn() -> str:
    return (
        os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("TRADES_DB_DSN")
        or "postgresql://postgres:12345@postgres:5432/scanner_analytics"
    )


def _redis():
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)


def _mode() -> str:
    return (os.getenv("ATR_POLICY_FULL_RECOVERY_MODE", "restore_if_missing") or "restore_if_missing").strip().lower()


def _run_id() -> str:
    base = f"{_mode()}|{int(time.time() * 1000)}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:20]


def _lock_key() -> str:
    return "lock:atr_policy:full_recovery"


def _acquire_lock(r, run_id: str) -> bool:
    ttl = int(os.getenv("ATR_POLICY_FULL_RECOVERY_LOCK_SEC", "900") or 900)
    return bool(r.set(_lock_key(), run_id, nx=True, ex=ttl))


def _release_lock(r, run_id: str) -> None:
    cur = r.get(_lock_key())
    if cur == run_id:
        r.delete(_lock_key())


def _active_key(obj: dict[str, Any]) -> str:
    return f"cfg:atr_policy:active:{obj['source']}:{obj['symbol']}:{obj['scenario']}:{obj['regime']}:{obj['risk_horizon_bucket']}"


def _last_good_key(obj: dict[str, Any]) -> str:
    return f"cfg:atr_policy:last_good:{obj['source']}:{obj['symbol']}:{obj['scenario']}:{obj['regime']}:{obj['risk_horizon_bucket']}"


def _proposal_key(pid: str) -> str:
    return f"cfg:proposals:atr_policy:{pid}"


def _decision_key(pid: str) -> str:
    return f"cfg:decisions:atr_policy:{pid}"


def _active_ref(key: str) -> str:
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]


def _active_ref_key(ref: str) -> str:
    return f"cfg:atr_policy:active_ref:{ref}"


def _scan_delete_prefix(r, pattern: str) -> int:
    cur = 0
    deleted = 0
    while True:
        cur, keys = r.scan(cur, match=pattern, count=10000)
        for key in keys:
            r.delete(key)
            deleted += 1
        if cur == 0:
            break
    return deleted


def _insert_run(conn, run_id: str, mode: str) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            INSERT INTO atr_policy_recovery_runs (run_id, mode, status, steps_json, summary_json)
            VALUES (%s, %s, 'started', '{}'::jsonb, '{}'::jsonb)
            """,
            (run_id, mode),
        )


def _update_run(conn, run_id: str, *, status: str, steps: dict[str, Any], summary: dict[str, Any]) -> None:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE atr_policy_recovery_runs
            SET status = %s,
                steps_json = %s::jsonb,
                summary_json = %s::jsonb,
                finished_at = CASE WHEN %s IN ('finished','failed') THEN now() ELSE finished_at END
            WHERE run_id = %s
            """,
            (
                status,
                json.dumps(steps, ensure_ascii=False, sort_keys=True),
                json.dumps(summary, ensure_ascii=False, sort_keys=True),
                status,
                run_id,
            ),
        )


def _load_current_snapshots(conn, kind: str) -> list[dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT snapshot_json
            FROM atr_policy_snapshots
            WHERE snapshot_kind = %s
              AND is_current = true
            """,
            (kind,),
        )
        return [dict(r["snapshot_json"]) for r in cur.fetchall()]


def _load_pending(conn) -> list[dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            SELECT proposal_json
            FROM atr_policy_proposals
            WHERE status = 'SUBMITTED'
            ORDER BY created_at_ms ASC
            """
        )
        return [dict(r["proposal_json"]) for r in cur.fetchall()]


def _load_decided(conn) -> list[tuple[dict[str, Any], dict[str, Any]]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """
            WITH last_decision AS (
              SELECT DISTINCT ON (proposal_id)
                     proposal_id, decision_json, ts_ms
              FROM atr_policy_decisions
              ORDER BY proposal_id, ts_ms DESC
            )
            SELECT p.proposal_json, d.decision_json
            FROM atr_policy_proposals p
            JOIN last_decision d
              ON d.proposal_id = p.proposal_id
            WHERE p.status IN ('APPROVED','REVOKE_REQUESTED','REVOKED')
            ORDER BY p.updated_at_ms ASC
            """
        )
        return [(dict(r["proposal_json"]), dict(r["decision_json"])) for r in cur.fetchall()]


def _expire_pending_confirms_on_boot(conn) -> int:
    with conn.cursor() as cur:
        cur.execute(
            """
            UPDATE atr_policy_confirm_requests
            SET status = 'EXPIRED_ON_BOOT'
            WHERE status = 'PENDING'
            """
        )
        return int(cur.rowcount or 0)


def run_once() -> dict[str, Any]:
    mode = _mode()
    run_id = _run_id()
    r = _redis()

    if not _acquire_lock(r, run_id):
        return {"ok": False, "reason_code": "RECOVERY_LOCK_BUSY"}

    conn = psycopg2.connect(_dsn(), connect_timeout=5, application_name="atr_policy_full_recovery_service")
    steps: dict[str, Any] = {}
    summary: dict[str, Any] = {}
    try:
        _insert_run(conn, run_id, mode)
        conn.commit()

        if mode == "rebuild_all_clean":
            steps["clean_namespace"] = {
                "deleted_active": _scan_delete_prefix(r, "cfg:atr_policy:active:*"),
                "deleted_last_good": _scan_delete_prefix(r, "cfg:atr_policy:last_good:*"),
                "deleted_refs": _scan_delete_prefix(r, "cfg:atr_policy:active_ref:*"),
                "deleted_proposals": _scan_delete_prefix(r, "cfg:proposals:atr_policy:*"),
                "deleted_decisions": _scan_delete_prefix(r, "cfg:decisions:atr_policy:*"),
                "deleted_confirms": _scan_delete_prefix(r, "cfg:atr_policy:confirm:*"),
            }

        active = _load_current_snapshots(conn, "active")
        last_good = _load_current_snapshots(conn, "last_good")
        pending = _load_pending(conn)
        decided = _load_decided(conn)
        expired_confirms = _expire_pending_confirms_on_boot(conn)
        conn.commit()

        restored_active = 0
        restored_last_good = 0
        rebuilt_pending = 0
        rebuilt_decided = 0
        rebuilt_refs = 0

        for obj in active:
            key = _active_key(obj)
            if mode in {"force_sql_over_redis", "rebuild_all_clean"} or not r.get(key):
                if mode != "audit_only":
                    r.set(key, json.dumps(obj, ensure_ascii=False, sort_keys=True))
                    restored_active += 1
            ref = _active_ref(key)
            if mode != "audit_only":
                r.set(_active_ref_key(ref), key, ex=int(os.getenv("ATR_POLICY_TELEGRAM_PACK_REF_TTL_SEC", "86400")))
                rebuilt_refs += 1

        for obj in last_good:
            key = _last_good_key(obj)
            if mode in {"force_sql_over_redis", "rebuild_all_clean"} or not r.get(key):
                if mode != "audit_only":
                    r.set(key, json.dumps(obj, ensure_ascii=False, sort_keys=True))
                    restored_last_good += 1

        for obj in pending:
            pid = str(obj["proposal_id"])
            if mode != "audit_only":
                r.set(_proposal_key(pid), json.dumps(obj, ensure_ascii=False, sort_keys=True))
                r.sadd("queue:atr_policy:pending", pid)
                rebuilt_pending += 1

        for proposal, decision in decided:
            pid = str(proposal["proposal_id"])
            if mode != "audit_only":
                r.set(_proposal_key(pid), json.dumps(proposal, ensure_ascii=False, sort_keys=True))
                r.set(_decision_key(pid), json.dumps(decision, ensure_ascii=False, sort_keys=True))
                r.sadd("queue:atr_policy:decided", pid)
                rebuilt_decided += 1

        if mode != "audit_only":
            r.set("atr_policy:full_recovery:last_run_ts_ms", str(int(time.time() * 1000)))
            r.set("atr_policy:full_recovery:last_run_id", run_id)

        steps["restore_serving_state"] = {
            "restored_active": restored_active,
            "restored_last_good": restored_last_good,
        }
        steps["restore_workflow_state"] = {
            "rebuilt_pending": rebuilt_pending,
            "rebuilt_decided": rebuilt_decided,
        }
        steps["restore_operator_state"] = {
            "rebuilt_refs": rebuilt_refs,
            "expired_confirms_on_boot": expired_confirms,
        }

        summary = {
            "mode": mode,
            "active_current_count": len(active),
            "last_good_current_count": len(last_good),
            "pending_sql_count": len(pending),
            "decided_sql_count": len(decided),
            "expired_confirms_on_boot": expired_confirms,
        }

        _update_run(conn, run_id, status="finished", steps=steps, summary=summary)
        conn.commit()

        if mode != "audit_only" and os.getenv("ATR_POLICY_FULL_RECOVERY_REPUBLISH_PACK", "1") == "1":
            try:
                from services.atr_policy_telegram_pack_service import publish_ops_pack
                publish_ops_pack()
            except Exception:
                pass
        return {"ok": True, "run_id": run_id, "steps": steps, "summary": summary}
    except Exception as exc:
        conn.rollback()
        try:
            _update_run(conn, run_id, status="failed", steps=steps, summary={"error": str(exc)})
            conn.commit()
        except Exception:
            pass
        raise
    finally:
        conn.close()
        _release_lock(r, run_id)


if __name__ == "__main__":
    print(json.dumps(run_once(), ensure_ascii=False, sort_keys=True))
