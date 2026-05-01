from __future__ import annotations

import json
import os
import time
from typing import Any, Dict, List, Set, Tuple

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
    return str(os.getenv("ATR_POLICY_DRIFT_FIX_MODE", "audit_only") or "audit_only").strip().lower()


def _scan_keys(r, pattern: str, count: int = 500) -> List[str]:
    cur = 0
    out: List[str] = []
    while True:
        cur, keys = r.scan(cur, match=pattern, count=count)
        out.extend(keys)
        if cur == 0:
            break
    return sorted(out)


def _active_key(obj: Dict[str, Any]) -> str:
    return f"cfg:atr_policy:active:{obj['source']}:{obj['symbol']}:{obj['scenario']}:{obj['regime']}:{obj['risk_horizon_bucket']}"


def _last_good_key(obj: Dict[str, Any]) -> str:
    return f"cfg:atr_policy:last_good:{obj['source']}:{obj['symbol']}:{obj['scenario']}:{obj['regime']}:{obj['risk_horizon_bucket']}"


def _proposal_key(pid: str) -> str:
    return f"cfg:proposals:atr_policy:{pid}"


def _decision_key(pid: str) -> str:
    return f"cfg:decisions:atr_policy:{pid}"


def _json_equal(a: Dict[str, Any], b: Dict[str, Any]) -> bool:
    return json.dumps(a, ensure_ascii=False, sort_keys=True) == json.dumps(b, ensure_ascii=False, sort_keys=True)


def _load_current_snapshots(conn, kind: str) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """,
            SELECT snapshot_json
            FROM atr_policy_snapshots
            WHERE snapshot_kind = %s
              AND is_current = true
            """,
            (kind,),
        )
        return [dict(r["snapshot_json"]) for r in cur.fetchall()]


def _load_pending(conn) -> List[Dict[str, Any]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """,
            SELECT proposal_json
            FROM atr_policy_proposals
            WHERE status = 'SUBMITTED'
            """,
        )
        return [dict(r["proposal_json"]) for r in cur.fetchall()]


def _load_decided_not_applied(conn) -> List[Tuple[Dict[str, Any], Dict[str, Any]]]:
    with conn.cursor(cursor_factory=psycopg2.extras.RealDictCursor) as cur:
        cur.execute(
            """,
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
            """,
        )
        return [(dict(r["proposal_json"]), dict(r["decision_json"])) for r in cur.fetchall()]


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


def _publish_drift_stream(r, payload: Dict[str, Any]) -> None:
    r.xadd(
        "stream:atr_policy:state_drifts",
        {"data": json.dumps(payload, ensure_ascii=False, sort_keys=True)},
        maxlen=50000,
        approximate=True,
    )


def _repair_key(r, key: str, obj: Dict[str, Any], mode: str) -> bool:
    if mode != "repair_redis":
        return False
    r.set(key, json.dumps(obj, ensure_ascii=False, sort_keys=True))
    return True


def _parse_json(raw: str) -> Dict[str, Any]:
    try:
        x = json.loads(raw)
        return x if isinstance(x, dict) else {}
    except Exception:
        return {}


def _check_snapshot_kind(conn, r, *, kind: str, mode: str) -> Dict[str, int]:
    sql_rows = _load_current_snapshots(conn, kind)
    repaired = 0
    drifted = 0
    sql_keys: Set[str] = set()

    for obj in sql_rows:
        key = _active_key(obj) if kind == "active" else _last_good_key(obj)
        sql_keys.add(key)
        raw = r.get(key)
        if not raw:
            drifted += 1
            if mode == "repair_redis":
                _repair_key(r, key, obj, mode)
                repaired += 1
                r.hincrby("atr_policy:metrics:repair_total", f"{kind}:ATR_POLICY_REPAIR_{kind.upper()}_FROM_SQL", 1)
            reason = f"ATR_POLICY_DRIFT_{kind.upper()}_MISSING_IN_REDIS"
            fix = f"ATR_POLICY_REPAIR_{kind.upper()}_FROM_SQL"
            r.hincrby("atr_policy:metrics:drift_total", f"{kind}:{reason}", 1)
            _insert_recovery_event(conn, event_type="drift_check", obj=obj, status="repaired" if mode == "repair_redis" else "detected", reason_code=fix if mode == "repair_redis" else reason, payload={"key": key, "mode": mode})
            _publish_drift_stream(r, {"kind": kind, "key": key, "reason_code": reason, "repair_reason_code": fix if mode == "repair_redis" else "", "ts_ms": int(time.time() * 1000)})
            continue

        redis_obj = _parse_json(raw)
        if not redis_obj:
            drifted += 1
            if mode == "repair_redis":
                _repair_key(r, key, obj, mode)
                repaired += 1
                r.hincrby("atr_policy:metrics:repair_total", f"{kind}:ATR_POLICY_REPAIR_{kind.upper()}_FROM_SQL", 1)
            reason = f"ATR_POLICY_DRIFT_{kind.upper()}_JSON_CORRUPTED"
            fix = f"ATR_POLICY_REPAIR_{kind.upper()}_FROM_SQL"
            r.hincrby("atr_policy:metrics:drift_total", f"{kind}:{reason}", 1)
            _insert_recovery_event(conn, event_type="drift_check", obj=obj, status="repaired" if mode == "repair_redis" else "detected", reason_code=fix if mode == "repair_redis" else reason, payload={"key": key, "mode": mode})
            _publish_drift_stream(r, {"kind": kind, "key": key, "reason_code": reason, "repair_reason_code": fix if mode == "repair_redis" else "", "ts_ms": int(time.time() * 1000)})
            continue

        if not _json_equal(redis_obj, obj):
            drifted += 1
            if mode == "repair_redis":
                _repair_key(r, key, obj, mode)
                repaired += 1
                r.hincrby("atr_policy:metrics:repair_total", f"{kind}:ATR_POLICY_REPAIR_{kind.upper()}_FROM_SQL", 1)
            reason = f"ATR_POLICY_DRIFT_{kind.upper()}_VALUE_MISMATCH"
            fix = f"ATR_POLICY_REPAIR_{kind.upper()}_FROM_SQL"
            r.hincrby("atr_policy:metrics:drift_total", f"{kind}:{reason}", 1)
            _insert_recovery_event(conn, event_type="drift_check", obj=obj, status="repaired" if mode == "repair_redis" else "detected", reason_code=fix if mode == "repair_redis" else reason, payload={"key": key, "mode": mode})
            _publish_drift_stream(r, {"kind": kind, "key": key, "reason_code": reason, "repair_reason_code": fix if mode == "repair_redis" else "", "ts_ms": int(time.time() * 1000)})

    # extra keys in Redis
    redis_keys = _scan_keys(r, f"cfg:atr_policy:{kind}:*")
    extra_count = 0
    for key in redis_keys:
        if key not in sql_keys:
            drifted += 1
            extra_count += 1
            if mode == "repair_redis":
                r.delete(key)
                repaired += 1
                r.hincrby("atr_policy:metrics:repair_total", f"{kind}:ATR_POLICY_REPAIR_EXTRA_{kind.upper()}_REMOVED", 1)
            reason = f"ATR_POLICY_DRIFT_REDIS_EXTRA_{kind.upper()}_KEY"
            fix = f"ATR_POLICY_REPAIR_EXTRA_{kind.upper()}_REMOVED"
            r.hincrby("atr_policy:metrics:drift_total", f"{kind}:{reason}", 1)
            # best-effort no exact cohort object => parse from key
            _publish_drift_stream(r, {"kind": kind, "key": key, "reason_code": reason, "repair_reason_code": fix if mode == "repair_redis" else "", "ts_ms": int(time.time() * 1000)})

    r.hset("atr_policy:metrics:extra_keys_total", kind, extra_count)
    return {"drifted": drifted, "repaired": repaired}


def _check_pending(conn, r, mode: str) -> Dict[str, int]:
    sql_rows = _load_pending(conn)
    sql_ids = {str(o["proposal_id"]) for o in sql_rows}
    repaired = 0
    drifted = 0

    redis_ids = set(r.smembers("queue:atr_policy:pending") or [])

    for obj in sql_rows:
        pid = str(obj["proposal_id"])
        raw = r.get(_proposal_key(pid))
        if (pid not in redis_ids) or (not raw):
            drifted += 1
            if mode == "repair_redis":
                r.set(_proposal_key(pid), json.dumps(obj, ensure_ascii=False, sort_keys=True))
                r.sadd("queue:atr_policy:pending", pid)
                repaired += 1
                r.hincrby("atr_policy:metrics:repair_total", "pending:ATR_POLICY_REPAIR_PENDING_QUEUE_FROM_SQL", 1)
            r.hincrby("atr_policy:metrics:drift_total", "pending:ATR_POLICY_DRIFT_PENDING_QUEUE_MISSING", 1)
            _insert_recovery_event(conn, event_type="drift_check_pending", obj=obj, status="repaired" if mode == "repair_redis" else "detected", reason_code="ATR_POLICY_REPAIR_PENDING_QUEUE_FROM_SQL" if mode == "repair_redis" else "ATR_POLICY_DRIFT_PENDING_QUEUE_MISSING", payload={"proposal_id": pid, "mode": mode})

    orphans = redis_ids - sql_ids
    for pid in orphans:
        drifted += 1
        if mode == "repair_redis":
            r.srem("queue:atr_policy:pending", pid)
            repaired += 1
            r.hincrby("atr_policy:metrics:repair_total", "pending:ATR_POLICY_REPAIR_ORPHAN_QUEUE_REMOVED", 1)
        r.hincrby("atr_policy:metrics:drift_total", "pending:ATR_POLICY_DRIFT_PENDING_QUEUE_ORPHAN", 1)
        _publish_drift_stream(r, {"kind": "pending", "proposal_id": pid, "reason_code": "ATR_POLICY_DRIFT_PENDING_QUEUE_ORPHAN", "repair_reason_code": "ATR_POLICY_REPAIR_ORPHAN_QUEUE_REMOVED" if mode == "repair_redis" else "", "ts_ms": int(time.time() * 1000)})

    r.hset("atr_policy:metrics:orphan_queue_total", "pending", len(orphans))
    return {"drifted": drifted, "repaired": repaired}


def _check_decided(conn, r, mode: str) -> Dict[str, int]:
    sql_rows = _load_decided_not_applied(conn)
    sql_ids = {str(p["proposal_id"]) for p, _ in sql_rows}
    repaired = 0
    drifted = 0
    redis_ids = set(r.smembers("queue:atr_policy:decided") or [])

    for proposal, decision in sql_rows:
        pid = str(proposal["proposal_id"])
        raw_p = r.get(_proposal_key(pid))
        raw_d = r.get(_decision_key(pid))
        if (pid not in redis_ids) or (not raw_p) or (not raw_d):
            drifted += 1
            if mode == "repair_redis":
                r.set(_proposal_key(pid), json.dumps(proposal, ensure_ascii=False, sort_keys=True))
                r.set(_decision_key(pid), json.dumps(decision, ensure_ascii=False, sort_keys=True))
                r.sadd("queue:atr_policy:decided", pid)
                repaired += 1
                r.hincrby("atr_policy:metrics:repair_total", "decided:ATR_POLICY_REPAIR_DECIDED_QUEUE_FROM_SQL", 1)
            r.hincrby("atr_policy:metrics:drift_total", "decided:ATR_POLICY_DRIFT_DECIDED_QUEUE_MISSING", 1)
            _insert_recovery_event(conn, event_type="drift_check_decided", obj=proposal, status="repaired" if mode == "repair_redis" else "detected", reason_code="ATR_POLICY_REPAIR_DECIDED_QUEUE_FROM_SQL" if mode == "repair_redis" else "ATR_POLICY_DRIFT_DECIDED_QUEUE_MISSING", payload={"proposal_id": pid, "mode": mode})

    orphans = redis_ids - sql_ids
    for pid in orphans:
        drifted += 1
        if mode == "repair_redis":
            r.srem("queue:atr_policy:decided", pid)
            repaired += 1
            r.hincrby("atr_policy:metrics:repair_total", "decided:ATR_POLICY_REPAIR_ORPHAN_QUEUE_REMOVED", 1)
        r.hincrby("atr_policy:metrics:drift_total", "decided:ATR_POLICY_DRIFT_DECIDED_QUEUE_ORPHAN", 1)
        _publish_drift_stream(r, {"kind": "decided", "proposal_id": pid, "reason_code": "ATR_POLICY_DRIFT_DECIDED_QUEUE_ORPHAN", "repair_reason_code": "ATR_POLICY_REPAIR_ORPHAN_QUEUE_REMOVED" if mode == "repair_redis" else "", "ts_ms": int(time.time() * 1000)})

    r.hset("atr_policy:metrics:orphan_queue_total", "decided", len(orphans))
    return {"drifted": drifted, "repaired": repaired}


def run_once() -> Dict[str, Any]:
    mode = _mode()
    if mode not in {"off", "audit_only", "repair_redis"}:
        mode = "audit_only"
    if mode == "off":
        return {"mode": mode, "active": {}, "last_good": {}, "pending": {}, "decided": {}}

    r = _redis()
    conn = psycopg2.connect(_dsn(), connect_timeout=5, application_name="atr_policy_state_consistency_checker")
    try:
        active = _check_snapshot_kind(conn, r, kind="active", mode=mode)
        last_good = _check_snapshot_kind(conn, r, kind="last_good", mode=mode)
        pending = _check_pending(conn, r, mode=mode)
        decided = _check_decided(conn, r, mode=mode)
        conn.commit()
        r.set("atr_policy:drift_check:last_run_ts_ms", str(int(time.time() * 1000)))
        return {
            "mode": mode,
            "active": active,
            "last_good": last_good,
            "pending": pending,
            "decided": decided,
        }
    except Exception as exc:
        conn.rollback()
        r.hincrby("atr_policy:metrics:checker_error_total", "run_once", 1)
        raise
    finally:
        conn.close()


if __name__ == "__main__":
    print(json.dumps(run_once(), ensure_ascii=False, sort_keys=True))
