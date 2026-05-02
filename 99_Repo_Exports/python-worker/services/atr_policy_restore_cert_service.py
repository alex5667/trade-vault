from __future__ import annotations

import hashlib
import json
import os
import time
from typing import Any, Dict

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


def _cert_id(drill_code: str, target: Dict[str, Any]) -> str:
    base = f"{drill_code}|{target['source']}|{target['symbol']}|{target['scenario']}|{target['regime']}|{target['risk_horizon_bucket']}|{int(time.time()*1000)}"
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:20]


def _active_key(obj: Dict[str, Any]) -> str:
    return f"cfg:atr_policy:active:{obj['source']}:{obj['symbol']}:{obj['scenario']}:{obj['regime']}:{obj['risk_horizon_bucket']}"


def _last_good_key(obj: Dict[str, Any]) -> str:
    return f"cfg:atr_policy:last_good:{obj['source']}:{obj['symbol']}:{obj['scenario']}:{obj['regime']}:{obj['risk_horizon_bucket']}"


def _active_ref(key: str) -> str:
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]


def certify(*, drill_code: str, target: Dict[str, Any], run_id: str = "", mode: str = "audit_only") -> Dict[str, Any]:
    r = _redis()
    active_key = _active_key(target)
    last_good_key = _last_good_key(target)
    active_ref_key = f"cfg:atr_policy:active_ref:{_active_ref(active_key)}"

    checks = {
        "active_restored": bool(r.get(active_key)),
        "last_good_restored": bool(r.get(last_good_key)),
        "active_ref_restored": bool(r.get(active_ref_key)),
        "pending_queue_present": len(r.smembers("queue:atr_policy:pending") or []) >= 0,
        "decided_queue_present": len(r.smembers("queue:atr_policy:decided") or []) >= 0,
    }

    # lightweight resolver compatibility check
    try:
        from services.atr_policy_resolver import get_atr_policy_resolver
        res = get_atr_policy_resolver().resolve(
            source=target["source"],
            symbol=target["symbol"],
            scenario=target["scenario"],
            regime=target["regime"],
            risk_horizon_bucket=target["risk_horizon_bucket"],
        )
        checks["resolver_exact_or_fallback_hit"] = bool(res.get("hit", False))
    except Exception:
        checks["resolver_exact_or_fallback_hit"] = False

    status = "passed" if all(bool(v) for v in checks.values()) else "failed"
    cert_id = _cert_id(drill_code, target)
    summary = {
        "status": status,
        "drill_code": drill_code,
        "target": target,
        "failed_checks": [k for k, v in checks.items() if not v],
    }

    conn = psycopg2.connect(_dsn(), connect_timeout=5, application_name="atr_policy_restore_cert_service")
    try:
        with conn, conn.cursor() as cur:
            cur.execute(
                """
                INSERT INTO atr_policy_restore_certifications (
                  cert_id, run_id, mode, drill_code,
                  source, symbol, scenario, regime, risk_horizon_bucket,
                  status, checks_json, summary_json
                ) VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb,%s::jsonb)
                """
                (
                    cert_id, run_id, mode, drill_code,
                    target["source"], target["symbol"], target["scenario"], target["regime"], target["risk_horizon_bucket"],
                    status,
                    json.dumps(checks, ensure_ascii=False, sort_keys=True),
                    json.dumps(summary, ensure_ascii=False, sort_keys=True),
                )
            )
    finally:
        conn.close()

    return {"cert_id": cert_id, "status": status, "checks": checks, "summary": summary}
