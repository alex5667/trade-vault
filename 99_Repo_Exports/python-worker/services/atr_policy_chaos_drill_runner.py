from __future__ import annotations
"""ATR Policy Chaos Drill Runner — Phase 3.8 (Disaster Layer).

DRY_RUN-first chaos simulator for the ATR policy control-plane.
Simulates disaster scenarios WITHOUT touching the live trading surface,
bounded to exactly ONE cohort specified by ENV.

Supported scenarios:
  TELEGRAM_CALLBACK_BLACKHOLE     — simulate callback silence (sets last_callback very old)
  RECONCILE_STUCK                 — set reconcile_last_success_ts very old
  ACTIVE_KEY_CORRUPT              — write broken JSON to one active key
  ACTIVE_KEY_DELETE               — delete one active key
  FLIP_STORM_SIM                  — inject 3+ APPROVE/REVOKE audit rows for cohort
  REDIS_PARTIAL_LOSS_SIM          — delete both active + last_good for cohort

ENV:
  ATR_POLICY_CHAOS_ENABLE         default 0     (must be explicitly set to 1)
  ATR_POLICY_CHAOS_MODE           DRY_RUN | EXECUTE
  ATR_POLICY_CHAOS_SCENARIO       scenario name (see above)
  ATR_POLICY_CHAOS_TARGET_JSON    '{"source":"...","symbol":"...","scenario":"...","regime":"...","risk_horizon_bucket":"..."}'
  REDIS_URL
  ANALYTICS_DB_DSN / TRADES_DB_DSN
"""

import json
import logging
import os
import time
from typing import Any, Dict, Optional

import redis
try:
    from core.redis_client import get_atr_redis
except Exception:
    get_atr_redis = None
from prometheus_client import Counter

logger = logging.getLogger(__name__)

STREAM_ESC = "stream:atr_policy:escalations"

VALID_SCENARIOS = {
    "TELEGRAM_CALLBACK_BLACKHOLE",
    "RECONCILE_STUCK",
    "ACTIVE_KEY_CORRUPT",
    "ACTIVE_KEY_DELETE",
    "FLIP_STORM_SIM",
    "REDIS_PARTIAL_LOSS_SIM",
}

# ── Prometheus ────────────────────────────────────────────────────────────────

c_drill_total = Counter(
    "atr_policy_chaos_drill_total",
    "ATR policy chaos drill executions",
    ["scenario", "mode", "status"],
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _rconn() -> redis.Redis:
    if get_atr_redis is not None:
        return get_atr_redis()
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)


def _dsn() -> str:
    return (
        os.getenv("ANALYTICS_DB_DSN")
        or os.getenv("TRADES_DB_DSN")
        or "postgresql://postgres:12345@postgres:5432/scanner_analytics"
    )


def _publish(r: redis.Redis, payload: Dict[str, Any]) -> None:
    try:
        r.xadd(STREAM_ESC, {k: str(v) for k, v in payload.items()}, maxlen=2000)
    except Exception as exc:
        logger.warning("chaos_drill: stream publish failed: %s", exc)


def _active_key(t: Dict[str, Any]) -> str:
    return (
        f"cfg:atr_policy:active:"
        f"{t['source']}:{t['symbol']}:{t['scenario']}:{t['regime']}:{t['risk_horizon_bucket']}"
    )


def _last_good_key(t: Dict[str, Any]) -> str:
    return (
        f"cfg:atr_policy:last_good:"
        f"{t['source']}:{t['symbol']}:{t['scenario']}:{t['regime']}:{t['risk_horizon_bucket']}"
    )


def _target_complete(target: Dict[str, Any]) -> bool:
    return all(
        target.get(k)
        for k in ("source", "symbol", "scenario", "regime", "risk_horizon_bucket")
    )


# ── Scenario handlers ──────────────────────────────────────────────────────────

def _drill_telegram_callback_blackhole(
    target: Dict[str, Any], mode: str, r: redis.Redis
) -> Dict[str, Any]:
    """Simulate callback stream death by setting last_callback_ts very far back."""
    old_ts = int((time.time() - 86_400 * 2) * 1000)  # 2 days ago
    if mode == "DRY_RUN":
        return {
            "ok": True,
            "scenario": "TELEGRAM_CALLBACK_BLACKHOLE",
            "mode": "DRY_RUN",
            "reason": "would_set_last_callback_to_old",
            "would_set_ts_ms": old_ts,
        }
    r.set("atr_policy:telegram:last_callback_ts_ms", old_ts)
    logger.warning("chaos_drill: EXECUTE TELEGRAM_CALLBACK_BLACKHOLE — last_callback set to %d", old_ts)
    return {
        "ok": True,
        "scenario": "TELEGRAM_CALLBACK_BLACKHOLE",
        "mode": "EXECUTE",
        "reason": "last_callback_ts_set_old",
        "set_ts_ms": old_ts,
    }


def _drill_reconcile_stuck(
    target: Dict[str, Any], mode: str, r: redis.Redis
) -> Dict[str, Any]:
    """Simulate reconcile stuck by setting last_success_ts very old."""
    old_ts = int((time.time() - 86_400) * 1000)  # 1 day ago
    if mode == "DRY_RUN":
        return {
            "ok": True,
            "scenario": "RECONCILE_STUCK",
            "mode": "DRY_RUN",
            "reason": "would_set_reconcile_last_success_to_old",
            "would_set_ts_ms": old_ts,
        }
    r.set("atr_policy:reconcile:last_success_ts_ms", old_ts)
    logger.warning("chaos_drill: EXECUTE RECONCILE_STUCK — reconcile ts set to %d", old_ts)
    return {
        "ok": True,
        "scenario": "RECONCILE_STUCK",
        "mode": "EXECUTE",
        "reason": "reconcile_ts_set_old",
        "set_ts_ms": old_ts,
    }


def _drill_active_key_corrupt(
    target: Dict[str, Any], mode: str, r: redis.Redis
) -> Dict[str, Any]:
    """Corrupt exactly one active key with invalid JSON (one cohort only)."""
    akey = _active_key(target)
    if mode == "DRY_RUN":
        return {
            "ok": True,
            "scenario": "ACTIVE_KEY_CORRUPT",
            "mode": "DRY_RUN",
            "reason": "would_corrupt_active_key",
            "target_key": akey,
        }
    r.set(akey, '{"broken_json":')  # intentionally invalid
    logger.warning("chaos_drill: EXECUTE ACTIVE_KEY_CORRUPT — key=%s", akey)
    return {
        "ok": True,
        "scenario": "ACTIVE_KEY_CORRUPT",
        "mode": "EXECUTE",
        "reason": "active_key_corrupted",
        "target_key": akey,
    }


def _drill_active_key_delete(
    target: Dict[str, Any], mode: str, r: redis.Redis
) -> Dict[str, Any]:
    """Delete exactly one active key (simulate partial loss)."""
    akey = _active_key(target)
    if mode == "DRY_RUN":
        exists = bool(r.exists(akey))
        return {
            "ok": True,
            "scenario": "ACTIVE_KEY_DELETE",
            "mode": "DRY_RUN",
            "reason": "would_delete_active_key",
            "target_key": akey,
            "exists": exists,
        }
    r.delete(akey)
    logger.warning("chaos_drill: EXECUTE ACTIVE_KEY_DELETE — key=%s", akey)
    return {
        "ok": True,
        "scenario": "ACTIVE_KEY_DELETE",
        "mode": "EXECUTE",
        "reason": "active_key_deleted",
        "target_key": akey,
    }


def _drill_flip_storm_sim(
    target: Dict[str, Any], mode: str, r: redis.Redis
) -> Dict[str, Any]:
    """Inject 4 APPROVE/REVOKE rows into audit table for the cohort (SQL INSERT)."""
    if mode == "DRY_RUN":
        return {
            "ok": True,
            "scenario": "FLIP_STORM_SIM",
            "mode": "DRY_RUN",
            "reason": "would_insert_4_flip_audit_rows",
            "target": target,
        }

    import psycopg2

    now_ms = int(time.time() * 1000)
    rows: list = []
    for i, action in enumerate(["APPROVE", "REVOKE", "APPROVE", "REVOKE"]):
        ts_offset = i * 3600  # spread over last 4h
        rows.append({
            "source": target["source"],
            "symbol": target["symbol"],
            "scenario": target["scenario"],
            "regime": target["regime"],
            "risk_horizon_bucket": target["risk_horizon_bucket"],
            "stop_ttl_mode": "canary",
            "trailing_mode": "canary",
            "reason_code": f"CHAOS_DRILL_{action}",
            "decision_json": json.dumps({"action": action, "ts_ms": now_ms - ts_offset * 1000}),
        })

    try:
        conn = psycopg2.connect(_dsn(), connect_timeout=5, application_name="atr_policy_chaos_drill")
        with conn.cursor() as cur:
            for row in rows:
                cur.execute(
                    """
                    INSERT INTO atr_promotion_policy_audit
                      (source, symbol, scenario, regime, risk_horizon_bucket,
                       stop_ttl_mode, trailing_mode, reason_code,
                       approved, applied, decision_json)
                    VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s::jsonb)
                    """
                    (
                        row["source"], row["symbol"], row["scenario"],
                        row["regime"], row["risk_horizon_bucket"],
                        row["stop_ttl_mode"], row["trailing_mode"],
                        row["reason_code"], False, False, row["decision_json"],
                    ),
                )
        conn.commit()
        conn.close()
        logger.warning("chaos_drill: EXECUTE FLIP_STORM_SIM — 4 rows inserted for %s", target)
        return {
            "ok": True,
            "scenario": "FLIP_STORM_SIM",
            "mode": "EXECUTE",
            "reason": "flip_storm_audit_rows_inserted",
            "rows_inserted": len(rows),
        }
    except Exception as exc:
        logger.error("chaos_drill: FLIP_STORM_SIM DB insert failed: %s", exc)
        return {
            "ok": False,
            "scenario": "FLIP_STORM_SIM",
            "mode": "EXECUTE",
            "reason": "db_error",
            "error": str(exc),
        }


def _drill_redis_partial_loss_sim(
    target: Dict[str, Any], mode: str, r: redis.Redis
) -> Dict[str, Any]:
    """Delete both active key AND last_good for one cohort (worst case)."""
    akey = _active_key(target)
    lgkey = _last_good_key(target)
    if mode == "DRY_RUN":
        return {
            "ok": True,
            "scenario": "REDIS_PARTIAL_LOSS_SIM",
            "mode": "DRY_RUN",
            "reason": "would_delete_active_and_last_good",
            "active_key": akey,
            "last_good_key": lgkey,
        }
    r.delete(akey, lgkey)
    logger.warning(
        "chaos_drill: EXECUTE REDIS_PARTIAL_LOSS_SIM — deleted %s AND %s", akey, lgkey
    )
    return {
        "ok": True,
        "scenario": "REDIS_PARTIAL_LOSS_SIM",
        "mode": "EXECUTE",
        "reason": "active_and_last_good_deleted",
        "active_key": akey,
        "last_good_key": lgkey,
    }


# ── Entry point ───────────────────────────────────────────────────────────────

_HANDLERS = {
    "TELEGRAM_CALLBACK_BLACKHOLE": _drill_telegram_callback_blackhole,
    "RECONCILE_STUCK": _drill_reconcile_stuck,
    "ACTIVE_KEY_CORRUPT": _drill_active_key_corrupt,
    "ACTIVE_KEY_DELETE": _drill_active_key_delete,
    "FLIP_STORM_SIM": _drill_flip_storm_sim,
    "REDIS_PARTIAL_LOSS_SIM": _drill_redis_partial_loss_sim,
}


def run_once(r: Optional[redis.Redis] = None) -> Dict[str, Any]:
    """
    Read ENV and execute / dry-run the specified chaos scenario.
    Always bounded to ONE cohort; never touches hot path.
    """
    if os.getenv("ATR_POLICY_CHAOS_ENABLE", "0") != "1":
        return {"ok": False, "reason": "CHAOS_DISABLED"}

    mode = str(os.getenv("ATR_POLICY_CHAOS_MODE", "DRY_RUN") or "DRY_RUN").upper()
    if mode not in ("DRY_RUN", "EXECUTE"):
        return {"ok": False, "reason": f"INVALID_MODE_{mode}"}

    scenario = str(os.getenv("ATR_POLICY_CHAOS_SCENARIO", "") or "").upper()
    if scenario not in VALID_SCENARIOS:
        return {"ok": False, "reason": f"UNKNOWN_SCENARIO_{scenario}"}

    try:
        target = json.loads(os.getenv("ATR_POLICY_CHAOS_TARGET_JSON", "{}") or "{}")
    except Exception:
        return {"ok": False, "reason": "TARGET_JSON_PARSE_ERROR"}

    if not _target_complete(target):
        return {"ok": False, "reason": "TARGET_INCOMPLETE", "target": target}

    r = r or _rconn()
    now_ms = int(time.time() * 1000)

    logger.info(
        "chaos_drill: scenario=%s mode=%s target=%s", scenario, mode, target
    )

    handler = _HANDLERS[scenario]
    try:
        result = handler(target, mode, r)
    except Exception as exc:
        logger.exception("chaos_drill: handler %s raised: %s", scenario, exc)
        result = {"ok": False, "reason": "HANDLER_EXCEPTION", "error": str(exc)}

    status = "ok" if result.get("ok") else "error"
    c_drill_total.labels(scenario=scenario, mode=mode, status=status).inc()

    _publish(r, {
        "event": f"CHAOS_DRILL_{scenario}_{mode}",
        "status": status,
        "ts_ms": now_ms,
        **{k: str(v) for k, v in target.items()},
    })

    return result


if __name__ == "__main__":
    import sys
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
    )
    result = run_once()
    json.dump(result, sys.stdout, indent=2, ensure_ascii=False)
    print()
