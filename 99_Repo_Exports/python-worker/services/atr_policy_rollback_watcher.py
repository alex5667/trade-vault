from __future__ import annotations

"""ATR Policy Rollback Watcher — Phase 3.8 (Disaster Layer).

When verifier signals a bad active policy this watcher performs bounded rollback:
  1. Try to restore active key from last_good mirror.
  2. If last_good is absent → arm kill_switch + critical escalation.
  3. Write audit to stream:atr_policy:rollback_results and escalations.

Uses ADVISORY_ONLY mode for safe staging rollout.

ENV:
  ATR_POLICY_ROLLBACK_ENABLE            default 1
  ATR_POLICY_ROLLBACK_ADVISORY_ONLY     default 0
  REDIS_URL
"""

import json
import logging
import os
import time
from typing import Any

import redis
from prometheus_client import Counter

logger = logging.getLogger(__name__)

STREAM_ROLLBACK = "stream:atr_policy:rollback_results"
STREAM_ESC = "stream:atr_policy:escalations"

# ── Prometheus ────────────────────────────────────────────────────────────────

c_rollback_total = Counter(
    "atr_policy_rollback_total",
    "ATR policy rollback attempts",
    ["reason_code"],
)
c_kill_switch_total = Counter(
    "atr_policy_kill_switch_total",
    "ATR policy kill_switch activations",
    ["reason_code"],
)
c_partial_loss_recovery = Counter(
    "atr_policy_partial_loss_recovery_total",
    "ATR policy partial loss recovery from last_good",
    ["status"],
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _redis() -> redis.Redis:
    if get_atr_redis is not None:
        return get_atr_redis()
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)


def _enable() -> bool:
    return os.getenv("ATR_POLICY_ROLLBACK_ENABLE", "1") == "1"


def _advisory_only() -> bool:
    return os.getenv("ATR_POLICY_ROLLBACK_ADVISORY_ONLY", "0") == "1"


def _active_key(p: dict[str, Any]) -> str:
    return (
        f"cfg:atr_policy:active:"
        f"{p['source']}:{p['symbol']}:{p['scenario']}:{p['regime']}:{p['risk_horizon_bucket']}"
    )


def _last_good_key(p: dict[str, Any]) -> str:
    return (
        f"cfg:atr_policy:last_good:"
        f"{p['source']}:{p['symbol']}:{p['scenario']}:{p['regime']}:{p['risk_horizon_bucket']}"
    )


def _kill_switch_key(p: dict[str, Any]) -> str:
    return (
        f"cfg:atr_policy:kill_switch:"
        f"{p['source']}:{p['symbol']}:{p['scenario']}:{p['regime']}:{p['risk_horizon_bucket']}"
    )


def _publish(r: redis.Redis, stream: str, payload: dict[str, Any]) -> None:
    try:
        r.xadd(stream, {k: str(v) for k, v in payload.items()}, maxlen=2000)
    except Exception as exc:
        logger.warning("rollback_watcher: stream publish failed %s: %s", stream, exc)


# ── Core ──────────────────────────────────────────────────────────────────────

def rollback_to_last_good(
    policy_ref: dict[str, Any],
    r: redis.Redis | None = None,
    *,
    trigger_reason: str = "VERIFIER_FAIL",
) -> dict[str, Any]:
    """
    Restore active policy from last_good mirror.

    policy_ref must have: source, symbol, scenario, regime, risk_horizon_bucket

    Returns:
      rollback_ok: bool
      reason_code: str,
      advisory_only: bool
    """
    if not _enable():
        return {"rollback_ok": False, "reason_code": "ROLLBACK_DISABLED", "advisory_only": False}

    required = ("source", "symbol", "scenario", "regime", "risk_horizon_bucket")
    if not all(policy_ref.get(k) for k in required):
        return {"rollback_ok": False, "reason_code": "ROLLBACK_INCOMPLETE_REF", "advisory_only": False}

    r = r or _redis()
    now_ms = int(time.time() * 1000)
    advisory = _advisory_only()

    lg_key = _last_good_key(policy_ref)
    active_key = _active_key(policy_ref)
    ks_key = _kill_switch_key(policy_ref)

    raw_lg = r.get(lg_key)

    if raw_lg:
        # ── Path 1: last_good available → restore ─────────────────────────
        try:
            lg_obj = json.loads(raw_lg)
        except Exception:
            lg_obj = None

        if lg_obj is None:
            reason = "LAST_GOOD_CORRUPTED_KILL_SWITCHED"
            _arm_kill_switch(r, ks_key, policy_ref, reason, now_ms)
            c_partial_loss_recovery.labels(status="last_good_corrupted").inc()
            result = {
                "rollback_ok": False,
                "reason_code": reason,
                "advisory_only": advisory,
                "ts_ms": now_ms,
            }
            _publish(r, STREAM_ROLLBACK, result)
            _publish(r, STREAM_ESC, {"event": reason, **policy_ref, "ts_ms": now_ms})
            logger.error("rollback_watcher: last_good corrupted — kill_switch armed for %s", policy_ref)
            c_rollback_total.labels(reason_code=reason).inc()
            return result

        reason = "ROLLBACK_TO_LAST_GOOD"
        if advisory:
            result = {
                "rollback_ok": True,
                "reason_code": reason + "_ADVISORY",
                "advisory_only": True,
                "ts_ms": now_ms,
                "trigger_reason": trigger_reason,
            }
            c_rollback_total.labels(reason_code=reason + "_ADVISORY").inc()
            _publish(r, STREAM_ROLLBACK, result)
            logger.info(
                "rollback_watcher: ADVISORY — would restore %s from last_good", active_key
            )
            return result

        # Actually restore
        r.set(active_key, raw_lg)
        c_rollback_total.labels(reason_code=reason).inc()
        c_partial_loss_recovery.labels(status="restored_from_last_good").inc()
        result = {
            "rollback_ok": True,
            "reason_code": reason,
            "advisory_only": False,
            "ts_ms": now_ms,
            "trigger_reason": trigger_reason,
            "restored_key": active_key,
        }
        _publish(r, STREAM_ROLLBACK, result)
        logger.info(
            "rollback_watcher: restored active from last_good — key=%s trigger=%s",
            active_key, trigger_reason,
        )
        return result

    # ── Path 2: no last_good → kill_switch + critical escalation ──────────
    reason = "NO_LAST_GOOD_KILL_SWITCHED"

    if advisory:
        result = {
            "rollback_ok": False,
            "reason_code": reason + "_ADVISORY",
            "advisory_only": True,
            "ts_ms": now_ms,
            "trigger_reason": trigger_reason,
        }
        c_rollback_total.labels(reason_code=reason + "_ADVISORY").inc()
        _publish(r, STREAM_ROLLBACK, result)
        _publish(r, STREAM_ESC, {
            "event": "NO_LAST_GOOD_ADVISORY",
            **policy_ref,
            "ts_ms": now_ms,
            "trigger_reason": trigger_reason,
        })
        logger.warning("rollback_watcher: ADVISORY — no last_good, would kill_switch %s", ks_key)
        return result

    _arm_kill_switch(r, ks_key, policy_ref, reason, now_ms)
    c_rollback_total.labels(reason_code=reason).inc()
    c_kill_switch_total.labels(reason_code=reason).inc()
    c_partial_loss_recovery.labels(status="no_last_good_kill_switched").inc()
    result = {
        "rollback_ok": False,
        "reason_code": reason,
        "advisory_only": False,
        "ts_ms": now_ms,
        "trigger_reason": trigger_reason,
    }
    _publish(r, STREAM_ROLLBACK, result)
    _publish(r, STREAM_ESC, {
        "event": "CRITICAL_NO_LAST_GOOD_KILL_SWITCHED",
        **{k: policy_ref.get(k, "") for k in ("source", "symbol", "scenario", "regime", "risk_horizon_bucket")},
        "ts_ms": now_ms,
        "trigger_reason": trigger_reason,
    })
    logger.error(
        "rollback_watcher: CRITICAL — no last_good, kill_switch armed for %s trigger=%s",
        ks_key, trigger_reason,
    )
    return result


def _arm_kill_switch(
    r: redis.Redis,
    ks_key: str,
    policy_ref: dict[str, Any],
    reason_code: str,
    now_ms: int,
) -> None:
    payload = {
        "enabled": True,
        "ts_ms": now_ms,
        "reason_code": reason_code,
        "cohort": {k: policy_ref.get(k, "") for k in (
            "source", "symbol", "scenario", "regime", "risk_horizon_bucket"
        )},
    }
    try:
        r.set(ks_key, json.dumps(payload, ensure_ascii=False, sort_keys=True))
    except Exception as exc:
        logger.error("rollback_watcher: failed to arm kill_switch %s: %s", ks_key, exc)


def clear_kill_switch(
    policy_ref: dict[str, Any],
    *,
    actor: str = "manual",
    r: redis.Redis | None = None,
) -> bool:
    """
    Manual administrative clear of a kill_switch for a cohort.
    Requires actor. Publishes escalation event.
    """
    r = r or _redis()
    ks_key = _kill_switch_key(policy_ref)
    now_ms = int(time.time() * 1000)
    try:
        r.delete(ks_key)
        _publish(r, STREAM_ESC, {
            "event": "KILL_SWITCH_CLEARED",
            **{k: policy_ref.get(k, "") for k in (
                "source", "symbol", "scenario", "regime", "risk_horizon_bucket"
            )},
            "actor": actor,
            "ts_ms": now_ms,
        })
        logger.info("rollback_watcher: kill_switch cleared for %s by %s", ks_key, actor)
        return True
    except Exception as exc:
        logger.error("rollback_watcher: kill_switch clear failed %s: %s", ks_key, exc)
        return False
