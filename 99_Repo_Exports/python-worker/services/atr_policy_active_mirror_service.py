"""ATR Policy Active Mirror Service — Phase 3.8 (Disaster Layer).

Maintains a last-good snapshot of cfg:atr_policy:active:* ONLY after verifier
confirms the active key is schema-valid and kill_switch-free.

Redis keys managed:
  cfg:atr_policy:last_good:<src>:<sym>:<scenario>:<regime>:<bucket>
  cfg:atr_policy:last_good_meta:<src>:<sym>:<scenario>:<regime>:<bucket>
  cfg:atr_policy:kill_switch:<src>:<sym>:<scenario>:<regime>:<bucket>

Redis streams written:
  stream:atr_policy:mirror_results      — every mirror attempt
  stream:atr_policy:escalations         — on mirror block / kill_switch trigger

ENV:
  ATR_POLICY_MIRROR_ENABLE              default 1
  ATR_POLICY_MIRROR_ADVISORY_ONLY       default 0  (1 = never mutate last_good)
  REDIS_URL
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, Optional
from services.atr_policy_state_store import transition_snapshot, get_conn

import redis
from prometheus_client import Counter

logger = logging.getLogger(__name__)

STREAM_MIRROR = "stream:atr_policy:mirror_results"
STREAM_ESC = "stream:atr_policy:escalations"

# ── Prometheus ────────────────────────────────────────────────────────────────

c_mirror_total = Counter(
    "atr_policy_last_good_mirror_total",
    "Last-good mirror write attempts",
    ["status"],
)
c_mirror_skip = Counter(
    "atr_policy_last_good_mirror_skip_total",
    "Last-good mirror skips",
    ["reason_code"],
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _redis() -> redis.Redis:
    return redis.Redis.from_url(
        os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"),
        decode_responses=True,
    )


def _enable() -> bool:
    return os.getenv("ATR_POLICY_MIRROR_ENABLE", "1") == "1"


def _advisory_only() -> bool:
    return os.getenv("ATR_POLICY_MIRROR_ADVISORY_ONLY", "0") == "1"


def _last_good_key(p: Dict[str, Any]) -> str:
    return (
        f"cfg:atr_policy:last_good:"
        f"{p['source']}:{p['symbol']}:{p['scenario']}:{p['regime']}:{p['risk_horizon_bucket']}"
    )


def _last_good_meta_key(p: Dict[str, Any]) -> str:
    return (
        f"cfg:atr_policy:last_good_meta:"
        f"{p['source']}:{p['symbol']}:{p['scenario']}:{p['regime']}:{p['risk_horizon_bucket']}"
    )


def _kill_switch_key(p: Dict[str, Any]) -> str:
    return (
        f"cfg:atr_policy:kill_switch:"
        f"{p['source']}:{p['symbol']}:{p['scenario']}:{p['regime']}:{p['risk_horizon_bucket']}"
    )


def _publish_stream(r: redis.Redis, stream: str, payload: Dict[str, Any]) -> None:
    try:
        r.xadd(stream, {k: str(v) for k, v in payload.items()}, maxlen=2000)
    except Exception as exc:
        logger.warning("mirror_service: stream publish failed %s: %s", stream, exc)


# ── Core API ──────────────────────────────────────────────────────────────────

def mirror_after_verified_apply(
    policy: Dict[str, Any],
    verify_result: Dict[str, Any],
    r: Optional[redis.Redis] = None,
) -> bool:
    """
    Update last_good mirror ONLY after verifier confirms the active key is good.

    Returns True if mirror was written (or would be in ADVISORY_ONLY mode).
    """
    if not _enable():
        return False

    if not verify_result.get("verified_ok", False):
        reason = str(verify_result.get("reason_code", "VERIFY_FAILED"))
        c_mirror_skip.labels(reason_code=reason).inc()
        logger.debug("mirror_service: skip — verify not ok: %s", reason)
        return False

    r = r or _redis()

    # Kill-switch check: never update last_good while kill_switch is active
    ks_key = _kill_switch_key(policy)
    ks_raw = r.get(ks_key)
    if ks_raw:
        try:
            ks = json.loads(ks_raw)
            if ks.get("enabled"):
                c_mirror_skip.labels(reason_code="KILL_SWITCH_ACTIVE").inc()
                _publish_stream(r, STREAM_ESC, {
                    "event": "MIRROR_BLOCKED_KILL_SWITCH",
                    "source": policy.get("source", ""),
                    "symbol": policy.get("symbol", ""),
                    "scenario": policy.get("scenario", ""),
                    "regime": policy.get("regime", ""),
                    "bucket": policy.get("risk_horizon_bucket", ""),
                    "ts_ms": int(time.time() * 1000),
                })
                return False
        except Exception:
            pass

    lg_key = _last_good_key(policy)
    meta_key = _last_good_meta_key(policy)
    now_ms = int(time.time() * 1000)
    policy_ver = int(policy.get("policy_ver", 0))

    meta = {
        "mirrored_at_ms": now_ms,
        "reason_code": "LAST_GOOD_AFTER_VERIFIED_APPLY",
        "policy_ver": policy_ver,
        "stop_ttl_mode": str(policy.get("stop_ttl_mode", "")),
        "trailing_mode": str(policy.get("trailing_mode", "")),
        "applied_from_proposal_id": str(policy.get("proposal_id") or ""),
        "advisory_only": _advisory_only(),
    }

    if _advisory_only():
        c_mirror_total.labels(status="advisory").inc()
        _publish_stream(r, STREAM_MIRROR, {
            "event": "MIRROR_ADVISORY",
            "lg_key": lg_key,
            **meta,
        })
        logger.info("mirror_service: ADVISORY_ONLY — would write %s", lg_key)
        return True

    try:
        policy_json = json.dumps(policy, ensure_ascii=False, sort_keys=True)
        meta_json = json.dumps(meta, ensure_ascii=False, sort_keys=True)
        r.set(lg_key, policy_json)
        r.set(meta_key, meta_json)
        
        try:
            with get_conn() as conn:
                transition_snapshot(
                    conn,
                    snapshot_kind="last_good",
                    policy=policy,
                    applied_from_proposal_id=str(policy.get("proposal_id") or ""),
                    effective_from_ms=now_ms,
                )
                conn.commit()
        except Exception as exc:
            logger.error("mirror_service: pg sync failed: %s", exc)

        c_mirror_total.labels(status="ok").inc()
        _publish_stream(r, STREAM_MIRROR, {
            "event": "MIRROR_WRITTEN",
            "lg_key": lg_key,
            **meta,
        })
        logger.info(
            "mirror_service: last_good written — key=%s ver=%d stop=%s trail=%s",
            lg_key, policy_ver,
            policy.get("stop_ttl_mode"), policy.get("trailing_mode"),
        )
        return True
    except Exception as exc:
        c_mirror_total.labels(status="error").inc()
        logger.error("mirror_service: write failed %s: %s", lg_key, exc)
        return False


def read_last_good(
    *,
    source: str,
    symbol: str,
    scenario: str,
    regime: str,
    risk_horizon_bucket: str,
    r: Optional[redis.Redis] = None,
) -> Optional[Dict[str, Any]]:
    """Return last_good policy dict or None if not present / corrupted."""
    r = r or _redis()
    ref = {
        "source": source, "symbol": symbol, "scenario": scenario,
        "regime": regime, "risk_horizon_bucket": risk_horizon_bucket,
    }
    raw = r.get(_last_good_key(ref))
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        logger.warning("mirror_service: last_good corrupted for %s", ref)
        return None


def read_last_good_meta(
    *,
    source: str,
    symbol: str,
    scenario: str,
    regime: str,
    risk_horizon_bucket: str,
    r: Optional[redis.Redis] = None,
) -> Optional[Dict[str, Any]]:
    """Return last_good_meta dict or None."""
    r = r or _redis()
    ref = {
        "source": source, "symbol": symbol, "scenario": scenario,
        "regime": regime, "risk_horizon_bucket": risk_horizon_bucket,
    }
    raw = r.get(_last_good_meta_key(ref))
    if not raw:
        return None
    try:
        return json.loads(raw)
    except Exception:
        return None
