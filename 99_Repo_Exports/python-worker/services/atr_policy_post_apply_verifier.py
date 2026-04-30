"""ATR Policy Post-Apply Verifier — Phase 3.8 (Disaster Layer).

After every reconcile/apply step this verifier checks that:
  1. active key exists in Redis
  2. JSON is parseable
  3. Required fields are present
  4. stop_ttl_mode / trailing_mode are in allowed set
  5. kill_switch is not armed for the cohort

Results are published to stream:atr_policy:verify_results.
On failure the rollback watcher is expected to pick up the event.

ENV:
  ATR_POLICY_VERIFY_ENABLE              default 1
  REDIS_URL
"""
from __future__ import annotations

import json
import logging
import os
import time
from typing import Any, Dict, List, Optional

import redis
from prometheus_client import Counter

logger = logging.getLogger(__name__)

STREAM_VERIFY = "stream:atr_policy:verify_results"
STREAM_ESC = "stream:atr_policy:escalations"

VALID_MODES = {"shadow", "canary", "live"}
REQUIRED_FIELDS = [
    "source"
    "symbol"
    "scenario"
    "regime"
    "risk_horizon_bucket"
    "stop_ttl_mode"
    "trailing_mode"
]

# ── Prometheus ────────────────────────────────────────────────────────────────

c_verify_total = Counter(
    "atr_policy_verify_total"
    "ATR policy verify attempts"
    ["reason_code"]
)
c_verify_fail = Counter(
    "atr_policy_verify_fail_total"
    "ATR policy verify failures"
    ["reason_code"]
)
c_corruption = Counter(
    "atr_policy_active_corruption_total"
    "ATR active policy corruptions detected"
    ["reason_code"]
)


# ── Helpers ───────────────────────────────────────────────────────────────────

def _redis() -> redis.Redis:
    return redis.Redis.from_url(
        os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        decode_responses=True
    )


def _enable() -> bool:
    return os.getenv("ATR_POLICY_VERIFY_ENABLE", "1") == "1"


def _kill_switch_key(obj: Dict[str, Any]) -> str:
    return (
        f"cfg:atr_policy:kill_switch:"
        f"{obj['source']}:{obj['symbol']}:{obj['scenario']}:{obj['regime']}:{obj['risk_horizon_bucket']}"
    )


def _publish(r: redis.Redis, stream: str, payload: Dict[str, Any]) -> None:
    try:
        r.xadd(stream, {k: str(v) for k, v in payload.items()}, maxlen=2000)
    except Exception as exc:
        logger.warning("verifier: stream publish failed %s: %s", stream, exc)


# ── Core ──────────────────────────────────────────────────────────────────────

def verify_active_policy(
    policy_key: str
    r: Optional[redis.Redis] = None
    *
    publish: bool = True
) -> Dict[str, Any]:
    """
    Verify that the active policy key is schema-valid and kill_switch-free.

    Returns a dict with:
      verified_ok: bool
      reason_code: str
      missing: list[str]  (only when ACTIVE_KEY_FIELDS_MISSING)
      policy: dict        (only when verified_ok=True)
    """
    if not _enable():
        return {"verified_ok": True, "reason_code": "VERIFY_DISABLED"}

    r = r or _redis()
    now_ms = int(time.time() * 1000)

    def _fail(reason_code: str, extra: Optional[Dict] = None) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "verified_ok": False
            "reason_code": reason_code
            "policy_key": policy_key
            "ts_ms": now_ms
        }
        if extra:
            result.update(extra)
        c_verify_fail.labels(reason_code=reason_code).inc()
        c_verify_total.labels(reason_code=reason_code).inc()
        if reason_code in (
            "ACTIVE_KEY_JSON_CORRUPTED"
            "ACTIVE_KEY_FIELDS_MISSING"
            "ACTIVE_KEY_STOP_MODE_INVALID"
            "ACTIVE_KEY_TRAIL_MODE_INVALID"
        ):
            c_corruption.labels(reason_code=reason_code).inc()
        if publish:
            _publish(r, STREAM_VERIFY, result)
        logger.warning("verifier: FAIL %s — key=%s", reason_code, policy_key)
        return result

    # ── 1. Key existence ──────────────────────────────────────────────────
    raw = r.get(policy_key)
    if not raw:
        return _fail("ACTIVE_KEY_MISSING")

    # ── 2. JSON validity ──────────────────────────────────────────────────
    try:
        obj = json.loads(raw)
    except Exception:
        return _fail("ACTIVE_KEY_JSON_CORRUPTED")

    if not isinstance(obj, dict):
        return _fail("ACTIVE_KEY_JSON_CORRUPTED", {"detail": "not_dict"})

    # ── 3. Required fields ────────────────────────────────────────────────
    missing: List[str] = [k for k in REQUIRED_FIELDS if not obj.get(k)]
    if missing:
        return _fail("ACTIVE_KEY_FIELDS_MISSING", {"missing": missing})

    # ── 4. Mode validity ──────────────────────────────────────────────────
    if obj["stop_ttl_mode"] not in VALID_MODES:
        return _fail(
            "ACTIVE_KEY_STOP_MODE_INVALID"
            {"stop_ttl_mode": obj["stop_ttl_mode"], "valid": list(VALID_MODES)}
        )
    if obj["trailing_mode"] not in VALID_MODES:
        return _fail(
            "ACTIVE_KEY_TRAIL_MODE_INVALID"
            {"trailing_mode": obj["trailing_mode"], "valid": list(VALID_MODES)}
        )

    # ── 5. Kill-switch ────────────────────────────────────────────────────
    ks_raw = r.get(_kill_switch_key(obj))
    if ks_raw:
        try:
            ks = json.loads(ks_raw)
            if ks.get("enabled"):
                return _fail("COHORT_KILL_SWITCHED", {"kill_switch_meta": ks})
        except Exception:
            pass

    # ── OK ─────────────────────────────────────────────────────────────────
    result: Dict[str, Any] = {
        "verified_ok": True
        "reason_code": "ACTIVE_POLICY_VERIFIED"
        "policy_key": policy_key
        "ts_ms": now_ms
        "stop_ttl_mode": obj["stop_ttl_mode"]
        "trailing_mode": obj["trailing_mode"]
        "policy_ver": int(obj.get("policy_ver", 0))
        "policy": obj
    }
    c_verify_total.labels(reason_code="ACTIVE_POLICY_VERIFIED").inc()
    if publish:
        # Publish without the full policy blob (large)
        pub = {k: v for k, v in result.items() if k != "policy"}
        _publish(r, STREAM_VERIFY, pub)
    logger.debug(
        "verifier: OK — key=%s stop=%s trail=%s"
        policy_key, obj["stop_ttl_mode"], obj["trailing_mode"]
    )
    return result


def verify_policy_dict(
    policy: Dict[str, Any]
    r: Optional[redis.Redis] = None
) -> Dict[str, Any]:
    """
    Convenience: verify an already-fetched policy dict WITHOUT round-tripping Redis.
    Useful in mirror_after_verified_apply path.
    """
    now_ms = int(time.time() * 1000)

    def _fail(reason_code: str, extra: Optional[Dict] = None) -> Dict[str, Any]:
        result: Dict[str, Any] = {
            "verified_ok": False
            "reason_code": reason_code
            "ts_ms": now_ms
        }
        if extra:
            result.update(extra)
        c_verify_fail.labels(reason_code=reason_code).inc()
        c_verify_total.labels(reason_code=reason_code).inc()
        return result

    if not isinstance(policy, dict):
        return _fail("POLICY_NOT_DICT")

    missing = [k for k in REQUIRED_FIELDS if not policy.get(k)]
    if missing:
        return _fail("ACTIVE_KEY_FIELDS_MISSING", {"missing": missing})

    if policy.get("stop_ttl_mode") not in VALID_MODES:
        return _fail("ACTIVE_KEY_STOP_MODE_INVALID", {"stop_ttl_mode": policy.get("stop_ttl_mode")})
    if policy.get("trailing_mode") not in VALID_MODES:
        return _fail("ACTIVE_KEY_TRAIL_MODE_INVALID", {"trailing_mode": policy.get("trailing_mode")})

    # Kill-switch check (requires Redis)
    if r is not None:
        ks_raw = r.get(
            f"cfg:atr_policy:kill_switch:"
            f"{policy['source']}:{policy['symbol']}:{policy['scenario']}:"
            f"{policy['regime']}:{policy['risk_horizon_bucket']}"
        )
        if ks_raw:
            try:
                ks = json.loads(ks_raw)
                if ks.get("enabled"):
                    return _fail("COHORT_KILL_SWITCHED", {"kill_switch_meta": ks})
            except Exception:
                pass

    c_verify_total.labels(reason_code="ACTIVE_POLICY_VERIFIED").inc()
    return {"verified_ok": True, "reason_code": "ACTIVE_POLICY_VERIFIED", "ts_ms": now_ms}
