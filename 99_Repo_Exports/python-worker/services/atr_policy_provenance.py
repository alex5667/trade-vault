from __future__ import annotations

import hashlib
import os
import time
from typing import Any, Dict

import redis


def _redis() -> redis.Redis:
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(v)
    except Exception:
        return default


def _safe_str(v: Any, default: str = "") -> str:
    try:
        s = str(v)
        return s if s else default
    except Exception:
        return default


def _compact_tag(obj: Dict[str, Any]) -> str:
    base = "|".join([
        _safe_str(obj.get("policy_source")),
        _safe_str(obj.get("symbol")).upper(),
        _safe_str(obj.get("scenario")).lower(),
        _safe_str(obj.get("regime")).lower(),
        _safe_str(obj.get("risk_horizon_bucket")).lower(),
        str(_safe_int(obj.get("policy_ver"), 0)),
        _safe_str(obj.get("stop_ttl_mode")),
        _safe_str(obj.get("trailing_mode")),
    ])
    return hashlib.sha1(base.encode("utf-8")).hexdigest()[:12]


def build_policy_provenance(signal: Dict[str, Any]) -> Dict[str, Any]:
    meta = signal.get("meta", {}) if isinstance(signal.get("meta"), dict) else {}
    pol = meta.get("atr_policy_snapshot", {}) if isinstance(meta.get("atr_policy_snapshot"), dict) else {}

    try:
        r = _redis()
        recovery_run_id = _safe_str(r.get("atr_policy:full_recovery:last_run_id"), "")
        cert_status = _safe_str(r.get("atr_policy:restore_cert:last_status"), "")
        cert_id = _safe_str(r.get("atr_policy:restore_cert:last_cert_id"), "")
    except Exception:
        recovery_run_id = ""
        cert_status = ""
        cert_id = ""

    out = {
        "policy_ver": _safe_int(pol.get("policy_ver"), 0),
        "policy_source": _safe_str(pol.get("source")),
        "symbol": _safe_str(pol.get("symbol") or signal.get("symbol")).upper(),
        "scenario": _safe_str(pol.get("scenario") or signal.get("kind")).lower(),
        "regime": _safe_str(pol.get("regime")),
        "risk_horizon_bucket": _safe_str(pol.get("risk_horizon_bucket")).lower(),
        "stop_ttl_mode": _safe_str(pol.get("stop_ttl_mode"), "canary"),
        "trailing_mode": _safe_str(pol.get("trailing_mode"), "canary"),
        "active_key": _safe_str(pol.get("active_key")),
        "policy_updated_at_ms": _safe_int(pol.get("updated_at_ms"), 0),
        "recovery_run_id": recovery_run_id,
        "restore_cert_id": cert_id,
        "restore_cert_status": cert_status,
        "attached_at_ms": int(time.time() * 1000),
    }
    out["policy_tag"] = _compact_tag(out)
    return out
