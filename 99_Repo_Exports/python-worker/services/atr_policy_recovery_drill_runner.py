from __future__ import annotations

import json
import os

import redis

from services.atr_policy_drill_catalog import DRILLS
from services.atr_policy_full_recovery_service import run_once as full_recovery_run_once
from services.atr_policy_restore_cert_service import certify


def _redis():
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)


def _target() -> dict[str, str]:
    return {
        "source": os.getenv("ATR_POLICY_CERT_SOURCE", "CryptoOrderFlow"),
        "symbol": os.getenv("ATR_POLICY_CERT_SYMBOL", "BTCUSDT"),
        "scenario": os.getenv("ATR_POLICY_CERT_SCENARIO", "restore_cert"),
        "regime": os.getenv("ATR_POLICY_CERT_REGIME", "na"),
        "risk_horizon_bucket": os.getenv("ATR_POLICY_CERT_BUCKET", "test"),
    }


def _active_key(t: dict[str, str]) -> str:
    return f"cfg:atr_policy:active:{t['source']}:{t['symbol']}:{t['scenario']}:{t['regime']}:{t['risk_horizon_bucket']}"


def _last_good_key(t: dict[str, str]) -> str:
    return f"cfg:atr_policy:last_good:{t['source']}:{t['symbol']}:{t['scenario']}:{t['regime']}:{t['risk_horizon_bucket']}"


def run_once() -> dict[str, any]:
    drill_code = (os.getenv("ATR_POLICY_DRILL_CODE", "ACTIVE_KEY_DELETE") or "ACTIVE_KEY_DELETE")
    mode = (os.getenv("ATR_POLICY_DRILL_MODE", "audit_only") or "audit_only")
    t = _target()
    r = _redis()

    if drill_code not in DRILLS:
        return {"ok": False, "reason_code": "UNKNOWN_DRILL"}

    if mode == "bounded_execute":
        if drill_code == "ACTIVE_KEY_DELETE":
            r.delete(_active_key(t))
        elif drill_code == "LAST_GOOD_DELETE":
            r.delete(_last_good_key(t))
        elif drill_code == "ACTIVE_REF_DELETE":
            import hashlib
            ref = hashlib.sha1(_active_key(t).encode("utf-8")).hexdigest()[:12]
            r.delete(f"cfg:atr_policy:active_ref:{ref}")
        elif drill_code == "PENDING_QUEUE_DROP":
            # bounded only: certification cohort proposal id is expected to exist
            pass
        elif drill_code == "DECIDED_QUEUE_DROP":
            pass
        elif drill_code == "CONFIRM_TOKEN_WIPE":
            cur = 0
            while True:
                cur, keys = r.scan(cur, match="cfg:atr_policy:confirm:*", count=10000)
                for key in keys:
                    r.delete(key)
                if cur == 0:
                    break

    recovery = full_recovery_run_once()
    cert = certify(drill_code=drill_code, target=t, run_id=(recovery.get("run_id", "")), mode=mode)
    return {"ok": True, "drill_code": drill_code, "mode": mode, "recovery": recovery, "cert": cert}


if __name__ == "__main__":
    print(json.dumps(run_once(), ensure_ascii=False, sort_keys=True))
