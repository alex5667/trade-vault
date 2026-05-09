from __future__ import annotations

import hashlib
import json
import os
import time
from typing import Any

import redis

from services.atr_policy_operator_state_store import (
    expire_pending_confirms_on_boot,
    get_conn,
    load_current_active_snapshots,
)


def _redis():
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)


def _active_key(obj: dict[str, Any]) -> str:
    return (
        f"cfg:atr_policy:active:{obj['source']}:{obj['symbol']}:"
        f"{obj['scenario']}:{obj['regime']}:{obj['risk_horizon_bucket']}"
    )


def _ref(key: str) -> str:
    return hashlib.sha1(key.encode("utf-8")).hexdigest()[:12]


def _ref_key(ref: str) -> str:
    return f"cfg:atr_policy:active_ref:{ref}"


def _clear_redis_confirm_tokens(r) -> int:
    cur = 0
    deleted = 0
    while True:
        cur, keys = r.scan(cur, match="cfg:atr_policy:confirm:*", count=10000)
        for key in keys:
            r.delete(key)
            deleted += 1
        if cur == 0:
            break
    return deleted


def run_once() -> dict[str, Any]:
    r = _redis()
    rebuilt_refs = 0
    expired_confirms = 0
    cleared_redis_tokens = 0

    with get_conn() as conn:
        try:
            # 1) rebuild active ref mappings from current active snapshots
            for obj in load_current_active_snapshots(conn):
                akey = _active_key(obj)
                ref = _ref(akey)
                r.set(_ref_key(ref), akey, ex=int(os.getenv("ATR_POLICY_TELEGRAM_PACK_REF_TTL_SEC", "86400")))
                rebuilt_refs += 1

            # 2) expire outstanding confirm requests in SQL
            expired_confirms = expire_pending_confirms_on_boot(conn)
            conn.commit()
        except Exception:
            conn.rollback()
            raise

    # 3) clear all stale Redis confirm tokens
    cleared_redis_tokens = _clear_redis_confirm_tokens(r)

    # 4) stamp boot state
    r.set("atr_policy:operator_bootstrap:last_run_ts_ms", str(int(time.time() * 1000)))
    r.set("atr_policy:operator_bootstrap:last_result_json", json.dumps({
        "rebuilt_refs": rebuilt_refs,
        "expired_confirms": expired_confirms,
        "cleared_redis_tokens": cleared_redis_tokens,
    }, ensure_ascii=False, sort_keys=True))

    return {
        "rebuilt_refs": rebuilt_refs,
        "expired_confirms": expired_confirms,
        "cleared_redis_tokens": cleared_redis_tokens,
    }


if __name__ == "__main__":
    print(json.dumps(run_once(), ensure_ascii=False, sort_keys=True))
