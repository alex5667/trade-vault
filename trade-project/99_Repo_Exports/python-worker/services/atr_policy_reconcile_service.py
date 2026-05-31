from __future__ import annotations

import os
import time

import redis

from services.atr_policy_workflow import proposal_key
from services.atr_promotion_policy_apply_runner import apply_one
import contextlib


def _redis():
    return redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)


def run_once() -> int:
    r = _redis()
    applied = 0
    ids = list(r.smembers("queue:atr_policy:decided") or [])
    for proposal_id in ids:
        if apply_one(proposal_key(proposal_id)):
            applied += 1
            r.srem("queue:atr_policy:decided", proposal_id)
            try:
                from services.atr_promotion_policy_metrics import atr_policy_reconcile_apply_total
                atr_policy_reconcile_apply_total.inc()
            except Exception:
                pass
    # SLO-4: stamp last success for reconcile freshness exporter
    with contextlib.suppress(Exception):
        r.set("atr_policy:reconcile:last_success_ts_ms", str(int(time.time() * 1000)))
    return applied


if __name__ == "__main__":
    print(run_once())
