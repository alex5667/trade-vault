# -*- coding: utf-8 -*-
"""
FreezePromotionService — shadow→hard auto-promotion for EntryPolicyFreezeV1.

Purpose:
  Automatically promotes an active shadow freeze to hard after an observation
  window has elapsed and the metrics still look bad (confirmed by blocked/seen
  counters written by smt_entry_policy_service._record_shadow_block).

Design:
  - Polling loop (PROMOTER_POLL_S, default 30s)
  - SCAN Redis keys: cfg:entry_policy:freeze:v1:*
  - For each active shadow freeze:
      1. Check observation window: (now - created_ts_ms) >= SHADOW_OBSERVE_MS
      2. Read shadow_stats hash: cfg:entry_policy:freeze:shadow_stats:{sym}:{grp}:{scn}
      3. Decision: blocked_count >= SHADOW_MIN_BLOCKED
                   AND seen_count >= SHADOW_MIN_SEEN
                   AND bad_metric_count >= SHADOW_PROMOTE_BAD_CNT
      4. Promote: overwrite freeze key with mode="hard", promoted_ts_ms=now
      5. Publish event to ops:eventlog stream
  - Fail-open: any Redis error → keep shadow, skip
  - Monotonic: only shadow→hard, never downgrade

ENV:
  REDIS_URL                 redis://redis-worker-1:6379/0
  PROMOTER_POLL_S           30          polling interval (seconds)
  SHADOW_OBSERVE_MS         600000      observation window before promotion (10 min)
  SHADOW_MIN_BLOCKED        5           min blocked candidates to confirm promotion
  SHADOW_MIN_SEEN           10          min total seen candidates
  SHADOW_PROMOTE_BAD_CNT    2           min bad metrics to confirm promotion
  CB_SPREAD_Z_P95_MAX       3.0         spread_z threshold (from circuit breaker)
  CB_OBI_AGE_P95_MAX_MS     1500        obi_age_ms threshold
  CB_PRESSURE_P95_MAX       1.4         pressure_sps threshold
  OPS_EVENT_STREAM          ops:eventlog
"""

from __future__ import annotations

import asyncio
import json
import os
import time
from typing import Any, Dict, Optional, Tuple

import redis.asyncio as aioredis  # type: ignore

from core.entry_policy_freeze import EntryPolicyFreezeV1


def _now_ms() -> int:
    return int(time.time() * 1000)


def _f(x: Any, d: float = 0.0) -> float:
    try:
        v = float(x)
        return d if v != v else v  # NaN guard
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(x)
    except Exception:
        return d


def _s(x: Any, d: str = "") -> str:
    try:
        return str(x) if x is not None else d
    except Exception:
        return d


# ---------------------------------------------------------------------------
# Decision helper
# ---------------------------------------------------------------------------

def _promotion_decision(
    *,
    fz: EntryPolicyFreezeV1,
    stats: Dict[str, str],
    now_ms: int,
    observe_ms: int,
    min_blocked: int,
    min_seen: int,
    bad_cnt_needed: int,
    thr_spread_z: float,
    thr_obi_age_ms: float,
    thr_pressure: float,
) -> Tuple[bool, str]:
    """Pure-function promotion decision (testable without Redis).

    Returns:
        (should_promote: bool, reason: str)
    """
    # 1. Observation window must have elapsed
    elapsed_ms = now_ms - _i(fz.created_ts_ms, 0)
    if elapsed_ms < observe_ms:
        return False, f"observe_window_not_elapsed elapsed_ms={elapsed_ms}"

    # 2. Must still be active
    if not fz.is_active(now_ms):
        return False, "freeze_already_expired"

    # 3. Must be shadow mode (only promote shadow→hard)
    if fz.mode != "shadow":
        return False, f"already_{fz.mode}"

    # 4. Must not have already been promoted this cycle
    if _i(fz.promoted_ts_ms, 0) > 0:
        return False, "already_promoted"

    # 5. Stats thresholds
    blocked = _i(stats.get("blocked_count", 0), 0)
    seen = _i(stats.get("seen_count", 0), 0)

    if seen < min_seen:
        return False, f"not_enough_seen seen={seen}<{min_seen}"
    if blocked < min_blocked:
        return False, f"not_enough_blocked blocked={blocked}<{min_blocked}"

    # 6. Metrics still bad
    sp = _f(stats.get("last_spread_z", 0.0), 0.0)
    ob = _f(stats.get("last_obi_age_ms", 0.0), 0.0)
    pr = _f(stats.get("last_pressure_sps", 0.0), 0.0)

    bad_sp = thr_spread_z > 0 and sp >= thr_spread_z
    bad_ob = thr_obi_age_ms > 0 and ob >= thr_obi_age_ms
    bad_pr = thr_pressure > 0 and pr >= thr_pressure
    bad_cnt = int(bad_sp) + int(bad_ob) + int(bad_pr)

    if bad_cnt < bad_cnt_needed:
        return False, f"metrics_recovered bad_cnt={bad_cnt}<{bad_cnt_needed} spread={sp:.2f} obi={ob:.0f} pr={pr:.2f}"

    reason = (
        f"blocked_{blocked}_seen_{seen}_bad_cnt_{bad_cnt}"
        f"_spread={sp:.2f}_obi={ob:.0f}_pr={pr:.2f}"
    )
    return True, reason


# ---------------------------------------------------------------------------
# Service
# ---------------------------------------------------------------------------

class FreezePromotionService:
    """Polls active shadow freezes and promotes them to hard when warranted.

    One instance per process; runs as an asyncio task alongside other services.
    """

    def __init__(self) -> None:
        redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.r: aioredis.Redis = aioredis.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=10,
            socket_timeout=15,
            max_connections=10,
        )

        self.poll_s = float(os.getenv("PROMOTER_POLL_S", "30"))
        self.freeze_key_prefix = "cfg:entry_policy:freeze:v1:"
        self.stats_key_prefix = "cfg:entry_policy:freeze:shadow_stats:"
        self.ops_stream = os.getenv("OPS_EVENT_STREAM", "ops:eventlog")

        # Decision parameters
        self.observe_ms = _i(os.getenv("SHADOW_OBSERVE_MS", "600000"), 600_000)
        self.min_blocked = _i(os.getenv("SHADOW_MIN_BLOCKED", "5"), 5)
        self.min_seen = _i(os.getenv("SHADOW_MIN_SEEN", "10"), 10)
        self.bad_cnt_needed = _i(os.getenv("SHADOW_PROMOTE_BAD_CNT", "2"), 2)

        # Metric thresholds (reuse CB env vars so config is single-source)
        self.thr_spread_z = _f(os.getenv("CB_SPREAD_Z_P95_MAX", "3.0"), 3.0)
        self.thr_obi_age_ms = _f(os.getenv("CB_OBI_AGE_P95_MAX_MS", "1500"), 1500.0)
        self.thr_pressure = _f(os.getenv("CB_PRESSURE_P95_MAX", "1.4"), 1.4)

    # ------------------------------------------------------------------
    # Main loop
    # ------------------------------------------------------------------

    async def run_forever(self) -> None:
        """Main polling loop. Runs indefinitely, catches all exceptions."""
        while True:
            try:
                await self._scan_and_promote()
            except Exception:
                pass  # fail-open — sleep and retry
            await asyncio.sleep(self.poll_s)

    # ------------------------------------------------------------------
    # Scan & promote
    # ------------------------------------------------------------------

    async def _scan_and_promote(self) -> None:
        """SCAN all cfg:entry_policy:freeze:v1:* keys, evaluate each shadow freeze."""
        pattern = f"{self.freeze_key_prefix}*"
        cursor = 0
        while True:
            try:
                cursor, keys = await self.r.scan(cursor, match=pattern, count=100)
            except Exception:
                return  # fail-open: abort this cycle
            for key in keys:
                await self._maybe_promote(key)
            if cursor == 0:
                break

    async def _maybe_promote(self, fkey: str) -> None:
        """Evaluate a single freeze key and promote if warranted."""
        now = _now_ms()
        try:
            raw = await self.r.get(fkey)
        except Exception:
            return
        if not raw:
            return

        fz, err = EntryPolicyFreezeV1.from_json(str(raw))
        if fz is None or err not in ("", None):
            return
        if not fz.is_active(now) or fz.mode != "shadow":
            return

        # Build stats key from freeze fields
        stats_key = (
            f"{self.stats_key_prefix}"
            f"{fz.symbol.upper()}:{fz.group.lower()}:{fz.scenario.lower()}"
        )
        try:
            stats: Dict[str, str] = await self.r.hgetall(stats_key) or {}
        except Exception:
            return  # fail-open

        should_promote, reason = _promotion_decision(
            fz=fz,
            stats=stats,
            now_ms=now,
            observe_ms=self.observe_ms,
            min_blocked=self.min_blocked,
            min_seen=self.min_seen,
            bad_cnt_needed=self.bad_cnt_needed,
            thr_spread_z=self.thr_spread_z,
            thr_obi_age_ms=self.thr_obi_age_ms,
            thr_pressure=self.thr_pressure,
        )

        if not should_promote:
            return

        await self._do_promote(fkey=fkey, fz=fz, reason=reason, now_ms=now)

    async def _do_promote(
        self,
        *,
        fkey: str,
        fz: EntryPolicyFreezeV1,
        reason: str,
        now_ms: int,
    ) -> None:
        """Atomically promote shadow→hard by overwriting the freeze key."""
        fz.mode = "hard"
        fz.promoted_ts_ms = now_ms
        fz.promoted_reason = reason[:200]

        ttl_s = max(60, int((fz.until_ts_ms - now_ms) / 1000) + 300)
        try:
            await self.r.set(fkey, fz.to_json(), ex=ttl_s)
        except Exception:
            return  # fail-open: don't publish event if write failed

        # Publish observability event (best-effort)
        await self._publish_event(fz=fz, reason=reason, now_ms=now_ms)

    async def _publish_event(
        self,
        *,
        fz: EntryPolicyFreezeV1,
        reason: str,
        now_ms: int,
    ) -> None:
        """Publish freeze_promoted_hard event to ops:eventlog stream."""
        event = {
            "type": "freeze_promoted_hard",
            "ts_ms": now_ms,
            "symbol": fz.symbol,
            "group": fz.group,
            "scenario": fz.scenario,
            "promoted_reason": reason,
            "until_ts_ms": fz.until_ts_ms,
            "src": "freeze_promoter",
        }
        try:
            await self.r.xadd(
                self.ops_stream,
                {"ts_ms": str(now_ms), "event": json.dumps(event, ensure_ascii=False)[:4000]},
                maxlen=5000,
                approximate=True,
            )
        except Exception:
            pass  # best-effort


# ---------------------------------------------------------------------------
# Entrypoint
# ---------------------------------------------------------------------------

async def _main() -> None:
    svc = FreezePromotionService()
    await svc.run_forever()


if __name__ == "__main__":
    asyncio.run(_main())
