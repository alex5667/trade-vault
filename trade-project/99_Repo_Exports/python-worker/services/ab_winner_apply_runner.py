from __future__ import annotations

import asyncio
import os
import time
from typing import Any

import redis.asyncio as aioredis

from services.ab_winner_apply_lib import apply_sid_if_ready
from services.orderflow.auto_apply_guard import get_block_state
from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS

try:
    from prometheus_client import Counter, Gauge, start_http_server
except Exception:  # fail-open if prometheus_client not installed
    Counter = Gauge = None
    start_http_server = None


class ABWinnerApplyRunner:
    """
    Periodically scans:
      cfg:suggestions:entry_policy:latest:ab_winner:*  -> sid
    then applies approved meta:{sid} into cfg:entry_policy:active_arm:*.
    """

    def __init__(self, r: Any) -> None:
        self.r = r
        self.latest_prefix = os.getenv("AB_WINNER_LATEST_PREFIX", "cfg:suggestions:entry_policy:latest:ab_winner")
        self.meta_prefix = os.getenv("AB_WINNER_META_PREFIX", "cfg:suggestions:entry_policy:meta")
        self.approvals_prefix = os.getenv("AB_WINNER_APPROVALS_PREFIX", "cfg:suggestions:entry_policy:approvals")
        self.applied_prefix = os.getenv("AB_WINNER_APPLIED_PREFIX", "cfg:suggestions:entry_policy:applied")

        self.audit_stream = os.getenv("CFG_APPLY_AUDIT_STREAM", RS.ENTRY_AUDIT)

        self.interval_sec = float(os.getenv("AB_APPLY_RUNNER_INTERVAL_SEC", "10"))
        self.scan_count = int(os.getenv("AB_APPLY_RUNNER_SCAN_COUNT", "200"))
        self.max_apply_per_cycle = int(os.getenv("AB_APPLY_RUNNER_MAX_APPLY_PER_CYCLE", "50"))

        self.approvals_required = int(os.getenv("ENTRY_POLICY_APPROVALS_REQUIRED", "2"))
        self.lock_sec = int(os.getenv("AB_ACTIVE_ARM_LOCK_SEC", "21600"))
        self.active_ttl_sec = int(os.getenv("AB_ACTIVE_ARM_TTL_SEC", "0"))
        self.applied_ttl_sec = int(os.getenv("AB_APPLIED_TTL_SEC", "604800"))

        self._scan_cursor = 0

        # Optional: ops/alerts stream
        self.ops_alert_stream = os.getenv("OPS_ALERT_STREAM", RS.OPS_ALERTS)
        self.alert_on_locked = bool(int(os.getenv("AB_APPLY_ALERT_ON_LOCKED", "1")))
        self.lock_alert_gap_sec = int(os.getenv("AB_APPLY_LOCK_ALERT_GAP_SEC", "600"))  # 10m
        self._last_lock_alert_ts_ms: dict[str, int] = {}

        # Prometheus metrics (optional)
        self._m_enabled = bool(int(os.getenv("AB_APPLY_METRICS_ENABLE", "1")))
        self._m_port = int(os.getenv("AB_APPLY_METRICS_PORT", "9109"))
        if self._m_enabled and start_http_server and Counter and Gauge:
            start_http_server(self._m_port)
            self.m_applied_total = Counter("ab_apply_applied_total", "Applied suggestions total")
            self.m_skipped_total = Counter("ab_apply_skipped_total", "Skipped suggestions total", ["reason"])
            self.m_errors_total = Counter("ab_apply_errors_total", "Apply runner errors total")
            self.m_considered_total = Counter("ab_apply_considered_total", "Considered sids total")
            self.m_last_success_ts_ms = Gauge("ab_apply_last_success_ts_ms", "Last success ts_ms")
            self.m_backlog_gauge = Gauge("ab_apply_backlog_gauge", "Backlog estimate (seen in cycle)")
        else:
            self.m_applied_total = None
            self.m_skipped_total = None
            self.m_errors_total = None
            self.m_considered_total = None
            self.m_last_success_ts_ms = None
            self.m_backlog_gauge = None

    async def _scan_latest_keys(self) -> list[str]:
        match = f"{self.latest_prefix}:*"
        keys: list[str] = []
        try:
            cursor, batch = await self.r.scan(self._scan_cursor, match=match, count=self.scan_count)
            self._scan_cursor = int(cursor or 0)
            if batch:
                keys.extend([str(k) for k in batch])
        except Exception:
            self._scan_cursor = 0
        return keys

    async def tick_once(self) -> tuple[int, int]:
        # Step 26: Guard — block apply if tick-quality gate is blocking
        # Step 26: Guard — block apply if tick-quality gate is blocking
        is_blocked, meta = get_block_state()
        if is_blocked:
            # P6.11: Graceful blocking instead of exit(20) to avoid restart loops
            # We log once per N seconds via internal throttling if needed, or just print
            # Since this runs in run_forever loop with sleep, printing is fine (logs will show "blocked")
            # print(f"[ab-apply-runner] Blocked by guard: {meta.get('reason')} (pinned: {meta.get('pinned_reason')})")
            return 0, 0

        keys = await self._scan_latest_keys()
        if not keys:
            return 0, 0

        # Get sids in one RTT
        try:
            pipe = self.r.pipeline()
            for k in keys:
                pipe.get(k)
            sids = await pipe.execute()
        except Exception:
            sids = []

        applied = 0
        considered = 0
        backlog = 0
        for sid in sids:
            if applied >= self.max_apply_per_cycle:
                break
            sid_s = (sid or "").strip()
            if not sid_s:
                continue
            considered += 1
            if self.m_considered_total:
                self.m_considered_total.inc()
            res = await apply_sid_if_ready(
                r=self.r,
                sid=sid_s,
                meta_prefix=self.meta_prefix,
                approvals_prefix=self.approvals_prefix,
                applied_prefix=self.applied_prefix,
                approvals_required=self.approvals_required,
                lock_sec=self.lock_sec,
                active_ttl_sec=self.active_ttl_sec,
                applied_ttl_sec=self.applied_ttl_sec,
                audit_stream=self.audit_stream,
                by="apply_runner",
            )
            if res.applied:
                applied += 1
                if self.m_applied_total:
                    self.m_applied_total.inc()
                if self.m_last_success_ts_ms:
                    self.m_last_success_ts_ms.set(get_ny_time_millis())
                # attempt audit (applied)
                await self._audit_attempt(res, ok=True)
            else:
                backlog += 1
                if self.m_skipped_total:
                    self.m_skipped_total.labels(reason=str(res.reason)).inc()
                await self._audit_attempt(res, ok=False)
                await self._maybe_alert_locked(res)
        if self.m_backlog_gauge:
            self.m_backlog_gauge.set(backlog)
        return applied, considered

    async def _audit_attempt(self, res, ok: bool) -> None:
        """
        Writes lightweight attempt event (for RCA/monitoring) into audit stream.
        This is separate from cfg_apply_active_arm emitted by apply_lib on success.
        """
        try:
            msg = {
                "type": "ab_apply_attempt",
                "ts_ms": str(get_ny_time_millis()),
                "ok": "1" if ok else "0",
                "reason": str(res.reason),
                "sid": str(res.sid),
                "symbol": str(res.symbol),
                "regime": str(res.regime),
                "group": str(res.group),
                "winner": str(res.winner),
                "approvals_n": str(int(getattr(res, "approvals_n", 0) or 0)),
            }
            await self.r.xadd(self.audit_stream, msg, maxlen=50000, approximate=True)
        except Exception:
            pass

    async def _maybe_alert_locked(self, res) -> None:
        """
        Optional ops alert when we keep seeing 'locked' for same tuple.
        """
        if not self.alert_on_locked:
            return
        if str(res.reason) != "locked":
            return
        k = f"{res.symbol}:{res.regime}:{res.group}"
        now = get_ny_time_millis()
        last = int(self._last_lock_alert_ts_ms.get(k, 0) or 0)
        if last > 0 and (now - last) < int(self.lock_alert_gap_sec * 1000):
            return
        self._last_lock_alert_ts_ms[k] = now
        try:
            msg = {
                "type": "ops_alert",
                "ts_ms": str(now),
                "severity": "warn",
                "component": "ab_apply_runner",
                "reason": "LOCKED_BLOCKING_AUTO_APPLY",
                "key": k,
                "sid": str(res.sid),
            }
            await self.r.xadd(self.ops_alert_stream, msg, maxlen=20000, approximate=True)
        except Exception:
            pass

    async def run_forever(self) -> None:
        last_error = ""
        while True:
            t0 = time.time()
            try:
                applied, considered = await self.tick_once()
                if applied > 0:
                    print(f"[ab-apply-runner] applied={applied} considered={considered}")
                last_error = ""
            except Exception as e:
                err_str = f"{type(e).__name__}: {e}"
                if type(e).__name__ in ("ConnectionError", "BusyLoadingError", "TimeoutError", "ConnectionRefusedError"):
                    if err_str != last_error:
                        print(f"[ab-apply-runner] error={err_str}")
                        last_error = err_str
                else:
                    print(f"[ab-apply-runner] error={err_str}")
                if self.m_errors_total:
                    self.m_errors_total.inc()
            dt = time.time() - t0
            await asyncio.sleep(max(0.2, self.interval_sec - dt))


async def _async_main() -> None:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = aioredis.from_url(redis_url, decode_responses=True, socket_connect_timeout=10, socket_timeout=30, max_connections=10)
    runner = ABWinnerApplyRunner(r)
    await runner.run_forever()


if __name__ == "__main__":
    asyncio.run(_async_main())
