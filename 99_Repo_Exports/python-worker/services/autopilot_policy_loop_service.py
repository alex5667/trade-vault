from utils.time_utils import get_ny_time_millis
# -*- coding: utf-8 -*-
"""
AutopilotPolicyLoopService
Runs inside container (no systemd needed) and does:
  - hourly: apply approved proposals (overrides_v1)
  - daily: export NDJSON + tuner report + write proposal + send Telegram report

Distributed safety:
  - Redis SETNX lock with TTL prevents double-run (two replicas).
"""

import asyncio
import os
import time
from datetime import datetime, timezone
from common.log import setup_logger
from core.redis_keys import RedisStreams as RS

import redis.asyncio as aioredis


def _now_ms() -> int:
    return get_ny_time_millis()


def _utc_date() -> str:
    return datetime.now(timezone.utc).strftime("%Y-%m-%d")


async def _send_telegram_report(r, text_html: str) -> None:
    """
    Your telegram-worker/notify_worker.py accepts report messages:
      {"type":"report","text":"..."}
    Stream name is configurable.
    """
    stream = os.getenv("TELEGRAM_NOTIFY_STREAM", RS.NOTIFY_TELEGRAM)
    msg = {"type": "report", "text": text_html, "ts_ms": str(_now_ms())}
    # best-effort
    try:
        await r.xadd(stream, msg, maxlen=20000, approximate=True)
    except Exception:
        pass


async def _run_cmd(cmd: str) -> int:
    """
    Run shell command inside container.
    Fail-open: returns non-zero on error.
    """
    p = await asyncio.create_subprocess_shell(cmd)
    return await p.wait()


class AutopilotPolicyLoopService:
    def __init__(self) -> None:
        redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
        self.r = aioredis.from_url(redis_url, decode_responses=True)

        self.interval_sec = int(os.getenv("AUTOPILOT_INTERVAL_SEC", "3600"))  # hourly
        self.window_days = int(os.getenv("AUTOPILOT_WINDOW_DAYS", "7"))
        self.report_hour_local = int(os.getenv("AUTOPILOT_REPORT_HOUR_LOCAL", "8"))
        self.report_min_local = int(os.getenv("AUTOPILOT_REPORT_MIN_LOCAL", "10"))
        # If true: create proposal keys; else only send report
        self.write_proposal = int(os.getenv("AUTOPILOT_WRITE_PROPOSAL", "1"))

        # lock
        self.lock_key = os.getenv("AUTOPILOT_LOCK_KEY", "lock:autopilot:tm_policy")
        self.lock_ttl_sec = int(os.getenv("AUTOPILOT_LOCK_TTL_SEC", "3300"))  # slightly < 1h

        # daily dedup key
        self.daily_key = os.getenv("AUTOPILOT_DAILY_DONE_KEY", "autopilot:tm_policy:last_day")

    async def _acquire_lock(self) -> bool:
        try:
            ok = await self.r.set(self.lock_key, str(_now_ms()), nx=True, ex=self.lock_ttl_sec)
            return bool(ok)
        except Exception:
            return False

    async def _release_lock(self) -> None:
        try:
            await self.r.delete(self.lock_key)
        except Exception:
            pass

    async def _apply_approved(self) -> None:
        """
        ApplyRunner should exist and be safe (checks approvals + applied markers).
        """
        cmd = "cd python-worker && PYTHONPATH='.:..' python -m services.entry_policy_apply_runner_v2 --kind overrides_v1"
        await _run_cmd(cmd)

    async def _should_do_daily(self) -> bool:
        """
        Daily report/proposal once per UTC day, but aligned to local time is handled by schedule.
        """
        day = _utc_date()
        try:
            last = str(await self.r.get(self.daily_key) or "")
            return last != day
        except Exception:
            return True

    async def _mark_daily_done(self) -> None:
        try:
            await self.r.set(self.daily_key, _utc_date(), ex=3 * 86400)
        except Exception:
            pass

    async def _daily_report_and_propose(self) -> None:
        # 1) Export NDJSON from stream
        out_path = f"/tmp/closed_{self.window_days}d.ndjson"
        since_hours = float(self.window_days) * 24.0
        cmd_export = (
            f"cd python-worker && PYTHONPATH='.:..' "
            f"python tools/export_trade_closed_ndjson.py --since-hours {since_hours:.2f} --out {out_path}"
        )
        rc1 = await _run_cmd(cmd_export)

        # 2) Tuner report (+ proposal)
        cmd_tune = (
            f"cd python-worker && PYTHONPATH='.:..' "
            f"python tools/tm_policy_tuner.py --input {out_path} --window-days {self.window_days} "
            f"{'--redis-write' if self.write_proposal else ''}"
        )
        # capture output by running with output file arg
        rep_path = f"/tmp/tm_report.md"
        cmd_tune2 = cmd_tune + f" --out-md {rep_path}"
        rc2 = await _run_cmd(cmd_tune2)

        try:
            with open(rep_path, "r", encoding="utf-8") as f:
                md_content = f.read().strip()
                # Wrap in pre for Telegram HTML (matches tm_autopilot_service behavior)
                # Need to escape HTML chars in MD to avoid parse errors
                md_escaped = md_content.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;")
                report_html = f"<b>TM Autopilot Report</b>\n<pre>{md_escaped}</pre>"
        except Exception:
            report_html = f"<b>TM Autopilot</b>\nexport_rc={rc1} tune_rc={rc2}"

        await _send_telegram_report(self.r, report_html)

        # Only mark daily done if tuner ran (avoid losing day on failures)
        if rc1 == 0 and rc2 == 0:
            await self._mark_daily_done()

    async def run_forever(self) -> None:
        """
        Loop:
          - Every interval: apply approved proposals
          - Once per day (local time gate): report+propose
        """
        while True:
            # distributed lock (hourly)
            got = await self._acquire_lock()
            if got:
                try:
                    await self._apply_approved()

                    # Daily gate: local time window
                    # We avoid timezone deps: use env-provided "local" assumption by hour/min check on wall clock
                    # If you want strict TZ: replace with zoneinfo("Europe/Zaporozhye").
                    lt = time.localtime()
                    if lt.tm_hour == self.report_hour_local and lt.tm_min >= self.report_min_local:
                        if await self._should_do_daily():
                            await self._daily_report_and_propose()
                finally:
                    await self._release_lock()

            await asyncio.sleep(max(10, int(self.interval_sec)))


async def _async_main() -> None:
    svc = AutopilotPolicyLoopService()
    await svc.run_forever()


if __name__ == "__main__":
    asyncio.run(_async_main())
