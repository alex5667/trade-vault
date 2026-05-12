from __future__ import annotations

"""services.autopilot_orchestrator_service

Runs the "Autopilot" daily loop inside a container:
  1) export closed trades NDJSON (rolling window)
  2) run tm_policy_tuner.py to compute EV/LCB and write proposals (overrides_v1)
  3) send the report + recommendation to Telegram

This service is intentionally *batch* and *separate* from signal generation:
  - It should never block trading pipeline.
  - It uses Redis lock to avoid duplicate runs.

Usage (inside python-worker container):
  PYTHONPATH=".:.." python -m services.autopilot_orchestrator_service

Env:
  REDIS_URL
  TELEGRAM_BOT_TOKEN
  TELEGRAM_CHAT_ID

  AUTOPILOT_SINCE_HOURS=168
  AUTOPILOT_WINDOW_DAYS=7
  AUTOPILOT_RUN_HOUR_LOCAL=9
  AUTOPILOT_RUN_MINUTE_LOCAL=5
  AUTOPILOT_TZ=Europe/Zaporozhye
  AUTOPILOT_LOCK_TTL_SEC=21600
"""

import os
import subprocess
import time
from dataclasses import dataclass

import redis

from core.redis_lock import acquire_lock_sync, lock_key_daily, release_lock_sync, utc_yyyymmdd
from utils.telegram_notify import send_telegram_message
from utils.time_utils import get_ny_time_millis
import contextlib


@dataclass
class AutopilotSchedule:
    tz: str = "Europe/Zaporozhye"
    hour_local: int = 9
    minute_local: int = 5

    def should_run_now(self, now_ms: int) -> bool:
        try:
            import datetime as dt
            from zoneinfo import ZoneInfo

            z = ZoneInfo(self.tz)
            now = dt.datetime.fromtimestamp(now_ms / 1000.0, tz=z)
            return (now.hour == int(self.hour_local)) and (now.minute == int(self.minute_local))
        except Exception:
            # fallback: run at UTC 09:05
            import datetime as dt

            now = dt.datetime.fromtimestamp(now_ms / 1000.0, tz=dt.timezone.utc)
            return (now.hour == 9) and (now.minute == 5)


def _run_cmd(cmd: str, cwd: str, timeout_sec: int = 3600) -> tuple[int, str]:
    """Run a shell command and return (rc, combined_output)."""
    try:
        p = subprocess.run(
            cmd,
            cwd=cwd,
            shell=True,
            stdout=subprocess.PIPE,
            stderr=subprocess.STDOUT,
            text=True,
            timeout=timeout_sec,
        )
        return int(p.returncode), str(p.stdout or "")
    except subprocess.TimeoutExpired as e:
        return 1, f"timeout_error: exceeded {timeout_sec}s"
    except Exception as e:
        return 1, f"run_error:{e}"


def run_once(*, repo_root: str, since_hours: int, window_days: int, out_path: str) -> str:
    """Executes export + tuner and returns a Markdown report (best-effort)."""
    py_path = "PYTHONPATH=\".:..\""
    redis_write = os.getenv("AUTOPILOT_REDIS_WRITE", "0") == "1"

    # 1) Export
    cmd1 = f"{py_path} python tools/export_trade_closed_ndjson.py --since-hours {int(since_hours)} --out {out_path}"
    rc1, out1 = _run_cmd(cmd1, cwd=repo_root)

    # 2) Tuner
    cmd2 = f"{py_path} python tools/tm_policy_tuner.py --input {out_path} --window-days {int(window_days)}"
    if redis_write:
        cmd2 += " --redis-write"

    rc2, out2 = _run_cmd(cmd2, cwd=repo_root)

    # Compose report
    ts = get_ny_time_millis()
    md = []
    md.append(f"*Autopilot report* | ts_ms={ts}")
    md.append("")
    md.append(f"export rc={rc1} | tuner rc={rc2}")
    md.append("")
    if out2.strip():
        md.append(out2[-3500:])  # keep within Telegram chunking
    else:
        md.append("(no tuner output)")
    if rc1 != 0 or rc2 != 0:
        md.append("\n---\n")
        md.append("*debug tail*\n")
        tail = (out1 + "\n" + out2)[-3500:]
        md.append(tail)
    return "\n".join(md)


def main() -> int:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = redis.from_url(redis_url, decode_responses=True)

    repo_root = os.getenv("AUTOPILOT_REPO_ROOT", os.getcwd())
    since_hours = int(os.getenv("AUTOPILOT_SINCE_HOURS", "168"))
    window_days = int(os.getenv("AUTOPILOT_WINDOW_DAYS", "7"))
    out_path = os.getenv("AUTOPILOT_OUT_PATH", "/tmp/closed_7d.ndjson")

    sch = AutopilotSchedule(
        tz=os.getenv("AUTOPILOT_TZ", "Europe/Zaporozhye"),
        hour_local=int(os.getenv("AUTOPILOT_RUN_HOUR_LOCAL", "9")),
        minute_local=int(os.getenv("AUTOPILOT_RUN_MINUTE_LOCAL", "5")),
    )

    lock_ttl = int(os.getenv("AUTOPILOT_LOCK_TTL_SEC", "21600"))
    lock_prefix = os.getenv("AUTOPILOT_LOCK_PREFIX", "lock:autopilot:daily")

    poll_sec = int(os.getenv("AUTOPILOT_POLL_SEC", "20"))
    if poll_sec < 5:
        poll_sec = 5

    while True:
        now_ms = get_ny_time_millis()
        if sch.should_run_now(now_ms):
            day = utc_yyyymmdd(now_ms)
            lk = lock_key_daily(lock_prefix, day)
            tok = acquire_lock_sync(r=r, key=lk, ttl_sec=lock_ttl)
            if tok:
                try:
                    report = run_once(repo_root=repo_root, since_hours=since_hours, window_days=window_days, out_path=out_path)
                    send_telegram_message(text=report)
                    # Keep last report pointer for observability
                    with contextlib.suppress(Exception):
                        r.set("autopilot:last_report_ts_ms", str(now_ms), ex=7 * 24 * 3600)
                finally:
                    release_lock_sync(r=r, key=lk, token=tok)
                # do not re-run within the same minute
                time.sleep(65)
                continue
        time.sleep(poll_sec)


if __name__ == "__main__":
    raise SystemExit(main())
