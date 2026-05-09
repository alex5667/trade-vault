from utils.time_utils import get_ny_time_millis

#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
tm_autopilot_report_service.py

"Идеально-закольцованный контур" (без авто-применения по умолчанию):
  1) Export closed trades -> NDJSON
  2) Run policy tuner -> markdown + json
  3) Send markdown to Telegram via notify stream (type=report)
  4) (optional) write proposal into cfg:suggestions:* for manual approvals/apply

Runs INSIDE CONTAINER as a long-running process (no systemd required).

Scheduling:
  - Daily run at TM_DAILY_HHMM (local TZ) if TM_AUTOPILOT_MODE=daily
  - Hourly run if TM_AUTOPILOT_MODE=hourly

Safe-lock:
  Redis SETNX with token + TTL to avoid concurrent runs.
"""

import json
import os
import time
import uuid
from dataclasses import dataclass
from datetime import datetime, timedelta
from typing import Any
from zoneinfo import ZoneInfo

import redis

from core.redis_keys import RedisStreams as RS
import contextlib


def _now_ms() -> int:
    return get_ny_time_millis()


def _s(v: Any, d: str = "") -> str:
    try:
        return str(v if v is not None else d)
    except Exception:
        return d


@dataclass
class RedisLock:
    r: "redis.Redis"
    key: str
    ttl_sec: int
    token: str = ""

    def acquire(self) -> bool:
        self.token = str(uuid.uuid4())
        try:
            ok = self.r.set(self.key, self.token, nx=True, ex=int(self.ttl_sec))
            return bool(ok)
        except Exception:
            return False

    def release(self) -> None:
        # release only if token matches (Lua for safety)
        lua = """
        if redis.call("GET", KEYS[1]) == ARGV[1] then
          return redis.call("DEL", KEYS[1])
        else
          return 0
        end
        """
        with contextlib.suppress(Exception):
            self.r.eval(lua, 1, self.key, self.token)


def build_proposal_buttons(sid: str) -> str:
    """
    Build inline keyboard JSON for approve/reject buttons.
    Returns JSON string for embedding in Redis stream entry.
    """
    buttons = [
        [{"text": f"✅ Approve {sid[:8]}", "callback_data": f"approve:{sid}"},
         {"text": f"❌ Reject {sid[:8]}", "callback_data": f"reject:{sid}"}]
    ]
    return json.dumps(buttons, ensure_ascii=False)


def send_telegram_report(r: "redis.Redis", *, stream: str, text: str, ts_ms: int,
                         buttons: str | None = None) -> None:
    """
    notify_worker.py accepts:
      {"type":"report","text":"...","buttons":"[[...]]"}
    Keep payload small enough; Telegram HTML supported by your notifier.
    """
    msg: dict[str, str] = {
        "type": "report",
        "ts_ms": str(int(ts_ms)),
        "text": text,
    }
    if buttons:
        msg["buttons"] = buttons
    r.xadd(stream, msg, maxlen=20000, approximate=True)


def run_pipeline(*, redis_url: str, window_hours: float, window_days: int, out_dir: str) -> tuple[str, dict[str, Any]]:
    """
    Runs exporter + tuner in-process by shelling out via python -m is avoided.
    We import the modules for determinism and speed.
    Returns (markdown, json_obj).
    """
    # Lazy imports (so unit tests can import service without tools path issues)
    from tools.export_trade_closed_ndjson import iter_position_closed
    from tools.tm_policy_tuner import compute_stats, recommend_tiers, render_markdown

    r = redis.from_url(redis_url, decode_responses=True)
    now = _now_ms()
    since_ms = now - int(float(window_hours) * 3600.0 * 1000.0)
    nd_path = os.path.join(out_dir, f"closed_{int(window_days)}d.ndjson")

    n = 0
    with open(nd_path, "w", encoding="utf-8") as f:
        for rec in iter_position_closed(
            r=r,
            stream=os.getenv("TRADE_EVENTS_STREAM", RS.EVENTS_TRADES),
            since_ms=since_ms,
            batch=int(os.getenv("TM_EXPORT_BATCH", "2000")),
            max_items=int(os.getenv("TM_EXPORT_MAX_ITEMS", "1000000")),
        ):
            f.write(json.dumps(rec, ensure_ascii=False, separators=(",", ":")) + "\n")
            n += 1

    rows = []
    with open(nd_path, encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            try:
                rows.append(json.loads(s))
            except Exception:
                continue

    stats, meta = compute_stats(rows)
    recs = recommend_tiers(stats, min_n=int(os.getenv("TM_TUNER_MIN_N", "30")))
    md = render_markdown(recs, stats, window_days=window_days)
    out = {"meta": meta, "recs": list(recs.values()), "exported": n, "ndjson": nd_path, "ts_ms": now}
    return md, out


def maybe_write_proposal(r: "redis.Redis", *, proposal: dict[str, Any]) -> str | None:
    """
    Optional: store proposal into cfg:suggestions:* for manual approvals.
    This does NOT apply anything automatically.
    """
    if int(os.getenv("TM_AUTOPILOT_PROPOSE", "0") or "0") != 1:
        return None
    try:
        sid = _s(proposal.get("sid", "")) or hashlib_sha1(json.dumps(proposal, separators=(",", ":")))
        meta_key = f"cfg:suggestions:entry_policy:meta:{sid}"
        r.set(meta_key, json.dumps(proposal, ensure_ascii=False, separators=(",", ":")), ex=int(os.getenv("TM_PROPOSAL_TTL_SEC", "1209600")))  # 14d
        return sid
    except Exception:
        return None


def hashlib_sha1(s: str) -> str:
    import hashlib
    return hashlib.sha1(s.encode("utf-8"), usedforsecurity=False).hexdigest()


def sleep_until_next_run(tz: ZoneInfo, hhmm: str) -> None:
    hh, mm = 8, 10
    try:
        a, b = hhmm.split(":")
        hh, mm = int(a), int(b)
    except Exception:
        pass
    now = datetime.now(tz=tz)
    target = now.replace(hour=hh, minute=mm, second=0, microsecond=0)
    if target <= now:
        target = target + timedelta(days=1)
    time.sleep(max(1.0, (target - now).total_seconds()))


def main() -> int:
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    notify_stream = os.getenv("TM_TELEGRAM_STREAM", os.getenv("TELEGRAM_NOTIFY_STREAM", RS.NOTIFY_TELEGRAM))
    tzname = os.getenv("TM_TZ", "Europe/Zaporozhye")
    tz = ZoneInfo(tzname)

    mode = os.getenv("TM_AUTOPILOT_MODE", "daily").strip().lower()  # daily|hourly
    daily_hhmm = os.getenv("TM_DAILY_HHMM", "08:10")
    import tempfile
    window_days = int(os.getenv("TM_WINDOW_DAYS", "7"))
    window_hours = float(os.getenv("TM_LOOKBACK_HOURS", str(window_days * 24)))
    out_dir = os.getenv("TM_OUT_DIR", tempfile.gettempdir())
    os.makedirs(out_dir, exist_ok=True)

    lock_key = os.getenv("TM_AUTOPILOT_LOCK_KEY", "lock:tm_autopilot_report")
    lock_ttl = int(os.getenv("TM_AUTOPILOT_LOCK_TTL_SEC", "7200"))  # >= max runtime

    r = redis.from_url(redis_url, decode_responses=True)

    while True:
        if mode == "daily":
            sleep_until_next_run(tz, daily_hhmm)
        else:
            # hourly: align to next hour
            now = datetime.now(tz=tz)
            nxt = (now.replace(minute=0, second=0, microsecond=0) + timedelta(hours=1))
            time.sleep(max(1.0, (nxt - now).total_seconds()))

        lock = RedisLock(r=r, key=lock_key, ttl_sec=lock_ttl)
        if not lock.acquire():
            # another instance is running
            continue
        try:
            md, out = run_pipeline(redis_url=redis_url, window_hours=window_hours, window_days=window_days, out_dir=out_dir)
            # Persist last report snapshot for UI/debug
            with contextlib.suppress(Exception):
                r.set("reports:tm_policy_tuner:last", json.dumps(out, ensure_ascii=False, separators=(",", ":")), ex=86400)
            # Try to write proposal and attach approve/reject buttons
            buttons_json: str | None = None
            try:
                sid = maybe_write_proposal(r, proposal=out)
                if sid:
                    buttons_json = build_proposal_buttons(sid)
            except Exception:
                pass
            send_telegram_report(r, stream=notify_stream, text=md, ts_ms=int(out.get("ts_ms", _now_ms())),
                                 buttons=buttons_json)
        except Exception as e:
            # Fail-open: send minimal error report (still visible)
            with contextlib.suppress(Exception):
                send_telegram_report(r, stream=notify_stream, text=f"<b>TM Autopilot ERROR</b>\n{_s(e)}", ts_ms=_now_ms())
        finally:
            lock.release()


if __name__ == "__main__":
    raise SystemExit(main())
