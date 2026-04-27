# -*- coding: utf-8 -*-
"""
Autopilot Reporter
 - exports closed trades NDJSON (7d)
 - runs tm_policy_tuner (LCB winners)
 - sends report to Telegram
 - writes auto-proposals as EntryPolicyOverridesV1 (kind=overrides_v1) into cfg:suggestions:* (no apply here)

Runs inside container with a Redis SETNX lock (prevents double-run).
"""

from __future__ import annotations
import asyncio
import json
import logging
import os
import subprocess
import sys
import time
import html
from typing import List

import redis.asyncio as aioredis

from core.entry_policy_overrides_v1 import EntryPolicyOverridesV1
from core.redis_keys import RedisStreams as RS

def _now_ms() -> int:
    return int(time.time() * 1000)

async def _send_telegram(r: aioredis.Redis, stream: str, text: str) -> None:
    """
    Publish to Telegram stream.
    """
    if not stream: return
    msg = {
        "type": "report",
        "ts_ms": str(_now_ms()),
        "text": f"<pre>{html.escape(str(text))}</pre>"
    }
    try:
        await r.xadd(stream, msg, maxlen=20000, approximate=True)
    except Exception:
        pass

async def _acquire_lock(r, key: str, ttl_sec: int) -> bool:
    try:
        ok = await r.set(key, str(_now_ms()), nx=True, ex=int(ttl_sec))
        return bool(ok)
    except Exception:
        return False

def _run(cmd: List[str]) -> None:
    subprocess.check_call(cmd)

async def run_once() -> None:
    # Redis lock
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = aioredis.from_url(redis_url, decode_responses=True)
    lock_key = os.getenv("AUTOPILOT_LOCK_KEY", "lock:autopilot:reporter:v1")
    if not await _acquire_lock(r, lock_key, ttl_sec=int(os.getenv("AUTOPILOT_LOCK_TTL_SEC", "3300"))):
        return




    # Paths
    tmp_ndjson = os.getenv("AUTOPILOT_NDJSON_PATH", "/tmp/closed_7d.ndjson")
    tmp_md = os.getenv("AUTOPILOT_REPORT_MD", "/tmp/autopilot_report.md")
    tmp_json = os.getenv("AUTOPILOT_REPORT_JSON", "/tmp/autopilot_report.json")

    since_hours = int(os.getenv("AUTOPILOT_SINCE_HOURS", "168"))
    min_n = int(os.getenv("AUTOPILOT_MIN_N", "30"))
    lcb_z = float(os.getenv("AUTOPILOT_LCB_Z", "1.645"))

    # 1) Export NDJSON
    stream = os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM)
    try:
        _run([sys.executable, "tools/export_trade_closed_ndjson.py", "--since-hours", str(since_hours), "--out", tmp_ndjson])
    except Exception as e:
        await _send_telegram(r, stream, f"❌ Autopilot Reporter Error: {html.escape(str(e))}")
        logging.error(f"❌ Autopilot Reporter error: {e}", exc_info=True)
        return

    # 2) Tuner
    try:
        _run([sys.executable, "tools/tm_policy_tuner.py", "--input", tmp_ndjson, "--min-n", str(min_n), "--lcb-z", str(lcb_z), "--out-json", tmp_json, "--out-md", tmp_md])
    except Exception as e:
        await _send_telegram(r, os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM), f"Autopilot Tuner Failed: {e}")
        return

    # 3) Telegram report
    try:
        with open(tmp_md, "r", encoding="utf-8") as f:
            md = f.read()
        await _send_telegram(r, os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM), md)
    except Exception:
        pass

    # 4) Auto-proposal (kind=overrides_v1)
    try:
        with open(tmp_json, "r", encoding="utf-8") as f:
            rep = json.load(f)
        winners = list(rep.get("winners") or [])
    except Exception:
        winners = []

    prefix_latest = os.getenv("AUTOPILOT_LATEST_PREFIX", "cfg:suggestions:entry_policy:latest:autopilot")
    prefix_meta = os.getenv("AUTOPILOT_META_PREFIX", "cfg:suggestions:entry_policy:meta")
    stream = os.getenv("AUTOPILOT_SUGGEST_STREAM", "stream:ab:suggestions")

    for w in winners:
        sym = str(w.get("symbol","")).upper()
        rg = str(w.get("regime","na")).lower()
        scn = str(w.get("scenario","na")).lower()
        grp = str(w.get("group","default")).lower()
        arm = str(w.get("winner_arm","A")).upper()
        
        # Consistent with EntryPolicyOverridesV1 schema
        o = EntryPolicyOverridesV1(
            updated_ts_ms=_now_ms(),
            enabled=1,
            symbol=sym,
            regime=rg,
            scenario=scn,
            group=grp,
            force_active_arm=arm,
            freeze_active=0,
            ab_split_b=int(os.getenv("AB_SPLIT_B", "10")),
            ab_split_c=int(os.getenv("AB_SPLIT_C", "10")),
            ab_salt=str(os.getenv("AB_SALT", "v1")),
            extra={
                "src": "autopilot_reporter",
                "n": int(w.get("n",0) or 0),
                "mean_r": float(w.get("mean_r",0.0) or 0.0),
                "lcb_r": float(w.get("lcb_r",0.0) or 0.0),
            },
        )

        sid = f"auto:{sym}:{rg}:{scn}:{grp}:{arm}:{int(o.updated_ts_ms)}"
        meta = {
            "kind": "overrides_v1",
            "symbol": sym,
            "regime": rg,
            "scenario": scn,
            "group": grp,
            "winner_arm": arm,
            "overrides_json": json.loads(o.to_json()),
            "updated_ts_ms": int(o.updated_ts_ms),
            "approvals_required": int(os.getenv("AUTOPILOT_APPROVALS_REQUIRED", "2")),
        }
        try:
            ok, _ = o.validate()
            if not ok:
                continue
            await r.set(f"{prefix_meta}:{sid}", json.dumps(meta, ensure_ascii=False, separators=(",", ":")), ex=int(os.getenv("AUTOPILOT_META_TTL_SEC", "1209600")))
            await r.set(f"{prefix_latest}:{sym}:{rg}:{scn}:{grp}", sid, ex=int(os.getenv("AUTOPILOT_LATEST_TTL_SEC", "1209600")))
            # notify stream for UI/audit
            try:
                await r.xadd(stream, {"type":"autopilot_proposal","sid":sid,"ts_ms":str(_now_ms()),"payload":json.dumps(meta, ensure_ascii=False, separators=(",", ":"))}, maxlen=50000, approximate=True)
            except Exception:
                pass
        except Exception:
            continue

async def run_forever() -> None:
    # hourly cadence default
    period_sec = int(os.getenv("AUTOPILOT_PERIOD_SEC", "3600"))
    while True:
        t0 = time.time()
        try:
            await run_once()
        except Exception:
            pass
        dt = time.time() - t0
        sleep_s = max(5.0, float(period_sec) - dt)
        await asyncio.sleep(sleep_s)

if __name__ == "__main__":
    asyncio.run(run_forever())
