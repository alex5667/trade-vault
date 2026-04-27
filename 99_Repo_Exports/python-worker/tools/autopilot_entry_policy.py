from utils.time_utils import get_ny_time_millis

# -*- coding: utf-8 -*-
"""
Autopilot runner:
  1) export NDJSON from events:trades (POSITION_CLOSED)
  2) run tm_policy_tuner to compute LCB winners
  3) emit:
      - cfg:suggestions:entry_policy:meta:{sid} (winner suggestions, apply_kind=active_arm)
      - cfg:suggestions:entry_policy:meta:{sid2} (overrides suggestions, apply_kind=overrides_v1) [optional]
      - approvals/applied keys (auto-apply optional)
  4) send Telegram report via notify stream (type=report)

This script is designed to be called by a container scheduler service.
"""

import json
import os
import time
import hashlib
from typing import Dict, Any, Tuple, Optional, List

import redis

from tools.export_trade_closed_ndjson import export_ndjson
from tools.tm_policy_tuner import load_ndjson, compute_stats, choose_winner, render_report


def _r() -> redis.Redis:
    url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    return redis.from_url(url, decode_responses=True)


def _sha1(s: str) -> str:
    return hashlib.sha1(s.encode("utf-8")).hexdigest()


def _now_ms() -> int:
    return get_ny_time_millis()


def _notify_report(r: redis.Redis, *, html: str) -> None:
    """
    Telegram notify worker already supports:
      {"type":"report","text":"..."}
    We'll publish it into a configurable Redis stream.
    """
    stream = os.getenv("TELEGRAM_NOTIFY_STREAM", "stream:notify:telegram")
    msg = {"type": "report", "ts_ms": str(_now_ms()), "text": str(html)}
    try:
        r.xadd(stream, msg, maxlen=int(os.getenv("TELEGRAM_NOTIFY_STREAM_MAXLEN", "20000")), approximate=True)
    except Exception:
        pass


def _acquire_lock(r: redis.Redis, *, key: str, ttl_sec: int) -> bool:
    try:
        return bool(r.set(key, str(_now_ms()), nx=True, ex=int(ttl_sec)))
    except Exception:
        return False


def _mk_sid(kind: str, ctx: str) -> str:
    return _sha1(f"{kind}|{ctx}|{_now_ms()}")


def _write_suggestion_meta(r: redis.Redis, *, sid: str, meta: Dict[str, Any], ttl_sec: int) -> None:
    r.set(f"cfg:suggestions:entry_policy:meta:{sid}", json.dumps(meta, ensure_ascii=False, separators=(",", ":")), ex=int(ttl_sec))


def _write_latest_pointer(r: redis.Redis, *, key: str, sid: str, ttl_sec: int) -> None:
    r.set(key, sid, ex=int(ttl_sec))


def _auto_approve_and_apply(r: redis.Redis, *, sid: str, appliers: List[str]) -> None:
    """
    Minimal auto-approval protocol:
      - approvals key contains list of appliers (stringified)
      - applied key is set by ApplyRunner or by this script (optional)
    """
    try:
        r.set(f"cfg:suggestions:entry_policy:approvals:{sid}", json.dumps(appliers, ensure_ascii=False), ex=int(os.getenv("AUTOPILOT_SUGGEST_TTL_SEC", "604800")))
    except Exception:
        pass
    # Optional direct apply (if you run without ApplyRunner)
    if int(os.getenv("AUTOPILOT_DIRECT_APPLY", "0")) == 1:
        # We only mark; actual apply is expected via ApplyRunner.
        r.set(f"cfg:suggestions:entry_policy:applied:{sid}", "0", ex=int(os.getenv("AUTOPILOT_SUGGEST_TTL_SEC", "604800")))


def run_once() -> Dict[str, Any]:
    r = _r()
    lock_key = os.getenv("AUTOPILOT_LOCK_KEY", "lock:autopilot:entry_policy")
    if not _acquire_lock(r, key=lock_key, ttl_sec=int(os.getenv("AUTOPILOT_LOCK_TTL_SEC", "3300"))):
        return {"ok": False, "reason": "lock_busy"}

    since_hours = float(os.getenv("AUTOPILOT_SINCE_HOURS", "168"))
    tmp_path = os.getenv("AUTOPILOT_TMP_NDJSON", "/tmp/closed_7d.ndjson")
    suggest_ttl = int(os.getenv("AUTOPILOT_SUGGEST_TTL_SEC", "604800"))  # 7d

    url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    export_ndjson(
        redis_url=url,
        since_hours=since_hours,
        out_path=tmp_path,
        stream=os.getenv("TRADE_EVENTS_STREAM", "events:trades")
    )
    rows = load_ndjson(tmp_path)
    stats = compute_stats(rows)
    reco = choose_winner(stats)
    report_txt = render_report(reco, top_n=int(os.getenv("AUTOPILOT_REPORT_TOP", "25")))

    # Emit suggestions per context
    n_ctx = 0
    n_suggest = 0
    for (sym, rg, scn, grp), d in reco.items():
        n_ctx += 1
        w = d.get("winner_arm", "A")
        # create suggestion only if not A? configurable
        if int(os.getenv("AUTOPILOT_SUGGEST_ONLY_NON_A", "1")) == 1 and w == "A":
            continue
        sid = _mk_sid("winner", f"{sym}:{rg}:{scn}:{grp}:{w}")
        meta = {
            "v": 2,
            "apply_kind": "active_arm",
            "symbol": sym,
            "regime": rg,
            "scenario": scn,
            "group": grp,
            "winner_arm": w,
            "thresholds": d.get("thresholds", {}),
            "per_arm": d.get("per_arm", {}),
            "updated_ts_ms": _now_ms(),
            "reason": "LCB_winner",
        }
        _write_suggestion_meta(r, sid=sid, meta=meta, ttl_sec=suggest_ttl)
        latest_key = f"cfg:suggestions:entry_policy:latest:ab_winner:{sym}:{rg}:{grp}"
        _write_latest_pointer(r, key=latest_key, sid=sid, ttl_sec=suggest_ttl)
        _auto_approve_and_apply(r, sid=sid, appliers=str(os.getenv("AUTOPILOT_APPROVERS", "auto")).split(","))
        n_suggest += 1

    # Optional: overrides_v1 proposal (global knobs: hold-down, hysteresis, evaluator thresholds)
    if int(os.getenv("AUTOPILOT_EMIT_OVERRIDES_V1", "1")) == 1:
        ov_sid = _mk_sid("overrides_v1", "global")
        overrides = {
            "v": 1,
            "enabled": 1,
            "updated_ts_ms": _now_ms(),
            "overrides_hold_down_ms": int(os.getenv("OVERRIDES_HOLD_DOWN_MS", "60000")),
            "hysteresis_ts_ms": int(os.getenv("OVERRIDES_HYSTERESIS_MS", "300000")),
            # evaluator knobs (documented via env; entry_policy_service can read these too)
            "lcb_alpha": float(os.getenv("AUTOPILOT_LCB_ALPHA", "0.05")),
            "min_edge_lcb_r": float(os.getenv("AUTOPILOT_MIN_EDGE_LCB_R", "0.05")),
        }
        meta2 = {
            "v": 2,
            "apply_kind": "overrides_v1",
            "symbol": "GLOBAL",
            "regime": "na",
            "scenario": "na",
            "group": "default",
            "overrides": overrides,
            "overrides_json": json.dumps(overrides, ensure_ascii=False, separators=(",", ":")),
            "updated_ts_ms": _now_ms(),
            "reason": "autopilot_global_overrides",
            "mirror_global": 1,
        }
        _write_suggestion_meta(r, sid=ov_sid, meta=meta2, ttl_sec=suggest_ttl)
        _write_latest_pointer(r, key="cfg:suggestions:entry_policy:latest:overrides_v1:global", sid=ov_sid, ttl_sec=suggest_ttl)
        _auto_approve_and_apply(r, sid=ov_sid, appliers=str(os.getenv("AUTOPILOT_APPROVERS", "auto")).split(","))

    # Telegram report (HTML)
    html = "<pre>" + report_txt.replace("&", "&amp;").replace("<", "&lt;").replace(">", "&gt;") + "</pre>"
    _notify_report(r, html=html)

    return {"ok": True, "contexts": n_ctx, "suggestions": n_suggest, "ndjson": tmp_path}


if __name__ == "__main__":
    print(run_once())
