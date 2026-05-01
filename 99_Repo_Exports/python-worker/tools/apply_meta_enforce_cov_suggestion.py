#!/usr/bin/env python3
# -*- coding: utf-8 -*-
from __future__ import annotations
"""
apply_meta_enforce_cov_suggestion.py

P33: ApplyRunner-style applier for meta ENFORCE per-coverage bucket shares.

Reads a suggestion (JSON) written by meta_cov_outcome_auto_apply_v1.py:
- meta:    {PREFIX}:meta:{sid}
- approvals (set): {PREFIX}:approvals:{sid}
- latest pointer:  {PREFIX}:latest

Applies patch into cfg2 (Redis hash settings:dynamic_cfg) with:
- approvals gate (>= approvals_required)
- anti-flap hold-down via cfg2 key meta_cov_rollout_last_change_ms
- switch-budget via Redis JSON key {PREFIX}:switch_state:v1
- respects Auto-Apply global guard latch (tick-quality gate / manual pause)

This tool is safe to run on a schedule.

ENV
  REDIS_URL (default redis://localhost:6379/0)
  DYN_CFG_KEY (default settings:dynamic_cfg)

  META_ENFORCE_COV_PREFIX (default cfg:suggestions:meta_enforce_cov)
  META_ENFORCE_COV_APPROVALS_REQUIRED (default 2)
  META_ENFORCE_COV_MIN_HOLD_SEC (default 1800)
  META_ENFORCE_COV_MAX_SWITCHES_PER_DAY (default 6)
  META_ENFORCE_COV_MIN_GAP_SEC (default 3600)

  NOTIFY_TELEGRAM_STREAM (default notify:telegram)  # optional
"""

from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import time
from typing import Any, Dict, Optional, Tuple

try:
    import redis  # type: ignore
except Exception:
    redis = None  # type: ignore

try:
    from core.switch_budget import SwitchState, can_switch, apply_switch
except Exception:
    SwitchState = None  # type: ignore
    can_switch = None  # type: ignore
    apply_switch = None  # type: ignore

try:
    # Global auto-apply latch (tick-quality, manual pause)
    from services.orderflow.auto_apply_guard import assert_auto_apply_not_blocked
except Exception:
    def assert_auto_apply_not_blocked() -> None:  # type: ignore
        return


def now_ms() -> int:
    return get_ny_time_millis()


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _jloads(x: Any) -> Optional[Dict[str, Any]]:
    if x is None:
        return None
    if isinstance(x, dict):
        return x
    try:
        return json.loads(str(x))
    except Exception:
        return None


def _redis() -> Any:
    if redis is None:
        raise RuntimeError("redis library is not available")
    url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    return redis.Redis.from_url(url, decode_responses=True)


def _keys(prefix: str, sid: str) -> Dict[str, str]:
    return {
        "latest": f"{prefix}:latest",
        "meta": f"{prefix}:meta:{sid}",
        "approvals": f"{prefix}:approvals:{sid}",
        "applied": f"{prefix}:applied:{sid}",
        "switch_state": f"{prefix}:switch_state:v1",
    }


def _load_cfg2(r: Any, dyn_key: str) -> Dict[str, Any]:
    d = r.hgetall(dyn_key) or {}
    # keep raw strings, but provide convenient numeric access via helpers
    return {str(k): v for k, v in d.items()}


def _write_cfg2_patch(r: Any, dyn_key: str, patch: Dict[str, Any]) -> None:
    # HSET expects strings; keep float formatting stable
    m: Dict[str, str] = {}
    for k, v in patch.items():
        if v is None:
            continue
        if isinstance(v, float):
            m[str(k)] = f"{float(v):.6g}"
        else:
            m[str(k)] = str(v)
    if m:
        r.hset(dyn_key, mapping=m)


def _switch_budget_check_and_update(
    *,
    r: Any,
    state_key: str,
    now_ts_ms: int,
    max_per_day: int,
    min_gap_ms: int,
    dry_run: bool,
) -> Tuple[bool, str]:
    if SwitchState is None or can_switch is None or apply_switch is None:
        # fail-open: budget system not available
        return True, "ok_no_budget"
    raw = r.get(state_key)
    st = SwitchState.from_dict(_jloads(raw) or {})
    ok, reason = can_switch(st=st, now_ms=now_ts_ms, max_per_day=max_per_day, min_gap_ms=min_gap_ms)
    if not ok:
        return False, reason
    if not dry_run:
        st2 = apply_switch(st=st, now_ms=now_ts_ms, max_per_day=max_per_day, min_gap_ms=min_gap_ms)
        r.set(state_key, json.dumps(st2.to_dict(), separators=(",", ":")), ex=14 * 24 * 3600)
    return True, "ok"


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--sid", default="", help="Suggestion id. If empty, uses {PREFIX}:latest")
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--force", action="store_true", help="Ignore hold-down + switch-budget (still respects global auto-apply latch)")
    args = ap.parse_args()

    prefix = os.environ.get("META_ENFORCE_COV_PREFIX", "cfg:suggestions:meta_enforce_cov")
    approvals_required = int(os.environ.get("META_ENFORCE_COV_APPROVALS_REQUIRED", "2") or 2)
    min_hold_sec = int(os.environ.get("META_ENFORCE_COV_MIN_HOLD_SEC", "1800") or 1800)
    max_per_day = int(os.environ.get("META_ENFORCE_COV_MAX_SWITCHES_PER_DAY", "6") or 6)
    min_gap_sec = int(os.environ.get("META_ENFORCE_COV_MIN_GAP_SEC", "3600") or 3600)

    dyn_key = os.environ.get("DYN_CFG_KEY", "settings:dynamic_cfg")
    notify_stream = os.environ.get("NOTIFY_TELEGRAM_STREAM", "notify:telegram")

    # Global latch: if tick-quality gate blocks auto-apply, do nothing.
    assert_auto_apply_not_blocked()

    r = _redis()
    sid = str(args.sid or "").strip()
    if not sid:
        sid = str(r.get(f"{prefix}:latest") or "").strip()
    if not sid:
        print(json.dumps({"ok": 0, "reason": "no_sid"}))
        return 0

    k = _keys(prefix, sid)
    meta_raw = r.get(k["meta"])
    meta = _jloads(meta_raw) or {}
    patch = meta.get("patch") or meta.get("cfg2_patch") or {}
    if not isinstance(patch, dict) or not patch:
        print(json.dumps({"ok": 0, "reason": "empty_patch", "sid": sid}))
        return 0

    # Approvals gate
    try:
        approvals_n = int(r.scard(k["approvals"]) or 0)
        approvals = sorted(list(r.smembers(k["approvals"]) or set()))
    except Exception:
        approvals_n = 0
        approvals = []
    if approvals_n < approvals_required:
        print(json.dumps({"ok": 0, "sid": sid, "reason": "insufficient_approvals", "have": approvals_n, "need": approvals_required}))
        return 0

    now_ts = now_ms()
    cfg2 = _load_cfg2(r, dyn_key)
    last_change_ms = _i(cfg2.get("meta_cov_rollout_last_change_ms"), 0)
    if (not args.force) and min_hold_sec > 0 and last_change_ms > 0 and (now_ts - last_change_ms) < (min_hold_sec * 1000):
        print(json.dumps({"ok": 0, "sid": sid, "reason": "min_hold_active", "last_change_ms": last_change_ms, "min_hold_sec": min_hold_sec}))
        return 0

    # Switch-budget (3rd layer stabilization)
    if not args.force:
        ok_budget, reason = _switch_budget_check_and_update(
            r=r,
            state_key=k["switch_state"],
            now_ts_ms=now_ts,
            max_per_day=max_per_day,
            min_gap_ms=min_gap_sec * 1000,
            dry_run=args.dry_run,
        )
        if not ok_budget:
            print(json.dumps({"ok": 0, "sid": sid, "reason": f"switch_budget_block:{reason}"}))
            return 0

    # Apply patch
    # Always stamp last_change to "now" (even if patch provides it)
    patch2 = dict(patch)
    patch2["meta_cov_rollout_last_change_ms"] = int(now_ts)
    patch2["meta_cov_outcome_last_apply_ms"] = int(now_ts)

    # Record before/after snapshot for rollback/debug
    before: Dict[str, Any] = {}
    for kk in patch2.keys():
        before[str(kk)] = cfg2.get(str(kk))

    if not args.dry_run:
        pipe = r.pipeline(transaction=True)
        # cfg2 patch
        _write_cfg2_patch(pipe, dyn_key, patch2)
        # applied marker
        applied_obj = {
            "sid": sid,
            "ts_ms": int(now_ts),
            "approvals": approvals,
            "before": before,
            "patch": patch2,
            "meta": {k2: meta.get(k2) for k2 in ("reason", "window_hours", "decisions", "summary") if k2 in meta},
        }
        pipe.set(k["applied"], json.dumps(applied_obj, ensure_ascii=False, separators=(",", ":")), ex=7 * 24 * 3600)
        try:
            # Optional: notify stream for operator visibility
            msg = {
                "ts_ms": int(now_ts),
                "event": "META_COV_OUTCOME_APPLIED",
                "sid": sid,
                "keys": sorted(list(patch2.keys())),
                "approvals": approvals,
            }
            pipe.xadd(notify_stream, {"json": json.dumps(msg, ensure_ascii=False, separators=(",", ":"))}, maxlen=5000, approximate=True)
        except Exception:
            pass
        pipe.execute()

    print(json.dumps({"ok": 1, "sid": sid, "dry_run": int(args.dry_run), "applied_keys": sorted(list(patch2.keys())), "approvals": approvals}))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
