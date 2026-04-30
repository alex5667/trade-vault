#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""meta_cov_outcome_auto_apply_v1.py

P33+P34: Auto-downgrade + quarantine loop for meta ENFORCE per-coverage bucket shares
based on realized outcomes from POSITION_CLOSED events.

Key ideas
---------
P33: outcome downgrade
- Reads recent closed trades from Redis stream (TRADE_EVENTS_STREAM).
- For each coverage bucket (a/b/c/d), compares enforce vs control outcomes.
- If enforce underperforms materially or tail risk rises, emits a cfg2 patch that reduces
  meta_enforce_share_{bucket}.

P34: bucket quarantine + auto-recovery (anti-flap)
- On *severe* tail-risk (tail_rate(enforce) >= QUARANTINE_TAIL), quarantines bucket:
    meta_enforce_share_{bucket} := 0
    meta_cov_quarantine_{bucket} := 1
    meta_cov_quarantine_until_ms_{bucket} := now + TTL
    meta_cov_quarantine_prev_share_{bucket} := previous share
- After TTL expiry, requires a GOOD_STREAK of control outcomes to release quarantine.
  Release starts with a small canary share (START_SHARE) and sets a recovery target.
- Optionally ramps share up towards target via small steps when enforce outcomes remain healthy.

State
-----
To avoid touching cfg2 on every tick (which would trigger min-hold), streak counters are stored
in a separate Redis JSON key:
  {PREFIX}:qstate:v1

Redis key format (suggestions)
------------------------------
PREFIX = cfg:suggestions:meta_enforce_cov (default)
- {PREFIX}:latest
- {PREFIX}:meta:{sid}              (JSON)
- {PREFIX}:approvals:{sid}         (SET)
- {PREFIX}:applied:{sid}           (JSON)   # written by applier

ENV (P33)
---------
  REDIS_URL (default redis://localhost:6379/0)
  TRADE_EVENTS_STREAM (default events:trades)
  DYN_CFG_KEY (default settings:dynamic_cfg)

  META_ENFORCE_COV_PREFIX (default cfg:suggestions:meta_enforce_cov)
  META_ENFORCE_COV_AUTO_APPROVE (default 1)
  META_ENFORCE_COV_AUTO_APPROVERS (default auto_guard_1,auto_guard_2)
  META_ENFORCE_COV_RUN_APPLY (default 1)         # when --apply=1

  META_COV_OUTCOME_LOOKBACK_HOURS (default 72)
  META_COV_OUTCOME_MIN_N_ENFORCE (default 30)
  META_COV_OUTCOME_MIN_N_CONTROL (default 30)

  META_COV_OUTCOME_TAIL_THRESH (default 0.35)
  META_COV_OUTCOME_TAIL_DELTA_THRESH (default 0.08)
  META_COV_OUTCOME_MEAN_DELTA_THRESH (default -0.10)

  META_COV_OUTCOME_DOWN_STEP (default 0.10)
  META_COV_OUTCOME_PANIC_TAIL (default 0.45)

  META_COV_MIN_HOLD_SEC (default 1800)

ENV (P34)
---------
Quarantine trigger
  META_COV_QUARANTINE_TAIL (default = META_COV_OUTCOME_PANIC_TAIL)
  META_COV_QUARANTINE_TTL_SEC (default 7200)
  META_COV_QUARANTINE_BYPASS_HOLD (default 1)

Release (after TTL)
  META_COV_QUARANTINE_GOOD_STREAK_N (default 3)
  META_COV_QUARANTINE_RELEASE_MIN_N_CTL (default 30)
  META_COV_QUARANTINE_RELEASE_CTL_TAIL_MAX (default 0.35)
  META_COV_QUARANTINE_RELEASE_CTL_MEANR_MIN (default -0.05)
  META_COV_QUARANTINE_START_SHARE (default 0.02)

Ramp (optional, gradual recovery)
  META_COV_QUARANTINE_RAMP_STEP (default 0.05)
  META_COV_QUARANTINE_RAMP_GOOD_N (default 2)
  META_COV_QUARANTINE_RAMP_MIN_N_ENF (default 30)
  META_COV_QUARANTINE_RAMP_TAIL_MAX (default 0.30)
  META_COV_QUARANTINE_RAMP_MEAN_DELTA_MIN (default 0.00)

Exit codes
----------
0 : OK / no action
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import hashlib
import json
import os
import subprocess
import sys
import time
from typing import Any, Dict, List, Optional

try:
    import redis  # type: ignore
except Exception:
    redis = None  # type: ignore

# P41 fallback enrichment configuration
_ENRICH_ENABLED = bool(int(os.environ.get("META_COV_OUTCOME_ENRICH_FROM_TRADES_CLOSED", "1") or 1))
_ENRICH_STREAM = os.environ.get("TRADES_CLOSED_STREAM", "trades:closed")
_ENRICH_MAX_SCAN = int(os.environ.get("META_COV_OUTCOME_ENRICH_MAX_SCAN", "200000") or 200000)


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


def _loads_maybe_json(v: Any) -> Any:
    if v is None:
        return None
    if isinstance(v, (bytes, bytearray)):
        try:
            v = v.decode("utf-8", "replace")
        except Exception:
            return None
    if isinstance(v, str):
        s = v.strip()
        if not s:
            return None
        if (s.startswith("{") and s.endswith("}")) or (s.startswith("[") and s.endswith("]")):
            try:
                return json.loads(s)
            except Exception:
                return v
        return v
    return v


def _parse_entry(fields: Dict[Any, Any]) -> Dict[str, Any]:
    out: Dict[str, Any] = {}
    payload_obj: Optional[Dict[str, Any]] = None
    for k, v in fields.items():
        ks = k.decode("utf-8", "replace") if isinstance(k, (bytes, bytearray)) else str(k)
        out[ks] = _loads_maybe_json(v)
    if isinstance(out.get("payload"), dict):
        payload_obj = out.get("payload")  # type: ignore[assignment]
    elif isinstance(out.get("json"), dict):
        payload_obj = out.get("json")  # type: ignore[assignment]
    if payload_obj:
        merged = dict(out)
        merged.update(payload_obj)
        return merged
    return out


def _is_position_closed(fields: Dict[str, Any]) -> bool:
    et = str(fields.get("event_type") or fields.get("event") or "").upper()
    if et in ("POSITION_CLOSED", "CLOSE"):
        return True
    if not et and fields.get("exit_ts_ms") and ("pnl" in fields or "pnl_net" in fields):
        return True
    return False


def _event_ts_ms(fields: Dict[str, Any]) -> int:
    return _i(fields.get("ts_ms") or fields.get("ts") or fields.get("exit_ts_ms") or fields.get("timestamp") or 0, 0)


def _redis() -> Any:
    if redis is None:
        raise RuntimeError("redis library is not available")
    url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    return redis.Redis.from_url(url, decode_responses=True)


def read_closed_trades(*, r: Any, stream: str, since_ms: int, max_scan: int = 500_000) -> List[Dict[str, Any]]:
    out: List[Dict[str, Any]] = []
    last_id = "+"
    scanned = 0
    while scanned < max_scan:
        batch = r.xrevrange(stream, max=last_id, min="-", count=2000)
        if not batch:
            break
        if len(batch) == 1 and batch[0][0] == last_id:
            break
        for msg_id, raw_fields in batch:
            scanned += 1
            if msg_id == last_id:
                continue
            last_id = msg_id
            if not isinstance(raw_fields, dict):
                continue
            fields = _parse_entry(raw_fields)
            ts = _event_ts_ms(fields)
            if ts and ts < since_ms:
                scanned = max_scan
                break
            if not _is_position_closed(fields):
                continue
            fields["_ts_ms"] = int(ts)
            out.append(fields)
    return out


def build_meta_fallback_map(r: Any, stream: str, since_ms: int, max_scan: int) -> Dict[str, tuple[str, int]]:
    """Builds sid -> (bucket, applied) map from trades:closed for P41 fallback."""
    out: Dict[str, tuple[str, int]] = {}
    last_id = "+"
    scanned = 0
    while scanned < max_scan:
        batch = r.xrevrange(stream, max=last_id, min="-", count=2000)
        if not batch:
            break
        if len(batch) == 1 and batch[0][0] == last_id:
            break
        for msg_id, raw_fields in batch:
            scanned += 1
            if msg_id == last_id:
                continue
            last_id = msg_id
            if not isinstance(raw_fields, dict):
                continue
            fields = _parse_entry(raw_fields)
            ts = _event_ts_ms(fields)
            if ts and ts < since_ms:
                scanned = max_scan
                break
            
            # Identify signal/trade id
            sid = str(fields.get("sid") or fields.get("signal_id") or "")
            if not sid:
                continue
            
            bucket = str(fields.get("meta_enforce_cov_bucket") or "").strip().lower()
            applied = _i(fields.get("meta_enforce_applied"), 0)
            if bucket:
                out[sid] = (bucket, applied)
    return out


def tail_rate(rs: List[float]) -> float:
    if not rs:
        return 0.0
    return sum(1 for x in rs if x <= -1.0) / float(len(rs))


def mean(rs: List[float]) -> float:
    if not rs:
        return 0.0
    return sum(rs) / float(len(rs))


def summarize(rs: List[float]) -> Dict[str, Any]:
    return {"n": int(len(rs)), "meanR": float(mean(rs)), "tail_rate": float(tail_rate(rs))}


def _qstate_key(prefix: str) -> str:
    return f"{prefix}:qstate:v1"


def load_qstate(r: Any, prefix: str) -> Dict[str, Any]:
    raw = r.get(_qstate_key(prefix))
    if not raw:
        return {"buckets": {}}
    try:
        obj = json.loads(raw)
        if isinstance(obj, dict):
            if "buckets" not in obj or not isinstance(obj.get("buckets"), dict):
                obj["buckets"] = {}
            return obj
    except Exception:
        pass
    return {"buckets": {}}


def save_qstate(r: Any, prefix: str, st: Dict[str, Any]) -> None:
    try:
        r.set(_qstate_key(prefix), json.dumps(st, ensure_ascii=False, separators=(",", ":")), ex=14 * 24 * 3600)
    except Exception:
        return


def bucket_state(st: Dict[str, Any], b: str) -> Dict[str, Any]:
    buckets = st.setdefault("buckets", {})
    if not isinstance(buckets, dict):
        buckets = {}
        st["buckets"] = buckets
    x = buckets.get(b)
    if not isinstance(x, dict):
        x = {}
        buckets[b] = x
    x.setdefault("release_streak", 0)
    x.setdefault("ramp_streak", 0)
    x.setdefault("last_eval_ms", 0)
    return x


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", type=int, default=0, help="1 to run applier (if enabled), 0 to only emit suggestion")
    ap.add_argument("--dry-run", action="store_true", help="1 to check logic without emitting Redis keys")
    ap.add_argument("--lookback-hours", type=float, default=float(os.environ.get("META_COV_OUTCOME_LOOKBACK_HOURS", "72") or 72))
    ap.add_argument("--max-scan", type=int, default=500_000)
    args = ap.parse_args()

    prefix = os.environ.get("META_ENFORCE_COV_PREFIX", "cfg:suggestions:meta_enforce_cov")
    dyn_key = os.environ.get("DYN_CFG_KEY", "settings:dynamic_cfg")
    stream = os.environ.get("TRADE_EVENTS_STREAM", "events:trades")

    auto_approve = bool(int(os.environ.get("META_ENFORCE_COV_AUTO_APPROVE", "1") or 0))
    auto_approvers = [x.strip() for x in (os.environ.get("META_ENFORCE_COV_AUTO_APPROVERS", "auto_guard_1,auto_guard_2") or "").split(",") if x.strip()]
    run_apply = bool(int(os.environ.get("META_ENFORCE_COV_RUN_APPLY", "1") or 0))

    # P33 thresholds
    min_n_enf = int(os.environ.get("META_COV_OUTCOME_MIN_N_ENFORCE", "30") or 30)
    min_n_ctl = int(os.environ.get("META_COV_OUTCOME_MIN_N_CONTROL", "30") or 30)

    tail_thresh = float(os.environ.get("META_COV_OUTCOME_TAIL_THRESH", "0.35") or 0.35)
    tail_delta_thresh = float(os.environ.get("META_COV_OUTCOME_TAIL_DELTA_THRESH", "0.08") or 0.08)
    mean_delta_thresh = float(os.environ.get("META_COV_OUTCOME_MEAN_DELTA_THRESH", "-0.10") or -0.10)

    down_step = float(os.environ.get("META_COV_OUTCOME_DOWN_STEP", "0.10") or 0.10)
    panic_tail = float(os.environ.get("META_COV_OUTCOME_PANIC_TAIL", "0.45") or 0.45)

    min_hold_sec = int(os.environ.get("META_COV_MIN_HOLD_SEC", "1800") or 1800)

    # P34 quarantine config
    quarantine_tail = float(os.environ.get("META_COV_QUARANTINE_TAIL", str(panic_tail)) or panic_tail)
    quarantine_ttl_sec = int(os.environ.get("META_COV_QUARANTINE_TTL_SEC", "7200") or 7200)
    quarantine_bypass_hold = bool(int(os.environ.get("META_COV_QUARANTINE_BYPASS_HOLD", "1") or 0))

    good_streak_n = int(os.environ.get("META_COV_QUARANTINE_GOOD_STREAK_N", "3") or 3)
    rel_min_n_ctl = int(os.environ.get("META_COV_QUARANTINE_RELEASE_MIN_N_CTL", "30") or 30)
    rel_ctl_tail_max = float(os.environ.get("META_COV_QUARANTINE_RELEASE_CTL_TAIL_MAX", "0.35") or 0.35)
    rel_ctl_mean_min = float(os.environ.get("META_COV_QUARANTINE_RELEASE_CTL_MEANR_MIN", "-0.05") or -0.05)
    start_share = float(os.environ.get("META_COV_QUARANTINE_START_SHARE", "0.02") or 0.02)

    ramp_step = float(os.environ.get("META_COV_QUARANTINE_RAMP_STEP", "0.05") or 0.05)
    ramp_good_n = int(os.environ.get("META_COV_QUARANTINE_RAMP_GOOD_N", "2") or 2)
    ramp_min_n_enf = int(os.environ.get("META_COV_QUARANTINE_RAMP_MIN_N_ENF", str(min_n_enf)) or min_n_enf)
    ramp_tail_max = float(os.environ.get("META_COV_QUARANTINE_RAMP_TAIL_MAX", "0.30") or 0.30)
    ramp_mean_delta_min = float(os.environ.get("META_COV_QUARANTINE_RAMP_MEAN_DELTA_MIN", "0.00") or 0.0)

    r = _redis()
    cfg2 = r.hgetall(dyn_key) or {}

    now_ts = now_ms()
    since_ms = now_ts - int(args.lookback_hours * 3600 * 1000)

    rows = read_closed_trades(r=r, stream=stream, since_ms=since_ms, max_scan=args.max_scan)

    # P41 Enrichment Fallback
    fallback_map: Dict[str, tuple[str, int]] = {}
    p41_stats = {"rows_total": len(rows), "rows_enriched": 0, "rows_missing_meta": 0, "rows_native_meta": 0}
    
    if _ENRICH_ENABLED:
        fallback_map = build_meta_fallback_map(r, _ENRICH_STREAM, since_ms, _ENRICH_MAX_SCAN)

    buckets = ["trend", "range", "other"]
    enf: Dict[str, List[float]] = {b: [] for b in buckets}
    ctl: Dict[str, List[float]] = {b: [] for b in buckets}

    for f in rows:
        b = str(f.get("meta_enforce_cov_bucket") or "").strip().lower()
        applied = _i(f.get("meta_enforce_applied"), -1)
        
        # Fallback if fields are missing in trade event
        if not b or applied == -1:
            sid = str(f.get("sid") or f.get("signal_id") or "")
            if sid in fallback_map:
                fb_bucket, fb_applied = fallback_map[sid]
                b = b or fb_bucket
                if applied == -1:
                    applied = fb_applied
                p41_stats["rows_enriched"] += 1
            else:
                p41_stats["rows_missing_meta"] += 1
        else:
            p41_stats["rows_native_meta"] += 1

        if b not in enf:
            continue
        
        if applied == -1: # Still unknown
            applied = 0 # Default to control if unknown

        r_mult = _f(f.get("r_mult") or f.get("r_multiple") or 0.0, 0.0)
        if r_mult == 0.0:
            pnl = _f(f.get("pnl") or f.get("pnl_net") or 0.0, 0.0)
            risk = _f(f.get("risk_usd") or 0.0, 0.0)
            if risk > 0:
                r_mult = pnl / risk
        if applied == 1:
            enf[b].append(float(r_mult))
        else:
            ctl[b].append(float(r_mult))

    summary: Dict[str, Any] = {"_p41_enrich": p41_stats}
    for b in buckets:
        summary[b] = {"enf": summarize(enf[b]), "ctl": summarize(ctl[b])}

    # Load quarantine streak state (separate from cfg2)
    st = load_qstate(r, prefix)

    decisions: List[Dict[str, Any]] = []
    patch: Dict[str, Any] = {}

    def cur_share_for(b: str) -> float:
        v = _f(cfg2.get(f"meta_enforce_share_{b}") or cfg2.get("meta_enforce_share") or 1.0, 1.0)
        return max(0.0, min(1.0, float(v)))

    def q_active(b: str) -> int:
        return _i(cfg2.get(f"meta_cov_quarantine_{b}"), 0)

    def q_until(b: str) -> int:
        return _i(cfg2.get(f"meta_cov_quarantine_until_ms_{b}"), 0)

    def q_prev_share(b: str) -> float:
        return max(0.0, min(1.0, _f(cfg2.get(f"meta_cov_quarantine_prev_share_{b}"), 0.0)))

    def recovery_target_share(b: str) -> float:
        return max(0.0, min(1.0, _f(cfg2.get(f"meta_cov_recovery_target_share_{b}"), 0.0)))

    # Pass 1: quarantine release decisions (after TTL expiry)
    for b in buckets:
        bs = bucket_state(st, b)
        if q_active(b) != 1:
            bs["release_streak"] = 0
            continue

        until_ms = q_until(b)
        if until_ms > 0 and now_ts < until_ms:
            bs["release_streak"] = 0
            continue

        s_ctl = summary[b]["ctl"]
        good = (int(s_ctl["n"]) >= rel_min_n_ctl) and (float(s_ctl["tail_rate"]) <= rel_ctl_tail_max) and (float(s_ctl["meanR"]) >= rel_ctl_mean_min)
        if good:
            bs["release_streak"] = int(bs.get("release_streak", 0)) + 1
        else:
            bs["release_streak"] = 0

        if int(bs["release_streak"]) >= good_streak_n:
            prev = q_prev_share(b)
            if prev <= 0:
                prev = cur_share_for(b)
            tgt = prev
            start = min(tgt, start_share)
            # release quarantine + set recovery target
            patch[f"meta_cov_quarantine_{b}"] = 0
            patch[f"meta_cov_quarantine_until_ms_{b}"] = 0
            patch[f"meta_enforce_share_{b}"] = float(start)
            patch[f"meta_cov_recovery_target_share_{b}"] = float(tgt)
            patch[f"meta_cov_quarantine_reason_{b}"] = "released"
            decisions.append({
                "action": "unquarantine"
                "bucket": b
                "start_share": float(start)
                "target_share": float(tgt)
                "good_streak": int(bs["release_streak"])
                "ctl": s_ctl
            })
            bs["release_streak"] = 0

    # Pass 2: quarantine trigger + downgrade for non-quarantined buckets
    for b in buckets:
        if q_active(b) == 1:
            continue

        s_enf = summary[b]["enf"]
        s_ctl = summary[b]["ctl"]
        if int(s_enf["n"]) < min_n_enf or int(s_ctl["n"]) < min_n_ctl:
            continue

        mean_delta = float(s_enf["meanR"]) - float(s_ctl["meanR"])
        tail_delta = float(s_enf["tail_rate"]) - float(s_ctl["tail_rate"])

        # Quarantine triggers on severe tail-risk
        if float(s_enf["tail_rate"]) >= quarantine_tail:
            cur = cur_share_for(b)
            ttl_ms = int(quarantine_ttl_sec) * 1000
            patch[f"meta_enforce_share_{b}"] = 0.0
            patch[f"meta_cov_quarantine_{b}"] = 1
            patch[f"meta_cov_quarantine_until_ms_{b}"] = int(now_ts + ttl_ms)
            patch[f"meta_cov_quarantine_prev_share_{b}"] = float(cur)
            patch[f"meta_cov_quarantine_reason_{b}"] = f"tail_enf={float(s_enf['tail_rate']):.3f}"
            # keep recovery target as prev share (used on release)
            if recovery_target_share(b) <= 0:
                patch[f"meta_cov_recovery_target_share_{b}"] = float(cur)

            decisions.append({
                "action": "quarantine"
                "bucket": b
                "cur_share": float(cur)
                "new_share": 0.0
                "ttl_sec": int(quarantine_ttl_sec)
                "mean_delta": mean_delta
                "tail_delta": tail_delta
                "tail_enf": float(s_enf["tail_rate"])
                "tail_ctl": float(s_ctl["tail_rate"])
                "n_enf": int(s_enf["n"])
                "n_ctl": int(s_ctl["n"])
            })
            continue

        # Downgrade rule (P33)
        bad = (float(s_enf["tail_rate"]) > tail_thresh) or (tail_delta > tail_delta_thresh) or (mean_delta < mean_delta_thresh)
        if not bad:
            continue

        cur = cur_share_for(b)
        new_share = max(0.0, cur - down_step)
        if new_share >= cur:
            continue

        patch[f"meta_enforce_share_{b}"] = float(new_share)
        decisions.append({
            "action": "downgrade"
            "bucket": b
            "cur_share": float(cur)
            "new_share": float(new_share)
            "mean_delta": mean_delta
            "tail_delta": tail_delta
            "tail_enf": float(s_enf["tail_rate"])
            "tail_ctl": float(s_ctl["tail_rate"])
            "n_enf": int(s_enf["n"])
            "n_ctl": int(s_ctl["n"])
        })

    # Pass 3: gradual ramp towards recovery target (if configured)
    for b in buckets:
        if q_active(b) == 1:
            continue
        tgt = recovery_target_share(b)
        if tgt <= 0:
            continue
        cur = cur_share_for(b)
        if cur >= (tgt - 1e-9):
            # done
            patch[f"meta_cov_recovery_target_share_{b}"] = 0.0
            continue

        bs = bucket_state(st, b)

        s_enf = summary[b]["enf"]
        s_ctl = summary[b]["ctl"]
        if int(s_enf["n"]) < ramp_min_n_enf or int(s_ctl["n"]) < min_n_ctl:
            bs["ramp_streak"] = 0
            continue

        mean_delta = float(s_enf["meanR"]) - float(s_ctl["meanR"])
        good = (float(s_enf["tail_rate"]) <= ramp_tail_max) and (mean_delta >= ramp_mean_delta_min)
        if good:
            bs["ramp_streak"] = int(bs.get("ramp_streak", 0)) + 1
        else:
            bs["ramp_streak"] = 0

        if int(bs["ramp_streak"]) >= ramp_good_n and ramp_step > 0:
            new_share = min(tgt, cur + ramp_step)
            patch[f"meta_enforce_share_{b}"] = float(new_share)
            decisions.append({
                "action": "ramp_up"
                "bucket": b
                "cur_share": float(cur)
                "new_share": float(new_share)
                "target_share": float(tgt)
                "good_streak": int(bs["ramp_streak"])
                "mean_delta": mean_delta
                "tail_enf": float(s_enf["tail_rate"])
                "tail_ctl": float(s_ctl["tail_rate"])
                "n_enf": int(s_enf["n"])
                "n_ctl": int(s_ctl["n"])
            })
            bs["ramp_streak"] = 0
            if new_share >= (tgt - 1e-9):
                patch[f"meta_cov_recovery_target_share_{b}"] = 0.0

    # Persist state (streak counters) unconditionally
    if not args.dry_run:
        save_qstate(r, prefix, st)

    if not patch:
        print(json.dumps({"ok": 1, "skipped": 1, "reason": "no_action", "summary": summary}, ensure_ascii=False))
        return 0

    # Always ensure per-cov mode is ON
    patch["meta_enforce_per_cov"] = 1

    # Min-hold gate (unless emergency quarantine bypass)
    last_change_ms = _i(cfg2.get("meta_cov_rollout_last_change_ms"), 0)
    hold_active = (min_hold_sec > 0 and last_change_ms > 0 and (now_ts - last_change_ms) < (min_hold_sec * 1000))
    has_quarantine = any(k.startswith("meta_cov_quarantine_") and k.endswith(tuple(buckets)) and _i(v, 0) == 1 for k, v in patch.items())
    if hold_active and not (quarantine_bypass_hold and has_quarantine):
        print(json.dumps({"ok": 1, "skipped": 1, "reason": "min_hold_active", "last_change_ms": last_change_ms, "pending_patch": patch, "decisions": decisions}, ensure_ascii=False))
        return 0

    # Build suggestion id
    reason = "cov_outcome_guard_v2"
    key_obj = {"patch": patch, "reason": reason}
    h = hashlib.sha1(json.dumps(key_obj, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()[:12]
    sid = f"cov_guard:{int(now_ts)}:{h}"

    meta = {
        "sid": sid
        "ts_ms": int(now_ts)
        "reason": reason
        "window_hours": float(args.lookback_hours)
        "decisions": decisions
        "summary": summary
        "patch": patch
    }

    if args.dry_run:
        print(json.dumps({"ok": 1, "sid": sid, "decisions": decisions, "patch": patch, "summary": summary, "dry_run": 1}, ensure_ascii=False))
        return 0

    meta_key = f"{prefix}:meta:{sid}"
    approvals_key = f"{prefix}:approvals:{sid}"
    r.set(meta_key, json.dumps(meta, ensure_ascii=False, separators=(",", ":")), ex=7 * 24 * 3600)
    r.set(f"{prefix}:latest", sid, ex=7 * 24 * 3600)

    if auto_approve and auto_approvers:
        for who in auto_approvers:
            r.sadd(approvals_key, who)
        r.expire(approvals_key, 14 * 24 * 3600)

    print(json.dumps({"ok": 1, "sid": sid, "decisions": decisions, "patch": patch}, ensure_ascii=False))

    if int(args.apply) == 1 and run_apply:
        try:
            cmd = [sys.executable, os.path.join(os.path.dirname(__file__), "apply_meta_enforce_cov_suggestion.py"), "--sid", sid]
            subprocess.run(cmd, check=False)
        except Exception as e:
            print(json.dumps({"ok": 0, "sid": sid, "apply_error": str(e)}, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
