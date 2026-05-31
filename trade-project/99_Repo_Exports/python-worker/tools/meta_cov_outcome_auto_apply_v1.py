#!/usr/bin/env python3
from __future__ import annotations

from domain.evidence_keys import MetaKeys
from core.redis_keys import RedisStreams as RS

"""
meta_cov_outcome_auto_apply_v1.py

P33: Auto-downgrade (and optionally auto-apply) meta ENFORCE per-coverage bucket shares,
based on *realized outcomes* from POSITION_CLOSED events.

High-level behavior
-------------------
1) Reads recent closed trades from Redis stream (events:trades by default).
2) Groups returns by (meta_enforce_cov_bucket) and compares:
   - enforce subset (meta_enforce_applied=1)
   - control subset (meta_enforce_applied=0)
3) If enforce underperforms materially or tail risk rises, produces a cfg2 patch that
   *reduces* meta_enforce_share_cov_{bucket} (downgrade).
4) Writes the suggestion into Redis (meta + approvals) and, if enabled, runs the applier.

Key format (Redis)
------------------
PREFIX = cfg:suggestions:meta_enforce_cov (default)

- {PREFIX}:latest
- {PREFIX}:meta:{sid}              (JSON)
- {PREFIX}:approvals:{sid}         (SET)
- {PREFIX}:applied:{sid}           (JSON)   # written by applier

ENV
  REDIS_URL (default redis://localhost:6379/0)
  TRADE_EVENTS_STREAM (default events:trades)

  DYN_CFG_KEY (default settings:dynamic_cfg)

  META_ENFORCE_COV_PREFIX (default cfg:suggestions:meta_enforce_cov)
  META_ENFORCE_COV_AUTO_APPROVE (default 1)
  META_ENFORCE_COV_AUTO_APPROVERS (default auto_guard_1,auto_guard_2)
  META_ENFORCE_COV_RUN_APPLY (default 1)         # when --apply=1
  META_ENFORCE_COV_APPROVALS_REQUIRED (default 2)

  META_COV_OUTCOME_LOOKBACK_HOURS (default 72)
  META_COV_OUTCOME_MIN_N_ENFORCE (default 30)
  META_COV_OUTCOME_MIN_N_CONTROL (default 30)

  META_COV_OUTCOME_TAIL_THRESH (default 0.35)        # tail_rate(enforce) > => downgrade
  META_COV_OUTCOME_TAIL_DELTA_THRESH (default 0.08)  # (tail_enf - tail_ctl) > => downgrade
  META_COV_OUTCOME_MEAN_DELTA_THRESH (default -0.10) # (mean_enf - mean_ctl) < => downgrade

  META_COV_OUTCOME_DOWN_STEP (default 0.10)          # share -= step
  META_COV_OUTCOME_PANIC_TAIL (default 0.45)         # severe -> share=0

  META_COV_MIN_HOLD_SEC (default 1800)               # fast skip if changed recently
"""

import argparse
import hashlib
import json
import os
import subprocess
import sys
from typing import Any

from utils.time_utils import get_ny_time_millis

try:
    import redis  # type: ignore
except Exception:
    redis = None  # type: ignore


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


def _parse_entry(fields: dict[Any, Any]) -> dict[str, Any]:
    out: dict[str, Any] = {}
    payload_obj: dict[str, Any] | None = None
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


def _is_position_closed(fields: dict[str, Any]) -> bool:
    et = str(fields.get("event_type") or fields.get("event") or "").upper()
    if et in ("POSITION_CLOSED", "CLOSE"):
        return True
    if not et and fields.get("exit_ts_ms") and ("pnl" in fields or "pnl_net" in fields):
        return True
    if not et and fields.get("close_ts_ms") and "r_mult" in fields:
        return True
    return False


def _event_ts_ms(fields: dict[str, Any]) -> int:
    return _i(fields.get("ts_ms") or fields.get("ts") or fields.get("exit_ts_ms") or fields.get("close_ts_ms") or fields.get("timestamp") or 0, 0)


def _redis() -> Any:
    if redis is None:
        raise RuntimeError("redis library is not available")
    url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
    return redis.Redis.from_url(url, decode_responses=True)


def read_closed_trades(
    *,
    r: Any,
    stream: str,
    since_ms: int,
    max_scan: int = 500_000,
) -> list[dict[str, Any]]:
    out: list[dict[str, Any]] = []
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


def tail_rate(rs: list[float]) -> float:
    if not rs:
        return 0.0
    return sum(1 for x in rs if x <= -1.0) / float(len(rs))


def mean(rs: list[float]) -> float:
    if not rs:
        return 0.0
    return sum(rs) / float(len(rs))


def summarize(rs: list[float]) -> dict[str, Any]:
    return {
        "n": int(len(rs)),
        "meanR": float(mean(rs)),
        "tail_rate": float(tail_rate(rs)),
    }


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--apply", type=int, default=0, help="1 to run applier (if enabled), 0 to only emit suggestion")
    ap.add_argument("--lookback-hours", type=float, default=float(os.environ.get("META_COV_OUTCOME_LOOKBACK_HOURS", "72") or 72))
    ap.add_argument("--max-scan", type=int, default=500_000)
    # Standard SRE flags (ignored but accepted)
    ap.add_argument("--emit-metrics", action="store_true")
    ap.add_argument("--notify", action="store_true")
    args = ap.parse_args()

    prefix = os.environ.get("META_ENFORCE_COV_PREFIX", "cfg:suggestions:meta_enforce_cov")
    dyn_key = os.environ.get("DYN_CFG_KEY", "settings:dynamic_cfg")
    stream = os.environ.get("TRADE_EVENTS_STREAM", RS.EVENTS_TRADES)

    auto_approve = bool(int(os.environ.get("META_ENFORCE_COV_AUTO_APPROVE", "1") or 0))
    auto_approvers = [x.strip() for x in (os.environ.get("META_ENFORCE_COV_AUTO_APPROVERS", "auto_guard_1,auto_guard_2") or "").split(",") if x.strip()]
    run_apply = bool(int(os.environ.get("META_ENFORCE_COV_RUN_APPLY", "1") or 0))

    min_n_enf = int(os.environ.get("META_COV_OUTCOME_MIN_N_ENFORCE", "30") or 30)
    min_n_ctl = int(os.environ.get("META_COV_OUTCOME_MIN_N_CONTROL", "30") or 30)

    tail_thresh = float(os.environ.get("META_COV_OUTCOME_TAIL_THRESH", "0.35") or 0.35)
    tail_delta_thresh = float(os.environ.get("META_COV_OUTCOME_TAIL_DELTA_THRESH", "0.08") or 0.08)
    mean_delta_thresh = float(os.environ.get("META_COV_OUTCOME_MEAN_DELTA_THRESH", "-0.10") or -0.10)

    down_step = float(os.environ.get("META_COV_OUTCOME_DOWN_STEP", "0.10") or 0.10)
    panic_tail = float(os.environ.get("META_COV_OUTCOME_PANIC_TAIL", "0.45") or 0.45)

    min_hold_sec = int(os.environ.get("META_COV_MIN_HOLD_SEC", "1800") or 1800)

    r = _redis()
    cfg2 = r.hgetall(dyn_key) or {}
    last_change_ms = _i(cfg2.get("meta_cov_rollout_last_change_ms"), 0)
    now_ts = now_ms()
    if min_hold_sec > 0 and last_change_ms > 0 and (now_ts - last_change_ms) < (min_hold_sec * 1000):
        print(json.dumps({"ok": 1, "skipped": 1, "reason": "min_hold_active", "last_change_ms": last_change_ms}))
        return 0

    since_ms = now_ts - int(args.lookback_hours * 3600 * 1000)
    rows = read_closed_trades(r=r, stream=stream, since_ms=since_ms, max_scan=args.max_scan)

    # Group returns by cov bucket and enforce/control
    buckets = ["a", "b", "c", "d"]
    enf: dict[str, list[float]] = {b: [] for b in buckets}
    ctl: dict[str, list[float]] = {b: [] for b in buckets}

    for f in rows:
        b = (f.get(MetaKeys.ENFORCE_COV_BUCKET) or "").strip().lower()
        if b not in enf:
            continue
        applied = _i(f.get(MetaKeys.ENFORCE_APPLIED), 0)
        r_mult = _f(f.get("r_mult") or f.get("r_multiple") or 0.0, 0.0)
        if r_mult == 0.0:
            # fallback: pnl/risk (if present)
            pnl = _f(f.get("pnl") or f.get("pnl_net") or 0.0, 0.0)
            risk = _f(f.get("risk_usd") or 0.0, 0.0)
            if risk > 0:
                r_mult = pnl / risk
        if applied == 1:
            enf[b].append(float(r_mult))
        else:
            ctl[b].append(float(r_mult))

    decisions: list[dict[str, Any]] = []
    patch: dict[str, Any] = {}
    summary: dict[str, Any] = {}

    for b in buckets:
        s_enf = summarize(enf[b])
        s_ctl = summarize(ctl[b])
        summary[b] = {"enf": s_enf, "ctl": s_ctl}
        if s_enf["n"] < min_n_enf or s_ctl["n"] < min_n_ctl:
            continue

        mean_delta = float(s_enf["meanR"]) - float(s_ctl["meanR"])
        tail_delta = float(s_enf["tail_rate"]) - float(s_ctl["tail_rate"])
        bad = (float(s_enf["tail_rate"]) > tail_thresh) or (tail_delta > tail_delta_thresh) or (mean_delta < mean_delta_thresh)

        if not bad:
            continue

        cur_share = _f(cfg2.get(f"meta_enforce_share_cov_{b}") or cfg2.get(MetaKeys.ENFORCE_SHARE) or 1.0, 1.0)
        cur_share = max(0.0, min(1.0, float(cur_share)))
        severe = float(s_enf["tail_rate"]) >= panic_tail
        new_share = 0.0 if severe else max(0.0, cur_share - down_step)
        if new_share >= cur_share:
            continue

        decisions.append({
            "bucket": b,
            "cur_share": cur_share,
            "new_share": new_share,
            "mean_delta": mean_delta,
            "tail_delta": tail_delta,
            "tail_enf": float(s_enf["tail_rate"]),
            "tail_ctl": float(s_ctl["tail_rate"]),
            "n_enf": int(s_enf["n"]),
            "n_ctl": int(s_ctl["n"]),
            "severe": int(severe),
        })
        patch[f"meta_enforce_share_cov_{b}"] = float(new_share)

    if not patch:
        print(json.dumps({"ok": 1, "skipped": 1, "reason": "no_downgrade", "summary": summary}, ensure_ascii=False))
        return 0

    # Always ensure per-cov mode is ON when we are managing buckets
    patch["meta_enforce_per_cov"] = 1

    # Build suggestion id (stable-ish within minute, but unique across applies)
    key_obj = {"patch": patch, "reason": "cov_outcome_downgrade"}
    h = hashlib.sha1(json.dumps(key_obj, sort_keys=True, separators=(",", ":")).encode("utf-8")).hexdigest()[:12]
    sid = f"cov_outcome:{int(now_ts)}:{h}"

    meta = {
        "sid": sid,
        "ts_ms": int(now_ts),
        "reason": "cov_outcome_downgrade",
        "window_hours": float(args.lookback_hours),
        "decisions": decisions,
        "summary": summary,
        "patch": patch,
    }

    # Write suggestion + latest pointer
    meta_key = f"{prefix}:meta:{sid}"
    approvals_key = f"{prefix}:approvals:{sid}"
    r.set(meta_key, json.dumps(meta, ensure_ascii=False, separators=(",", ":")), ex=7 * 24 * 3600)
    r.set(f"{prefix}:latest", sid, ex=7 * 24 * 3600)

    if auto_approve and auto_approvers:
        for who in auto_approvers:
            r.sadd(approvals_key, who)
        r.expire(approvals_key, 14 * 24 * 3600)

    out = {"ok": 1, "sid": sid, "decisions": decisions, "patch": patch}
    print(json.dumps(out, ensure_ascii=False))

    # Optionally run applier
    if int(args.apply) == 1 and run_apply:
        try:
            cmd = [sys.executable, os.path.join(os.path.dirname(__file__), "apply_meta_enforce_cov_suggestion.py"), "--sid", sid]
            subprocess.run(cmd, check=False)
        except Exception as e:
            print(json.dumps({"ok": 0, "sid": sid, "apply_error": str(e)}, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
