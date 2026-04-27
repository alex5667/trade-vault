#!/usr/bin/env python3
"""meta_enforce_guardrails_v1.py

P31: Safety guardrails for meta-model ENFORCE.

Goal
----
If meta-model ENFORCE starts blocking too many signals (often due to a bad threshold,
feature pipeline regression, or miswired schema), automatically switch to fail-open
by setting cfg2:

- meta_model_freeze = 1
- meta_freeze_mode = OPEN

This keeps the system trading (deterministically) while preserving shadow scoring
and evidence for postmortem.

Data source
-----------
Reads recent samples from metrics:of_gate (default). Requires OF_GATE_METRICS_ENABLE=1
and that the OF gate metrics payload includes meta_* fields (added in P31).

Usage
-----
# dry run
python3 -m tools.meta_enforce_guardrails_v1 --lookback-min 30 --apply 0

# apply freeze if triggered
python3 -m tools.meta_enforce_guardrails_v1 --lookback-min 30 --apply 1

Key written (cfg2)
------------------
- meta_enforce_guard_last_change_ms
- meta_enforce_guard_last_decision (json)
- (on trigger) meta_model_freeze=1, meta_freeze_mode=OPEN, meta_freeze_reason, meta_freeze_ts_ms
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import time
from typing import Any, Dict, List, Optional

try:
    import redis  # type: ignore
except Exception:
    redis = None  # type: ignore


def now_ms() -> int:
    return get_ny_time_millis()


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


def _f(x: Any, default: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return default


def _i(x: Any, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default


def read_recent(r: Any, stream: str, since_ms: int, max_scan: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    last_id = "+"
    scanned = 0
    while scanned < max_scan:
        batch = r.xrevrange(stream, max=last_id, min="-", count=2000)
        if not batch:
            break
        for msg_id, fields in batch:
            scanned += 1
            if msg_id == last_id:
                continue
            last_id = msg_id
            d = _parse_entry(fields)
            ts = _i(d.get("ts_ms", d.get("ts", d.get("timestamp", 0))), 0)
            if ts and ts < since_ms:
                return rows
            rows.append(d)
            if scanned >= max_scan:
                break
    return rows


def load_cfg2(r: Any, key: str) -> Dict[str, Any]:
    d = r.hgetall(key) or {}
    out: Dict[str, Any] = {}
    for k, v in d.items():
        out[str(k)] = _loads_maybe_json(v)
    return out


def write_cfg2(r: Any, key: str, patch: Dict[str, Any]) -> None:
    m: Dict[str, str] = {}
    for k, v in patch.items():
        if isinstance(v, (dict, list)):
            m[k] = json.dumps(v, ensure_ascii=False, separators=(",", ":"))
        else:
            m[k] = str(v)
    r.hset(key, mapping=m)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--cfg2-key", default=os.getenv("DYN_CFG_KEY", "settings:dynamic_cfg"))
    ap.add_argument(
        "--stream",
        default=os.getenv("META_GUARD_STREAM", os.getenv("OF_GATE_METRICS_STREAM", "metrics:of_gate")),
    )
    ap.add_argument("--lookback-min", type=int, default=int(os.getenv("META_GUARD_LOOKBACK_MIN", "30") or 30))
    ap.add_argument("--max-scan", type=int, default=int(os.getenv("META_GUARD_MAX_SCAN", "50000") or 50000))
    ap.add_argument("--min-events", type=int, default=int(os.getenv("META_GUARD_MIN_EVENTS", "500") or 500))
    ap.add_argument("--min-canary", type=int, default=int(os.getenv("META_GUARD_MIN_CANARY", "100") or 100))
    ap.add_argument("--block-rate-max", type=float, default=float(os.getenv("META_GUARD_BLOCK_RATE_MAX", "0.80") or 0.80))
    ap.add_argument("--cov-c-ge", type=float, default=float(os.getenv("META_COV_BUCKET_C_GE", "0.90") or 0.90))
    ap.add_argument("--cov-bad-rate-max", type=float, default=float(os.getenv("META_GUARD_COV_BAD_RATE_MAX", "0.50") or 0.50))
    ap.add_argument("--min-hold-sec", type=int, default=int(os.getenv("META_GUARD_MIN_HOLD_SEC", "1800") or 1800))
    ap.add_argument("--apply", type=int, default=0)
    ap.add_argument("--force", type=int, default=0)
    args = ap.parse_args()

    if redis is None:
        print(json.dumps({"ok": False, "reason": "redis_python_not_installed"}, ensure_ascii=False))
        return 2

    r = redis.Redis.from_url(args.redis_url, decode_responses=False)
    try:
        r.ping()
        cfg2 = load_cfg2(r, args.cfg2_key)
    except Exception as e:
        print(json.dumps({"ok": False, "reason": "redis_connection_error", "error": str(e)}, ensure_ascii=False))
        return 1

    meta_enable = _i(cfg2.get("meta_model_enable", 0), 0)
    meta_mode = str(cfg2.get("meta_model_mode", "SHADOW") or "SHADOW").upper()
    meta_freeze = _i(cfg2.get("meta_model_freeze", 0), 0)

    last_change = _i(cfg2.get("meta_enforce_guard_last_change_ms", 0), 0)
    too_soon = (now_ms() - last_change) < int(args.min_hold_sec) * 1000

    decision: Dict[str, Any] = {
        "ts_ms": now_ms(),
        "stream": str(args.stream),
        "lookback_min": int(args.lookback_min),
        "min_events": int(args.min_events),
        "min_canary": int(args.min_canary),
        "block_rate_max": float(args.block_rate_max),
        "cov_c_ge": float(args.cov_c_ge),
        "cov_bad_rate_max": float(args.cov_bad_rate_max),
        "cfg_meta_enable": int(meta_enable),
        "cfg_meta_mode": str(meta_mode),
        "cfg_meta_freeze": int(meta_freeze),
        "min_hold_sec": int(args.min_hold_sec),
        "too_soon": bool(too_soon),
        "trigger": False,
        "reason": "",
    }

    # If not in ENFORCE, do nothing (guard is for ENFORCE only).
    if meta_enable != 1 or meta_mode != "ENFORCE":
        decision["reason"] = "skip:not_enforce"
        print(json.dumps({"ok": True, "apply": bool(args.apply), "decision": decision}, ensure_ascii=False))
        if args.apply:
            write_cfg2(r, args.cfg2_key, {"meta_enforce_guard_last_decision": decision})
        return 0

    # If already frozen, keep it latched.
    if meta_freeze == 1:
        decision["reason"] = "skip:already_frozen"
        decision["trigger"] = True
        print(json.dumps({"ok": True, "apply": bool(args.apply), "decision": decision}, ensure_ascii=False))
        if args.apply:
            write_cfg2(r, args.cfg2_key, {"meta_enforce_guard_last_decision": decision})
        return 0

    if too_soon and not args.force:
        decision["reason"] = "skip:min_hold"
        print(json.dumps({"ok": True, "apply": bool(args.apply), "decision": decision}, ensure_ascii=False))
        if args.apply:
            write_cfg2(r, args.cfg2_key, {"meta_enforce_guard_last_decision": decision})
        return 0

    since_ms = now_ms() - int(args.lookback_min) * 60 * 1000
    rows = read_recent(r, str(args.stream), since_ms=since_ms, max_scan=int(args.max_scan))

    # Filter to meta samples (payload includes meta_enable/meta_mode from evidence).
    frows = []
    for d in rows:
        if str(d.get("meta_mode", "") or "").upper() != "ENFORCE":
            continue
        if _i(d.get("meta_enable", 0), 0) != 1:
            continue
        frows.append(d)

    n = len(frows)
    decision["n"] = int(n)

    if n < int(args.min_events):
        decision["reason"] = "skip:not_enough_events"
        print(json.dumps({"ok": True, "apply": bool(args.apply), "decision": decision}, ensure_ascii=False))
        if args.apply:
            write_cfg2(r, args.cfg2_key, {"meta_enforce_guard_last_decision": decision})
        return 0

    canary = 0
    blocked = 0
    covs_canary: List[float] = []
    cov_bad = 0

    for d in frows:
        if _i(d.get("meta_enforce_applied", 0), 0) == 1:
            canary += 1
            cov = _f(d.get("meta_feature_coverage", float("nan")), float("nan"))
            if cov == cov:
                covs_canary.append(float(cov))
                if cov < float(args.cov_c_ge):
                    cov_bad += 1

            veto = _i(d.get("meta_veto", 0), 0) == 1
            ok = _i(d.get("ok", 0), 0) == 1
            reason = str(d.get("reason", "") or "")
            # Attribute to meta only if reason clearly indicates meta veto.
            if veto and (not ok) and ("meta_veto" in reason):
                blocked += 1

    decision["canary_n"] = int(canary)
    decision["blocked_n"] = int(blocked)

    block_rate = (blocked / float(canary)) if canary > 0 else 0.0
    decision["block_rate"] = float(block_rate)

    cov_bad_rate = (cov_bad / float(canary)) if canary > 0 else 0.0
    decision["cov_bad_rate"] = float(cov_bad_rate)

    if canary < int(args.min_canary):
        decision["reason"] = "skip:not_enough_canary"
        print(json.dumps({"ok": True, "apply": bool(args.apply), "decision": decision}, ensure_ascii=False))
        if args.apply:
            write_cfg2(r, args.cfg2_key, {"meta_enforce_guard_last_decision": decision})
        return 0

    trigger_reasons = []
    if block_rate > float(args.block_rate_max):
        trigger_reasons.append(f"block_rate {block_rate:.3f}>{float(args.block_rate_max):.3f}")
    if cov_bad_rate > float(args.cov_bad_rate_max):
        trigger_reasons.append(f"cov_bad_rate {cov_bad_rate:.3f}>{float(args.cov_bad_rate_max):.3f}")

    if trigger_reasons:
        decision["trigger"] = True
        decision["reason"] = "trigger:" + ",".join(trigger_reasons)
        patch = {
            "meta_enforce_guard_last_change_ms": now_ms(),
            "meta_enforce_guard_last_decision": decision,
            # fail-open latch
            "meta_model_freeze": 1,
            "meta_freeze_mode": "OPEN",
            "meta_freeze_ts_ms": now_ms(),
            "meta_freeze_reason": decision["reason"][:120],
        }
        print(json.dumps({"ok": True, "apply": bool(args.apply), "patch": patch, "decision": decision}, ensure_ascii=False))
        if args.apply:
            write_cfg2(r, args.cfg2_key, patch)
        return 0

    decision["reason"] = "ok"
    print(json.dumps({"ok": True, "apply": bool(args.apply), "decision": decision}, ensure_ascii=False))
    if args.apply:
        write_cfg2(r, args.cfg2_key, {"meta_enforce_guard_last_decision": decision})
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
