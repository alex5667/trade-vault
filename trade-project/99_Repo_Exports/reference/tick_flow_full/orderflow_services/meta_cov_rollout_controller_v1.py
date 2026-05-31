#!/usr/bin/env python3
"""meta_cov_rollout_controller_v1.py

P30: Canary rollout for meta ENFORCE by *feature coverage* buckets.

What it does
------------
- Reads recent events from metrics:of_gate (or any stream via --stream)
- Computes coverage distribution and per-bucket share targets
- Writes cfg2 keys into a Redis hash (default: settings:dynamic_cfg)

Keys written (cfg2)
-------------------
- meta_enforce_per_cov = 1
- meta_cov_bucket_a_ge / _b_ge / _c_ge (thresholds)
- meta_enforce_share_cov_a / _b / _c / _d
- meta_cov_rollout_last_change_ms
- meta_cov_rollout_last_decision (json)

This controller is intentionally conservative:
- no changes unless --apply is passed
- anti-flap via --min-hold-sec (default 10m)

Usage
-----
python3 -m tools.meta_cov_rollout_controller_v1 --lookback-min 60 --apply 0  # default stream=metrics:of_gate
python3 -m tools.meta_cov_rollout_controller_v1 --lookback-min 60 --apply 1
"""

from __future__ import annotations

import argparse
import json
import os
import time
from typing import Any, Dict, List, Optional, Tuple

from orderflow_services.research_guard_blocker_v1 import assert_research_guard_open

try:
    import redis  # type: ignore
except Exception:
    redis = None  # type: ignore


def now_ms() -> int:
    return int(time.time() * 1000)


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
        if isinstance(k, (bytes, bytearray)):
            ks = k.decode("utf-8", "replace")
        else:
            ks = str(k)
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


def pctl(xs: List[float], q: float) -> float:
    if not xs:
        return 0.0
    if q <= 0:
        return min(xs)
    if q >= 1:
        return max(xs)
    xs2 = sorted(xs)
    n = len(xs2)
    pos = (n - 1) * q
    lo = int(pos)
    hi = min(n - 1, lo + 1)
    frac = pos - lo
    return xs2[lo] * (1.0 - frac) + xs2[hi] * frac


def cov_bucket(cov: float, a_ge: float, b_ge: float, c_ge: float) -> str:
    if cov >= a_ge:
        return "a"
    if cov >= b_ge:
        return "b"
    if cov >= c_ge:
        return "c"
    return "d"


def read_metrics(r: Any, stream: str, since_ms: int, max_scan: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    last_id = "+"
    scanned = 0
    while scanned < max_scan:
        batch = r.xrevrange(stream, max=last_id, min="-", count=2000)
        if not batch:
            break
        if len(batch) == 1 and batch[0][0] == last_id:
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
        out[str(k.decode() if isinstance(k, bytes) else k)] = _loads_maybe_json(v)
    return out


def write_cfg2(r: Any, key: str, patch: Dict[str, Any]) -> None:
    m: Dict[str, str] = {}
    for k, v in patch.items():
        if isinstance(v, (dict, list)):
            m[k] = json.dumps(v, ensure_ascii=False, separators=(",", ":"))
        else:
            m[k] = str(v)
    if m:
        r.hset(key, mapping=m)


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--cfg2-key", default=os.getenv("DYN_CFG_KEY", "settings:dynamic_cfg"))
    ap.add_argument("--stream", default=os.getenv("META_COV_SOURCE_STREAM", os.getenv("ML_CONFIRM_METRICS_STREAM", os.getenv("OF_GATE_METRICS_STREAM", "metrics:of_gate"))))
    ap.add_argument("--lookback-min", type=int, default=int(os.getenv("META_COV_ROLLOUT_LOOKBACK_MIN", "60") or 60))
    ap.add_argument("--max-scan", type=int, default=int(os.getenv("META_COV_ROLLOUT_MAX_SCAN", "50000") or 50000))
    ap.add_argument("--apply", type=int, default=0, help="1=write cfg2, 0=print only")
    ap.add_argument("--force", type=int, default=0, help="1=ignore min-hold")
    ap.add_argument("--min-hold-sec", type=int, default=int(os.getenv("META_COV_ROLLOUT_MIN_HOLD_SEC", "600") or 600))
    ap.add_argument("--base-share", type=float, default=float(os.getenv("META_COV_ROLLOUT_BASE_SHARE", "-1") or -1))
    ap.add_argument("--mult-a", type=float, default=float(os.getenv("META_COV_ROLLOUT_MULT_A", "1.0") or 1.0))
    ap.add_argument("--mult-b", type=float, default=float(os.getenv("META_COV_ROLLOUT_MULT_B", "0.75") or 0.75))
    ap.add_argument("--mult-c", type=float, default=float(os.getenv("META_COV_ROLLOUT_MULT_C", "0.25") or 0.25))
    ap.add_argument("--mult-d", type=float, default=float(os.getenv("META_COV_ROLLOUT_MULT_D", "0.0") or 0.0))
    ap.add_argument("--cov-a-ge", type=float, default=float(os.getenv("META_COV_BUCKET_A_GE", "0.98") or 0.98))
    ap.add_argument("--cov-b-ge", type=float, default=float(os.getenv("META_COV_BUCKET_B_GE", "0.95") or 0.95))
    ap.add_argument("--cov-c-ge", type=float, default=float(os.getenv("META_COV_BUCKET_C_GE", "0.90") or 0.90))
    args = ap.parse_args()

    # Research guard hard-gate (P5.2): meta coverage rollout changes live cfg2 shares and is therefore
    # treated as a rollout-sensitive path, blocked when the nightly research guard is unsafe/stale.
    if int(args.apply) == 1 and os.getenv("ENABLE_STRATEGY_RESEARCH_GUARD_HARD_GATE", "0") == "1":
        assert_research_guard_open(
            args.redis_url,
            purpose="meta_cov_rollout_controller",
            stage_mode=False,
        )

    if redis is None:
        print(json.dumps({"ok": False, "reason": "redis_python_not_installed"}, ensure_ascii=False))
        return 2

    r = redis.Redis.from_url(args.redis_url, decode_responses=False)

    since_ms = now_ms() - int(args.lookback_min) * 60 * 1000
    rows = read_metrics(r, args.stream, since_ms=since_ms, max_scan=int(args.max_scan))

    covs: List[float] = []
    bucket_counts = {"a": 0, "b": 0, "c": 0, "d": 0}
    for d in rows:
        cov = None
        if "meta_feature_coverage" in d:
            cov = _f(d.get("meta_feature_coverage"), None)  # type: ignore[arg-type]
        if cov is None or cov != cov:  # NaN
            tot = _i(d.get("meta_model_feature_total"), 0)
            mis = _i(d.get("meta_model_feature_missing"), 0)
            if tot > 0:
                cov = max(0.0, min(1.0, 1.0 - (mis / float(tot))))
        if cov is None:
            continue
        covs.append(float(cov))
        b = cov_bucket(float(cov), args.cov_a_ge, args.cov_b_ge, args.cov_c_ge)
        bucket_counts[b] = bucket_counts.get(b, 0) + 1

    p10 = pctl(covs, 0.10)
    p50 = pctl(covs, 0.50)
    bad_rate = 0.0
    if covs:
        bad_rate = sum(1 for x in covs if x < float(args.cov_c_ge)) / float(len(covs))

    cfg2 = load_cfg2(r, args.cfg2_key)

    cur_last = _i(cfg2.get("meta_cov_rollout_last_change_ms"), 0)
    too_soon = (now_ms() - cur_last) < int(args.min_hold_sec) * 1000

    if args.base_share >= 0:
        base_share = float(args.base_share)
    else:
        base_share = _f(cfg2.get("meta_enforce_share"), 1.0)

    base_share = max(0.0, min(1.0, float(base_share)))

    desired = {
        "meta_enforce_per_cov": 1,
        "meta_cov_bucket_a_ge": float(args.cov_a_ge),
        "meta_cov_bucket_b_ge": float(args.cov_b_ge),
        "meta_cov_bucket_c_ge": float(args.cov_c_ge),
        "meta_enforce_share_cov_a": max(0.0, min(1.0, base_share * float(args.mult_a))),
        "meta_enforce_share_cov_b": max(0.0, min(1.0, base_share * float(args.mult_b))),
        "meta_enforce_share_cov_c": max(0.0, min(1.0, base_share * float(args.mult_c))),
        "meta_enforce_share_cov_d": max(0.0, min(1.0, base_share * float(args.mult_d))),
    }

    decision = {
        "ts_ms": now_ms(),
        "stream": str(args.stream),
        "lookback_min": int(args.lookback_min),
        "n": int(len(covs)),
        "p10": float(p10),
        "p50": float(p50),
        "bad_rate_lt_c": float(bad_rate),
        "bucket_counts": bucket_counts,
        "base_share": float(base_share),
        "desired": desired,
        "min_hold_sec": int(args.min_hold_sec),
        "too_soon": bool(too_soon),
    }

    patch = dict(desired)
    patch["meta_cov_rollout_last_decision"] = decision
    patch["meta_cov_rollout_last_change_ms"] = now_ms()

    ok_to_apply = bool(args.force) or (not too_soon)

    report = {
        "ok": True,
        "apply": bool(int(args.apply) == 1 and ok_to_apply),
        "ok_to_apply": ok_to_apply,
        "reason": "min_hold" if (too_soon and not args.force) else "ok",
        "decision": decision,
        "patch": patch,
    }
    print(json.dumps(report, ensure_ascii=False, indent=2, sort_keys=False))

    if int(args.apply) == 1 and ok_to_apply:
        write_cfg2(r, args.cfg2_key, patch)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
