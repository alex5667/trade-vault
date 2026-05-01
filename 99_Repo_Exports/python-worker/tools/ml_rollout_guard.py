from __future__ import annotations
"""ML rollout guard: automatic freeze/unfreeze proposals based on metrics.

Reads metrics:ml_confirm stream and proposes enforce_share changes:
- FREEZE: reduce to min(cur, 0.05) if missing/err/latency are bad
- UNFREEZE: restore pre_freeze_share after N good runs

Uses two-phase proposal system (preview2 -> confirm -> reject) via recs_callback_worker_v2.
"""

from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import time
import hmac
import hashlib
import secrets
from typing import Any, Dict, List

import redis


def now_ms() -> int:
    """Returns current timestamp in milliseconds (epoch)."""
    return get_ny_time_millis()


def sign(bundle_id: str, secret: str) -> str:
    """Generates short HMAC signature for bundle_id (8 hex characters)."""
    d = hmac.new(secret.encode("utf-8"), bundle_id.encode("utf-8"), hashlib.sha256).hexdigest()
    return d[:8]


def notify(r: redis.Redis, text: str, buttons=None) -> None:
    """Send notification to Telegram stream."""
    fields = {"type": "report", "text": text, "ts": str(now_ms())}
    if buttons is not None:
        fields["buttons"] = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"), fields, maxlen=200000, approximate=True)


def read_metrics(r: redis.Redis, stream: str, since_ms: int, max_scan: int) -> List[Dict[str, Any]]:
    """Read metrics from Redis stream since timestamp.
    
    Args:
        r: Redis client
        stream: Stream name
        since_ms: Minimum timestamp in milliseconds
        max_scan: Maximum number of messages to scan
        
    Returns:
        List of metric dicts
    """
    rows = []
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
            ts = 0
            try:
                ts = int(float(fields.get("ts_ms", fields.get("ts", fields.get("timestamp", 0))) or 0))
            except Exception:
                ts = 0
            if ts and ts < since_ms:
                scanned = max_scan
                break
            rows.append(dict(fields))
    return rows


def pctl(xs: List[float], q: float) -> float:
    """Calculate percentile.
    
    Args:
        xs: List of values
        q: Quantile (0.0-1.0)
        
    Returns:
        Percentile value
    """
    if not xs:
        return 0.0
    xs = sorted(xs)
    i = int(round((len(xs)-1)*q))
    i = max(0, min(len(xs)-1, i))
    return float(xs[i])


def summarize(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    """Summarize metrics rows.
    
    Args:
        rows: List of metric dicts
        
    Returns:
        Summary dict with n, p50, p10, lat_p99, err_rate, missing_rate
    """
    n = len(rows)
    if n == 0:
        return {"n": 0.0}
    pedge = []
    lat = []
    err = 0
    miss = 0
    for r in rows:
        pedge.append(float(r.get("p_edge", 0.0) or 0.0))
        # latency: prefer latency_ms; fallback latency_us
        try:
            if r.get("latency_ms") is not None and str(r.get("latency_ms")).strip() != "":
                lat.append(float(r.get("latency_ms", 0.0) or 0.0))
            else:
                lat_us = float(r.get("latency_us", 0.0) or 0.0)
                lat.append(lat_us / 1000.0)
        except Exception:
            lat.append(0.0)

        # err: in stream it's usually a non-empty string
        err_s = str(r.get("err", "") or "").strip()
        err += 1 if err_s != "" else 0

        # missing: either missing flag or status starts with MISSING
        try:
            miss_flag = int(float(r.get("missing", r.get("missing_n", 0)) or 0)) > 0
        except Exception:
            miss_flag = False
        st = str(r.get("status", "") or "").upper()
        miss += 1 if (miss_flag or st.startswith("MISSING")) else 0
    return {
        "n": float(n),
        "p50": pctl(pedge, 0.50),
        "p10": pctl(pedge, 0.10),
        "lat_p99": pctl(lat, 0.99),
        "err_rate": float(err / n) if n > 0 else 0.0,
        "missing_rate": float(miss / n) if n > 0 else 0.0,
    }


def mk_bundle_ops(cfg_key: str, updates: Dict[str, str]) -> List[Dict[str, Any]]:
    """Create bundle operations list.
    
    Args:
        cfg_key: Redis hash key
        updates: Dict of field -> value updates
        
    Returns:
        List of operation dicts
    """
    ops = []
    for k, v in updates.items():
        ops.append({"op": "HSET", "key": cfg_key, "field": k, "value": str(v)})
    return ops


def propose(r: redis.Redis, *, cfg_key: str, updates: Dict[str, str], title: str, details: Dict[str, Any]) -> None:
    """Propose configuration changes via two-phase system.
    
    Args:
        r: Redis client
        cfg_key: Configuration hash key
        updates: Dict of field -> value updates
        title: Proposal title
        details: Proposal details dict
    """
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)
    bundle_id = secrets.token_hex(6)
    sig = sign(bundle_id, secret)
    ts = now_ms()
    bundle = {
        "id": bundle_id,
        "created_ms": ts,
        "ttl_sec": ttl,
        "who": "ml_rollout_guard",
        "ops": mk_bundle_ops(cfg_key, updates),
        "meta": {"title": title, "details": details},
    }
    r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl)
    r.set(f"recs:status:{bundle_id}", "PENDING", ex=ttl)

    buttons = [[
        {"text": "👀 Preview diff", "callback": f"recs:preview2:{bundle_id}:{sig}"},
        {"text": "✅ Confirm apply", "callback": f"recs:confirm:{bundle_id}:{sig}"},
        {"text": "❌ Reject", "callback": f"recs:reject:{bundle_id}:{sig}"},
    ]]
    notify(r, f"<b>{title}</b>\n<code>{json.dumps(details, ensure_ascii=False)[:900]}</code>", buttons=buttons)


def main() -> None:
    """Main guard loop."""
    ap = argparse.ArgumentParser(description="ML rollout guard: freeze/unfreeze proposals")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--cfg-key", default=os.getenv("ML_CONFIRM_CFG_KEY", "cfg:ml_confirm"))
    ap.add_argument("--metrics-stream", default=os.getenv("ML_CONFIRM_METRICS_STREAM", "metrics:ml_confirm"))
    ap.add_argument("--since-min", type=int, default=60, help="Look back N minutes")
    ap.add_argument("--max-scan", type=int, default=200000, help="Max messages to scan")
    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)

    # Thresholds (env with defaults)
    pedge_p50_min = float(os.getenv("ML_SRE_PEDGE_P50_MIN", "0.20") or 0.20)
    miss_max = float(os.getenv("ML_SRE_MISSING_RATE_MAX", "0.02") or 0.02)
    err_max = float(os.getenv("ML_SRE_ERR_RATE_MAX", "0.01") or 0.01)
    lat_p99_max = float(os.getenv("ML_SRE_LAT_P99_MAX_MS", "6.0") or 6.0)

    freeze_floor = float(os.getenv("ML_ROLLOUT_FREEZE_FLOOR", "0.05") or 0.05)
    good_days_need = int(os.getenv("ML_ROLLOUT_GOOD_DAYS_TO_UNFREEZE", "7") or 7)

    # State keys
    streak_key = os.getenv("ML_ROLLOUT_GOOD_STREAK_KEY", "ml:rollout:good_streak_days")
    prefreeze_key = os.getenv("ML_ROLLOUT_PREFREEZE_SHARE_KEY", "ml:rollout:pre_freeze_share")

    since_ms = now_ms() - args.since_min * 60_000
    rows = read_metrics(r, args.metrics_stream, since_ms, args.max_scan)
    sm = summarize(rows)

    # Determine "day good" proxy on window (for timers you'd run daily; but can be run hourly too)
    day_good = (
        sm.get("n", 0.0) >= 200 and
        sm.get("p50", 0.0) >= pedge_p50_min and
        sm.get("missing_rate", 0.0) <= miss_max and
        sm.get("err_rate", 0.0) <= err_max and
        sm.get("lat_p99", 0.0) <= lat_p99_max
    )

    # current enforce_share
    cfg = r.hgetall(args.cfg_key) or {}
    try:
        cur_share = float(cfg.get("enforce_share", cfg.get("canary_share", "1.0")) or 1.0)
    except Exception:
        cur_share = 1.0

    # freeze condition (hard)
    hard_bad = (
        sm.get("missing_rate", 0.0) > miss_max or
        sm.get("err_rate", 0.0) > err_max or
        sm.get("lat_p99", 0.0) > lat_p99_max
    )

    if hard_bad and cur_share > freeze_floor + 1e-9:
        # freeze to min(cur, floor) => set to floor
        r.set(prefreeze_key, str(cur_share), ex=86400*30)
        updates = {
            "enforce_share": str(min(cur_share, freeze_floor)),
            "freeze_reason": "ml_rollout_guard",
            "freeze_ts_ms": str(now_ms())
        }
        propose(r, cfg_key=args.cfg_key, updates=updates,
                title="ML rollout FREEZE (reduce enforce_share)",
                details={"cur_share": cur_share, "new_share": min(cur_share, freeze_floor), "metrics": sm})
        r.set(streak_key, "0", ex=86400*30)
        return

    # update streak
    prev = int(float(r.get(streak_key) or "0"))
    streak = prev + 1 if day_good else 0
    r.set(streak_key, str(streak), ex=86400*30)

    # auto-unfreeze proposal after N good "runs"
    if streak >= good_days_need:
        pre = r.get(prefreeze_key)
        if pre:
            try:
                pre_share = float(pre)
            except Exception:
                pre_share = None
            if pre_share is not None and cur_share < pre_share - 1e-9:
                updates = {
                    "enforce_share": str(pre_share),
                    "freeze_reason": "",
                    "unfreeze_ts_ms": str(now_ms())
                }
                propose(r, cfg_key=args.cfg_key, updates=updates,
                        title="ML rollout UNFREEZE (restore enforce_share)",
                        details={"cur_share": cur_share, "restore_share": pre_share, "metrics": sm, "streak": streak})
                # reset streak to avoid repeated spam
                r.set(streak_key, "0", ex=86400*30)


if __name__ == "__main__":
    main()

