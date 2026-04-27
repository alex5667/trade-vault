from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
import hmac
import hashlib
import secrets
from typing import Any, Dict, List, Tuple

import redis

from tools.redis_window import read_recent_stream
from tools.ml_metrics_agg import agg_health_ml_confirm


def now_ms() -> int:
    """Returns current timestamp in milliseconds (epoch)."""
    return get_ny_time_millis()


def sign(bundle_id: str, secret: str) -> str:
    """Generate short HMAC signature for bundle_id (8 hex characters)."""
    d = hmac.new(secret.encode("utf-8"), bundle_id.encode("utf-8"), hashlib.sha256).hexdigest()
    return d[:8]


def notify(r: redis.Redis, text: str, buttons=None) -> None:
    """Send notification to notify:telegram stream."""
    fields = {"type": "report", "text": text, "ts": str(now_ms())}
    if buttons is not None:
        fields["buttons"] = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"), fields, maxlen=200000, approximate=True)


def make_bundle_hset(cfg_key: str, changes: Dict[str, str], *, who: str, ttl: int) -> Tuple[str, str, Dict[str, Any]]:
    """Create bundle for HSET operations (compatible with recs_callback_worker_v2)."""
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    bundle_id = secrets.token_hex(6)
    sig = sign(bundle_id, secret)
    ts = now_ms()
    ops = [{"op": "HSET", "key": cfg_key, "field": k, "value": str(v)} for k, v in changes.items()]
    bundle = {"id": bundle_id, "created_ms": ts, "ttl_sec": ttl, "who": who, "ops": ops, "meta": {"kind": "ml_rollout_guard_v2"}}
    return bundle_id, sig, bundle


def write_bundle(r: redis.Redis, bundle_id: str, bundle: Dict[str, Any], ttl: int) -> None:
    """Write bundle to Redis (compatible with recs_callback_worker_v2)."""
    r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl)
    r.set(f"recs:status:{bundle_id}", "PENDING", ex=ttl)


def health_ok(h: Dict[str, Any], miss_max: float, err_max: float, lat_max: float, min_n: int) -> bool:
    """Check if health metrics pass thresholds."""
    return (h.get("n", 0) >= min_n and h["missing_rate"] <= miss_max and h["err_rate"] <= err_max and h["lat_p99_ms"] <= lat_max)


def main() -> None:
    """Main rollout guard v2: dual-window health (30m and 2h), freeze floor, auto-unfreeze proposal."""
    r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)
    cfg_key = os.getenv("ML_CONFIRM_CFG_KEY", "cfg:ml_confirm")
    ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)

    pending_key = os.getenv("ML_GUARD_PENDING_KEY", "meta:ml:guard:pending")
    if r.get(pending_key):
        return

    cfg = r.hgetall(cfg_key) or {}

    stream = os.getenv("ML_CONFIRM_METRICS_STREAM", "metrics:ml_confirm")
    max_scan = int(os.getenv("ML_GUARD_MAX_SCAN", "200000") or 200000)

    h30 = agg_health_ml_confirm(read_recent_stream(r, stream, now_ms() - 30 * 60_000, max_scan))
    h120 = agg_health_ml_confirm(read_recent_stream(r, stream, now_ms() - 120 * 60_000, max_scan))

    miss_max = float(os.getenv("ML_SRE_MISSING_RATE_MAX", "0.02") or 0.02)
    err_max = float(os.getenv("ML_SRE_ERR_RATE_MAX", "0.01") or 0.01)
    lat_max = float(os.getenv("ML_SRE_LAT_P99_MAX_MS", "6.0") or 6.0)
    min_n = int(os.getenv("ML_GUARD_MIN_N", "200") or 200)

    ok30 = health_ok(h30, miss_max, err_max, lat_max, min_n)
    ok120 = health_ok(h120, miss_max, err_max, lat_max, min_n)

    floor = float(os.getenv("ML_ROLLOUT_FREEZE_FLOOR", "0.05") or 0.05)
    good_days = int(os.getenv("ML_ROLLOUT_GOOD_DAYS_TO_UNFREEZE", "7") or 7)

    # streak key
    streak_key = os.getenv("ML_GUARD_GOOD_STREAK_KEY", "meta:ml:guard:good_streak_days")
    streak = int(float(r.get(streak_key) or 0))

    # read shares
    def _share(bucket: str) -> float:
        return float(cfg.get(f"enforce_share_{bucket}", cfg.get("enforce_share", "0.0")) or 0.0)

    cur_t = _share("trend")
    cur_r = _share("range")

    # Freeze if any health window bad
    if not (ok30 and ok120):
        changes = {
            "enforce_share_trend": f"{min(cur_t, floor):.4f}",
            "enforce_share_range": f"{min(cur_r, floor):.4f}",
            "updated_ms": str(now_ms()),
        }
        # store pre-freeze
        changes["pre_freeze_share_trend"] = f"{cur_t:.4f}"
        changes["pre_freeze_share_range"] = f"{cur_r:.4f}"

        bid, sig, bundle = make_bundle_hset(cfg_key, changes, who="ml_guard_v2_freeze", ttl=ttl)
        write_bundle(r, bid, bundle, ttl)
        r.set(pending_key, json.dumps({"bundle_id": bid, "kind": "freeze"}, separators=(",", ":")), ex=ttl)

        buttons = [[
            {"text": "👀 Preview diff", "callback": f"recs:preview2:{bid}:{sig}"},
            {"text": "✅ Confirm apply", "callback": f"recs:confirm:{bid}:{sig}"},
            {"text": "❌ Reject", "callback": f"recs:reject:{bid}:{sig}"},
        ]]
        notify(r,
               "<b>ML Guard v2: FREEZE (dual-window health)</b>\\n"
               f"ok30=<code>{int(ok30)}</code> h30=<code>{h30}</code>\\n"
               f"ok120=<code>{int(ok120)}</code> h120=<code>{h120}</code>\\n"
               f"shares trend/range: <code>{cur_t:.4f}/{cur_r:.4f}</code> -> floor <= <code>{floor}</code>",
               buttons)
        # reset streak
        r.set(streak_key, "0", ex=ttl)
        return

    # both windows OK: increment streak daily-ish (guard runs hourly; we convert to "days" by requiring 24 good runs)
    # simple: every run adds 1, and threshold is good_days*24
    streak += 1
    r.set(streak_key, str(streak), ex=ttl)

    needed = good_days * 24
    if streak < needed:
        return

    # unfreeze proposal: restore to pre_freeze shares if present
    pre_t = float(cfg.get("pre_freeze_share_trend", f"{cur_t:.4f}") or cur_t)
    pre_r = float(cfg.get("pre_freeze_share_range", f"{cur_r:.4f}") or cur_r)

    changes = {
        "enforce_share_trend": f"{max(cur_t, pre_t):.4f}",
        "enforce_share_range": f"{max(cur_r, pre_r):.4f}",
        "updated_ms": str(now_ms()),
    }

    bid, sig, bundle = make_bundle_hset(cfg_key, changes, who="ml_guard_v2_unfreeze", ttl=ttl)
    write_bundle(r, bid, bundle, ttl)
    r.set(pending_key, json.dumps({"bundle_id": bid, "kind": "unfreeze"}, separators=(",", ":")), ex=ttl)

    buttons = [[
        {"text": "👀 Preview diff", "callback": f"recs:preview2:{bid}:{sig}"},
        {"text": "✅ Confirm apply", "callback": f"recs:confirm:{bid}:{sig}"},
        {"text": "❌ Reject", "callback": f"recs:reject:{bid}:{sig}"},
    ]]
    notify(r,
           "<b>ML Guard v2: UNFREEZE proposal</b>\\n"
           f"good_streak_runs=<code>{streak}</code> (need {needed})\\n"
           f"restore shares trend/range: <code>{cur_t:.4f}/{cur_r:.4f}</code> -> <code>{pre_t:.4f}/{pre_r:.4f}</code>",
           buttons)

    # reset streak after proposal
    r.set(streak_key, "0", ex=ttl)


if __name__ == "__main__":
    main()

