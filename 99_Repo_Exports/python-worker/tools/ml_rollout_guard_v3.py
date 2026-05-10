from __future__ import annotations

import hashlib
import hmac
import html
import json
import logging
import os
import secrets
import time
from typing import Any

import redis

from core.share_map import clamp_map, dump_map, parse_map
from tools.ml_metrics_agg import agg_health_ml_confirm
from tools.redis_window import read_recent_stream
from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS

log = logging.getLogger(__name__)


def now_ms() -> int:
    """Returns current timestamp in milliseconds (epoch)."""
    return get_ny_time_millis()


def _wait_for_redis(url: str, max_wait_sec: int = 120) -> redis.Redis:
    """Create a Redis connection, retrying until it's ready (handles BusyLoadingError)."""
    deadline = time.monotonic() + max_wait_sec
    attempt = 0
    while True:
        attempt += 1
        try:
            r = redis.Redis.from_url(url, decode_responses=True, socket_timeout=5)
            r.ping()
            return r
        except redis.exceptions.BusyLoadingError:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise
            wait = min(5.0, remaining)
            log.warning("Redis is loading dataset (attempt %d), retrying in %.0fs…", attempt, wait)
            time.sleep(wait)
        except (redis.exceptions.ConnectionError, redis.exceptions.TimeoutError) as exc:
            remaining = deadline - time.monotonic()
            if remaining <= 0:
                raise
            wait = min(5.0, remaining)
            log.warning("Redis not ready (%s, attempt %d), retrying in %.0fs…", exc, attempt, wait)
            time.sleep(wait)


def sign(bundle_id: str, secret: str) -> str:
    """Generate short HMAC signature for bundle_id (8 hex characters)."""
    return hmac.new(secret.encode(), bundle_id.encode(), hashlib.sha256).hexdigest()[:8]


def notify(r: redis.Redis, text: str, buttons=None) -> None:
    """Send notification to notify:telegram stream."""
    fields = {"type": "report", "text": text, "ts": str(now_ms())}
    if buttons is not None:
        fields["buttons"] = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM), fields, maxlen=200000, approximate=True)


def make_bundle(cfg_key: str, changes: dict[str, str], who: str, ttl: int):
    """Create bundle for HSET operations (compatible with recs_callback_worker_v2)."""
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    bid = secrets.token_hex(6)
    sig = sign(bid, secret)
    ts = now_ms()
    ops = [{"op": "HSET", "key": cfg_key, "field": k, "value": str(v)} for k, v in changes.items()]
    bundle = {"id": bid, "created_ms": ts, "ttl_sec": ttl, "who": who, "ops": ops, "meta": {"kind": "ml_rollout_guard_v3"}}
    return bid, sig, bundle


def write_bundle(r: redis.Redis, bid: str, bundle: dict[str, Any], ttl: int) -> None:
    """Write bundle to Redis (compatible with recs_callback_worker_v2)."""
    r.set(f"recs:bundle:{bid}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl)
    r.set(f"recs:status:{bid}", "PENDING", ex=ttl)


def health_ok(h: dict[str, Any], miss_max: float, err_max: float, lat_max: float, min_n: int) -> bool:
    """Check if health metrics pass thresholds."""
    return (h.get("n", 0) >= min_n and h["missing_rate"] <= miss_max and h["err_rate"] <= err_max and h["lat_p99_ms"] <= lat_max)


def main() -> None:
    """Main rollout guard v3: freeze/unfreeze clamps bucket shares + symbol maps to floor."""
    r = _wait_for_redis(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    cfg_key = os.getenv("ML_CONFIRM_CFG_KEY", "cfg:ml_confirm")
    ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)

    pending_key = os.getenv("ML_GUARD_PENDING_KEY", "meta:ml:guard:pending")
    if r.get(pending_key):
        return

    cfg = r.hgetall(cfg_key) or {}

    stream = os.getenv("ML_CONFIRM_METRICS_STREAM", RS.ML_CONFIRM_METRICS)
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
    streak_key = os.getenv("ML_GUARD_GOOD_STREAK_KEY", "meta:ml:guard:good_streak_runs")
    streak = int(float(r.get(streak_key) or 0))

    def share(bucket: str) -> float:
        return float(cfg.get(f"enforce_share_{bucket}", cfg.get("enforce_share", "0.0")) or 0.0)

    cur_t = share("trend")
    cur_r = share("range")

    # parse maps
    map_t = parse_map(cfg.get("enforce_share_trend_by_symbol") or "")
    map_r = parse_map(cfg.get("enforce_share_range_by_symbol") or "")

    if not (ok30 and ok120):
        changes = {
            "enforce_share_trend": f"{min(cur_t, floor):.4f}",
            "enforce_share_range": f"{min(cur_r, floor):.4f}",
            "pre_freeze_share_trend": f"{cur_t:.4f}",
            "pre_freeze_share_range": f"{cur_r:.4f}",
            # freeze symbol maps too (clamp)
            "pre_freeze_share_trend_by_symbol": dump_map(map_t),
            "pre_freeze_share_range_by_symbol": dump_map(map_r),
            "enforce_share_trend_by_symbol": dump_map(clamp_map(map_t, floor)),
            "enforce_share_range_by_symbol": dump_map(clamp_map(map_r, floor)),
            "updated_ms": str(now_ms()),
        }
        bid, sig, bundle = make_bundle(cfg_key, changes, who="ml_guard_v3_freeze", ttl=ttl)
        write_bundle(r, bid, bundle, ttl)
        r.set(pending_key, json.dumps({"bundle_id": bid, "kind": "freeze"}, separators=(",", ":")), ex=ttl)

        buttons = [[
            {"text": "👀 Preview diff", "callback": f"recs:preview2:{bid}:{sig}"},
            {"text": "✅ Confirm apply", "callback": f"recs:confirm:{bid}:{sig}"},
            {"text": "❌ Reject", "callback": f"recs:reject:{bid}:{sig}"},
        ]]
        notify(r,
               "<b>ML Guard v3: FREEZE (dual health windows, clamp symbol maps)</b>\\n"
               f"ok30=<code>{int(ok30)}</code> h30=<code>{html.escape(str(h30))}</code>\\n"
               f"ok120=<code>{int(ok120)}</code> h120=<code>{html.escape(str(h120))}</code>\\n"
               f"bucket shares t/r: <code>{cur_t:.4f}/{cur_r:.4f}</code> -&gt; floor &lt;= <code>{floor}</code>",
               buttons)
        r.set(streak_key, "0", ex=ttl)
        return

    # good run
    streak += 1
    r.set(streak_key, str(streak), ex=ttl)
    needed = good_days * 24
    if streak < needed:
        return

    pre_t = float(cfg.get("pre_freeze_share_trend", f"{cur_t:.4f}") or cur_t)
    pre_r = float(cfg.get("pre_freeze_share_range", f"{cur_r:.4f}") or cur_r)
    pre_map_t = parse_map(cfg.get("pre_freeze_share_trend_by_symbol") or "")
    pre_map_r = parse_map(cfg.get("pre_freeze_share_range_by_symbol") or "")

    changes = {
        "enforce_share_trend": f"{max(cur_t, pre_t):.4f}",
        "enforce_share_range": f"{max(cur_r, pre_r):.4f}",
        "enforce_share_trend_by_symbol": dump_map(pre_map_t if pre_map_t else map_t),
        "enforce_share_range_by_symbol": dump_map(pre_map_r if pre_map_r else map_r),
        "updated_ms": str(now_ms()),
    }
    bid, sig, bundle = make_bundle(cfg_key, changes, who="ml_guard_v3_unfreeze", ttl=ttl)
    write_bundle(r, bid, bundle, ttl)
    r.set(pending_key, json.dumps({"bundle_id": bid, "kind": "unfreeze"}, separators=(",", ":")), ex=ttl)

    buttons = [[
        {"text": "👀 Preview diff", "callback": f"recs:preview2:{bid}:{sig}"},
        {"text": "✅ Confirm apply", "callback": f"recs:confirm:{bid}:{sig}"},
        {"text": "❌ Reject", "callback": f"recs:reject:{bid}:{sig}"},
    ]]
    notify(r,
           "<b>ML Guard v3: UNFREEZE proposal</b>\\n"
           f"good_streak_runs=<code>{streak}</code> need=<code>{needed}</code>\\n"
           f"restore bucket shares t/r: <code>{cur_t:.4f}/{cur_r:.4f}</code> -> <code>{pre_t:.4f}/{pre_r:.4f}</code>",
           buttons)
    r.set(streak_key, "0", ex=ttl)


if __name__ == "__main__":
    main()

