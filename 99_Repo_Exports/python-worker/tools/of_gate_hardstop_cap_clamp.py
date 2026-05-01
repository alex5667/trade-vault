from __future__ import annotations
"""Emergency cap-clamp: auto-apply upper bounds on meta_enforce_share per-regime when hard-stop persists N cycles.

Reads metrics:of_gate for a time window, detects hard-stop conditions (latency, exec_risk, soft_rate, ok_rate),
tracks streak (consecutive hard-stop cycles), and when streak >= N:
  - Auto-applies bundle: clamps meta_enforce_share_{bucket} to caps (trend≤0.10, range≤0.05, news=0, other=0)
  - Ensures meta_enforce_per_regime=1 (fail-safe)
  - Writes audit log for rollback via recs_callback_worker
  - Sends Telegram notification with Rollback button

This is a "next level" safety mechanism that works alongside v5 Stage2 degrade-only:
  - v5 Stage2: degrade-only (no increases) on hard-stop
  - This clamp: emergency upper bounds to quickly fix shares at safe levels

Usage:
  python -m tools.of_gate_hardstop_cap_clamp
  (reads ENV vars for thresholds, streak N, caps, symbols, cooldown)

Environment Variables:
  - Metrics: OF_GATE_METRICS_STREAM, META_HARDSTOP_WINDOW_MIN, META_HARDSTOP_METRICS_MAX_SCAN, META_HARDSTOP_MIN_N
  - Hard-stop thresholds: META_HARDSTOP_LAT_P99_US, META_HARDSTOP_EXEC_P90, META_HARDSTOP_SOFT_RATE, META_HARDSTOP_OK_RATE_MIN
  - Streak: META_HARDSTOP_STREAK_N, META_HARDSTOP_STREAK_KEY, META_HARDSTOP_LAST_MS_KEY
  - Clamp state: META_CLAMP_ACTIVE_KEY, META_CLAMP_COOLDOWN_SEC
  - Caps: META_CLAMP_CAP_TREND, META_CLAMP_CAP_RANGE, META_CLAMP_CAP_NEWS, META_CLAMP_CAP_OTHER
  - Symbols: META_CLAMP_SYMBOLS (or CANARY_SYMBOLS if empty)
  - Rec/bot: NOTIFY_TELEGRAM_STREAM, RECS_TTL_SEC, RECS_HMAC_SECRET, CFG_HASH_PREFIX
"""

from utils.time_utils import get_ny_time_millis

import os
import time
import json
import html
import hmac
import hashlib
import secrets
from typing import Any, Dict, List, Tuple

import redis

from common.log import setup_logger
from common.redis_errors import retry_redis_operation
from core.redis_client import get_redis, wait_for_redis
from core.ok_fields import parse_ok_fields, get_ts_ms

logger = setup_logger("OfGateHardstopCapClamp")


def now_ms() -> int:
    """Returns current timestamp in milliseconds (epoch)."""
    return get_ny_time_millis()


def pctl(xs: List[float], q: float) -> float:
    """Computes percentile q (0.0-1.0) from sorted list xs."""
    if not xs:
        return 0.0
    xs = sorted(xs)
    i = int(round((len(xs) - 1) * q))
    i = max(0, min(len(xs) - 1, i))
    return float(xs[i])


def _f(x: Any, d: float = 0.0) -> float:
    """Converts value to float with default."""
    try:
        return float(x)
    except Exception:
        return float(d)


def _i(x: Any, d: int = 0) -> int:
    """Converts value to int with default."""
    try:
        return int(float(x))
    except Exception:
        return int(d)


def sign(bundle_id: str, secret: str) -> str:
    """Computes HMAC-SHA256 signature for bundle_id (first 8 hex chars)."""
    d = hmac.new(secret.encode("utf-8"), bundle_id.encode("utf-8"), hashlib.sha256).hexdigest()
    return d[:8]


def read_metrics_window(r: redis.Redis, stream: str, since_ms: int, max_scan: int) -> List[Dict[str, Any]]:
    """
    Reads metrics from Redis stream within time window.
    
    Args:
        r: Redis client
        stream: Stream name (e.g., "metrics:of_gate")
        since_ms: Start timestamp (epoch ms)
        max_scan: Maximum number of messages to scan
        
    Returns:
        List of metric records (dict with fields + _ts_ms)
    """
    rows: List[Dict[str, Any]] = []
    last_id = "+"
    scanned = 0
    while scanned < max_scan:
        # Retry xrevrange with exponential backoff and jitter on BusyLoadingError
        batch = retry_redis_operation(
            operation=lambda: r.xrevrange(stream, max=last_id, min="-", count=2000),
            operation_name="read_metrics_window",
            max_retries=10,
            base_delay=1.0,
            max_delay=30.0,
            logger_instance=logger,
        )
        if not batch:
            break
        for msg_id, fields in batch:
            scanned += 1
            if msg_id == last_id:
                continue
            last_id = msg_id

            ts = get_ts_ms(fields)

            if ts and ts < since_ms:
                scanned = max_scan
                break

            row = dict(fields)
            row["_ts_ms"] = ts
            rows.append(row)

    rows.reverse()
    return rows


def summarize_health(rows: List[Dict[str, Any]]) -> Dict[str, float]:
    """
    Summarizes health metrics from metric rows.
    
    Args:
        rows: List of metric records
        
    Returns:
        Dict with: n, ok_rate, soft_rate, lat_p99_us, exec_p90
    """
    n = len(rows)
    if n == 0:
        return {"n": 0.0}

    ok = 0
    soft = 0
    lat = []
    ex = []

    for r in rows:
        ok_i, soft_i = parse_ok_fields(r)
        ok += 1 if ok_i == 1 else 0
        soft += 1 if soft_i == 1 else 0
        lat.append(_f(r.get("latency_us", 0.0), 0.0))
        ex.append(_f(r.get("exec_risk_norm", 0.0), 0.0))

    return {
        "n": float(n),
        "ok_rate": float(ok / n) if n > 0 else 0.0,
        "soft_rate": float(soft / n) if n > 0 else 0.0,
        "lat_p99_us": float(pctl(lat, 0.99)),
        "exec_p90": float(pctl(ex, 0.90)),
    }


def hard_stop(health: Dict[str, float]) -> Tuple[bool, List[str]]:
    """
    Determines if hard-stop conditions are met (fail-closed by default).
    
    Args:
        health: Health summary dict (from summarize_health)
        
    Returns:
        (is_hard_stop, list_of_reasons)
    """
    reasons = []

    n = float(health.get("n", 0.0))
    lat_p99 = float(health.get("lat_p99_us", 0.0))
    exec_p90 = float(health.get("exec_p90", 0.0))
    soft = float(health.get("soft_rate", 0.0))
    ok = float(health.get("ok_rate", 0.0))

    min_n = int(os.getenv("META_HARDSTOP_MIN_N", "200") or 200)
    if n < min_n:
        reasons.append(f"low_n<{min_n}")

    lat_thr = float(os.getenv("META_HARDSTOP_LAT_P99_US", "12000") or 12000)
    exec_thr = float(os.getenv("META_HARDSTOP_EXEC_P90", "0.92") or 0.92)
    soft_thr = float(os.getenv("META_HARDSTOP_SOFT_RATE", "0.60") or 0.60)
    ok_min = float(os.getenv("META_HARDSTOP_OK_RATE_MIN", "0.10") or 0.10)

    if lat_p99 > lat_thr:
        reasons.append(f"lat_p99_us>{lat_thr}")
    if exec_p90 > exec_thr:
        reasons.append(f"exec_p90>{exec_thr}")
    if soft > soft_thr:
        reasons.append(f"soft_rate>{soft_thr}")
    if ok < ok_min:
        reasons.append(f"ok_rate<{ok_min}")

    return (len(reasons) > 0), reasons


def apply_emergency_bundle(
    r: redis.Redis,
    *,
    symbols: List[str],
    cfg_prefix: str,
    secret: str,
    ttl_sec: int,
    caps: Dict[str, float],
) -> Tuple[str, str]:
    """
    Auto-apply HSET changes, write audit, mark bundle APPLIED, return (bundle_id, sig).
    
    Rollback is expected to use recs:audit:<bundle_id> via recs_callback_worker.
    
    Args:
        r: Redis client
        symbols: List of symbols to clamp
        cfg_prefix: Config hash prefix (e.g., "config:orderflow:")
        secret: HMAC secret for bundle signature
        ttl_sec: TTL for bundle/audit/status keys
        caps: Dict of bucket -> cap value (e.g., {"trend": 0.10, "range": 0.05, "news": 0.00, "other": 0.00})
        
    Returns:
        (bundle_id, signature)
    """
    bundle_id = secrets.token_hex(6)
    sig = sign(bundle_id, secret)
    ts = now_ms()

    # Build ops: per-regime shares clamp (never increase)
    ops: List[Dict[str, str]] = []
    audit: List[Dict[str, Any]] = []

    for sym in symbols:
        hk = f"{cfg_prefix}{sym}"

        # ensure per-regime logic is ON (fail-safe)
        ops.append({"op": "HSET", "key": hk, "field": "meta_enforce_per_regime", "value": "1"})

        for bucket, cap in caps.items():
            field = f"meta_enforce_share_{bucket}"
            old = r.hget(hk, field)
            try:
                oldf = float(old) if old is not None else None
            except Exception:
                oldf = None

            # clamp: new = min(old, cap), if old missing -> cap (fail-closed clamp)
            newv = min(oldf, cap) if oldf is not None else cap
            newv = max(0.0, min(1.0, float(newv)))

            ops.append({"op": "HSET", "key": hk, "field": field, "value": f"{newv:.2f}"})

    # Apply ops + audit
    pipe = r.pipeline()
    for op in ops:
        key = op["key"]
        field = op["field"]
        newv = op["value"]
        old = r.hget(key, field)
        audit.append({
            "op": "HSET",
            "key": key,
            "field": field,
            "old": ("" if old is None else str(old)),
            "old_null": (1 if old is None else 0),
            "new": newv,
            "ts_ms": ts,
            "who": "of_gate_hardstop_cap_clamp",
        })
        pipe.hset(key, field, newv)
    pipe.execute()

    # Store bundle + audit for rollback
    bundle = {
        "id": bundle_id,
        "created_ms": ts,
        "ttl_sec": ttl_sec,
        "who": "of_gate_hardstop_cap_clamp",
        "ops": ops,
        "meta": {
            "kind": "meta_hardstop_cap_clamp",
            "caps": caps,
            "symbols": symbols,
        },
    }
    r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl_sec)
    r.set(f"recs:status:{bundle_id}", "APPLIED", ex=ttl_sec)

    for a in audit:
        r.rpush(f"recs:audit:{bundle_id}", json.dumps(a, ensure_ascii=False, separators=(",", ":")))
    r.expire(f"recs:audit:{bundle_id}", ttl_sec)

    return bundle_id, sig


def main() -> None:
    """Main entry point: reads metrics, checks hard-stop, updates streak, applies clamp if needed."""
    try:
        r = get_redis(retry_attempts=10, retry_delay=2)
        # Wait for Redis to be fully ready (handles BusyLoading)
        logger.info("Waiting for Redis to be ready...")
        if not wait_for_redis(r, max_retries=30, delay=10.0):
            logger.error("Redis is still loading after maximum wait time")
            raise RuntimeError("Redis is not ready after waiting")
        logger.info("Redis is ready")
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        raise

    # Settings
    metrics_stream = os.getenv("OF_GATE_METRICS_STREAM", "metrics:of_gate")
    window_min = float(os.getenv("META_HARDSTOP_WINDOW_MIN", "30") or 30)
    max_scan = int(os.getenv("META_HARDSTOP_METRICS_MAX_SCAN", "200000") or 200000)

    streak_key = os.getenv("META_HARDSTOP_STREAK_KEY", "meta:hardstop:streak")
    last_key = os.getenv("META_HARDSTOP_LAST_MS_KEY", "meta:hardstop:last_ms")
    clamp_active_key = os.getenv("META_CLAMP_ACTIVE_KEY", "meta:hardstop:clamp:active")
    clamp_cooldown_sec = int(os.getenv("META_CLAMP_COOLDOWN_SEC", "21600") or 21600)  # 6h

    need_streak = int(os.getenv("META_HARDSTOP_STREAK_N", "3") or 3)

    cfg_prefix = os.getenv("CFG_HASH_PREFIX", "config:orderflow:")
    ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    notify_stream = os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram")

    # Caps (upper bounds)
    cap_trend = float(os.getenv("META_CLAMP_CAP_TREND", "0.10") or 0.10)
    cap_range = float(os.getenv("META_CLAMP_CAP_RANGE", "0.05") or 0.05)
    cap_news = float(os.getenv("META_CLAMP_CAP_NEWS", "0.00") or 0.00)
    cap_other = float(os.getenv("META_CLAMP_CAP_OTHER", "0.00") or 0.00)

    caps = {"trend": cap_trend, "range": cap_range, "news": cap_news, "other": cap_other}

    # Symbols
    sym_csv = (os.getenv("META_CLAMP_SYMBOLS", "") or "").strip() or (os.getenv("CANARY_SYMBOLS", "") or "").strip()
    symbols = [s.strip().upper() for s in sym_csv.split(",") if s.strip()]
    if not symbols:
        # nothing to clamp
        logger.info("No symbols configured (META_CLAMP_SYMBOLS or CANARY_SYMBOLS), skipping")
        return

    # Read health
    since_ms = now_ms() - int(window_min * 60_000)
    rows = read_metrics_window(r, metrics_stream, since_ms, max_scan=max_scan)
    health = summarize_health(rows)
    hs, reasons = hard_stop(health)

    # Update streak
    prev_streak = _i(r.get(streak_key), 0)
    if hs:
        streak = prev_streak + 1
    else:
        streak = 0
    r.set(streak_key, str(streak), ex=ttl)
    r.set(last_key, str(now_ms()), ex=ttl)

    logger.info(f"Hard-stop check: streak={streak}/{need_streak}, is_hard_stop={hs}, reasons={reasons}, health={health}")

    # If not hard-stop → nothing
    if not hs:
        return

    # Check cooldown / already active
    active = r.get(clamp_active_key)
    if active:
        # already clamped; do not spam
        logger.info(f"Clamp already active (bundle_id={active}), skipping")
        return

    # Cooldown since last clamp (optional)
    last_clamp_ms = _i(r.get("meta:hardstop:clamp:last_ms"), 0)
    if last_clamp_ms and (now_ms() - last_clamp_ms) < clamp_cooldown_sec * 1000:
        logger.info(f"Cooldown active (last_clamp_ms={last_clamp_ms}, cooldown_sec={clamp_cooldown_sec}), skipping")
        return

    if streak < need_streak:
        logger.info(f"Streak {streak} < {need_streak}, not applying clamp yet")
        return

    # Apply clamp (auto)
    logger.info(f"Applying emergency cap-clamp: streak={streak}, symbols={symbols}, caps={caps}")
    bundle_id, sig = apply_emergency_bundle(
        r,
        symbols=symbols,
        cfg_prefix=cfg_prefix,
        secret=secret,
        ttl_sec=ttl,
        caps=caps,
    )

    r.set(clamp_active_key, bundle_id, ex=ttl)
    r.set("meta:hardstop:clamp:last_ms", str(now_ms()), ex=ttl)

    # Notify with rollback button
    buttons = [[
        {"text": "↩ Rollback clamp", "callback": f"recs:rollback:{bundle_id}:{sig}"},
    ]]

    # Properly escape HTML and serialize complex data structures
    # Telegram HTML parse mode requires proper escaping of special characters
    caps_str = html.escape(json.dumps(caps, ensure_ascii=False, separators=(",", ":")))
    reasons_str = html.escape(json.dumps(reasons, ensure_ascii=False, separators=(",", ":")))
    health_str = html.escape(json.dumps(health, ensure_ascii=False, separators=(",", ":")))
    symbols_str = html.escape(",".join(symbols))
    
    txt = (
        "<b>HARD-STOP CAP CLAMP APPLIED</b>\n"
        f"id=<code>{html.escape(bundle_id)}</code>\n"
        f"streak=<code>{streak}/{need_streak}</code>\n"
        f"symbols=<code>{symbols_str}</code>\n"
        f"caps=<code>{caps_str}</code>\n"
        f"reasons=<code>{reasons_str}</code>\n"
        f"health=<code>{health_str}</code>\n"
        "<i>ENFORCE-share clamped (upper bounds). Use Rollback to revert.</i>"
    )

    r.xadd(notify_stream, {
        "type": "report",
        "text": txt,
        "buttons": json.dumps(buttons, ensure_ascii=False, separators=(",", ":")),
        "ts": str(now_ms()),
    }, maxlen=200000, approximate=True)

    logger.info(f"Emergency cap-clamp applied: bundle_id={bundle_id}, sig={sig}")


if __name__ == "__main__":
    main()

