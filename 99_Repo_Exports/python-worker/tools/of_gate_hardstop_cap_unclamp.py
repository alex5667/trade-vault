from __future__ import annotations
"""Staged auto-unclamp with hysteresis: relax then fully restore pre-clamp values.

This is the next level above auto-clamp: when hard-stop disappears and health holds,
gradually restore caps to pre-clamp values.

Stage A (RELAX): when hard-stop is gone and health holds RELAX_N cycles → partially
  restore caps (e.g., trend≤0.25, range≤0.15) not higher than pre-clamp (from clamp audit).

Stage B (UNCLAMP): when health holds UNCLAMP_N cycles → fully restore pre-clamp values
  from clamp audit and remove active flag.

All actions are auto-applied, but Telegram sends Rollback button (via recs_callback_worker).

Usage:
  python -m tools.of_gate_hardstop_cap_unclamp
  (reads ENV vars for thresholds, streak N, relax caps, cooldown)

Environment Variables:
  - Metrics: OF_GATE_METRICS_STREAM, META_HARDSTOP_WINDOW_MIN, META_HARDSTOP_METRICS_MAX_SCAN, META_HARDSTOP_MIN_N
  - Hard-stop thresholds: META_HARDSTOP_LAT_P99_US, META_HARDSTOP_EXEC_P90, META_HARDSTOP_SOFT_RATE, META_HARDSTOP_OK_RATE_MIN
  - Clamp state: META_CLAMP_ACTIVE_KEY, META_CLAMP_STAGE_KEY, META_HEALTHY_STREAK_KEY
  - Unclamp hysteresis: META_UNCLAMP_RELAX_STREAK_N, META_UNCLAMP_REMOVE_STREAK_N, META_UNCLAMP_ACTION_COOLDOWN_SEC
  - Relax caps: META_RELAX_CAP_TREND, META_RELAX_CAP_RANGE, META_RELAX_CAP_NEWS, META_RELAX_CAP_OTHER
  - Rec/bot: NOTIFY_TELEGRAM_STREAM, RECS_TTL_SEC, RECS_HMAC_SECRET
"""

from utils.time_utils import get_ny_time_millis

import os
import time
import json
import hmac
import hashlib
import secrets
from typing import Any, Dict, List, Tuple

import redis

from common.log import setup_logger

logger = setup_logger("OfGateHardstopCapUnclamp")


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

            try:
                ts = int(float(fields.get("ts_ms", fields.get("ts", fields.get("timestamp", 0))) or 0))
            except Exception:
                ts = 0

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
        ok += 1 if _i(r.get("ok", 0), 0) == 1 else 0
        soft += 1 if _i(r.get("ok_soft", 0), 0) == 1 else 0
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
    Hard-stop = unhealthy. If low_n -> fail-closed (treat as hard-stop).
    
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


def _notify(r: redis.Redis, text: str, buttons: List[List[Dict[str, str]]] | None = None) -> None:
    """Sends notification to Telegram stream with optional buttons."""
    fields = {"type": "report", "text": text, "ts": str(now_ms())}
    if buttons is not None:
        fields["buttons"] = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    notify_stream = os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram")
    r.xadd(notify_stream, fields, maxlen=200000, approximate=True)


def _read_audit_list(r: redis.Redis, bundle_id: str) -> List[Dict[str, Any]]:
    """
    Reads audit log from Redis list.
    
    Args:
        r: Redis client
        bundle_id: Bundle identifier
        
    Returns:
        List of audit entries (dicts)
    """
    key = f"recs:audit:{bundle_id}"
    # Use lrange for compatibility with fakeredis
    entries = r.lrange(key, 0, -1)
    out = []
    for s in entries:
        if not s:
            continue
        try:
            out.append(json.loads(s))
        except Exception:
            pass
    return out


def _apply_hash_restores(
    r: redis.Redis,
    *,
    who: str,
    ttl_sec: int,
    restores: List[Dict[str, Any]],
) -> Tuple[str, str]:
    """
    Apply list of {key, field, old, old_null, new} as RESTORE (HSET/HDEL),
    write recs:bundle + recs:audit so rollback works.
    
    Args:
        r: Redis client
        who: Who is applying (for audit)
        ttl_sec: TTL for bundle/audit/status keys
        restores: List of restore operations {key, field, old, old_null}
        
    Returns:
        (bundle_id, signature)
    """
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    bundle_id = secrets.token_hex(6)
    sig = sign(bundle_id, secret)
    ts = now_ms()

    pipe = r.pipeline()
    audit_out = []
    ops_out = []

    for x in restores:
        key = str(x["key"])
        field = str(x["field"])
        # "target" is old value from clamp audit
        old_null = int(x.get("old_null", 0) or 0)
        target_old = "" if x.get("old") is None else str(x.get("old", ""))

        # capture current before writing (for rollback of this action)
        cur = r.hget(key, field)
        audit_out.append({
            "op": "HSET" if old_null == 0 else "HDEL",
            "key": key,
            "field": field,
            "old": ("" if cur is None else str(cur)),
            "old_null": (1 if cur is None else 0),
            "new": target_old if old_null == 0 else "",
            "ts_ms": ts,
            "who": who,
        })

        if old_null == 1:
            # HDEL in pipeline (same as recs_callback_worker)
            pipe.hdel(key, field)
            ops_out.append({"op": "HDEL", "key": key, "field": field})
        else:
            pipe.hset(key, field, target_old)
            ops_out.append({"op": "HSET", "key": key, "field": field, "value": target_old})

    pipe.execute()

    bundle = {
        "id": bundle_id,
        "created_ms": ts,
        "ttl_sec": ttl_sec,
        "who": who,
        "ops": ops_out,
        "meta": {"kind": "meta_hardstop_cap_unclamp_step"},
    }
    r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl_sec)
    r.set(f"recs:status:{bundle_id}", "APPLIED", ex=ttl_sec)
    for a in audit_out:
        r.rpush(f"recs:audit:{bundle_id}", json.dumps(a, ensure_ascii=False, separators=(",", ":")))
    r.expire(f"recs:audit:{bundle_id}", ttl_sec)

    return bundle_id, sig


def main() -> None:
    """Main entry point: checks clamp state, health, applies relax/unclamp if conditions met."""
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = redis.Redis.from_url(redis_url, decode_responses=True)

    ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)

    # clamp state keys
    clamp_active_key = os.getenv("META_CLAMP_ACTIVE_KEY", "meta:hardstop:clamp:active")
    clamp_stage_key = os.getenv("META_CLAMP_STAGE_KEY", "meta:hardstop:clamp:stage")  # CLAMPED|RELAXED
    healthy_streak_key = os.getenv("META_HEALTHY_STREAK_KEY", "meta:hardstop:healthy_streak")

    clamp_bundle_id = (r.get(clamp_active_key) or "").strip()
    if not clamp_bundle_id:
        # nothing active
        r.delete(healthy_streak_key)
        r.delete(clamp_stage_key)
        logger.info("No active clamp, cleaning up state")
        return

    # health window
    metrics_stream = os.getenv("OF_GATE_METRICS_STREAM", "metrics:of_gate")
    window_min = float(os.getenv("META_HARDSTOP_WINDOW_MIN", "30") or 30)
    max_scan = int(os.getenv("META_HARDSTOP_METRICS_MAX_SCAN", "200000") or 200000)
    since_ms = now_ms() - int(window_min * 60_000)

    rows = read_metrics_window(r, metrics_stream, since_ms, max_scan=max_scan)
    health = summarize_health(rows)
    hs, reasons = hard_stop(health)

    # update healthy streak
    prev = _i(r.get(healthy_streak_key), 0)
    if hs:
        r.set(healthy_streak_key, "0", ex=ttl)
        # when hard-stop снова включился — stage остаётся CLAMPED
        r.set(clamp_stage_key, "CLAMPED", ex=ttl)
        logger.info(f"Hard-stop detected again, resetting streak. reasons={reasons}, health={health}")
        return
    else:
        streak = prev + 1
        r.set(healthy_streak_key, str(streak), ex=ttl)

    stage = (r.get(clamp_stage_key) or "CLAMPED").strip().upper()
    if stage not in ("CLAMPED", "RELAXED"):
        stage = "CLAMPED"
        r.set(clamp_stage_key, stage, ex=ttl)

    # thresholds
    relax_n = int(os.getenv("META_UNCLAMP_RELAX_STREAK_N", "6") or 6)      # e.g. 6*10min = 60min
    unclamp_n = int(os.getenv("META_UNCLAMP_REMOVE_STREAK_N", "18") or 18) # e.g. 3h

    # caps for RELAX
    relax_cap_trend = float(os.getenv("META_RELAX_CAP_TREND", "0.25") or 0.25)
    relax_cap_range = float(os.getenv("META_RELAX_CAP_RANGE", "0.15") or 0.15)
    relax_cap_news = float(os.getenv("META_RELAX_CAP_NEWS", "0.00") or 0.00)
    relax_cap_other = float(os.getenv("META_RELAX_CAP_OTHER", "0.00") or 0.00)
    relax_caps = {
        "meta_enforce_share_trend": relax_cap_trend,
        "meta_enforce_share_range": relax_cap_range,
        "meta_enforce_share_news": relax_cap_news,
        "meta_enforce_share_other": relax_cap_other,
    }

    # cooldown between actions (avoid spam)
    action_cooldown_sec = int(os.getenv("META_UNCLAMP_ACTION_COOLDOWN_SEC", "1800") or 1800)
    last_action_key = os.getenv("META_UNCLAMP_LAST_ACTION_MS_KEY", "meta:hardstop:unclamp:last_action_ms")
    last_action_ms = _i(r.get(last_action_key), 0)
    if last_action_ms and (now_ms() - last_action_ms) < action_cooldown_sec * 1000:
        logger.info(f"Cooldown active (last_action_ms={last_action_ms}, cooldown_sec={action_cooldown_sec}), skipping")
        return

    # read clamp audit (source of truth for "pre-clamp")
    clamp_audit = _read_audit_list(r, clamp_bundle_id)
    if not clamp_audit:
        # can't restore safely
        logger.warning(f"No audit found for clamp_bundle_id={clamp_bundle_id}, cannot restore safely")
        return

    # helper: build restore list for RELAX using clamp_audit.old capped by relax caps
    def build_relax_restores() -> List[Dict[str, Any]]:
        """Build restore list for RELAX stage: min(old from audit, relax_cap)."""
        out = []
        for a in clamp_audit:
            if a.get("op") != "HSET":
                continue
            field = str(a.get("field", ""))
            if field not in relax_caps:
                continue
            key = str(a.get("key", ""))
            old_null = int(a.get("old_null", 0) or 0)
            if old_null == 1:
                # if field didn't exist pre-clamp, don't create it in relax
                continue
            try:
                oldf = float(a.get("old", 0.0) or 0.0)
            except Exception:
                oldf = 0.0
            cap = float(relax_caps[field])
            target = min(oldf, cap)
            out.append({"key": key, "field": field, "old": f"{target:.2f}", "old_null": 0})
        return out

    # helper: build full restores from clamp audit (restore old/hdel)
    def build_full_restores() -> List[Dict[str, Any]]:
        """Build full restore list from clamp audit (restore pre-clamp values)."""
        out = []
        for a in clamp_audit:
            if str(a.get("op")) != "HSET":
                continue
            out.append({
                "key": str(a.get("key", "")),
                "field": str(a.get("field", "")),
                "old": ("" if a.get("old") is None else str(a.get("old", ""))),
                "old_null": int(a.get("old_null", 0) or 0),
            })
        return out

    # Stage A: RELAX
    if stage == "CLAMPED" and prev + 1 >= relax_n:
        restores = build_relax_restores()
        if restores:
            bid, sig = _apply_hash_restores(r, who="of_gate_hardstop_cap_unclamp_relax", ttl_sec=ttl, restores=restores)
            r.set(clamp_stage_key, "RELAXED", ex=ttl)
            r.set(last_action_key, str(now_ms()), ex=ttl)

            buttons = [[{"text": "↩ Rollback relax", "callback": f"recs:rollback:{bid}:{sig}"}]]
            txt = (
                "<b>CAP CLAMP RELAXED</b>\n"
                f"clamp_id=<code>{clamp_bundle_id}</code>\n"
                f"relax_id=<code>{bid}</code>\n"
                f"healthy_streak=<code>{prev+1}</code>\n"
                f"relax_caps=<code>{relax_caps}</code>\n"
                f"health=<code>{health}</code>"
            )
            _notify(r, txt, buttons=buttons)
            logger.info(f"Relax applied: bundle_id={bid}, streak={prev+1}, restores={len(restores)}")
        return

    # Stage B: FULL UNCLAMP (restore pre-clamp)
    if prev + 1 >= unclamp_n:
        restores = build_full_restores()
        if restores:
            bid, sig = _apply_hash_restores(r, who="of_gate_hardstop_cap_unclamp_remove", ttl_sec=ttl, restores=restores)

            # clear clamp active + stage
            r.delete(clamp_active_key)
            r.delete(clamp_stage_key)
            r.delete(healthy_streak_key)
            r.set(last_action_key, str(now_ms()), ex=ttl)

            buttons = [[{"text": "↩ Rollback unclamp", "callback": f"recs:rollback:{bid}:{sig}"}]]
            txt = (
                "<b>CAP CLAMP REMOVED (RESTORED PRE-CLAMP)</b>\n"
                f"clamp_id=<code>{clamp_bundle_id}</code>\n"
                f"unclamp_id=<code>{bid}</code>\n"
                f"healthy_streak=<code>{prev+1}</code>\n"
                f"health=<code>{health}</code>\n"
                "<i>Rollback will re-apply the previous (clamped/relaxed) values.</i>"
            )
            _notify(r, txt, buttons=buttons)
            logger.info(f"Unclamp applied: bundle_id={bid}, streak={prev+1}, restores={len(restores)}")
        return


if __name__ == "__main__":
    main()

