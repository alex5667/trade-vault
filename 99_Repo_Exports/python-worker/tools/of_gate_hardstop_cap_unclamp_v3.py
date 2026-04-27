"""Staged auto-unclamp v3: triple-window health check (30m + 2h + 12h baseline) + outcome gate, AUTO/PROPOSE modes.

This is the next level above auto-clamp: when hard-stop disappears and health holds,
gradually restore caps to pre-clamp values.

Key features:
- Triple independent health windows: 30 minutes, 2 hours, and 12 hours (baseline, can be softer)
- Outcome gate: cross-check against events:trades (r_mult statistics for clamp symbols)
- AUTO mode (default): auto-applies RELAX/REMOVE actions
- PROPOSE mode: creates bundle, sends Approve/Reject buttons, waits for callback worker
- Optional gates: regression pass streak, emergency cooldown
- Side-effects (stage/clear active) executed by this runner after bundle is APPLIED

Stage A (RELAX): when all windows healthy + outcome OK and streak >= RELAX_N cycles → partially
  restore caps (e.g., trend≤0.25, range≤0.15) not higher than pre-clamp (from clamp audit).

Stage B (REMOVE): when all windows healthy + outcome OK and streak >= REMOVE_N cycles → fully restore
  pre-clamp values from clamp audit and remove active flag.

Usage:
  python -m tools.of_gate_hardstop_cap_unclamp_v3
  (reads ENV vars for thresholds, streak N, relax caps, cooldown, mode)

Environment Variables:
  - Mode: META_UNCLAMP_MODE (AUTO|PROPOSE, default AUTO), META_UNCLAMP_MODE_KEY (redis key override)
  - Metrics: OF_GATE_METRICS_STREAM, META_HARDSTOP_METRICS_MAX_SCAN, META_HARDSTOP_MIN_N
  - Windows: META_UNCLAMP_SHORT_WINDOW_MIN (30), META_UNCLAMP_LONG_WINDOW_MIN (120), META_UNCLAMP_BASELINE_WINDOW_MIN (720)
  - Hard-stop thresholds: META_HARDSTOP_LAT_P99_US, META_HARDSTOP_EXEC_P90, META_HARDSTOP_SOFT_RATE, META_HARDSTOP_OK_RATE_MIN
  - Baseline thresholds (softer): META_BASELINE_LAT_P99_US, META_BASELINE_EXEC_P90, META_BASELINE_SOFT_RATE, META_BASELINE_OK_RATE_MIN
  - Outcome gate: TRADE_EVENTS_STREAM, META_UNCLAMP_OUTCOME_WINDOW_HOURS, META_UNCLAMP_OUTCOME_MAX_SCAN, META_UNCLAMP_OUTCOME_MIN_N, META_UNCLAMP_OUTCOME_MEAN_MIN, META_UNCLAMP_OUTCOME_TAIL_MAX
  - Clamp state: META_CLAMP_ACTIVE_KEY, META_CLAMP_STAGE_KEY, META_HEALTHY_STREAK_KEY
  - Unclamp: META_UNCLAMP_RELAX_STREAK_N, META_UNCLAMP_REMOVE_STREAK_N, META_UNCLAMP_ACTION_COOLDOWN_SEC
  - Pending: META_UNCLAMP_PENDING_KEY, META_UNCLAMP_LAST_ACTION_MS_KEY
  - Relax caps: META_RELAX_CAP_TREND, META_RELAX_CAP_RANGE, META_RELAX_CAP_NEWS, META_RELAX_CAP_OTHER
  - Optional gates: META_UNCLAMP_REQUIRE_REGRESS, META_UNCLAMP_REGRESS_MIN_STREAK, META_UNCLAMP_REGRESS_MAX_AGE_HOURS, EMERG_COOLDOWN_KEY, META_UNCLAMP_MIN_HOURS_SINCE_EMERG
  - Rec/bot: NOTIFY_TELEGRAM_STREAM, RECS_TTL_SEC, RECS_HMAC_SECRET
"""

from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import os
import time
import json
import hmac
import hashlib
import secrets
from typing import Any, Dict, List, Tuple, Optional

import redis

from common.log import setup_logger
from core.redis_client import get_redis

logger = setup_logger("OfGateHardstopCapUnclampV3")


# ---------------- basic utils ----------------

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


def _notify(r: redis.Redis, text: str, buttons: Optional[List[List[Dict[str, str]]]] = None) -> None:
    """Sends notification to Telegram stream with optional buttons."""
    fields = {"type": "report", "text": text, "ts": str(now_ms())}
    if buttons is not None:
        fields["buttons"] = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    notify_stream = os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram")
    r.xadd(notify_stream, fields, maxlen=200000, approximate=True)


# ---------------- mode ----------------

def _mode(r: redis.Redis) -> str:
    """
    Default AUTO, but can override by redis key.
    
    Args:
        r: Redis client
        
    Returns:
        Mode string: "AUTO" or "PROPOSE"
    """
    m = (os.getenv("META_UNCLAMP_MODE", "AUTO") or "AUTO").strip().upper()
    key = os.getenv("META_UNCLAMP_MODE_KEY", "cfg:meta_unclamp:mode")
    try:
        v = (r.get(key) or "").strip().upper()
        if v in ("AUTO", "PROPOSE"):
            m = v
    except Exception:
        pass
    return m if m in ("AUTO", "PROPOSE") else "AUTO"


# ---------------- metrics read ----------------

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


def is_unhealthy(health: Dict[str, float], *, prefix: str, min_n: int,
                lat_thr: float, exec_thr: float, soft_thr: float, ok_min: float) -> Tuple[bool, List[str]]:
    """
    Checks if health summary indicates unhealthy state.
    
    Args:
        health: Health summary dict (from summarize_health)
        prefix: Prefix for reason labels (e.g., 'w30', 'w120', 'w720')
        min_n: Minimum number of samples required
        lat_thr: Latency P99 threshold (microseconds)
        exec_thr: Execution risk P90 threshold
        soft_thr: Soft failure rate threshold
        ok_min: Minimum OK rate
        
    Returns:
        (is_unhealthy, list_of_reasons)
    """
    reasons = []

    n = float(health.get("n", 0.0))
    lat_p99 = float(health.get("lat_p99_us", 0.0))
    exec_p90 = float(health.get("exec_p90", 0.0))
    soft = float(health.get("soft_rate", 0.0))
    ok = float(health.get("ok_rate", 0.0))

    if n < float(min_n):
        reasons.append(f"{prefix}:low_n<{min_n}")
    if lat_p99 > lat_thr:
        reasons.append(f"{prefix}:lat_p99>{lat_thr}")
    if exec_p90 > exec_thr:
        reasons.append(f"{prefix}:exec_p90>{exec_thr}")
    if soft > soft_thr:
        reasons.append(f"{prefix}:soft>{soft_thr}")
    if ok < ok_min:
        reasons.append(f"{prefix}:ok<{ok_min}")

    return (len(reasons) > 0), reasons


# ---------------- clamp audit read ----------------

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
    n = r.llen(key)
    out = []
    for i in range(n):
        s = r.lindex(key, i)
        if not s:
            continue
        try:
            out.append(json.loads(s))
        except Exception:
            pass
    return out


# ---------------- trades outcome gate ----------------

def _event_ts_ms(fields: Dict[str, Any]) -> int:
    """Extracts timestamp from event fields."""
    return _i(fields.get("ts_ms", fields.get("ts", fields.get("timestamp", 0))), 0)


def _is_closed(fields: Dict[str, Any]) -> bool:
    """
    Checks if event represents a closed position.
    
    Supports both direct fields and payload JSON.
    """
    et = str(fields.get("event_type", fields.get("type", "")) or "").upper()
    if et in ("POSITION_CLOSED", "CLOSE"):
        return True
    # tolerate payload JSON
    p = fields.get("payload")
    if isinstance(p, str) and p and p[0] == "{":
        try:
            j = json.loads(p)
            et2 = str(j.get("event_type", j.get("type", "")) or "").upper()
            return et2 in ("POSITION_CLOSED", "CLOSE")
        except Exception:
            return False
    return False


def _get_symbol(fields: Dict[str, Any]) -> str:
    """Extracts symbol from event fields (supports payload JSON)."""
    s = str(fields.get("symbol", "") or "").upper()
    if s:
        return s
    p = fields.get("payload")
    if isinstance(p, str) and p and p[0] == "{":
        try:
            j = json.loads(p)
            return str(j.get("symbol", "") or "").upper()
        except Exception:
            return ""
    return ""


def _get_r_mult(fields: Dict[str, Any]) -> Optional[float]:
    """Extracts r_mult from event fields (supports payload JSON)."""
    if "r_mult" in fields:
        try:
            return float(fields["r_mult"])
        except Exception:
            return None
    p = fields.get("payload")
    if isinstance(p, str) and p and p[0] == "{":
        try:
            j = json.loads(p)
            if "r_mult" in j:
                return float(j["r_mult"])
        except Exception:
            return None
    return None


def read_outcome_stats(
    r: redis.Redis,
    *,
    stream: str,
    since_ms: int,
    symbols: List[str],
    max_scan: int,
) -> Dict[str, float]:
    """
    Reads outcome statistics from events:trades stream for closed positions.
    
    Args:
        r: Redis client
        stream: Stream name (e.g., "events:trades")
        since_ms: Start timestamp (epoch ms)
        symbols: List of symbols to filter (empty = all)
        max_scan: Maximum number of messages to scan
        
    Returns:
        Dict with: n, meanR, tail_rate, p05, p50
    """
    symset = set([s.upper() for s in symbols if s])
    rs: List[float] = []
    scanned = 0
    last_id = "+"

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

            ts = _event_ts_ms(fields)
            if ts and ts < since_ms:
                scanned = max_scan
                break

            if not _is_closed(fields):
                continue
            sym = _get_symbol(fields)
            if symset and sym not in symset:
                continue
            rm = _get_r_mult(fields)
            if rm is None:
                continue
            rs.append(float(rm))

    n = len(rs)
    if n == 0:
        return {"n": 0.0}

    mean = sum(rs) / n
    tail = sum(1 for x in rs if x <= -1.0) / n
    return {
        "n": float(n),
        "meanR": float(mean),
        "tail_rate": float(tail),
        "p05": float(pctl(rs, 0.05)),
        "p50": float(pctl(rs, 0.50)),
    }


def outcome_ok(stats: Dict[str, float], *, min_n: int, mean_min: float, tail_max: float) -> Tuple[bool, List[str]]:
    """
    Checks if outcome statistics are acceptable.
    
    Args:
        stats: Outcome statistics dict
        min_n: Minimum number of closed trades required
        mean_min: Minimum mean R-multiple
        tail_max: Maximum tail rate (fraction of trades with r_mult <= -1.0)
        
    Returns:
        (is_ok, list_of_reasons)
    """
    reasons = []
    n = float(stats.get("n", 0.0))
    meanR = float(stats.get("meanR", 0.0))
    tail = float(stats.get("tail_rate", 0.0))

    if n < float(min_n):
        reasons.append(f"outcome:low_n<{min_n}")
    if meanR < mean_min:
        reasons.append(f"outcome:mean<{mean_min}")
    if tail > tail_max:
        reasons.append(f"outcome:tail>{tail_max}")

    return (len(reasons) == 0), reasons


# ---------------- apply helpers ----------------

def _apply_restores_direct(
    r: redis.Redis,
    *,
    who: str,
    ttl_sec: int,
    restores: List[Dict[str, Any]],
) -> Tuple[str, str]:
    """
    AUTO mode: apply now, write recs:bundle + recs:audit so rollback works.
    restores: list of {"op":"HSET"/"HDEL", "key":..., "field":..., "value":... optional}
    
    Args:
        r: Redis client
        who: Who is applying (for audit)
        ttl_sec: TTL for bundle/audit/status keys
        restores: List of restore operations {op, key, field, value}
        
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

    for op in restores:
        k = str(op["key"])
        f = str(op["field"])
        cur = r.hget(k, f)
        audit_out.append({
            "op": op["op"],
            "key": k,
            "field": f,
            "old": ("" if cur is None else str(cur)),
            "old_null": (1 if cur is None else 0),
            "new": (op.get("value", "") if op["op"] == "HSET" else ""),
            "ts_ms": ts,
            "who": who,
        })

        if op["op"] == "HDEL":
            pipe.hdel(k, f)
            ops_out.append({"op": "HDEL", "key": k, "field": f})
        else:
            v = str(op.get("value", ""))
            pipe.hset(k, f, v)
            ops_out.append({"op": "HSET", "key": k, "field": f, "value": v})

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


def _create_proposal_bundle(
    r: redis.Redis,
    *,
    who: str,
    ttl_sec: int,
    ops: List[Dict[str, Any]],
    meta: Dict[str, Any],
) -> Tuple[str, str]:
    """
    PROPOSE mode: create recs:bundle, status=PENDING, return id+sig (for buttons).
    
    Args:
        r: Redis client
        who: Who is proposing (for audit)
        ttl_sec: TTL for bundle/status keys
        ops: List of operations {op, key, field, value}
        meta: Metadata dict
        
    Returns:
        (bundle_id, signature)
    """
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    bundle_id = secrets.token_hex(6)
    sig = sign(bundle_id, secret)
    ts = now_ms()

    bundle = {
        "id": bundle_id,
        "created_ms": ts,
        "ttl_sec": ttl_sec,
        "who": who,
        "ops": ops,
        "meta": meta,
    }
    r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl_sec)
    r.set(f"recs:status:{bundle_id}", "PENDING", ex=ttl_sec)
    return bundle_id, sig


# ---------------- restore builders ----------------

def build_relax_ops_from_clamp_audit(clamp_audit: List[Dict[str, Any]], relax_caps: Dict[str, float]) -> List[Dict[str, Any]]:
    """
    Use pre-clamp old values, capped by relax caps.
    Only for fields that existed pre-clamp (old_null==0).
    
    Args:
        clamp_audit: List of audit entries from clamp bundle
        relax_caps: Dict mapping field names to cap values
        
    Returns:
        List of operations {op, key, field, value}
    """
    ops = []
    for a in clamp_audit:
        if str(a.get("op")) != "HSET":
            continue
        field = str(a.get("field", ""))
        if field not in relax_caps:
            continue
        old_null = int(a.get("old_null", 0) or 0)
        if old_null == 1:
            continue
        try:
            oldf = float(a.get("old", 0.0) or 0.0)
        except Exception:
            oldf = 0.0
        cap = float(relax_caps[field])
        target = min(oldf, cap)
        ops.append({"op": "HSET", "key": str(a.get("key", "")), "field": field, "value": f"{target:.2f}"})
    return ops


def build_full_restore_ops_from_clamp_audit(clamp_audit: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    """
    Restore exact pre-clamp: if old_null==1 -> HDEL, else HSET old.
    
    Args:
        clamp_audit: List of audit entries from clamp bundle
        
    Returns:
        List of operations {op, key, field, value}
    """
    ops = []
    for a in clamp_audit:
        if str(a.get("op")) != "HSET":
            continue
        key = str(a.get("key", ""))
        field = str(a.get("field", ""))
        old_null = int(a.get("old_null", 0) or 0)
        if old_null == 1:
            ops.append({"op": "HDEL", "key": key, "field": field})
        else:
            ops.append({"op": "HSET", "key": key, "field": field, "value": ("" if a.get("old") is None else str(a.get("old", "")))})
    return ops


# ---------------- optional regress + emergency gates ----------------

def regress_ok(r: redis.Redis) -> Tuple[bool, str]:
    """
    Optional: require baseline regression PASS streak.
    
    Args:
        r: Redis client
        
    Returns:
        (is_ok, debug_string)
    """
    if int(os.getenv("META_UNCLAMP_REQUIRE_REGRESS", "0") or 0) != 1:
        return True, "disabled"

    streak_key = os.getenv("REGRESS_PASS_STREAK_KEY", "sre:regress:pass_streak")
    last_status_key = os.getenv("REGRESS_LAST_STATUS_KEY", "sre:regress:last_status")
    last_ts_key = os.getenv("REGRESS_LAST_TS_KEY", "sre:regress:last_ts_ms")
    min_streak = int(os.getenv("META_UNCLAMP_REGRESS_MIN_STREAK", "3") or 3)
    max_age_h = float(os.getenv("META_UNCLAMP_REGRESS_MAX_AGE_HOURS", "30") or 30.0)

    streak = _i(r.get(streak_key), 0)
    last_status = str(r.get(last_status_key) or "").upper()
    last_ts = _i(r.get(last_ts_key), 0)
    age_ok = True
    if last_ts > 0:
        age_ok = (now_ms() - last_ts) <= int(max_age_h * 3600_000)

    ok = (last_status == "PASS") and age_ok and (streak >= min_streak)
    return ok, f"streak={streak} need={min_streak} last={last_status} age_ok={int(age_ok)}"


def no_recent_emergency(r: redis.Redis) -> Tuple[bool, str]:
    """
    Checks if enough time has passed since last emergency.
    
    Args:
        r: Redis client
        
    Returns:
        (is_ok, debug_string)
    """
    key = os.getenv("EMERG_COOLDOWN_KEY", "sre:of_gate:emergency:last_ms")
    min_h = float(os.getenv("META_UNCLAMP_MIN_HOURS_SINCE_EMERG", "24") or 24)
    last = _i(r.get(key), 0)
    if last <= 0:
        return True, "none"
    ok = (now_ms() - last) >= int(min_h * 3600_000)
    return ok, f"last_ms={last} ok={int(ok)} min_h={min_h}"


# ---------------- main ----------------

def main() -> None:
    """Main entry point: checks clamp state, triple-window health + outcome gate, applies relax/unclamp if conditions met."""
    try:
        r = get_redis(retry_attempts=10, retry_delay=2)
    except Exception as e:
        logger.error(f"Failed to connect to Redis: {e}")
        raise

    ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)

    clamp_active_key = os.getenv("META_CLAMP_ACTIVE_KEY", "meta:hardstop:clamp:active")
    clamp_stage_key = os.getenv("META_CLAMP_STAGE_KEY", "meta:hardstop:clamp:stage")  # CLAMPED|RELAXED
    healthy_streak_key = os.getenv("META_HEALTHY_STREAK_KEY", "meta:hardstop:healthy_streak")
    pending_key = os.getenv("META_UNCLAMP_PENDING_KEY", "meta:hardstop:unclamp:pending")
    last_action_key = os.getenv("META_UNCLAMP_LAST_ACTION_MS_KEY", "meta:hardstop:unclamp:last_action_ms")

    clamp_bundle_id = (r.get(clamp_active_key) or "").strip()
    if not clamp_bundle_id:
        r.delete(healthy_streak_key)
        r.delete(clamp_stage_key)
        r.delete(pending_key)
        logger.info("No active clamp, cleaning up state")
        return

    mode = _mode(r)

    # pending lifecycle (PROPOSE)
    pending_raw = r.get(pending_key)
    if pending_raw:
        try:
            pend = json.loads(pending_raw)
        except Exception:
            pend = None
        if isinstance(pend, dict) and pend.get("bundle_id"):
            bid = str(pend["bundle_id"])
            action = str(pend.get("action", "")).upper()
            st = (r.get(f"recs:status:{bid}") or "").strip().upper()

            if st == "APPLIED":
                if action == "RELAX":
                    r.set(clamp_stage_key, "RELAXED", ex=ttl)
                elif action == "REMOVE":
                    r.delete(clamp_active_key)
                    r.delete(clamp_stage_key)
                    r.delete(healthy_streak_key)
                r.delete(pending_key)
                r.set(last_action_key, str(now_ms()), ex=ttl)
                _notify(r, f"<b>Unclamp applied</b>\naction=<code>{action}</code>\nid=<code>{bid}</code>\nmode=<code>{mode}</code>")
                logger.info(f"Pending proposal applied: bundle_id={bid}, action={action}")
                return

            if st == "REJECTED":
                r.delete(pending_key)
                r.set(last_action_key, str(now_ms()), ex=ttl)
                _notify(r, f"<b>Unclamp rejected</b>\naction=<code>{action}</code>\nid=<code>{bid}</code>\nmode=<code>{mode}</code>")
                logger.info(f"Pending proposal rejected: bundle_id={bid}, action={action}")
                return

            # still pending -> no spam
            logger.debug(f"Pending proposal still pending: bundle_id={bid}, status={st}")
            return

    # cooldown
    cooldown_sec = int(os.getenv("META_UNCLAMP_ACTION_COOLDOWN_SEC", "1800") or 1800)
    last_action_ms = _i(r.get(last_action_key), 0)
    if last_action_ms and (now_ms() - last_action_ms) < cooldown_sec * 1000:
        logger.info(f"Cooldown active (last_action_ms={last_action_ms}, cooldown_sec={cooldown_sec}), skipping")
        return

    # optional gates
    ok_reg, reg_dbg = regress_ok(r)
    ok_em, em_dbg = no_recent_emergency(r)
    if not ok_reg or not ok_em:
        logger.info(f"Optional gates failed: regress={reg_dbg}, emergency={em_dbg}")
        return

    # 3 health windows: 30m, 2h, 12h
    metrics_stream = os.getenv("OF_GATE_METRICS_STREAM", "metrics:of_gate")
    max_scan = int(os.getenv("META_HARDSTOP_METRICS_MAX_SCAN", "200000") or 200000)

    w30 = int(os.getenv("META_UNCLAMP_SHORT_WINDOW_MIN", "30") or 30)
    w120 = int(os.getenv("META_UNCLAMP_LONG_WINDOW_MIN", "120") or 120)
    w720 = int(os.getenv("META_UNCLAMP_BASELINE_WINDOW_MIN", "720") or 720)  # 12h

    rows30 = read_metrics_window(r, metrics_stream, now_ms() - w30 * 60_000, max_scan=max_scan)
    rows120 = read_metrics_window(r, metrics_stream, now_ms() - w120 * 60_000, max_scan=max_scan)
    rows720 = read_metrics_window(r, metrics_stream, now_ms() - w720 * 60_000, max_scan=max_scan)

    h30 = summarize_health(rows30)
    h120 = summarize_health(rows120)
    h720 = summarize_health(rows720)

    # thresholds: 30m and 2h strict; 12h can be softer (separate env)
    min_n = int(os.getenv("META_HARDSTOP_MIN_N", "200") or 200)

    lat_thr = float(os.getenv("META_HARDSTOP_LAT_P99_US", "12000") or 12000)
    exec_thr = float(os.getenv("META_HARDSTOP_EXEC_P90", "0.92") or 0.92)
    soft_thr = float(os.getenv("META_HARDSTOP_SOFT_RATE", "0.60") or 0.60)
    ok_min = float(os.getenv("META_HARDSTOP_OK_RATE_MIN", "0.10") or 0.10)

    # baseline (12h) softer defaults
    lat_thr_b = float(os.getenv("META_BASELINE_LAT_P99_US", str(lat_thr)) or lat_thr)
    exec_thr_b = float(os.getenv("META_BASELINE_EXEC_P90", str(exec_thr)) or exec_thr)
    soft_thr_b = float(os.getenv("META_BASELINE_SOFT_RATE", str(soft_thr)) or soft_thr)
    ok_min_b = float(os.getenv("META_BASELINE_OK_RATE_MIN", str(ok_min)) or ok_min)

    bad30, r30 = is_unhealthy(h30, prefix="w30", min_n=min_n, lat_thr=lat_thr, exec_thr=exec_thr, soft_thr=soft_thr, ok_min=ok_min)
    bad120, r120 = is_unhealthy(h120, prefix="w120", min_n=min_n, lat_thr=lat_thr, exec_thr=exec_thr, soft_thr=soft_thr, ok_min=ok_min)
    bad720, r720 = is_unhealthy(h720, prefix="w720", min_n=min_n, lat_thr=lat_thr_b, exec_thr=exec_thr_b, soft_thr=soft_thr_b, ok_min=ok_min_b)

    healthy_all = (not bad30) and (not bad120) and (not bad720)
    reasons = r30 + r120 + r720

    # outcome gate from events:trades
    trades_stream = os.getenv("TRADE_EVENTS_STREAM", "events:trades")
    out_hours = float(os.getenv("META_UNCLAMP_OUTCOME_WINDOW_HOURS", "6") or 6)
    out_since = now_ms() - int(out_hours * 3600_000)
    out_max_scan = int(os.getenv("META_UNCLAMP_OUTCOME_MAX_SCAN", "400000") or 400000)

    # symbols = clamp symbols (or canary)
    sym_csv = (os.getenv("META_CLAMP_SYMBOLS", "") or "").strip() or (os.getenv("CANARY_SYMBOLS", "") or "").strip()
    symbols = [s.strip().upper() for s in sym_csv.split(",") if s.strip()]

    out_stats = read_outcome_stats(r, stream=trades_stream, since_ms=out_since, symbols=symbols, max_scan=out_max_scan)

    out_min_n = int(os.getenv("META_UNCLAMP_OUTCOME_MIN_N", "50") or 50)
    out_mean_min = float(os.getenv("META_UNCLAMP_OUTCOME_MEAN_MIN", "-0.02") or -0.02)
    out_tail_max = float(os.getenv("META_UNCLAMP_OUTCOME_TAIL_MAX", "0.30") or 0.30)

    ok_out, out_reasons = outcome_ok(out_stats, min_n=out_min_n, mean_min=out_mean_min, tail_max=out_tail_max)

    # update streak only if ALL gates pass
    prev = _i(r.get(healthy_streak_key), 0)
    if healthy_all and ok_out:
        streak = prev + 1
    else:
        streak = 0
    r.set(healthy_streak_key, str(streak), ex=ttl)

    stage = (r.get(clamp_stage_key) or "CLAMPED").strip().upper()
    if stage not in ("CLAMPED", "RELAXED"):
        stage = "CLAMPED"
        r.set(clamp_stage_key, stage, ex=ttl)

    relax_n = int(os.getenv("META_UNCLAMP_RELAX_STREAK_N", "6") or 6)
    remove_n = int(os.getenv("META_UNCLAMP_REMOVE_STREAK_N", "18") or 18)

    # Need clamp audit
    clamp_audit = _read_audit_list(r, clamp_bundle_id)
    if not clamp_audit:
        logger.warning(f"No audit found for clamp_bundle_id={clamp_bundle_id}, cannot restore safely")
        return

    relax_caps = {
        "meta_enforce_share_trend": float(os.getenv("META_RELAX_CAP_TREND", "0.25") or 0.25),
        "meta_enforce_share_range": float(os.getenv("META_RELAX_CAP_RANGE", "0.15") or 0.15),
        "meta_enforce_share_news": float(os.getenv("META_RELAX_CAP_NEWS", "0.00") or 0.00),
        "meta_enforce_share_other": float(os.getenv("META_RELAX_CAP_OTHER", "0.00") or 0.00),
    }

    action = None
    if stage == "CLAMPED" and streak >= relax_n:
        action = "RELAX"
    elif stage == "RELAXED" and streak >= remove_n:
        action = "REMOVE"
    else:
        logger.debug(f"No action needed: stage={stage}, streak={streak}, relax_n={relax_n}, remove_n={remove_n}")
        return

    if action == "RELAX":
        ops = build_relax_ops_from_clamp_audit(clamp_audit, relax_caps)
        who = "of_gate_hardstop_cap_unclamp_v3_relax"
        meta = {
            "kind": "meta_hardstop_cap_unclamp_relax",
            "clamp_id": clamp_bundle_id,
            "health30": h30, "health120": h120, "health720": h720,
            "outcome": out_stats,
            "gates": {"regress": reg_dbg, "emergency": em_dbg},
        }
        stage_after = "RELAXED"
    else:
        ops = build_full_restore_ops_from_clamp_audit(clamp_audit)
        who = "of_gate_hardstop_cap_unclamp_v3_remove"
        meta = {
            "kind": "meta_hardstop_cap_unclamp_remove",
            "clamp_id": clamp_bundle_id,
            "health30": h30, "health120": h120, "health720": h720,
            "outcome": out_stats,
            "gates": {"regress": reg_dbg, "emergency": em_dbg},
        }
        stage_after = "REMOVED"

    if not ops:
        logger.warning(f"No operations to apply for action={action}")
        return

    if mode == "AUTO":
        bid, sig = _apply_restores_direct(r, who=who, ttl_sec=ttl, restores=ops)

        # side-effects
        if action == "RELAX":
            r.set(clamp_stage_key, "RELAXED", ex=ttl)
        else:
            r.delete(clamp_active_key)
            r.delete(clamp_stage_key)
            r.delete(healthy_streak_key)

        r.set(last_action_key, str(now_ms()), ex=ttl)

        buttons = [[{"text": "↩ Rollback", "callback": f"recs:rollback:{bid}:{sig}"}]]
        _notify(
            r,
            "<b>Unclamp AUTO applied (v3)</b>\n"
            f"action=<code>{action}</code> stage_after=<code>{stage_after}</code>\n"
            f"id=<code>{bid}</code> streak=<code>{streak}</code>\n"
            f"mode=<code>{mode}</code>\n"
            f"health30=<code>{h30}</code>\n"
            f"health2h=<code>{h120}</code>\n"
            f"health12h=<code>{h720}</code>\n"
            f"outcome(<code>{out_hours}h</code>)=<code>{out_stats}</code>\n"
            f"unhealthy=<code>{reasons}</code>\noutcome_bad=<code>{out_reasons}</code>",
            buttons=buttons,
        )
        logger.info(f"Unclamp AUTO applied: bundle_id={bid}, action={action}, streak={streak}")
        return

    # PROPOSE mode
    bid, sig = _create_proposal_bundle(r, who=who, ttl_sec=ttl, ops=ops, meta=meta)
    pend = {"bundle_id": bid, "action": action, "stage_after": stage_after, "created_ms": now_ms()}
    r.set(pending_key, json.dumps(pend, ensure_ascii=False, separators=(",", ":")), ex=ttl)
    r.set(last_action_key, str(now_ms()), ex=ttl)

    buttons = [[
        {"text": "✅ Approve (preview)", "callback": f"recs:preview:{bid}:{sig}"},
        {"text": "❌ Reject", "callback": f"recs:reject:{bid}:{sig}"},
    ]]

    _notify(
        r,
        "<b>Unclamp PROPOSAL (v3)</b>\n"
        f"action=<code>{action}</code> stage_after=<code>{stage_after}</code>\n"
        f"id=<code>{bid}</code> streak=<code>{streak}</code>\n"
        f"mode=<code>{mode}</code>\n"
        f"health30=<code>{h30}</code>\n"
        f"health2h=<code>{h120}</code>\n"
        f"health12h=<code>{h720}</code>\n"
        f"outcome(<code>{out_hours}h</code>)=<code>{out_stats}</code>\n"
        f"unhealthy=<code>{reasons}</code>\noutcome_bad=<code>{out_reasons}</code>\n"
        f"gates=<code>{{'regress':'{reg_dbg}','emergency':'{em_dbg}'}}</code>",
        buttons=buttons,
    )
    logger.info(f"Unclamp PROPOSAL created: bundle_id={bid}, action={action}, streak={streak}")


if __name__ == "__main__":
    main()

