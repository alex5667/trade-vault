from __future__ import annotations
from core.redis_keys import RedisStreams as RS

"""Staged auto-unclamp v4: selective per-symbol RELAX/REMOVE, dual-window outcome (2h + 24h), AUTO/PROPOSE modes.

This is the next level above v3: selective unclamp per symbol based on per-symbol outcome stats.

Key features:
- Triple independent health windows: 30 minutes, 2 hours, and 12 hours (baseline, can be softer)
- Per-symbol outcome gate: 2h window (RELAX eligibility) + 24h window (REMOVE eligibility)
- Selective RELAX/REMOVE: only eligible symbols are unclamped
- Per-symbol state tracking: CLAMPED/RELAXED/RESTORED with remaining set
- AUTO mode (default): auto-applies RELAX/REMOVE actions
- PROPOSE mode: creates bundle, sends Approve/Reject buttons, waits for callback worker
- allow_remove flag: can disable REMOVE (only RELAX allowed)
- Side-effects (stage/clear active) executed by this runner after bundle is APPLIED

Stage A (RELAX): when all windows healthy + short-outcome OK (2h) and streak >= RELAX_N cycles → partially
  restore caps (e.g., trend≤0.25, range≤0.15) not higher than pre-clamp (from clamp audit) for eligible symbols only.

Stage B (REMOVE): when all windows healthy + short-outcome OK (2h) + long-outcome OK (24h) and streak >= REMOVE_N cycles
  and allow_remove=True → fully restore pre-clamp values from clamp audit for eligible symbols only.
  When remaining set becomes empty, clear clamp active flag.

Usage:
  python -m tools.of_gate_hardstop_cap_unclamp_v4
  (reads ENV vars for thresholds, streak N, relax caps, cooldown, mode, allow_remove)

Environment Variables:
  - Mode: META_UNCLAMP_MODE (AUTO|PROPOSE, default AUTO), META_UNCLAMP_MODE_KEY (redis key override)
  - Allow REMOVE: META_UNCLAMP_ALLOW_REMOVE (0|1, default 1), META_UNCLAMP_ALLOW_REMOVE_KEY (redis key override)
  - Metrics: OF_GATE_METRICS_STREAM, META_HARDSTOP_METRICS_MAX_SCAN, META_HARDSTOP_MIN_N
  - Windows: META_UNCLAMP_SHORT_WINDOW_MIN (30), META_UNCLAMP_LONG_WINDOW_MIN (120), META_UNCLAMP_BASELINE_WINDOW_MIN (720)
  - Hard-stop thresholds: META_HARDSTOP_LAT_P99_US, META_HARDSTOP_EXEC_P90, META_HARDSTOP_SOFT_RATE, META_HARDSTOP_OK_RATE_MIN
  - Baseline thresholds (softer): META_BASELINE_LAT_P99_US, META_BASELINE_EXEC_P90, META_BASELINE_SOFT_RATE, META_BASELINE_OK_RATE_MIN
  - Outcome gate (per-symbol): TRADE_EVENTS_STREAM, META_UNCLAMP_OUTCOME_MAX_SCAN
  - Short outcome (RELAX): META_UNCLAMP_OUTCOME_SHORT_HOURS (2), META_UNCLAMP_OUTCOME_SHORT_MIN_N, META_UNCLAMP_OUTCOME_SHORT_MEAN_MIN, META_UNCLAMP_OUTCOME_SHORT_TAIL_MAX
  - Long outcome (REMOVE): META_UNCLAMP_OUTCOME_LONG_HOURS (24), META_UNCLAMP_OUTCOME_LONG_MIN_N, META_UNCLAMP_OUTCOME_LONG_MEAN_MIN, META_UNCLAMP_OUTCOME_LONG_TAIL_MAX
  - Clamp state: META_CLAMP_ACTIVE_KEY, META_CLAMP_STAGE_KEY, META_HEALTHY_STREAK_KEY
  - State tracking: META_CLAMP_REMAINING_KEY, META_CLAMP_SYM_STATE_KEY
  - Unclamp: META_UNCLAMP_RELAX_STREAK_N, META_UNCLAMP_REMOVE_STREAK_N, META_UNCLAMP_ACTION_COOLDOWN_SEC
  - Pending: META_UNCLAMP_PENDING_KEY, META_UNCLAMP_LAST_ACTION_MS_KEY
  - Relax caps: META_RELAX_CAP_TREND, META_RELAX_CAP_RANGE, META_RELAX_CAP_NEWS, META_RELAX_CAP_OTHER
  - Rec/bot: NOTIFY_TELEGRAM_STREAM, RECS_TTL_SEC, RECS_HMAC_SECRET
  - Config: CFG_HASH_PREFIX
"""

import hashlib
import hmac
import json
import os
import secrets
from typing import Any

import redis

from core.redis_client import get_redis
from utils.time_utils import get_ny_time_millis
import contextlib

# ---------------- utils ----------------

def now_ms() -> int:
    """Returns current timestamp in milliseconds (epoch)."""
    return get_ny_time_millis()


def pctl(xs: list[float], q: float) -> float:
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
        return d


def _i(x: Any, d: int = 0) -> int:
    """Converts value to int with default."""
    try:
        return int(float(x))
    except Exception:
        return d


def sign(bundle_id: str, secret: str) -> str:
    """Computes HMAC-SHA256 signature for bundle_id (first 8 hex chars)."""
    d = hmac.new(secret.encode("utf-8"), bundle_id.encode("utf-8"), hashlib.sha256).hexdigest()
    return d[:8]


def _notify(r: redis.Redis, text: str, buttons: list[list[dict[str, str]]] | None = None) -> None:
    """Sends notification to Telegram stream with optional buttons."""
    fields = {"type": "report", "text": text, "ts": str(now_ms())}
    if buttons is not None:
        fields["buttons"] = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM), fields, maxlen=200000, approximate=True)


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


def _allow_remove(r: redis.Redis) -> bool:
    """
    Default allow, but can override by redis key.
    
    Args:
        r: Redis client
        
    Returns:
        True if REMOVE is allowed, False otherwise
    """
    allow = int(os.getenv("META_UNCLAMP_ALLOW_REMOVE", "1") or 1) == 1
    key = os.getenv("META_UNCLAMP_ALLOW_REMOVE_KEY", "cfg:meta_unclamp:allow_remove")
    try:
        v = (r.get(key) or "").strip()
        if v in ("0", "1"):
            allow = (v == "1")
    except Exception:
        pass
    return allow


# ---------------- metrics: health windows ----------------

def read_metrics_window(r: redis.Redis, stream: str, since_ms: int, max_scan: int) -> list[dict[str, Any]]:
    """
    Reads metrics from Redis stream within time window.
    
    Args:
        r: Redis client
        stream: Stream name (e.g., RS.OF_GATE_METRICS)
        since_ms: Start timestamp (epoch ms)
        max_scan: Maximum number of messages to scan
        
    Returns:
        List of metric records (dict with fields + _ts_ms)
    """
    rows: list[dict[str, Any]] = []
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


def summarize_health(rows: list[dict[str, Any]]) -> dict[str, float]:
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


def is_unhealthy(health: dict[str, float], *, prefix: str,
                min_n: int, lat_thr: float, exec_thr: float, soft_thr: float, ok_min: float) -> tuple[bool, list[str]]:
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


# ---------------- clamp audit read + symbol extraction ----------------

def _read_audit_list(r: redis.Redis, bundle_id: str) -> list[dict[str, Any]]:
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
        with contextlib.suppress(Exception):
            out.append(json.loads(s))
    return out


def _extract_symbols_from_audit(audit: list[dict[str, Any]], cfg_prefix: str) -> list[str]:
    """
    Extracts symbols from clamp audit entries.
    
    Args:
        audit: List of audit entries
        cfg_prefix: Config key prefix (e.g., "config:orderflow:")
        
    Returns:
        Sorted list of unique symbols
    """
    syms = set()
    for a in audit:
        k = (a.get("key", ""))
        if k.startswith(cfg_prefix):
            sym = k[len(cfg_prefix):].strip().upper()
            if sym:
                syms.add(sym)
    return sorted(list(syms))


# ---------------- events:trades outcome gate (per-symbol, 2 windows) ----------------

def _event_ts_ms(fields: dict[str, Any]) -> int:
    """Extracts timestamp from event fields."""
    return _i(fields.get("ts_ms", fields.get("ts", fields.get("timestamp", 0))), 0)


def _is_closed(fields: dict[str, Any]) -> bool:
    """
    Checks if event represents a closed position.
    
    Supports both direct fields and payload JSON.
    """
    et = (fields.get("event_type", fields.get("type", "")) or "").upper()
    if et in ("POSITION_CLOSED", "CLOSE"):
        return True
    p = fields.get("payload")
    if isinstance(p, str) and p and p[0] == "{":
        try:
            j = json.loads(p)
            et2 = (j.get("event_type", j.get("type", "")) or "").upper()
            return et2 in ("POSITION_CLOSED", "CLOSE")
        except Exception:
            return False
    return False


def _get_symbol(fields: dict[str, Any]) -> str:
    """Extracts symbol from event fields (supports payload JSON)."""
    s = (fields.get("symbol", "") or "").upper()
    if s:
        return s
    p = fields.get("payload")
    if isinstance(p, str) and p and p[0] == "{":
        try:
            j = json.loads(p)
            return (j.get("symbol", "") or "").upper()
        except Exception:
            return ""
    return ""


def _get_r_mult(fields: dict[str, Any]) -> float | None:
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


def _stats_r(rs: list[float]) -> dict[str, float]:
    """
    Computes statistics from list of R-multiples.
    
    Args:
        rs: List of R-multiples
        
    Returns:
        Dict with: n, meanR, tail_rate, p05, p50
    """
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


def read_outcome_stats_per_symbol(
    r: redis.Redis,
    *,
    stream: str,
    since_ms: int,
    symbols: list[str],
    max_scan: int,
) -> dict[str, dict[str, float]]:
    """
    Reads outcome statistics per symbol from events:trades stream for closed positions.
    
    Args:
        r: Redis client
        stream: Stream name (e.g., RS.EVENTS_TRADES)
        since_ms: Start timestamp (epoch ms)
        symbols: List of symbols to filter
        max_scan: Maximum number of messages to scan
        
    Returns:
        Dict mapping symbol to stats dict (n, meanR, tail_rate, p05, p50)
    """
    symset = set([s.upper() for s in symbols if s])
    acc: dict[str, list[float]] = {s: [] for s in symset}

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
            if sym not in symset:
                continue
            rm = _get_r_mult(fields)
            if rm is None:
                continue
            acc[sym].append(float(rm))

    out = {}
    for s, rs in acc.items():
        out[s] = _stats_r(rs)
    return out


def outcome_ok(stats: dict[str, float], *, min_n: int, mean_min: float, tail_max: float) -> tuple[bool, list[str]]:
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
        reasons.append(f"low_n<{min_n}")
    if meanR < mean_min:
        reasons.append(f"mean<{mean_min}")
    if tail > tail_max:
        reasons.append(f"tail>{tail_max}")

    return (len(reasons) == 0), reasons


# ---------------- apply helpers (AUTO / PROPOSE) ----------------

def _apply_restores_direct(
    r: redis.Redis,
    *,
    who: str,
    ttl_sec: int,
    restores: list[dict[str, Any]],
) -> tuple[str, str]:
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
            v = (op.get("value", ""))
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
    ops: list[dict[str, Any]],
    meta: dict[str, Any],
) -> tuple[str, str]:
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


# ---------------- restore builders (selective by symbol) ----------------

def build_relax_ops_selective(
    clamp_audit: list[dict[str, Any]],
    *,
    relax_caps: dict[str, float],
    cfg_prefix: str,
    eligible_syms: list[str],
) -> list[dict[str, Any]]:
    """
    For eligible symbols only: restore old values capped by relax caps.
    Only if field existed pre-clamp (old_null==0).
    
    Args:
        clamp_audit: List of audit entries from clamp bundle
        relax_caps: Dict mapping field names to cap values
        cfg_prefix: Config key prefix
        eligible_syms: List of eligible symbols (upper case)
        
    Returns:
        List of operations {op, key, field, value}
    """
    elig = set([s.upper() for s in eligible_syms])
    ops = []
    for a in clamp_audit:
        if (a.get("op")) != "HSET":
            continue
        key = (a.get("key", ""))
        if not key.startswith(cfg_prefix):
            continue
        sym = key[len(cfg_prefix):].strip().upper()
        if sym not in elig:
            continue

        field = (a.get("field", ""))
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
        ops.append({"op": "HSET", "key": key, "field": field, "value": f"{target:.2f}"})
    return ops


def build_remove_ops_selective(
    clamp_audit: list[dict[str, Any]],
    *,
    cfg_prefix: str,
    eligible_syms: list[str],
) -> list[dict[str, Any]]:
    """
    For eligible symbols only: restore exact pre-clamp (HSET old / HDEL if old_null==1).
    
    Args:
        clamp_audit: List of audit entries from clamp bundle
        cfg_prefix: Config key prefix
        eligible_syms: List of eligible symbols (upper case)
        
    Returns:
        List of operations {op, key, field, value}
    """
    elig = set([s.upper() for s in eligible_syms])
    ops = []
    for a in clamp_audit:
        if (a.get("op")) != "HSET":
            continue
        key = (a.get("key", ""))
        if not key.startswith(cfg_prefix):
            continue
        sym = key[len(cfg_prefix):].strip().upper()
        if sym not in elig:
            continue

        field = (a.get("field", ""))
        old_null = int(a.get("old_null", 0) or 0)
        if old_null == 1:
            ops.append({"op": "HDEL", "key": key, "field": field})
        else:
            ops.append({"op": "HSET", "key": key, "field": field, "value": ("" if a.get("old") is None else (a.get("old", "")))})
    return ops


# ---------------- state: remaining + sym_state ----------------

def _init_remaining_if_needed(
    r: redis.Redis,
    *,
    remaining_key: str,
    sym_state_key: str,
    clamp_audit: list[dict[str, Any]],
    cfg_prefix: str,
) -> list[str]:
    """
    Create remaining symbol set if missing.
    
    Args:
        r: Redis client
        remaining_key: Redis key for remaining symbols set
        sym_state_key: Redis key for symbol state hash
        clamp_audit: List of audit entries from clamp bundle
        cfg_prefix: Config key prefix
        
    Returns:
        Sorted list of remaining symbols
    """
    if r.scard(remaining_key) > 0:
        return sorted(list(r.smembers(remaining_key)))

    syms = _extract_symbols_from_audit(clamp_audit, cfg_prefix)
    if not syms:
        return []

    pipe = r.pipeline()
    for s in syms:
        pipe.sadd(remaining_key, s)
        pipe.hset(sym_state_key, s, "CLAMPED")
    pipe.execute()
    return syms


# ---------------- main ----------------

def main() -> None:
    """Main entry point: checks clamp state, triple-window health + per-symbol outcome gate, applies selective relax/unclamp if conditions met."""
    try:
        r = get_redis(retry_attempts=10, retry_delay=2)
    except Exception as e:
        print(f"ERROR: Failed to connect to Redis: {e}")
        raise

    ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)

    cfg_prefix = os.getenv("CFG_HASH_PREFIX", "config:orderflow:")
    clamp_active_key = os.getenv("META_CLAMP_ACTIVE_KEY", "meta:hardstop:clamp:active")
    clamp_stage_key = os.getenv("META_CLAMP_STAGE_KEY", "meta:hardstop:clamp:stage")  # CLAMPED|RELAXED
    healthy_streak_key = os.getenv("META_HEALTHY_STREAK_KEY", "meta:hardstop:healthy_streak")

    remaining_key = os.getenv("META_CLAMP_REMAINING_KEY", "meta:hardstop:clamp:remaining_syms")
    sym_state_key = os.getenv("META_CLAMP_SYM_STATE_KEY", "meta:hardstop:clamp:sym_state")

    pending_key = os.getenv("META_UNCLAMP_PENDING_KEY", "meta:hardstop:unclamp:pending")
    last_action_key = os.getenv("META_UNCLAMP_LAST_ACTION_MS_KEY", "meta:hardstop:unclamp:last_action_ms")

    clamp_bundle_id = (r.get(clamp_active_key) or "").strip()
    if not clamp_bundle_id:
        # reset
        r.delete(healthy_streak_key)
        r.delete(clamp_stage_key)
        r.delete(pending_key)
        r.delete(remaining_key)
        r.delete(sym_state_key)
        return

    mode = _mode(r)
    allow_remove = _allow_remove(r)

    # PROPOSE lifecycle: if pending exists and applied/rejected -> update state & exit
    pending_raw = r.get(pending_key)
    if pending_raw:
        try:
            pend = json.loads(pending_raw)
        except Exception:
            pend = None
        if isinstance(pend, dict) and pend.get("bundle_id"):
            bid = str(pend["bundle_id"])
            st = (r.get(f"recs:status:{bid}") or "").strip().upper()
            action = (pend.get("action", "")).upper()
            syms = pend.get("symbols") or []
            syms = [str(s).upper() for s in syms if str(s).strip()]

            if st == "APPLIED":
                # state side-effects
                pipe = r.pipeline()
                if action == "RELAX":
                    r.set(clamp_stage_key, "RELAXED", ex=ttl)
                    for s in syms:
                        pipe.hset(sym_state_key, s, "RELAXED")
                elif action == "REMOVE":
                    for s in syms:
                        pipe.hset(sym_state_key, s, "RESTORED")
                        pipe.srem(remaining_key, s)
                    pipe.execute()
                    # if remaining empty -> clear clamp
                    if r.scard(remaining_key) == 0:
                        r.delete(clamp_active_key)
                        r.delete(clamp_stage_key)
                        r.delete(healthy_streak_key)
                        r.delete(remaining_key)
                        r.delete(sym_state_key)

                r.delete(pending_key)
                r.set(last_action_key, str(now_ms()), ex=ttl)
                _notify(r, f"<b>Unclamp proposal applied</b>\naction=<code>{action}</code>\nid=<code>{bid}</code>\nsyms=<code>{syms}</code>\nmode=<code>{mode}</code>")
                return

            if st == "REJECTED":
                r.delete(pending_key)
                r.set(last_action_key, str(now_ms()), ex=ttl)
                _notify(r, f"<b>Unclamp proposal rejected</b>\naction=<code>{action}</code>\nid=<code>{bid}</code>\nsyms=<code>{syms}</code>\nmode=<code>{mode}</code>")
                return

            return  # still pending: no spam

    # cooldown
    cooldown_sec = int(os.getenv("META_UNCLAMP_ACTION_COOLDOWN_SEC", "1800") or 1800)
    last_action_ms = _i(r.get(last_action_key), 0)
    if last_action_ms and (now_ms() - last_action_ms) < cooldown_sec * 1000:
        return

    # read clamp audit + init remaining set if needed
    clamp_audit = _read_audit_list(r, clamp_bundle_id)
    if not clamp_audit:
        return
    remaining = _init_remaining_if_needed(
        r,
        remaining_key=remaining_key,
        sym_state_key=sym_state_key,
        clamp_audit=clamp_audit,
        cfg_prefix=cfg_prefix,
    )
    if not remaining:
        return

    # stage
    stage = (r.get(clamp_stage_key) or "CLAMPED").strip().upper()
    if stage not in ("CLAMPED", "RELAXED"):
        stage = "CLAMPED"
        r.set(clamp_stage_key, stage, ex=ttl)

    # health: 30m + 2h + 12h
    metrics_stream = os.getenv("OF_GATE_METRICS_STREAM", RS.OF_GATE_METRICS)
    max_scan = int(os.getenv("META_HARDSTOP_METRICS_MAX_SCAN", "200000") or 200000)

    w30 = int(os.getenv("META_UNCLAMP_SHORT_WINDOW_MIN", "30") or 30)
    w120 = int(os.getenv("META_UNCLAMP_LONG_WINDOW_MIN", "120") or 120)
    w720 = int(os.getenv("META_UNCLAMP_BASELINE_WINDOW_MIN", "720") or 720)

    h30 = summarize_health(read_metrics_window(r, metrics_stream, now_ms() - w30 * 60_000, max_scan=max_scan))
    h120 = summarize_health(read_metrics_window(r, metrics_stream, now_ms() - w120 * 60_000, max_scan=max_scan))
    h720 = summarize_health(read_metrics_window(r, metrics_stream, now_ms() - w720 * 60_000, max_scan=max_scan))

    min_n = int(os.getenv("META_HARDSTOP_MIN_N", "200") or 200)

    # strict thresholds for 30m/2h
    lat_thr = float(os.getenv("META_HARDSTOP_LAT_P99_US", "12000") or 12000)
    exec_thr = float(os.getenv("META_HARDSTOP_EXEC_P90", "0.92") or 0.92)
    soft_thr = float(os.getenv("META_HARDSTOP_SOFT_RATE", "0.60") or 0.60)
    ok_min = float(os.getenv("META_HARDSTOP_OK_RATE_MIN", "0.10") or 0.10)

    # baseline (12h) can be softer
    lat_thr_b = float(os.getenv("META_BASELINE_LAT_P99_US", "15000") or 15000)
    exec_thr_b = float(os.getenv("META_BASELINE_EXEC_P90", "0.95") or 0.95)
    soft_thr_b = float(os.getenv("META_BASELINE_SOFT_RATE", "0.70") or 0.70)
    ok_min_b = float(os.getenv("META_BASELINE_OK_RATE_MIN", "0.08") or 0.08)

    bad30, r30 = is_unhealthy(h30, prefix="w30", min_n=min_n, lat_thr=lat_thr, exec_thr=exec_thr, soft_thr=soft_thr, ok_min=ok_min)
    bad120, r120 = is_unhealthy(h120, prefix="w120", min_n=min_n, lat_thr=lat_thr, exec_thr=exec_thr, soft_thr=soft_thr, ok_min=ok_min)
    bad720, r720 = is_unhealthy(h720, prefix="w720", min_n=min_n, lat_thr=lat_thr_b, exec_thr=exec_thr_b, soft_thr=soft_thr_b, ok_min=ok_min_b)

    health_ok = (not bad30) and (not bad120) and (not bad720)
    health_reasons = r30 + r120 + r720

    # outcome: per-symbol short + long
    trades_stream = os.getenv("TRADE_EVENTS_STREAM", RS.EVENTS_TRADES)
    out_max_scan = int(os.getenv("META_UNCLAMP_OUTCOME_MAX_SCAN", "400000") or 400000)

    out_short_h = float(os.getenv("META_UNCLAMP_OUTCOME_SHORT_HOURS", "2") or 2)
    out_long_h = float(os.getenv("META_UNCLAMP_OUTCOME_LONG_HOURS", "24") or 24)

    st_short = read_outcome_stats_per_symbol(
        r,
        stream=trades_stream,
        since_ms=now_ms() - int(out_short_h * 3600_000),
        symbols=remaining,
        max_scan=out_max_scan,
    )
    st_long = read_outcome_stats_per_symbol(
        r,
        stream=trades_stream,
        since_ms=now_ms() - int(out_long_h * 3600_000),
        symbols=remaining,
        max_scan=out_max_scan,
    )

    # thresholds: short and long can differ
    s_min_n = int(os.getenv("META_UNCLAMP_OUTCOME_SHORT_MIN_N", "20") or 20)
    s_mean_min = float(os.getenv("META_UNCLAMP_OUTCOME_SHORT_MEAN_MIN", "-0.03") or -0.03)
    s_tail_max = float(os.getenv("META_UNCLAMP_OUTCOME_SHORT_TAIL_MAX", "0.35") or 0.35)

    l_min_n = int(os.getenv("META_UNCLAMP_OUTCOME_LONG_MIN_N", "80") or 80)
    l_mean_min = float(os.getenv("META_UNCLAMP_OUTCOME_LONG_MEAN_MIN", "-0.02") or -0.02)
    l_tail_max = float(os.getenv("META_UNCLAMP_OUTCOME_LONG_TAIL_MAX", "0.30") or 0.30)

    # eligibility
    relax_syms = []
    remove_syms = []
    out_debug = {}

    for sym in remaining:
        ss = st_short.get(sym, {"n": 0.0})
        ll = st_long.get(sym, {"n": 0.0})
        ok_s, rs_s = outcome_ok(ss, min_n=s_min_n, mean_min=s_mean_min, tail_max=s_tail_max)
        ok_l, rs_l = outcome_ok(ll, min_n=l_min_n, mean_min=l_mean_min, tail_max=l_tail_max)

        out_debug[sym] = {"short": ss, "short_ok": ok_s, "short_bad": rs_s, "long": ll, "long_ok": ok_l, "long_bad": rs_l}

        if ok_s:
            relax_syms.append(sym)
        if ok_s and ok_l:
            remove_syms.append(sym)

    # update streak: only if health_ok and at least some relax candidates exist
    prev = _i(r.get(healthy_streak_key), 0)
    if health_ok and len(relax_syms) > 0:
        streak = prev + 1
    else:
        streak = 0
    r.set(healthy_streak_key, str(streak), ex=ttl)

    relax_n = int(os.getenv("META_UNCLAMP_RELAX_STREAK_N", "6") or 6)
    remove_n = int(os.getenv("META_UNCLAMP_REMOVE_STREAK_N", "18") or 18)

    # decide action:
    # - RELAX: requires stage=CLAMPED and streak>=relax_n and has relax_syms
    # - REMOVE: requires stage=RELAXED and streak>=remove_n and allow_remove and has remove_syms
    action = None
    syms_to_act = []

    if stage == "CLAMPED" and streak >= relax_n and relax_syms:
        action = "RELAX"
        syms_to_act = relax_syms
    elif stage == "RELAXED" and streak >= remove_n and allow_remove and remove_syms:
        action = "REMOVE"
        syms_to_act = remove_syms
    else:
        return

    # build ops
    relax_caps = {
        "meta_enforce_share_trend": float(os.getenv("META_RELAX_CAP_TREND", "0.25") or 0.25),
        "meta_enforce_share_range": float(os.getenv("META_RELAX_CAP_RANGE", "0.15") or 0.15),
        "meta_enforce_share_news": float(os.getenv("META_RELAX_CAP_NEWS", "0.00") or 0.00),
        "meta_enforce_share_other": float(os.getenv("META_RELAX_CAP_OTHER", "0.00") or 0.00),
    }

    if action == "RELAX":
        ops = build_relax_ops_selective(
            clamp_audit,
            relax_caps=relax_caps,
            cfg_prefix=cfg_prefix,
            eligible_syms=syms_to_act,
        )
        who = "of_gate_hardstop_cap_unclamp_v4_relax"
        meta = {"kind": "meta_unclamp_v4_relax", "clamp_id": clamp_bundle_id, "symbols": syms_to_act,
                "health": {"30m": h30, "2h": h120, "12h": h720}, "outcome": out_debug}
    else:
        ops = build_remove_ops_selective(
            clamp_audit,
            cfg_prefix=cfg_prefix,
            eligible_syms=syms_to_act,
        )
        who = "of_gate_hardstop_cap_unclamp_v4_remove"
        meta = {"kind": "meta_unclamp_v4_remove", "clamp_id": clamp_bundle_id, "symbols": syms_to_act,
                "health": {"30m": h30, "2h": h120, "12h": h720}, "outcome": out_debug}

    if not ops:
        return

    # AUTO vs PROPOSE
    if mode == "AUTO":
        bid, sig = _apply_restores_direct(r, who=who, ttl_sec=ttl, restores=ops)

        pipe = r.pipeline()
        if action == "RELAX":
            r.set(clamp_stage_key, "RELAXED", ex=ttl)
            for s in syms_to_act:
                pipe.hset(sym_state_key, s, "RELAXED")
        else:
            for s in syms_to_act:
                pipe.hset(sym_state_key, s, "RESTORED")
                pipe.srem(remaining_key, s)
        pipe.execute()

        # clear clamp if all restored
        if action == "REMOVE" and r.scard(remaining_key) == 0:
            r.delete(clamp_active_key)
            r.delete(clamp_stage_key)
            r.delete(healthy_streak_key)
            r.delete(remaining_key)
            r.delete(sym_state_key)

        r.set(last_action_key, str(now_ms()), ex=ttl)

        buttons = [[{"text": "↩ Rollback", "callback": f"recs:rollback:{bid}:{sig}"}]]
        _notify(
            r,
            "<b>Unclamp AUTO applied (v4 selective)</b>\n"
            f"action=<code>{action}</code> id=<code>{bid}</code>\n"
            f"syms=<code>{syms_to_act}</code>\n"
            f"remaining=<code>{sorted(list(r.smembers(remaining_key)))}</code>\n"
            f"mode=<code>{mode}</code> allow_remove=<code>{int(allow_remove)}</code>\n"
            f"streak=<code>{streak}</code>\n"
            f"health_ok=<code>{int(health_ok)}</code> health_bad=<code>{health_reasons}</code>\n"
            f"h30=<code>{h30}</code>\n"
            f"h2h=<code>{h120}</code>\n"
            f"h12h=<code>{h720}</code>\n"
            f"out_short_h=<code>{out_short_h}</code> out_long_h=<code>{out_long_h}</code>",
            buttons=buttons,
        )
        return

    # PROPOSE mode: create bundle and wait for callback worker apply
    bid, sig = _create_proposal_bundle(r, who=who, ttl_sec=ttl, ops=ops, meta=meta)
    pend = {"bundle_id": bid, "action": action, "symbols": syms_to_act, "created_ms": now_ms()}
    r.set(pending_key, json.dumps(pend, ensure_ascii=False, separators=(",", ":")), ex=ttl)
    r.set(last_action_key, str(now_ms()), ex=ttl)

    buttons = [[
        {"text": "✅ Approve (preview)", "callback": f"recs:preview:{bid}:{sig}"},
        {"text": "❌ Reject",           "callback": f"recs:reject:{bid}:{sig}"},
    ]]
    _notify(
        r,
        "<b>Unclamp PROPOSAL (v4 selective)</b>\n"
        f"action=<code>{action}</code> id=<code>{bid}</code>\n"
        f"syms=<code>{syms_to_act}</code>\n"
        f"remaining=<code>{remaining}</code>\n"
        f"mode=<code>{mode}</code> allow_remove=<code>{int(allow_remove)}</code>\n"
        f"streak=<code>{streak}</code>\n"
        f"health_ok=<code>{int(health_ok)}</code> health_bad=<code>{health_reasons}</code>\n"
        f"h30=<code>{h30}</code>\n"
        f"h2h=<code>{h120}</code>\n"
        f"h12h=<code>{h720}</code>\n"
        f"out_short_h=<code>{out_short_h}</code> out_long_h=<code>{out_long_h}</code>",
        buttons=buttons,
    )


if __name__ == "__main__":
    main()

