from __future__ import annotations
from core.redis_keys import RedisStreams as RS

"""Staged auto-unclamp v6: triple gate for range (health_global + health_range_segment + outcome_range_long).

This is v6: adds bucket-specific health from metrics:of_gate for range segment.
Range cells require triple gate: health_global (30m+2h+12h) AND health_range_segment (exec_risk_norm p90 cap) AND outcome_range_long OK.

Key features:
- Triple gate for range: health_global AND health_range_segment AND outcome_range_long OK
- Range-segment health: filters metrics:of_gate by bucket=range (regime_group/regime/scenario_v4) and checks exec_risk_norm p90
- Trend cells: only require health_global + outcome (no segment health gate)
- Selective per-cell RELAX/RESTORE based on per-bucket eligibility
- AUTO mode by default, can switch to PROPOSE (Approve/Reject)
- Per-cell state tracking: CLAMPED/RELAXED/RESTORED with remaining cells set

Stage A (RELAX): when all windows healthy + short-outcome OK (2h) for bucket + (for range: range-segment health OK) and streak >= RELAX_N cycles
  → partially restore caps (trend≤0.30, range≤0.10) not higher than pre-clamp for eligible cells only.

Stage B (RESTORE): when all windows healthy + long-outcome OK (24h) for bucket + (for range: range-segment health OK) and streak >= REMOVE_N cycles
  and allow_remove=True → fully restore pre-clamp values from clamp audit for eligible cells only.
  When remaining set becomes empty, clear clamp active flag.

Usage:
  python -m tools.of_gate_hardstop_cap_unclamp_v6
  (reads ENV vars for thresholds, streak N, relax caps, cooldown, mode, allow_remove)

Environment Variables:
  - Mode: META_UNCLAMP_MODE (AUTO|PROPOSE, default AUTO), META_UNCLAMP_MODE_KEY (redis key override)
  - Allow REMOVE: META_UNCLAMP_ALLOW_REMOVE (0|1, default 1), META_UNCLAMP_ALLOW_REMOVE_KEY (redis key override)
  - Metrics: OF_GATE_METRICS_STREAM, META_HARDSTOP_METRICS_MAX_SCAN, META_HARDSTOP_MIN_N
  - Windows: META_UNCLAMP_SHORT_WINDOW_MIN (30), META_UNCLAMP_LONG_WINDOW_MIN (120), META_UNCLAMP_BASELINE_WINDOW_MIN (720)
  - Hard-stop thresholds: META_HARDSTOP_LAT_P99_US, META_HARDSTOP_EXEC_P90, META_HARDSTOP_SOFT_RATE, META_HARDSTOP_OK_RATE_MIN
  - Baseline thresholds (softer): META_BASELINE_LAT_P99_US, META_BASELINE_EXEC_P90, META_BASELINE_SOFT_RATE, META_BASELINE_OK_RATE_MIN
  - Segment health (range): META_SEG_HEALTH_ENABLED (0|1, default 1), META_SEG_MIN_N, META_SEG_RANGE_EXEC_P90_MAX_RELAX, META_SEG_RANGE_EXEC_P90_MAX_RESTORE
  - Outcome gate (per bucket): TRADE_EVENTS_STREAM, META_UNCLAMP_OUTCOME_MAX_SCAN
  - Short outcome (RELAX) per bucket: META_UNCLAMP_OUTCOME_SHORT_HOURS (2), META_OUT_S_MIN_N_TREND, META_OUT_S_MEAN_MIN_TREND, META_OUT_S_TAIL_MAX_TREND, META_OUT_S_MIN_N_RANGE, META_OUT_S_MEAN_MIN_RANGE, META_OUT_S_TAIL_MAX_RANGE
  - Long outcome (RESTORE) per bucket: META_UNCLAMP_OUTCOME_LONG_HOURS (24), META_OUT_L_MIN_N_TREND, META_OUT_L_MEAN_MIN_TREND, META_OUT_L_TAIL_MAX_TREND, META_OUT_L_MIN_N_RANGE, META_OUT_L_MEAN_MIN_RANGE, META_OUT_L_TAIL_MAX_RANGE
  - Clamp state: META_CLAMP_ACTIVE_KEY, META_CLAMP_STAGE_KEY
  - State tracking: META_CLAMP_REMAINING_CELLS_KEY, META_CLAMP_CELL_STATE_KEY
  - Unclamp: META_UNCLAMP_RELAX_STREAK_N, META_UNCLAMP_REMOVE_STREAK_N, META_UNCLAMP_ACTION_COOLDOWN_SEC
  - Pending: META_UNCLAMP_PENDING_KEY, META_UNCLAMP_LAST_ACTION_MS_KEY
  - Relax caps per bucket: META_RELAX_CAP_TREND, META_RELAX_CAP_RANGE
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

from common.redis_errors import retry_redis_operation
from utils.time_utils import get_ny_time_millis
import contextlib

# ---------------- basic utils ----------------

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
    notify_stream = os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM)
    retry_redis_operation(
        lambda: r.xadd(notify_stream, fields, maxlen=200000, approximate=True),
        operation_name=f"xadd {notify_stream}",
    )


def _mode(r: redis.Redis) -> str:
    """Reads mode from ENV or Redis key (AUTO|PROPOSE, default AUTO)."""
    m = (os.getenv("META_UNCLAMP_MODE", "AUTO") or "AUTO").strip().upper()
    key = os.getenv("META_UNCLAMP_MODE_KEY", "cfg:meta_unclamp:mode")
    try:
        v = retry_redis_operation(
            lambda: (r.get(key) or "").strip().upper(),
            operation_name=f"get {key} (mode)",
        )
        if v in ("AUTO", "PROPOSE"):
            m = v
    except Exception:
        pass
    return m if m in ("AUTO", "PROPOSE") else "AUTO"


def _allow_remove(r: redis.Redis) -> bool:
    """Reads allow_remove from ENV or Redis key (0|1, default 1)."""
    allow = int(os.getenv("META_UNCLAMP_ALLOW_REMOVE", "1") or 1) == 1
    key = os.getenv("META_UNCLAMP_ALLOW_REMOVE_KEY", "cfg:meta_unclamp:allow_remove")
    try:
        v = retry_redis_operation(
            lambda: (r.get(key) or "").strip(),
            operation_name=f"get {key} (allow_remove)",
        )
        if v in ("0", "1"):
            allow = (v == "1")
    except Exception:
        pass
    return allow


# ---------------- metrics:of_gate health ----------------

def read_metrics_window(r: redis.Redis, stream: str, since_ms: int, max_scan: int) -> list[dict[str, Any]]:
    """Reads metrics from Redis stream since timestamp, up to max_scan messages."""
    rows: list[dict[str, Any]] = []
    last_id = "+"
    scanned = 0
    while scanned < max_scan:
        batch = retry_redis_operation(
            lambda: r.xrevrange(stream, max=last_id, min="-", count=500),
            operation_name=f"xrevrange {stream}",
        )
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
    """Summarizes health metrics from rows: ok_rate, soft_rate, lat_p99_us, exec_p90."""
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
        "ok_rate": float(ok / n),
        "soft_rate": float(soft / n),
        "lat_p99_us": float(pctl(lat, 0.99)),
        "exec_p90": float(pctl(ex, 0.90)),
    }


def _metric_bucket(m: dict[str, Any]) -> str:
    """
    Derive bucket from metric fields: regime_group / regime / scenario_v4.
    Returns: "trend", "range", or "other"
    """
    g = str(m.get("regime_group", "") or m.get("regime", "") or m.get("scenario_v4", "") or "")
    s = g.lower()
    if "trend" in s or "bull" in s or "bear" in s:
        return "trend"
    from common.market_mode import is_range_regime; _r = is_range_regime(s)
    if _r:
        return "range"
    return "other"


def summarize_health_by_bucket(rows: list[dict[str, Any]]) -> dict[str, dict[str, float]]:
    """
    Summarizes health metrics grouped by bucket (trend/range/other).
    Returns: {bucket: {n, ok_rate, soft_rate, lat_p99_us, exec_p90}}
    """
    buckets: dict[str, list[dict[str, Any]]] = {"trend": [], "range": [], "other": []}
    for r in rows:
        b = _metric_bucket(r)
        buckets.setdefault(b, []).append(r)
    return {b: summarize_health(rs) for b, rs in buckets.items()}


def is_unhealthy(health: dict[str, float], *, prefix: str,
                min_n: int, lat_thr: float, exec_thr: float, soft_thr: float, ok_min: float) -> tuple[bool, list[str]]:
    """Checks if health metrics indicate unhealthy state. Returns (is_unhealthy, reasons)."""
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


def range_segment_ok(seg: dict[str, float], *, min_n: int, exec_p90_max: float) -> tuple[bool, str]:
    """
    Checks if range segment health is OK.
    Fail-closed: if seg.n < min_n -> not ok.
    Returns: (ok, debug_message)
    """
    n = float(seg.get("n", 0.0))
    ex = float(seg.get("exec_p90", 0.0))
    if n < float(min_n):
        return False, f"seg_low_n<{min_n} n={n}"
    if ex > exec_p90_max:
        return False, f"seg_exec_p90>{exec_p90_max} exec_p90={ex}"
    return True, f"ok n={n} exec_p90={ex}"


# ---------------- clamp audit + cells ----------------

def _read_audit_list(r: redis.Redis, bundle_id: str) -> list[dict[str, Any]]:
    """Reads audit list from Redis list key recs:audit:{bundle_id}."""
    key = f"recs:audit:{bundle_id}"
    n = retry_redis_operation(
        lambda: r.llen(key),
        operation_name=f"llen {key}",
    )
    out = []
    for i in range(n):
        s = retry_redis_operation(
            lambda: r.lindex(key, i),
            operation_name=f"lindex {key} {i}",
        )
        if not s:
            continue
        with contextlib.suppress(Exception):
            out.append(json.loads(s))
    return out


def _extract_symbols_from_audit(audit: list[dict[str, Any]], cfg_prefix: str) -> list[str]:
    """Extracts unique symbols from clamp audit entries."""
    syms = set()
    for a in audit:
        k = (a.get("key", ""))
        if k.startswith(cfg_prefix):
            sym = k[len(cfg_prefix):].strip().upper()
            if sym:
                syms.add(sym)
    return sorted(list(syms))


def _audit_has_field_for_sym(audit: list[dict[str, Any]], cfg_key: str, field: str) -> bool:
    """Checks if audit has HSET operation for given key and field."""
    for a in audit:
        if (a.get("op")) != "HSET":
            continue
        if (a.get("key")) == cfg_key and (a.get("field")) == field:
            return True
    return False


def _init_remaining_cells_if_needed(
    r: redis.Redis,
    *,
    remaining_cells_key: str,
    cell_state_key: str,
    clamp_audit: list[dict[str, Any]],
    cfg_prefix: str,
    ttl: int,
) -> list[str]:
    """
    Initializes remaining_cells set if empty.
    remaining_cells set contains cells like "SYM|trend" and "SYM|range" if those fields exist in clamp audit.
    """
    if retry_redis_operation(
        lambda: r.scard(remaining_cells_key),
        operation_name=f"scard {remaining_cells_key}",
    ) > 0:
        return sorted(list(retry_redis_operation(
            lambda: r.smembers(remaining_cells_key),
            operation_name=f"smembers {remaining_cells_key}",
        )))

    syms = _extract_symbols_from_audit(clamp_audit, cfg_prefix)
    if not syms:
        return []

    def _init_cells():
        pipe = r.pipeline()
        for sym in syms:
            hk = f"{cfg_prefix}{sym}"
            for bucket, field in (("trend", "meta_enforce_share_trend"), ("range", "meta_enforce_share_range")):
                if _audit_has_field_for_sym(clamp_audit, hk, field):
                    cell = f"{sym}|{bucket}"
                    pipe.sadd(remaining_cells_key, cell)
                    pipe.hset(cell_state_key, cell, "CLAMPED")
        pipe.execute()
        r.expire(remaining_cells_key, ttl)
        r.expire(cell_state_key, ttl)
        return sorted(list(r.smembers(remaining_cells_key)))

    return retry_redis_operation(
        _init_cells,
        operation_name="init remaining_cells",
    )


# ---------------- events:trades outcome per symbol per bucket ----------------

def _event_ts_ms(fields: dict[str, Any]) -> int:
    """Extracts timestamp in ms from event fields."""
    return _i(fields.get("ts_ms", fields.get("ts", fields.get("timestamp", 0))), 0)


def _is_closed(fields: dict[str, Any]) -> bool:
    """Checks if event is a closed position event."""
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
    """Extracts symbol from event fields."""
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


def _get_bucket(fields: dict[str, Any]) -> str:
    """Extracts bucket (trend/range/other) from event fields."""
    g = (fields.get("regime_group", fields.get("regime", fields.get("scenario_v4", ""))) or "").lower()
    if not g:
        p = fields.get("payload")
        if isinstance(p, str) and p and p[0] == "{":
            try:
                j = json.loads(p)
                g = (j.get("regime_group", j.get("regime", j.get("scenario_v4", ""))) or "").lower()
            except Exception:
                g = ""
    if "trend" in g or "bull" in g or "bear" in g:
        return "trend"
    if "range" in g or "chop" in g or "meanrev" in g:
        return "range"
    return "other"


def _get_r_mult(fields: dict[str, Any]) -> float | None:
    """Extracts r_mult from event fields."""
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
    """Computes statistics from list of R values: n, meanR, tail_rate, p05, p50."""
    n = len(rs)
    if n == 0:
        return {"n": 0.0}
    mean = sum(rs) / n
    tail = sum(1 for x in rs if x <= -1.0) / n
    return {"n": float(n), "meanR": float(mean), "tail_rate": float(tail), "p05": float(pctl(rs, 0.05)), "p50": float(pctl(rs, 0.50))}


def read_outcome_stats_sym_bucket(
    r: redis.Redis,
    *,
    stream: str,
    since_ms: int,
    symbols: list[str],
    max_scan: int,
) -> dict[str, dict[str, dict[str, float]]]:
    """
    Reads outcome stats per symbol per bucket from trades stream.
    Returns stats[sym][bucket] = {n, meanR, tail_rate, p05, p50}
    buckets: trend/range/other (we will use trend/range).
    """
    symset = set([s.upper() for s in symbols if s])
    acc: dict[str, dict[str, list[float]]] = {s: {"trend": [], "range": []} for s in symset}

    scanned = 0
    last_id = "+"

    while scanned < max_scan:
        batch = retry_redis_operation(
            lambda: r.xrevrange(stream, max=last_id, min="-", count=2000),
            operation_name=f"xrevrange {stream} (outcome)",
        )
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
            b = _get_bucket(fields)
            if b not in ("trend", "range"):
                continue
            rm = _get_r_mult(fields)
            if rm is None:
                continue
            acc[sym][b].append(float(rm))

    out: dict[str, dict[str, dict[str, float]]] = {}
    for s in symset:
        out[s] = {"trend": _stats_r(acc[s]["trend"]), "range": _stats_r(acc[s]["range"])}
    return out


def outcome_ok(stats: dict[str, float], *, min_n: int, mean_min: float, tail_max: float) -> bool:
    """Checks if outcome stats pass thresholds. Returns ok (bool)."""
    n = float(stats.get("n", 0.0))
    meanR = float(stats.get("meanR", 0.0))
    tail = float(stats.get("tail_rate", 0.0))
    if n < float(min_n):
        return False
    if meanR < mean_min:
        return False
    if tail > tail_max:
        return False
    return True


# ---------------- apply helpers ----------------

def _apply_restores_direct(
    r: redis.Redis,
    *,
    who: str,
    ttl_sec: int,
    restores: list[dict[str, Any]],
) -> tuple[str, str]:
    """Applies restore operations directly and creates bundle."""
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    bundle_id = secrets.token_hex(6)
    sig = sign(bundle_id, secret)
    ts = now_ms()

    pipe = r.pipeline()
    audit_out = []
    ops_out = []

    for op in restores:
        k = str(op["key"]); f = str(op["field"])
        cur = retry_redis_operation(
            lambda: r.hget(k, f),
            operation_name=f"hget {k} {f}",
        )
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

    def _apply_ops():
        pipe.execute()
        bundle = {"id": bundle_id, "created_ms": ts, "ttl_sec": ttl_sec, "who": who, "ops": ops_out, "meta": {"kind": "meta_unclamp_v6_step"}}
        r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl_sec)
        r.set(f"recs:status:{bundle_id}", "APPLIED", ex=ttl_sec)
        for a in audit_out:
            r.rpush(f"recs:audit:{bundle_id}", json.dumps(a, ensure_ascii=False, separators=(",", ":")))
        r.expire(f"recs:audit:{bundle_id}", ttl_sec)

    retry_redis_operation(
        _apply_ops,
        operation_name="apply_restores_direct",
    )
    return bundle_id, sig


def _create_proposal_bundle(
    r: redis.Redis,
    *,
    who: str,
    ttl_sec: int,
    ops: list[dict[str, Any]],
    meta: dict[str, Any],
) -> tuple[str, str]:
    """Creates proposal bundle (PENDING status)."""
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    bundle_id = secrets.token_hex(6)
    sig = sign(bundle_id, secret)
    ts = now_ms()
    bundle = {"id": bundle_id, "created_ms": ts, "ttl_sec": ttl_sec, "who": who, "ops": ops, "meta": meta}
    retry_redis_operation(
        lambda: r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl_sec),
        operation_name=f"set recs:bundle:{bundle_id}",
    )
    retry_redis_operation(
        lambda: r.set(f"recs:status:{bundle_id}", "PENDING", ex=ttl_sec),
        operation_name=f"set recs:status:{bundle_id}",
    )
    return bundle_id, sig


# ---------------- ops builders per cell ----------------

def build_relax_ops_cells(
    clamp_audit: list[dict[str, Any]],
    *,
    cfg_prefix: str,
    eligible_cells: list[str],
    cap_trend: float,
    cap_range: float,
) -> list[dict[str, Any]]:
    """Builds RELAX operations for eligible cells (per-cell, per-bucket caps)."""
    elig = set([c.upper() for c in eligible_cells])
    ops = []
    for a in clamp_audit:
        if (a.get("op")) != "HSET":
            continue
        key = (a.get("key", ""))
        if not key.startswith(cfg_prefix):
            continue
        sym = key[len(cfg_prefix):].strip().upper()

        field = (a.get("field", ""))
        if field not in ("meta_enforce_share_trend", "meta_enforce_share_range"):
            continue
        bucket = "trend" if field.endswith("_trend") else "range"
        cell = f"{sym}|{bucket}"
        if cell.upper() not in elig:
            continue

        old_null = int(a.get("old_null", 0) or 0)
        if old_null == 1:
            continue

        try:
            oldf = float(a.get("old", 0.0) or 0.0)
        except Exception:
            oldf = 0.0

        cap = cap_trend if bucket == "trend" else cap_range
        target = min(oldf, cap)
        ops.append({"op": "HSET", "key": key, "field": field, "value": f"{target:.2f}"})
    return ops


def build_restore_ops_cells(
    clamp_audit: list[dict[str, Any]],
    *,
    cfg_prefix: str,
    eligible_cells: list[str],
) -> list[dict[str, Any]]:
    """Builds RESTORE operations for eligible cells (restore pre-clamp values)."""
    elig = set([c.upper() for c in eligible_cells])
    ops = []
    for a in clamp_audit:
        if (a.get("op")) != "HSET":
            continue
        key = (a.get("key", ""))
        if not key.startswith(cfg_prefix):
            continue
        sym = key[len(cfg_prefix):].strip().upper()

        field = (a.get("field", ""))
        if field not in ("meta_enforce_share_trend", "meta_enforce_share_range"):
            continue
        bucket = "trend" if field.endswith("_trend") else "range"
        cell = f"{sym}|{bucket}"
        if cell.upper() not in elig:
            continue

        old_null = int(a.get("old_null", 0) or 0)
        if old_null == 1:
            ops.append({"op": "HDEL", "key": key, "field": field})
        else:
            ops.append({"op": "HSET", "key": key, "field": field, "value": ("" if a.get("old") is None else (a.get("old","")))})
    return ops


# ---------------- main ----------------

def main() -> None:
    """Main entry point: reads state, evaluates health/outcome, applies RELAX/RESTORE."""
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = redis.Redis.from_url(redis_url, decode_responses=True)

    ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)

    cfg_prefix = os.getenv("CFG_HASH_PREFIX", "config:orderflow:")
    clamp_active_key = os.getenv("META_CLAMP_ACTIVE_KEY", "meta:hardstop:clamp:active")
    clamp_stage_key = os.getenv("META_CLAMP_STAGE_KEY", "meta:hardstop:clamp:stage")  # CLAMPED|RELAXED

    remaining_cells_key = os.getenv("META_CLAMP_REMAINING_CELLS_KEY", "meta:hardstop:clamp:remaining_cells")
    cell_state_key = os.getenv("META_CLAMP_CELL_STATE_KEY", "meta:hardstop:clamp:cell_state")

    relax_streak_key = os.getenv("META_RELAX_STREAK_KEY", "meta:hardstop:relax_streak")
    remove_streak_key = os.getenv("META_REMOVE_STREAK_KEY", "meta:hardstop:remove_streak")

    pending_key = os.getenv("META_UNCLAMP_PENDING_KEY", "meta:hardstop:unclamp:pending")
    last_action_key = os.getenv("META_UNCLAMP_LAST_ACTION_MS_KEY", "meta:hardstop:unclamp:last_action_ms")

    clamp_bundle_id = retry_redis_operation(
        lambda: (r.get(clamp_active_key) or "").strip(),
        operation_name="get clamp_active_key",
    )
    if not clamp_bundle_id:
        retry_redis_operation(
            lambda: r.delete(clamp_stage_key),
            operation_name="delete clamp_stage_key",
        )
        retry_redis_operation(
            lambda: r.delete(remaining_cells_key),
            operation_name="delete remaining_cells_key",
        )
        retry_redis_operation(
            lambda: r.delete(cell_state_key),
            operation_name="delete cell_state_key",
        )
        retry_redis_operation(
            lambda: r.delete(relax_streak_key),
            operation_name="delete relax_streak_key",
        )
        retry_redis_operation(
            lambda: r.delete(remove_streak_key),
            operation_name="delete remove_streak_key",
        )
        retry_redis_operation(
            lambda: r.delete(pending_key),
            operation_name="delete pending_key",
        )
        return

    mode = _mode(r)
    allow_remove = _allow_remove(r)

    # Pending lifecycle
    pending_raw = retry_redis_operation(
        lambda: r.get(pending_key),
        operation_name="get pending_key",
    )
    if pending_raw:
        try:
            pend = json.loads(pending_raw)
        except Exception:
            pend = None
        if isinstance(pend, dict) and pend.get("bundle_id"):
            bid = str(pend["bundle_id"])
            st = retry_redis_operation(
                lambda: (r.get(f"recs:status:{bid}") or "").strip().upper(),
                operation_name=f"get recs:status:{bid}",
            )
            action = (pend.get("action","")).upper()
            cells = [str(x).upper() for x in (pend.get("cells") or []) if str(x).strip()]

            if st == "APPLIED":
                def _apply_pending():
                    pipe = r.pipeline()
                    if action == "RELAX":
                        r.set(clamp_stage_key, "RELAXED", ex=ttl)
                        for c in cells:
                            pipe.hset(cell_state_key, c, "RELAXED")
                    elif action == "RESTORE":
                        for c in cells:
                            pipe.hset(cell_state_key, c, "RESTORED")
                            pipe.srem(remaining_cells_key, c)
                    pipe.execute()

                    if r.scard(remaining_cells_key) == 0:
                        r.delete(clamp_active_key)
                        r.delete(clamp_stage_key)
                        r.delete(remaining_cells_key)
                        r.delete(cell_state_key)
                        r.delete(relax_streak_key)
                        r.delete(remove_streak_key)

                    r.delete(pending_key)
                    r.set(last_action_key, str(now_ms()), ex=ttl)

                retry_redis_operation(
                    _apply_pending,
                    operation_name="apply pending unclamp",
                )
                _notify(r, f"<b>Unclamp applied</b>\naction=<code>{action}</code>\nid=<code>{bid}</code>\ncells=<code>{cells}</code>\nmode=<code>{mode}</code>")
                return

            if st == "REJECTED":
                retry_redis_operation(
                    lambda: r.delete(pending_key),
                    operation_name="delete pending_key (rejected)",
                )
                retry_redis_operation(
                    lambda: r.set(last_action_key, str(now_ms()), ex=ttl),
                    operation_name="set last_action_key (rejected)",
                )
                _notify(r, f"<b>Unclamp rejected</b>\naction=<code>{action}</code>\nid=<code>{bid}</code>\nmode=<code>{mode}</code>")
                return

            return

    # cooldown
    cooldown_sec = int(os.getenv("META_UNCLAMP_ACTION_COOLDOWN_SEC", "1800") or 1800)
    last_action_ms = _i(
        retry_redis_operation(
            lambda: r.get(last_action_key),
            operation_name="get last_action_key",
        ),
        0,
    )
    if last_action_ms and (now_ms() - last_action_ms) < cooldown_sec * 1000:
        return

    # clamp audit
    clamp_audit = _read_audit_list(r, clamp_bundle_id)
    if not clamp_audit:
        return

    # init remaining cells
    remaining_cells = _init_remaining_cells_if_needed(
        r,
        remaining_cells_key=remaining_cells_key,
        cell_state_key=cell_state_key,
        clamp_audit=clamp_audit,
        cfg_prefix=cfg_prefix,
        ttl=ttl,
    )
    if not remaining_cells:
        return

    stage = retry_redis_operation(
        lambda: (r.get(clamp_stage_key) or "CLAMPED").strip().upper(),
        operation_name="get clamp_stage_key",
    )
    if stage not in ("CLAMPED", "RELAXED"):
        stage = "CLAMPED"
        retry_redis_operation(
            lambda: r.set(clamp_stage_key, stage, ex=ttl),
            operation_name="set clamp_stage_key",
        )

    # -------- global health (30m + 2h + 12h) --------
    metrics_stream = os.getenv("OF_GATE_METRICS_STREAM", "metrics:of_gate")
    max_scan = int(os.getenv("META_HARDSTOP_METRICS_MAX_SCAN", "200000") or 200000)

    w30 = int(os.getenv("META_UNCLAMP_SHORT_WINDOW_MIN", "30") or 30)
    w120 = int(os.getenv("META_UNCLAMP_LONG_WINDOW_MIN", "120") or 120)
    w720 = int(os.getenv("META_UNCLAMP_BASELINE_WINDOW_MIN", "720") or 720)

    rows30 = read_metrics_window(r, metrics_stream, now_ms() - w30 * 60_000, max_scan=max_scan)
    rows120 = read_metrics_window(r, metrics_stream, now_ms() - w120 * 60_000, max_scan=max_scan)
    rows720 = read_metrics_window(r, metrics_stream, now_ms() - w720 * 60_000, max_scan=max_scan)

    h30 = summarize_health(rows30)
    h120 = summarize_health(rows120)
    h720 = summarize_health(rows720)

    # strict thresholds for 30m/2h
    min_n = int(os.getenv("META_HARDSTOP_MIN_N", "200") or 200)
    lat_thr = float(os.getenv("META_HARDSTOP_LAT_P99_US", "12000") or 12000)
    exec_thr = float(os.getenv("META_HARDSTOP_EXEC_P90", "0.92") or 0.92)
    soft_thr = float(os.getenv("META_HARDSTOP_SOFT_RATE", "0.60") or 0.60)
    ok_min = float(os.getenv("META_HARDSTOP_OK_RATE_MIN", "0.10") or 0.10)

    # baseline (12h) softer
    lat_thr_b = float(os.getenv("META_BASELINE_LAT_P99_US", "15000") or 15000)
    exec_thr_b = float(os.getenv("META_BASELINE_EXEC_P90", "0.95") or 0.95)
    soft_thr_b = float(os.getenv("META_BASELINE_SOFT_RATE", "0.70") or 0.70)
    ok_min_b = float(os.getenv("META_BASELINE_OK_RATE_MIN", "0.08") or 0.08)

    bad30, r30 = is_unhealthy(h30, prefix="w30", min_n=min_n, lat_thr=lat_thr, exec_thr=exec_thr, soft_thr=soft_thr, ok_min=ok_min)
    bad120, r120 = is_unhealthy(h120, prefix="w120", min_n=min_n, lat_thr=lat_thr, exec_thr=exec_thr, soft_thr=soft_thr, ok_min=ok_min)
    bad720, r720 = is_unhealthy(h720, prefix="w720", min_n=min_n, lat_thr=lat_thr_b, exec_thr=exec_thr_b, soft_thr=soft_thr_b, ok_min=ok_min_b)

    health_ok = (not bad30) and (not bad120) and (not bad720)
    health_bad = r30 + r120 + r720

    # -------- bucket-specific health (range segment) --------
    seg_enabled = int(os.getenv("META_SEG_HEALTH_ENABLED", "1") or 1) == 1
    seg_min_n = int(os.getenv("META_SEG_MIN_N", "80") or 80)

    # we evaluate range segment on 2h window by default (more stable than 30m)
    seg120 = summarize_health_by_bucket(rows120)
    seg720 = summarize_health_by_bucket(rows720)

    # caps for RELAX / RESTORE (separate)
    seg_range_exec_p90_max_relax = float(os.getenv("META_SEG_RANGE_EXEC_P90_MAX_RELAX", "0.88") or 0.88)
    seg_range_exec_p90_max_restore = float(os.getenv("META_SEG_RANGE_EXEC_P90_MAX_RESTORE", "0.82") or 0.82)

    range_ok_relax, range_dbg_relax = range_segment_ok(seg120.get("range", {"n": 0.0}), min_n=seg_min_n, exec_p90_max=seg_range_exec_p90_max_relax)
    range_ok_restore, range_dbg_restore = range_segment_ok(seg120.get("range", {"n": 0.0}), min_n=seg_min_n, exec_p90_max=seg_range_exec_p90_max_restore)

    # if segmentation disabled, we treat as OK (opt-in hardening)
    if not seg_enabled:
        range_ok_relax, range_dbg_relax = True, "seg_disabled"
        range_ok_restore, range_dbg_restore = True, "seg_disabled"

    # -------- outcome per sym per bucket (short+long) --------
    trades_stream = os.getenv("TRADE_EVENTS_STREAM", RS.EVENTS_TRADES)
    out_max_scan = int(os.getenv("META_UNCLAMP_OUTCOME_MAX_SCAN", "400000") or 400000)

    out_short_h = float(os.getenv("META_UNCLAMP_OUTCOME_SHORT_HOURS", "2") or 2)
    out_long_h = float(os.getenv("META_UNCLAMP_OUTCOME_LONG_HOURS", "24") or 24)

    syms = sorted(list({c.split("|", 1)[0].upper() for c in remaining_cells if "|" in c}))

    st_short = read_outcome_stats_sym_bucket(r, stream=trades_stream, since_ms=now_ms() - int(out_short_h * 3600_000), symbols=syms, max_scan=out_max_scan)
    st_long  = read_outcome_stats_sym_bucket(r, stream=trades_stream, since_ms=now_ms() - int(out_long_h * 3600_000),  symbols=syms, max_scan=out_max_scan)

    # thresholds short (RELAX) per bucket
    s_min_n_tr = int(os.getenv("META_OUT_S_MIN_N_TREND", "20") or 20)
    s_mean_tr  = float(os.getenv("META_OUT_S_MEAN_MIN_TREND", "-0.03") or -0.03)
    s_tail_tr  = float(os.getenv("META_OUT_S_TAIL_MAX_TREND", "0.35") or 0.35)

    s_min_n_rg = int(os.getenv("META_OUT_S_MIN_N_RANGE", "20") or 20)
    s_mean_rg  = float(os.getenv("META_OUT_S_MEAN_MIN_RANGE", "-0.03") or -0.03)
    s_tail_rg  = float(os.getenv("META_OUT_S_TAIL_MAX_RANGE", "0.35") or 0.35)

    # thresholds long (RESTORE) per bucket
    l_min_n_tr = int(os.getenv("META_OUT_L_MIN_N_TREND", "80") or 80)
    l_mean_tr  = float(os.getenv("META_OUT_L_MEAN_MIN_TREND", "-0.02") or -0.02)
    l_tail_tr  = float(os.getenv("META_OUT_L_TAIL_MAX_TREND", "0.30") or 0.30)

    l_min_n_rg = int(os.getenv("META_OUT_L_MIN_N_RANGE", "80") or 80)
    l_mean_rg  = float(os.getenv("META_OUT_L_MEAN_MIN_RANGE", "-0.02") or -0.02)
    l_tail_rg  = float(os.getenv("META_OUT_L_TAIL_MAX_RANGE", "0.30") or 0.30)

    # eligibility by cell
    relax_cells = []
    restore_cells = []
    for sym in syms:
        for bucket in ("trend", "range"):
            cell = f"{sym}|{bucket}"
            if cell not in remaining_cells:
                continue
            ss = st_short.get(sym, {}).get(bucket, {"n": 0.0})
            ll = st_long.get(sym, {}).get(bucket, {"n": 0.0})

            if bucket == "trend":
                ok_s = outcome_ok(ss, min_n=s_min_n_tr, mean_min=s_mean_tr, tail_max=s_tail_tr)
                ok_l = outcome_ok(ll, min_n=l_min_n_tr, mean_min=l_mean_tr, tail_max=l_tail_tr)
            else:
                ok_s = outcome_ok(ss, min_n=s_min_n_rg, mean_min=s_mean_rg, tail_max=s_tail_rg)
                ok_l = outcome_ok(ll, min_n=l_min_n_rg, mean_min=l_mean_rg, tail_max=l_tail_rg)

                # triple gate for RANGE (requested): health_global + health_range_segment + outcome
                if bucket == "range":
                    ok_s = ok_s and range_ok_relax
                    ok_l = ok_l and range_ok_restore

            if ok_s:
                relax_cells.append(cell)
            if ok_l:
                restore_cells.append(cell)

    relax_cells = sorted(list(set(relax_cells)))
    restore_cells = sorted(list(set(restore_cells)))

    # streaks
    relax_prev = _i(
        retry_redis_operation(
            lambda: r.get(relax_streak_key),
            operation_name="get relax_streak_key",
        ),
        0,
    )
    rem_prev = _i(
        retry_redis_operation(
            lambda: r.get(remove_streak_key),
            operation_name="get remove_streak_key",
        ),
        0,
    )

    if health_ok and len(relax_cells) > 0:
        relax_streak = relax_prev + 1
    else:
        relax_streak = 0

    if health_ok and allow_remove and len(restore_cells) > 0:
        remove_streak = rem_prev + 1
    else:
        remove_streak = 0

    retry_redis_operation(
        lambda: r.set(relax_streak_key, str(relax_streak), ex=ttl),
        operation_name="set relax_streak_key",
    )
    retry_redis_operation(
        lambda: r.set(remove_streak_key, str(remove_streak), ex=ttl),
        operation_name="set remove_streak_key",
    )

    relax_n = int(os.getenv("META_UNCLAMP_RELAX_STREAK_N", "6") or 6)
    remove_n = int(os.getenv("META_UNCLAMP_REMOVE_STREAK_N", "18") or 18)

    # relax caps per bucket
    cap_relax_trend = float(os.getenv("META_RELAX_CAP_TREND", "0.30") or 0.30)
    cap_relax_range = float(os.getenv("META_RELAX_CAP_RANGE", "0.10") or 0.10)

    action = None
    cells_to_act: list[str] = []

    if stage == "CLAMPED" and relax_streak >= relax_n and relax_cells:
        action = "RELAX"
        cells_to_act = relax_cells
    elif stage == "RELAXED" and remove_streak >= remove_n and allow_remove and restore_cells:
        action = "RESTORE"
        cells_to_act = restore_cells
    else:
        return

    # build ops
    if action == "RELAX":
        ops = build_relax_ops_cells(
            clamp_audit,
            cfg_prefix=cfg_prefix,
            eligible_cells=cells_to_act,
            cap_trend=cap_relax_trend,
            cap_range=cap_relax_range,
        )
        who = "of_gate_hardstop_cap_unclamp_v6_relax"
        meta = {
            "kind": "meta_unclamp_v6_relax",
            "clamp_id": clamp_bundle_id,
            "cells": cells_to_act,
            "health": {"30m": h30, "2h": h120, "12h": h720, "bad": health_bad},
            "seg": {"enabled": seg_enabled, "range_2h": seg120.get("range", {}), "range_relax": range_dbg_relax, "range_restore": range_dbg_restore, "range_12h": seg720.get("range", {})},
            "caps": {"trend": cap_relax_trend, "range": cap_relax_range},
            "windows": {"out_short_h": out_short_h, "out_long_h": out_long_h},
        }
    else:
        ops = build_restore_ops_cells(
            clamp_audit,
            cfg_prefix=cfg_prefix,
            eligible_cells=cells_to_act,
        )
        who = "of_gate_hardstop_cap_unclamp_v6_restore"
        meta = {
            "kind": "meta_unclamp_v6_restore",
            "clamp_id": clamp_bundle_id,
            "cells": cells_to_act,
            "health": {"30m": h30, "2h": h120, "12h": h720, "bad": health_bad},
            "seg": {"enabled": seg_enabled, "range_2h": seg120.get("range", {}), "range_relax": range_dbg_relax, "range_restore": range_dbg_restore, "range_12h": seg720.get("range", {})},
            "windows": {"out_short_h": out_short_h, "out_long_h": out_long_h},
        }

    if not ops:
        return

    # AUTO / PROPOSE
    if mode == "AUTO":
        bid, sig = _apply_restores_direct(r, who=who, ttl_sec=ttl, restores=ops)

        pipe = r.pipeline()
        if action == "RELAX":
            r.set(clamp_stage_key, "RELAXED", ex=ttl)
            for c in cells_to_act:
                pipe.hset(cell_state_key, c, "RELAXED")
        else:
            for c in cells_to_act:
                pipe.hset(cell_state_key, c, "RESTORED")
                pipe.srem(remaining_cells_key, c)
        pipe.execute()

        if action == "RESTORE" and r.scard(remaining_cells_key) == 0:
            r.delete(clamp_active_key)
            r.delete(clamp_stage_key)
            r.delete(remaining_cells_key)
            r.delete(cell_state_key)
            r.delete(relax_streak_key)
            r.delete(remove_streak_key)

        r.set(last_action_key, str(now_ms()), ex=ttl)

        buttons = [[{"text": "↩ Rollback", "callback": f"recs:rollback:{bid}:{sig}"}]]
        _notify(
            r,
            "<b>Unclamp AUTO applied (v6 seg-health + bucket outcome)</b>\n"
            f"action=<code>{action}</code> id=<code>{bid}</code>\n"
            f"cells=<code>{cells_to_act}</code>\n"
            f"remaining=<code>{sorted(list(r.smembers(remaining_cells_key)))}</code>\n"
            f"mode=<code>{mode}</code> allow_remove=<code>{int(allow_remove)}</code>\n"
            f"relax_streak=<code>{relax_streak}</code> remove_streak=<code>{remove_streak}</code>\n"
            f"health_ok=<code>{int(health_ok)}</code> health_bad=<code>{health_bad}</code>\n"
            f"range_seg_relax=<code>{range_dbg_relax}</code>\n"
            f"range_seg_restore=<code>{range_dbg_restore}</code>\n",
            buttons=buttons,
        )
        return

    # PROPOSE
    bid, sig = _create_proposal_bundle(r, who=who, ttl_sec=ttl, ops=ops, meta=meta)
    pend = {"bundle_id": bid, "action": action, "cells": cells_to_act, "created_ms": now_ms()}
    retry_redis_operation(
        lambda: r.set(pending_key, json.dumps(pend, ensure_ascii=False, separators=(",", ":")), ex=ttl),
        operation_name="set pending_key (propose)",
    )
    retry_redis_operation(
        lambda: r.set(last_action_key, str(now_ms()), ex=ttl),
        operation_name="set last_action_key (propose)",
    )

    buttons = [[
        {"text": "✅ Approve (preview)", "callback": f"recs:preview:{bid}:{sig}"},
        {"text": "❌ Reject",           "callback": f"recs:reject:{bid}:{sig}"},
    ]]
    _notify(
        r,
        "<b>Unclamp PROPOSAL (v6 seg-health + bucket outcome)</b>\n"
        f"action=<code>{action}</code> id=<code>{bid}</code>\n"
        f"cells=<code>{cells_to_act}</code>\n"
        f"remaining=<code>{remaining_cells}</code>\n"
        f"mode=<code>{mode}</code> allow_remove=<code>{int(allow_remove)}</code>\n"
        f"relax_streak=<code>{relax_streak}</code> remove_streak=<code>{remove_streak}</code>\n"
        f"health_ok=<code>{int(health_ok)}</code> health_bad=<code>{health_bad}</code>\n"
        f"range_seg_relax=<code>{range_dbg_relax}</code>\n"
        f"range_seg_restore=<code>{range_dbg_restore}</code>\n",
        buttons=buttons,
    )


if __name__ == "__main__":
    main()

