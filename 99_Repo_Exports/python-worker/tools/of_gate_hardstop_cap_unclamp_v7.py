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

from core.redis_client import get_redis
from core.ok_fields import parse_ok_fields, get_ts_ms


# ---------------- basic utils ----------------

def now_ms() -> int:
    return get_ny_time_millis()


def pctl(xs: List[float], q: float) -> float:
    if not xs:
        return 0.0
    xs = sorted(xs)
    i = int(round((len(xs) - 1) * q))
    i = max(0, min(len(xs) - 1, i))
    return float(xs[i])


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(d)


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return int(d)


def sign(bundle_id: str, secret: str) -> str:
    d = hmac.new(secret.encode("utf-8"), bundle_id.encode("utf-8"), hashlib.sha256).hexdigest()
    return d[:8]


def _notify(r: redis.Redis, text: str, buttons: Optional[List[List[Dict[str, str]]]] = None) -> None:
    fields = {"type": "report", "text": text, "ts": str(now_ms())}
    if buttons is not None:
        fields["buttons"] = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"), fields, maxlen=200000, approximate=True)


def _mode(r: redis.Redis) -> str:
    m = (os.getenv("META_UNCLAMP_MODE", "AUTO") or "AUTO").strip().upper()
    key = os.getenv("META_UNCLAMP_MODE_KEY", "cfg:meta_unclamp:mode")
    try:
        v = (r.get(key) or "").strip().upper()
        if v in ("AUTO", "PROPOSE"):
            m = v
    except Exception:
        pass
    return m if m in ("AUTO", "PROPOSE") else "AUTO"


def _allow_restore(r: redis.Redis) -> bool:
    allow = int(os.getenv("META_UNCLAMP_ALLOW_REMOVE", "1") or 1) == 1
    key = os.getenv("META_UNCLAMP_ALLOW_REMOVE_KEY", "cfg:meta_unclamp:allow_remove")
    try:
        v = (r.get(key) or "").strip()
        if v in ("0", "1"):
            allow = (v == "1")
    except Exception:
        pass
    return allow


# ---------------- metrics:of_gate global + segment health ----------------

def read_metrics_window(r: redis.Redis, stream: str, since_ms: int, max_scan: int) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    last_id = "+"
    scanned = 0
    while scanned < max_scan:
        batch = r.xrevrange(stream, max=last_id, min="-", count=500)
        if not batch:
            break
        if len(batch) == 1 and batch[0][0] == last_id:
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
        "ok_rate": float(ok / n),
        "soft_rate": float(soft / n),
        "lat_p99_us": float(pctl(lat, 0.99)),
        "exec_p90": float(pctl(ex, 0.90)),
    }


def is_unhealthy(health: Dict[str, float], *, prefix: str,
                min_n: int, lat_thr: float, exec_thr: float, soft_thr: float, ok_min: float) -> Tuple[bool, List[str]]:
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


def _metric_bucket(m: Dict[str, Any]) -> str:
    g = str(m.get("regime_group", "") or m.get("regime", "") or m.get("scenario_v4", "") or "")
    s = g.lower()
    if "trend" in s or "bull" in s or "bear" in s:
        return "trend"
    from common.market_mode import is_range_regime; _r = is_range_regime(s)
    if _r:
        return "range"
    return "other"


def summarize_exec_p90_by_symbol_for_bucket(rows: List[Dict[str, Any]], bucket: str) -> Dict[str, Dict[str, float]]:
    """
    returns {SYM: {n, exec_p90}}
    requires metrics to include `symbol`.
    """
    acc: Dict[str, List[float]] = {}
    for r in rows:
        if _metric_bucket(r) != bucket:
            continue
        sym = str(r.get("symbol", "") or "").upper().strip()
        if not sym:
            continue
        acc.setdefault(sym, [])
        acc[sym].append(_f(r.get("exec_risk_norm", 0.0), 0.0))
    out: Dict[str, Dict[str, float]] = {}
    for sym, xs in acc.items():
        out[sym] = {"n": float(len(xs)), "exec_p90": float(pctl(xs, 0.90))}
    return out


def range_segment_ok(seg_exec: Dict[str, float], *, min_n: int, exec_p90_max: float) -> Tuple[bool, str]:
    n = float(seg_exec.get("n", 0.0))
    ex = float(seg_exec.get("exec_p90", 0.0))
    if n < float(min_n):
        return False, f"seg_low_n<{min_n} n={n}"
    if ex > exec_p90_max:
        return False, f"seg_exec_p90>{exec_p90_max} exec_p90={ex}"
    return True, f"ok n={n} exec_p90={ex}"


# ---------------- clamp audit + cell state ----------------

def _read_audit_list(r: redis.Redis, bundle_id: str) -> List[Dict[str, Any]]:
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


def _extract_symbols_from_audit(audit: List[Dict[str, Any]], cfg_prefix: str) -> List[str]:
    syms = set()
    for a in audit:
        k = str(a.get("key", ""))
        if k.startswith(cfg_prefix):
            sym = k[len(cfg_prefix):].strip().upper()
            if sym:
                syms.add(sym)
    return sorted(list(syms))


def _audit_has_field_for_sym(audit: List[Dict[str, Any]], cfg_key: str, field: str) -> bool:
    for a in audit:
        if str(a.get("op")) != "HSET":
            continue
        if str(a.get("key")) == cfg_key and str(a.get("field")) == field:
            return True
    return False


def _init_remaining_cells_if_needed(
    r: redis.Redis,
    *,
    remaining_cells_key: str,
    cell_state_key: str,
    clamp_audit: List[Dict[str, Any]],
    cfg_prefix: str,
    ttl: int,
) -> List[str]:
    if r.scard(remaining_cells_key) > 0:
        return sorted(list(r.smembers(remaining_cells_key)))

    syms = _extract_symbols_from_audit(clamp_audit, cfg_prefix)
    if not syms:
        return []

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


# ---------------- events:trades outcome per symbol per bucket ----------------

def _event_ts_ms(fields: Dict[str, Any]) -> int:
    return _i(fields.get("ts_ms", fields.get("ts", fields.get("timestamp", 0))), 0)


def _is_closed(fields: Dict[str, Any]) -> bool:
    et = str(fields.get("event_type", fields.get("type", "")) or "").upper()
    if et in ("POSITION_CLOSED", "CLOSE"):
        return True
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


def _get_bucket(fields: Dict[str, Any]) -> str:
    g = str(fields.get("regime_group", fields.get("regime", fields.get("scenario_v4", ""))) or "").lower()
    if not g:
        p = fields.get("payload")
        if isinstance(p, str) and p and p[0] == "{":
            try:
                j = json.loads(p)
                g = str(j.get("regime_group", j.get("regime", j.get("scenario_v4", ""))) or "").lower()
            except Exception:
                g = ""
    if "trend" in g or "bull" in g or "bear" in g:
        return "trend"
    if "range" in g or "chop" in g or "meanrev" in g:
        return "range"
    return "other"


def _get_r_mult(fields: Dict[str, Any]) -> Optional[float]:
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


def _stats_r(rs: List[float]) -> Dict[str, float]:
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
    symbols: List[str],
    max_scan: int,
) -> Dict[str, Dict[str, Dict[str, float]]]:
    symset = set([s.upper() for s in symbols if s])
    acc: Dict[str, Dict[str, List[float]]] = {s: {"trend": [], "range": []} for s in symset}
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
            b = _get_bucket(fields)
            if b not in ("trend", "range"):
                continue
            rm = _get_r_mult(fields)
            if rm is None:
                continue
            acc[sym][b].append(float(rm))
    out: Dict[str, Dict[str, Dict[str, float]]] = {}
    for s in symset:
        out[s] = {"trend": _stats_r(acc[s]["trend"]), "range": _stats_r(acc[s]["range"])}
    return out


def outcome_ok(stats: Dict[str, float], *, min_n: int, mean_min: float, tail_max: float) -> bool:
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
    restores: List[Dict[str, Any]],
) -> Tuple[str, str]:
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    bundle_id = secrets.token_hex(6)
    sig = sign(bundle_id, secret)
    ts = now_ms()

    pipe = r.pipeline()
    audit_out = []
    ops_out = []

    for op in restores:
        k = str(op["key"]); f = str(op["field"])
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

    bundle = {"id": bundle_id, "created_ms": ts, "ttl_sec": ttl_sec, "who": who, "ops": ops_out, "meta": {"kind": "meta_unclamp_v7_step"}}
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
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    bundle_id = secrets.token_hex(6)
    sig = sign(bundle_id, secret)
    ts = now_ms()
    bundle = {"id": bundle_id, "created_ms": ts, "ttl_sec": ttl_sec, "who": who, "ops": ops, "meta": meta}
    r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl_sec)
    r.set(f"recs:status:{bundle_id}", "PENDING", ex=ttl_sec)
    return bundle_id, sig


# ---------------- ops builders per cell ----------------

def build_relax_ops_cells(
    clamp_audit: List[Dict[str, Any]],
    *,
    cfg_prefix: str,
    eligible_cells: List[str],
    cap_trend: float,
    cap_range: float,
) -> List[Dict[str, Any]]:
    elig = set([c.upper() for c in eligible_cells])
    ops = []
    for a in clamp_audit:
        if str(a.get("op")) != "HSET":
            continue
        key = str(a.get("key", ""))
        if not key.startswith(cfg_prefix):
            continue
        sym = key[len(cfg_prefix):].strip().upper()

        field = str(a.get("field", ""))
        if field not in ("meta_enforce_share_trend", "meta_enforce_share_range"):
            continue
        bucket = "trend" if field.endswith("_trend") else "range"
        cell = f"{sym}|{bucket}"
        if cell not in elig:
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
    clamp_audit: List[Dict[str, Any]],
    *,
    cfg_prefix: str,
    eligible_cells: List[str],
) -> List[Dict[str, Any]]:
    elig = set([c.upper() for c in eligible_cells])
    ops = []
    for a in clamp_audit:
        if str(a.get("op")) != "HSET":
            continue
        key = str(a.get("key", ""))
        if not key.startswith(cfg_prefix):
            continue
        sym = key[len(cfg_prefix):].strip().upper()

        field = str(a.get("field", ""))
        if field not in ("meta_enforce_share_trend", "meta_enforce_share_range"):
            continue
        bucket = "trend" if field.endswith("_trend") else "range"
        cell = f"{sym}|{bucket}"
        if cell not in elig:
            continue

        old_null = int(a.get("old_null", 0) or 0)
        if old_null == 1:
            ops.append({"op": "HDEL", "key": key, "field": field})
        else:
            ops.append({"op": "HSET", "key": key, "field": field, "value": ("" if a.get("old") is None else str(a.get("old","")))})
    return ops


# ---------------- selection + quotas ----------------

def _cell_sym_bucket(cell: str) -> Tuple[str, str]:
    if "|" not in cell:
        return "", ""
    s, b = cell.split("|", 1)
    return s.strip().upper(), b.strip().lower()


# ---------------- budget limiter (v8) ----------------

def _cell_to_cfg_field(cell: str) -> tuple[str, str, str]:
    """
    Returns (sym, field, bucket) where field is meta_enforce_share_trend/range.
    """
    if "|" not in cell:
        return "", "", ""
    sym, bucket = cell.split("|", 1)
    sym = sym.strip().upper()
    bucket = bucket.strip().lower()
    if bucket == "trend":
        return sym, "meta_enforce_share_trend", "trend"
    if bucket == "range":
        return sym, "meta_enforce_share_range", "range"
    return sym, "", bucket


def _build_preclamp_map_from_audit(clamp_audit: list[dict], cfg_prefix: str) -> dict:
    """
    Map:
      cell -> {key, field, old_null, old_value(float or None)}
    Uses clamp audit (pre-clamp 'old').
    """
    m = {}
    for a in clamp_audit:
        if str(a.get("op")) != "HSET":
            continue
        key = str(a.get("key", ""))
        if not key.startswith(cfg_prefix):
            continue
        field = str(a.get("field", ""))
        if field not in ("meta_enforce_share_trend", "meta_enforce_share_range"):
            continue
        sym = key[len(cfg_prefix):].strip().upper()
        bucket = "trend" if field.endswith("_trend") else "range"
        cell = f"{sym}|{bucket}"
        old_null = int(a.get("old_null", 0) or 0)
        old_val = None
        if old_null == 0:
            try:
                old_val = float(a.get("old", 0.0) or 0.0)
            except Exception:
                old_val = 0.0
        m[cell] = {"key": key, "field": field, "bucket": bucket, "old_null": old_null, "old_value": old_val}
    return m


def _target_value_for_action(spec: dict, action: str, cap_trend: float, cap_range: float) -> tuple[bool, float]:
    """
    Returns (has_target, target_value).
    - RELAX: target = min(preclamp_old, cap_bucket), only if old_null==0
    - RESTORE: target = preclamp_old, only if old_null==0 (if old_null==1 => HDEL, treated as no increase)
    """
    old_null = int(spec.get("old_null", 0) or 0)
    if old_null == 1:
        return False, 0.0
    old_val = float(spec.get("old_value", 0.0) or 0.0)
    b = str(spec.get("bucket", ""))
    if action == "RELAX":
        cap = cap_trend if b == "trend" else cap_range
        return True, min(old_val, float(cap))
    # RESTORE
    return True, old_val


def _estimate_increase(r, spec: dict, action: str, cap_trend: float, cap_range: float) -> float:
    """
    increase = max(0, target - current)
    For HDEL (old_null==1) => increase=0
    """
    has_target, tgt = _target_value_for_action(spec, action, cap_trend, cap_range)
    if not has_target:
        return 0.0
    cur_raw = r.hget(spec["key"], spec["field"])
    try:
        cur = float(cur_raw) if cur_raw is not None else 0.0
    except Exception:
        cur = 0.0
    return max(0.0, float(tgt) - float(cur))


def apply_budget_limit(
    r,
    *,
    action: str,
    cells_ranked: list[str],
    preclamp_map: dict,
    cap_trend: float,
    cap_range: float,
    bud_trend: float,
    bud_range: float,
    bud_total: float,
) -> tuple[list[str], dict]:
    """
    Takes ranked cells and keeps adding while budgets allow.
    Returns (picked_cells, debug_usage).
    """
    used_t = 0.0
    used_r = 0.0
    used_all = 0.0
    picked = []

    for cell in cells_ranked:
        spec = preclamp_map.get(cell)
        if not spec:
            continue

        inc = _estimate_increase(r, spec, action, cap_trend, cap_range)
        b = spec.get("bucket", "")

        next_t = used_t + (inc if b == "trend" else 0.0)
        next_r = used_r + (inc if b == "range" else 0.0)
        next_all = used_all + inc

        if next_t > bud_trend + 1e-12:
            continue
        if next_r > bud_range + 1e-12:
            continue
        if next_all > bud_total + 1e-12:
            continue

        used_t = next_t
        used_r = next_r
        used_all = next_all
        picked.append(cell)

    dbg = {"used_trend": used_t, "used_range": used_r, "used_total": used_all,
           "bud_trend": bud_trend, "bud_range": bud_range, "bud_total": bud_total}
    return picked, dbg


def apply_quotas_and_rank(
    *,
    action: str,
    cells: List[str],
    st_long: Dict[str, Dict[str, Dict[str, float]]],
    seg_range_sym: Dict[str, Dict[str, float]],
    max_range: int,
    max_trend: int,
) -> List[str]:
    """
    Rank:
      range: (seg_exec_p90 asc, long_meanR desc)
      trend: (long_meanR desc)
    Then take up to quota per bucket.
    """
    range_cells = []
    trend_cells = []
    for c in cells:
        _, b = _cell_sym_bucket(c)
        if b == "range":
            range_cells.append(c)
        elif b == "trend":
            trend_cells.append(c)

    def range_key(c: str):
        s, _ = _cell_sym_bucket(c)
        seg = seg_range_sym.get(s, {"exec_p90": 9e9})
        ex = float(seg.get("exec_p90", 9e9))
        mean = float(st_long.get(s, {}).get("range", {}).get("meanR", -9e9))
        # sort asc ex, desc mean
        return (ex, -mean)

    def trend_key(c: str):
        s, _ = _cell_sym_bucket(c)
        mean = float(st_long.get(s, {}).get("trend", {}).get("meanR", -9e9))
        # sort desc mean
        return (-mean,)

    range_cells = sorted(range_cells, key=range_key)
    trend_cells = sorted(trend_cells, key=trend_key)

    picked = []
    picked.extend(trend_cells[:max_trend])
    picked.extend(range_cells[:max_range])
    return picked


# ---------------- main ----------------

def main() -> None:
    try:
        r = get_redis(retry_attempts=10, retry_delay=2)
    except Exception as e:
        print(f"ERROR: Failed to connect to Redis: {e}")
        raise

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

    clamp_bundle_id = (r.get(clamp_active_key) or "").strip()
    if not clamp_bundle_id:
        return

    mode = _mode(r)
    allow_restore = _allow_restore(r)

    # Pending lifecycle (PROPOSE)
    pending_raw = r.get(pending_key)
    if pending_raw:
        try:
            pend = json.loads(pending_raw)
        except Exception:
            pend = None
        if isinstance(pend, dict) and pend.get("bundle_id"):
            bid = str(pend["bundle_id"])
            st = (r.get(f"recs:status:{bid}") or "").strip().upper()
            action = str(pend.get("action","")).upper()
            cells = [str(x).upper() for x in (pend.get("cells") or []) if str(x).strip()]

            if st == "APPLIED":
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
                _notify(r, f"<b>Unclamp applied</b>\naction=<code>{action}</code>\nid=<code>{bid}</code>\ncells=<code>{cells}</code>\nmode=<code>{mode}</code>")
                return

            if st == "REJECTED":
                r.delete(pending_key)
                r.set(last_action_key, str(now_ms()), ex=ttl)
                _notify(r, f"<b>Unclamp rejected</b>\naction=<code>{action}</code>\nid=<code>{bid}</code>\nmode=<code>{mode}</code>")
                return

            return

    # cooldown
    cooldown_sec = int(os.getenv("META_UNCLAMP_ACTION_COOLDOWN_SEC", "1800") or 1800)
    last_action_ms = _i(r.get(last_action_key), 0)
    if last_action_ms and (now_ms() - last_action_ms) < cooldown_sec * 1000:
        return

    clamp_audit = _read_audit_list(r, clamp_bundle_id)
    if not clamp_audit:
        return

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

    stage = (r.get(clamp_stage_key) or "CLAMPED").strip().upper()
    if stage not in ("CLAMPED", "RELAXED"):
        stage = "CLAMPED"
        r.set(clamp_stage_key, stage, ex=ttl)

    # -------- global health --------
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

    min_n = int(os.getenv("META_HARDSTOP_MIN_N", "200") or 200)
    lat_thr = float(os.getenv("META_HARDSTOP_LAT_P99_US", "12000") or 12000)
    exec_thr = float(os.getenv("META_HARDSTOP_EXEC_P90", "0.92") or 0.92)
    soft_thr = float(os.getenv("META_HARDSTOP_SOFT_RATE", "0.60") or 0.60)
    ok_min = float(os.getenv("META_HARDSTOP_OK_RATE_MIN", "0.10") or 0.10)

    lat_thr_b = float(os.getenv("META_BASELINE_LAT_P99_US", "15000") or 15000)
    exec_thr_b = float(os.getenv("META_BASELINE_EXEC_P90", "0.95") or 0.95)
    soft_thr_b = float(os.getenv("META_BASELINE_SOFT_RATE", "0.70") or 0.70)
    ok_min_b = float(os.getenv("META_BASELINE_OK_RATE_MIN", "0.08") or 0.08)

    bad30, r30 = is_unhealthy(h30, prefix="w30", min_n=min_n, lat_thr=lat_thr, exec_thr=exec_thr, soft_thr=soft_thr, ok_min=ok_min)
    bad120, r120 = is_unhealthy(h120, prefix="w120", min_n=min_n, lat_thr=lat_thr, exec_thr=exec_thr, soft_thr=soft_thr, ok_min=ok_min)
    bad720, r720 = is_unhealthy(h720, prefix="w720", min_n=min_n, lat_thr=lat_thr_b, exec_thr=exec_thr_b, soft_thr=soft_thr_b, ok_min=ok_min_b)

    health_ok = (not bad30) and (not bad120) and (not bad720)
    health_bad = r30 + r120 + r720

    # -------- range segment health: global + per-symbol (2h) --------
    seg_enabled = int(os.getenv("META_SEG_HEALTH_ENABLED", "1") or 1) == 1
    seg_sym_enabled = int(os.getenv("META_SEG_SYM_ENABLED", "1") or 1) == 1

    seg_min_n = int(os.getenv("META_SEG_MIN_N", "80") or 80)
    seg_sym_min_n = int(os.getenv("META_SEG_SYM_MIN_N", "40") or 40)

    seg_range_exec_p90_max_relax = float(os.getenv("META_SEG_RANGE_EXEC_P90_MAX_RELAX", "0.88") or 0.88)
    seg_range_exec_p90_max_restore = float(os.getenv("META_SEG_RANGE_EXEC_P90_MAX_RESTORE", "0.82") or 0.82)

    # global range segment (aggregate)
    # reuse summarize_exec_p90_by_symbol... by aggregating with special key "__ALL__"
    # simplest: compute aggregate exec list directly
    range_execs = []
    for rr in rows120:
        if _metric_bucket(rr) == "range":
            range_execs.append(_f(rr.get("exec_risk_norm", 0.0), 0.0))
    seg_range_global = {"n": float(len(range_execs)), "exec_p90": float(pctl(range_execs, 0.90))} if range_execs else {"n": 0.0}
    range_global_ok_relax, range_global_dbg_relax = range_segment_ok(seg_range_global, min_n=seg_min_n, exec_p90_max=seg_range_exec_p90_max_relax)
    range_global_ok_restore, range_global_dbg_restore = range_segment_ok(seg_range_global, min_n=seg_min_n, exec_p90_max=seg_range_exec_p90_max_restore)

    # per-symbol range segment
    seg_range_sym = summarize_exec_p90_by_symbol_for_bucket(rows120, "range")  # {SYM:{n,exec_p90}}
    # if disabled, pass-through
    if not seg_enabled:
        range_global_ok_relax, range_global_dbg_relax = True, "seg_disabled"
        range_global_ok_restore, range_global_dbg_restore = True, "seg_disabled"
    if not seg_sym_enabled:
        seg_range_sym = {}
    # ---------------- outcome short+long per sym bucket ----------------
    trades_stream = os.getenv("TRADE_EVENTS_STREAM", "events:trades")
    out_max_scan = int(os.getenv("META_UNCLAMP_OUTCOME_MAX_SCAN", "400000") or 400000)

    out_short_h = float(os.getenv("META_UNCLAMP_OUTCOME_SHORT_HOURS", "2") or 2)
    out_long_h = float(os.getenv("META_UNCLAMP_OUTCOME_LONG_HOURS", "24") or 24)

    syms = sorted(list({c.split("|", 1)[0].upper() for c in remaining_cells if "|" in c}))

    st_short = read_outcome_stats_sym_bucket(r, stream=trades_stream, since_ms=now_ms() - int(out_short_h * 3600_000), symbols=syms, max_scan=out_max_scan)
    st_long  = read_outcome_stats_sym_bucket(r, stream=trades_stream, since_ms=now_ms() - int(out_long_h * 3600_000),  symbols=syms, max_scan=out_max_scan)

    # thresholds short (RELAX)
    s_min_n_tr = int(os.getenv("META_OUT_S_MIN_N_TREND", "20") or 20)
    s_mean_tr  = float(os.getenv("META_OUT_S_MEAN_MIN_TREND", "-0.03") or -0.03)
    s_tail_tr  = float(os.getenv("META_OUT_S_TAIL_MAX_TREND", "0.35") or 0.35)

    s_min_n_rg = int(os.getenv("META_OUT_S_MIN_N_RANGE", "20") or 20)
    s_mean_rg  = float(os.getenv("META_OUT_S_MEAN_MIN_RANGE", "-0.03") or -0.03)
    s_tail_rg  = float(os.getenv("META_OUT_S_TAIL_MAX_RANGE", "0.35") or 0.35)

    # thresholds long (RESTORE)
    l_min_n_tr = int(os.getenv("META_OUT_L_MIN_N_TREND", "80") or 80)
    l_mean_tr  = float(os.getenv("META_OUT_L_MEAN_MIN_TREND", "-0.02") or -0.02)
    l_tail_tr  = float(os.getenv("META_OUT_L_TAIL_MAX_TREND", "0.30") or 0.30)

    l_min_n_rg = int(os.getenv("META_OUT_L_MIN_N_RANGE", "80") or 80)
    l_mean_rg  = float(os.getenv("META_OUT_L_MEAN_MIN_RANGE", "-0.02") or -0.02)
    l_tail_rg  = float(os.getenv("META_OUT_L_TAIL_MAX_RANGE", "0.30") or 0.30)

    # quotas
    max_range_relax = int(os.getenv("META_MAX_RANGE_RELAX_PER_CYCLE", "4") or 4)
    max_range_restore = int(os.getenv("META_MAX_RANGE_RESTORE_PER_CYCLE", "2") or 2)
    max_trend_relax = int(os.getenv("META_MAX_TREND_RELAX_PER_CYCLE", "999") or 999)
    max_trend_restore = int(os.getenv("META_MAX_TREND_RESTORE_PER_CYCLE", "999") or 999)

    # eligibility by cell
    relax_cells_all = []
    restore_cells_all = []

    for sym in syms:
        # per-symbol seg health for range
        seg_sym = seg_range_sym.get(sym, {"n": 0.0, "exec_p90": 0.0})
        sym_range_ok_relax, _ = range_segment_ok(seg_sym, min_n=seg_sym_min_n, exec_p90_max=seg_range_exec_p90_max_relax)
        sym_range_ok_restore, _ = range_segment_ok(seg_sym, min_n=seg_sym_min_n, exec_p90_max=seg_range_exec_p90_max_restore)

        # if per-symbol seg disabled -> allow
        if not seg_sym_enabled:
            sym_range_ok_relax = True
            sym_range_ok_restore = True

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

                # triple gate for range:
                ok_s = ok_s and range_global_ok_relax and sym_range_ok_relax
                ok_l = ok_l and range_global_ok_restore and sym_range_ok_restore

            if ok_s:
                relax_cells_all.append(cell)
            if ok_l:
                restore_cells_all.append(cell)

    relax_cells_all = sorted(list(set(relax_cells_all)))
    restore_cells_all = sorted(list(set(restore_cells_all)))

    # streaks (only if global health ok and there is at least something to do)
    relax_prev = _i(r.get(relax_streak_key), 0)
    rem_prev = _i(r.get(remove_streak_key), 0)

    relax_streak = (relax_prev + 1) if (health_ok and relax_cells_all) else 0
    remove_streak = (rem_prev + 1) if (health_ok and allow_restore and restore_cells_all) else 0

    r.set(relax_streak_key, str(relax_streak), ex=ttl)
    r.set(remove_streak_key, str(remove_streak), ex=ttl)

    relax_n = int(os.getenv("META_UNCLAMP_RELAX_STREAK_N", "6") or 6)
    restore_n = int(os.getenv("META_UNCLAMP_REMOVE_STREAK_N", "18") or 18)

    # relax caps per bucket
    cap_relax_trend = float(os.getenv("META_RELAX_CAP_TREND", "0.30") or 0.30)
    cap_relax_range = float(os.getenv("META_RELAX_CAP_RANGE", "0.10") or 0.10)

    action = None
    cells_to_act: List[str] = []

    if stage == "CLAMPED" and relax_streak >= relax_n and relax_cells_all:
        action = "RELAX"
        # apply quotas + rank using long stats for tie-breaker
        cells_to_act = apply_quotas_and_rank(
            action=action,
            cells=relax_cells_all,
            st_long=st_long,
            seg_range_sym=seg_range_sym,
            max_range=max_range_relax,
            max_trend=max_trend_relax,
        )
    elif stage == "RELAXED" and remove_streak >= restore_n and allow_restore and restore_cells_all:
        action = "RESTORE"
        cells_to_act = apply_quotas_and_rank(
            action=action,
            cells=restore_cells_all,
            st_long=st_long,
            seg_range_sym=seg_range_sym,
            max_range=max_range_restore,
            max_trend=max_trend_restore,
        )
    else:
        return

    if not cells_to_act:
        return

    # Budget limiter (v8): limit total increase per cycle
    preclamp_map = _build_preclamp_map_from_audit(clamp_audit, cfg_prefix)

    if action == "RELAX":
        bud_trend = float(os.getenv("META_BUDGET_INC_TREND_RELAX", "0.20") or 0.20)
        bud_range = float(os.getenv("META_BUDGET_INC_RANGE_RELAX", "0.06") or 0.06)
        bud_total = float(os.getenv("META_BUDGET_INC_TOTAL_RELAX", "0.22") or 0.22)
    else:
        bud_trend = float(os.getenv("META_BUDGET_INC_TREND_RESTORE", "0.12") or 0.12)
        bud_range = float(os.getenv("META_BUDGET_INC_RANGE_RESTORE", "0.04") or 0.04)
        bud_total = float(os.getenv("META_BUDGET_INC_TOTAL_RESTORE", "0.14") or 0.14)

    cells_to_act, bud_dbg = apply_budget_limit(
        r,
        action=action,
        cells_ranked=cells_to_act,
        preclamp_map=preclamp_map,
        cap_trend=cap_relax_trend,
        cap_range=cap_relax_range,
        bud_trend=bud_trend,
        bud_range=bud_range,
        bud_total=bud_total,
    )

    if not cells_to_act:
        return

    if action == "RELAX":
        ops = build_relax_ops_cells(
            clamp_audit,
            cfg_prefix=cfg_prefix,
            eligible_cells=cells_to_act,
            cap_trend=cap_relax_trend,
            cap_range=cap_relax_range,
        )
        who = "of_gate_hardstop_cap_unclamp_v7_relax"
        meta = {
            "kind": "meta_unclamp_v7_relax",
            "clamp_id": clamp_bundle_id,
            "cells": cells_to_act,
            "health": {"30m": h30, "2h": h120, "12h": h720, "bad": health_bad},
            "range_seg_global": {"relax": range_global_dbg_relax, "restore": range_global_dbg_restore, "2h": seg_range_global},
            "range_seg_sym": seg_range_sym,
            "quotas": {"range": max_range_relax, "trend": max_trend_relax},
            "budget": bud_dbg,
        }
    else:
        ops = build_restore_ops_cells(
            clamp_audit,
            cfg_prefix=cfg_prefix,
            eligible_cells=cells_to_act,
        )
        who = "of_gate_hardstop_cap_unclamp_v7_restore"
        meta = {
            "kind": "meta_unclamp_v7_restore",
            "clamp_id": clamp_bundle_id,
            "cells": cells_to_act,
            "health": {"30m": h30, "2h": h120, "12h": h720, "bad": health_bad},
            "range_seg_global": {"relax": range_global_dbg_relax, "restore": range_global_dbg_restore, "2h": seg_range_global},
            "range_seg_sym": seg_range_sym,
            "quotas": {"range": max_range_restore, "trend": max_trend_restore},
            "budget": bud_dbg,
        }

    if not ops:
        return

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
            "<b>Unclamp AUTO applied (v7 sym-seg + quotas + budget)</b>\n"
            f"action=<code>{action}</code> id=<code>{bid}</code>\n"
            f"cells=<code>{cells_to_act}</code>\n"
            f"remaining=<code>{sorted(list(r.smembers(remaining_cells_key)))}</code>\n"
            f"mode=<code>{mode}</code> allow_restore=<code>{int(allow_restore)}</code>\n"
            f"health_ok=<code>{int(health_ok)}</code> health_bad=<code>{health_bad}</code>\n"
            f"range_global_relax=<code>{range_global_dbg_relax}</code>\n"
            f"range_global_restore=<code>{range_global_dbg_restore}</code>\n"
            f"budget: trend=<code>{bud_dbg['used_trend']:.3f}/{bud_dbg['bud_trend']:.3f}</code> "
            f"range=<code>{bud_dbg['used_range']:.3f}/{bud_dbg['bud_range']:.3f}</code> "
            f"total=<code>{bud_dbg['used_total']:.3f}/{bud_dbg['bud_total']:.3f}</code>\n",
            buttons=buttons,
        )
        return

    # PROPOSE
    bid, sig = _create_proposal_bundle(r, who=who, ttl_sec=ttl, ops=ops, meta=meta)
    pend = {"bundle_id": bid, "action": action, "cells": cells_to_act, "created_ms": now_ms()}
    r.set(pending_key, json.dumps(pend, ensure_ascii=False, separators=(",", ":")), ex=ttl)
    r.set(last_action_key, str(now_ms()), ex=ttl)

    buttons = [[
        {"text": "👀 Preview diff", "callback": f"recs:preview2:{bid}:{sig}"},
        {"text": "✅ Confirm apply", "callback": f"recs:confirm:{bid}:{sig}"},
        {"text": "❌ Reject",        "callback": f"recs:reject:{bid}:{sig}"},
    ]]
    _notify(
        r,
        "<b>Unclamp PROPOSAL (v7 sym-seg + quotas + budget)</b>\n"
        f"action=<code>{action}</code> id=<code>{bid}</code>\n"
        f"cells=<code>{cells_to_act}</code>\n"
        f"mode=<code>{mode}</code> allow_restore=<code>{int(allow_restore)}</code>\n"
        f"health_ok=<code>{int(health_ok)}</code> health_bad=<code>{health_bad}</code>\n"
        f"range_global_relax=<code>{range_global_dbg_relax}</code>\n"
        f"range_global_restore=<code>{range_global_dbg_restore}</code>\n"
        f"budget: trend=<code>{bud_dbg['used_trend']:.3f}/{bud_dbg['bud_trend']:.3f}</code> "
        f"range=<code>{bud_dbg['used_range']:.3f}/{bud_dbg['bud_range']:.3f}</code> "
        f"total=<code>{bud_dbg['used_total']:.3f}/{bud_dbg['bud_total']:.3f}</code>\n",
        buttons=buttons,
    )


if __name__ == "__main__":
    main()

