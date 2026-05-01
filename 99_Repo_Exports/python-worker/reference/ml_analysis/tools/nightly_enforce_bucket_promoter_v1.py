from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import os
import json
import time
import logging
import asyncio
import re  # P89: for _safe_ident SQL identifier validation
from dataclasses import dataclass
from typing import Any, Dict, List, Tuple, Optional


try:
    import redis.asyncio as aioredis  # type: ignore
except Exception:  # pragma: no cover
    aioredis = None


logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
)
logger = logging.getLogger("nightly_enforce_bucket_promoter")

try:
    from orderflow_services.auto_apply_guard import assert_auto_apply_not_blocked  # type: ignore
except Exception:  # pragma: no cover
    assert_auto_apply_not_blocked = None

try:
    from orderflow_services.redis_lock_v1 import acquire_lock as _acquire_lock, release_lock as _release_lock  # type: ignore
except Exception:  # pragma: no cover
    _acquire_lock = None
    _release_lock = None


# ------------------------ small utils ------------------------

def _env_int(name: str, default: str) -> int:
    try:
        return int(str(os.getenv(name, default)).strip())
    except Exception:
        return int(default)


def _env_float(name: str, default: str) -> float:
    try:
        return float(str(os.getenv(name, default)).strip())
    except Exception:
        return float(default)


def _safe_ident(name: str, default: str) -> str:
    """Validate SQL identifier (schema-qualified allowed: letters/digits/_/.) to prevent injection."""
    s = str(name or "").strip()
    if not s:
        return default
    # allow schema-qualified identifiers (letters/digits/_/.)
    if re.fullmatch(r"[A-Za-z_][A-Za-z0-9_\.]*", s):
        return s
    return default


def _now_ms() -> int:
    return get_ny_time_millis()


def _notify_stream_name() -> str:
    return (
        os.getenv("ENFORCE_BUCKET_NOTIFY_STREAM")
        or os.getenv("NOTIFY_TELEGRAM_STREAM")
        or os.getenv("CRYPTO_NOTIFY_STREAM")
        or "notify:telegram"
    )


def _notify_enabled() -> bool:
    return str(os.getenv("ENFORCE_BUCKET_NOTIFY", "1") or "1").strip().lower() in ("1", "true", "yes", "on")


async def _notify_once(r: Any, text: str, *, cooldown_key: str, cooldown_sec: int) -> None:
    if not _notify_enabled():
        return
    try:
        now = _now_ms()
        last = await r.get(cooldown_key)
        if last:
            try:
                if (now - _i(last, 0)) < (int(cooldown_sec) * 1000):
                    return
            except Exception:
                pass
        fields = {"type": "report", "text": str(text)[:3500], "ts": str(now)}
        await r.xadd(_notify_stream_name(), fields, maxlen=200000, approximate=True)
        # store last send time with TTL
        try:
            await r.set(cooldown_key, str(now), ex=int(cooldown_sec))
        except Exception:
            pass
    except Exception:
        return


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return int(d)


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return float(d)


def _norm_bucket(b: Any) -> str:
    s = str(b or "").strip().upper()
    return s or "NORMAL"


def _parse_allowlist(raw: str) -> List[str]:
    raw = str(raw or "").strip()
    if not raw:
        return []
    parts: List[str] = []
    for p in raw.replace(";", ",").split(","):
        x = p.strip().upper()
        if x and x not in parts:
            parts.append(x)
    return parts


def _allowlist_to_str(xs: List[str]) -> str:
    # stable order, comma-separated
    return ",".join(xs)


def _write_json_atomic(path: str, d: Dict[str, Any]) -> None:
    """Write JSON atomically (tmp + rename) so exporter always reads a complete file."""
    os.makedirs(os.path.dirname(os.path.abspath(path)) or ".", exist_ok=True)
    tmp = path + ".tmp"
    with open(tmp, "w", encoding="utf-8") as f:
        json.dump(d, f, ensure_ascii=False, indent=2, sort_keys=True)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, path)


def _ensure_auto_apply_reason(reason: str) -> None:
    """Ensure AUTO_APPLY_BLOCK_REASONS contains `reason` (process-local)."""
    cur = str(os.getenv("AUTO_APPLY_BLOCK_REASONS", "tick_gate") or "tick_gate").strip()
    parts = [p.strip() for p in cur.split(",") if p.strip()]
    if reason not in parts:
        parts.append(reason)
    os.environ["AUTO_APPLY_BLOCK_REASONS"] = ",".join(parts)


async def _xadd_event(r: Any, *, stream: str, fields: Dict[str, Any], maxlen: int = 10000) -> None:
    """Best-effort event log (audit) to Redis stream."""
    try:
        payload = {str(k): ("" if v is None else str(v)) for k, v in (fields or {}).items()}
        await r.xadd(stream, payload, maxlen=maxlen, approximate=True)
    except Exception:
        return


# ------------------------ promotion logic (pure) ------------------------

@dataclass(frozen=True)
class BucketHealth:
    bucket: str
    db_n: int
    resid_p95: float
    resid_p99: float
    edge_neg_share: float  # P89: share of trades where edge_minus_expected_bps < 0
    eligible_n: int
    ok_soft_rate: float


@dataclass(frozen=True)
class PromotionDecision:
    ok: bool
    new_allowlist: str
    added_bucket: str
    reasons: List[str]


def decide_next_allowlist(
    *,
    current_allow: str,
    health_by_bucket: Dict[str, BucketHealth],
    promote_order: List[str],
    default_bucket: str,
    min_db_n: int,
    max_p95: float,
    max_p99: float,
    max_edge_neg_share: float,  # P89: guardrail — block promote if edge_neg_share too high
    min_eligible_n: int,
    min_ok_soft_rate: float,
) -> PromotionDecision:
    cur = _parse_allowlist(current_allow)
    if not cur:
        cur = [_norm_bucket(default_bucket)]

    reasons: List[str] = []

    def bucket_ok(b: str) -> Tuple[bool, str]:
        h = health_by_bucket.get(_norm_bucket(b))
        if not h:
            return False, "no_health"
        if h.db_n < min_db_n:
            return False, f"low_db_n:{h.db_n}"
        if h.resid_p95 > max_p95:
            return False, f"p95_high:{h.resid_p95:.2f}"
        if h.resid_p99 > max_p99:
            return False, f"p99_high:{h.resid_p99:.2f}"
        # P89: block promotion if edge negative share exceeds threshold
        if h.edge_neg_share > max_edge_neg_share:
            return False, f"edge_neg_high:{h.edge_neg_share:.3f}"
        if h.eligible_n < min_eligible_n:
            return False, f"low_gate_n:{h.eligible_n}"
        if h.ok_soft_rate < min_ok_soft_rate:
            return False, f"ok_soft_low:{h.ok_soft_rate:.3f}"
        return True, "ok"

    # Add at most one bucket per run (risk controlled)
    for cand in promote_order:
        c = _norm_bucket(cand)
        if c in cur:
            continue
        ok, why = bucket_ok(c)
        if ok:
            new = cur + [c]
            return PromotionDecision(ok=True, new_allowlist=_allowlist_to_str(new), added_bucket=c, reasons=["promoted:" + c])
        reasons.append(f"skip:{c}:{why}")

    return PromotionDecision(ok=False, new_allowlist=_allowlist_to_str(cur), added_bucket="", reasons=reasons[:12])


# ------------------------ DB + Redis aggregation ------------------------

async def _fetch_db_health(
    conn: Any,
    *,
    lookback_h: int,
    mv: str,
    view: str,
) -> Dict[str, Dict[str, BucketHealth]]:
    """Returns sym -> bucket -> health (DB-side residuals and edge-neg share).

    Strategy:
      1) Try materialized view (fast): mv_exec_slippage_eval_1h_stats
         Aggregates pre-computed hourly buckets — much cheaper than scanning raw rows.
      2) Fallback to raw view (slow): v_exec_slippage_eval
         Uses percentile_cont + avg which requires scanning all rows in window.
    """
    # Sanitize identifiers to prevent SQL injection via ENV override
    mv = _safe_ident(mv, "mv_exec_slippage_eval_1h_stats")
    view = _safe_ident(view, "v_exec_slippage_eval")

    out: Dict[str, Dict[str, BucketHealth]] = {}

    # Fast path: MV aggregated per hour (P89)
    try:
        q = f"""
        SELECT
          sym,
          exec_regime_bucket,
          sum(n)::bigint as n,
          max(resid_p95_bps) as p95_resid,
          max(resid_p99_bps) as p99_resid,
          max(edge_neg_share) as edge_neg_share
        FROM {mv}
        WHERE t >= now() - interval '{int(lookback_h)} hours'
        GROUP BY sym, exec_regime_bucket
        """

        rows = await conn.fetch(q)
        for r in rows:
            sym = str(r.get("sym") or "").upper()
            if not sym:
                continue
            b = _norm_bucket(r.get("exec_regime_bucket") or "NORMAL")
            out.setdefault(sym, {})[b] = BucketHealth(
                bucket=b,
                db_n=_i(r.get("n"), 0),
                resid_p95=_f(r.get("p95_resid"), 0.0),
                resid_p99=_f(r.get("p99_resid"), 0.0),
                edge_neg_share=_f(r.get("edge_neg_share"), 0.0),
                eligible_n=0,
                ok_soft_rate=0.0,
            )
        return out
    except Exception as e:
        logger.warning("MV query failed (%s), falling back to raw view=%s", type(e).__name__, view)

    # Slow path: compute percentiles on raw view (fallback when MV unavailable)
    q2 = f"""
    SELECT
      sym,
      exec_regime_bucket,
      count(*) as n,
      percentile_cont(0.95) within group (order by slippage_residual_bps) as p95_resid,
      percentile_cont(0.99) within group (order by slippage_residual_bps) as p99_resid,
      avg(case when edge_minus_expected_bps < 0 then 1 else 0 end) as edge_neg_share
    FROM {view}
    WHERE ts >= now() - interval '{int(lookback_h)} hours'
    GROUP BY sym, exec_regime_bucket
    """

    rows2 = await conn.fetch(q2)
    for r in rows2:
        sym = str(r.get("sym") or "").upper()
        if not sym:
            continue
        b = _norm_bucket(r.get("exec_regime_bucket") or "NORMAL")
        out.setdefault(sym, {})[b] = BucketHealth(
            bucket=b,
            db_n=_i(r.get("n"), 0),
            resid_p95=_f(r.get("p95_resid"), 0.0),
            resid_p99=_f(r.get("p99_resid"), 0.0),
            edge_neg_share=_f(r.get("edge_neg_share"), 0.0),
            eligible_n=0,
            ok_soft_rate=0.0,
        )
    return out


async def _scan_gate_metrics(
    r: Any,
    *,
    stream: str,
    start_ms: int,
    max_scan: int,
) -> List[Dict[str, Any]]:
    rows: List[Dict[str, Any]] = []
    last_id = "+"
    scanned = 0

    # xrevrange: newest -> oldest
    while scanned < max_scan:
        try:
            batch = await r.xrevrange(stream, max=last_id, min="-", count=2000)
        except Exception:
            batch = []
        if not batch:
            break
        for msg_id, fields in batch:
            scanned += 1
            if msg_id == last_id:
                continue
            last_id = msg_id
            d = dict(fields or {})
            ts = _i(d.get("ts_ms", d.get("ts", d.get("timestamp", 0))), 0)
            if ts <= 0:
                continue
            if ts < start_ms:
                scanned = max_scan
                break
            d["_ts_ms"] = ts
            rows.append(d)
        if len(batch) < 2000:
            break

    rows.sort(key=lambda x: int(x.get("_ts_ms", 0)))
    return rows


def _merge_gate_health(db: Dict[str, Dict[str, BucketHealth]], gate_rows: List[Dict[str, Any]]) -> Dict[str, Dict[str, BucketHealth]]:
    # Aggregate ok_soft by sym,bucket
    agg: Dict[Tuple[str, str], Dict[str, int]] = {}
    for r in gate_rows:
        sym = str(r.get("symbol") or r.get("sym") or "").upper()
        if not sym:
            continue
        b = _norm_bucket(r.get("exec_regime_bucket") or "NORMAL")
        ok = _i(r.get("ok", 0), 0)
        ok_soft = _i(r.get("ok_soft", 0), 0)
        key = (sym, b)
        a = agg.setdefault(key, {"n": 0, "ok": 0, "ok_soft": 0})
        a["n"] += 1
        a["ok"] += 1 if ok == 1 else 0
        a["ok_soft"] += 1 if ok_soft == 1 else 0

    # Fill into db health map (or create if missing)
    out: Dict[str, Dict[str, BucketHealth]] = {}
    for sym, buckets in db.items():
        out.setdefault(sym, {})
        for b, h in buckets.items():
            a = agg.get((sym, b), {"n": 0, "ok": 0, "ok_soft": 0})
            n = int(a["n"])
            ok_soft_rate = float((a["ok"] + a["ok_soft"]) / n) if n > 0 else 0.0
            out[sym][b] = BucketHealth(
                bucket=b,
                db_n=h.db_n,
                resid_p95=h.resid_p95,
                resid_p99=h.resid_p99,
                edge_neg_share=h.edge_neg_share,  # P89: carry forward from DB
                eligible_n=n,
                ok_soft_rate=ok_soft_rate,
            )

    # Add symbols/buckets that exist only in gate stream (rare; keep them too)
    for (sym, b), a in agg.items():
        if sym not in out:
            out[sym] = {}
        if b not in out[sym]:
            n = int(a["n"])
            ok_soft_rate = float((a["ok"] + a["ok_soft"]) / n) if n > 0 else 0.0
            out[sym][b] = BucketHealth(bucket=b, db_n=0, resid_p95=0.0, resid_p99=0.0, edge_neg_share=0.0, eligible_n=n, ok_soft_rate=ok_soft_rate)

    return out


def _aggregate_global_by_bucket(sym_map: Dict[str, Dict[str, BucketHealth]]) -> Dict[str, BucketHealth]:
    # Weighted aggregation by db_n (for residuals/edge) and eligible_n (for ok rate)
    by_b: Dict[str, Dict[str, float]] = {}
    for sym, buckets in sym_map.items():
        for b, h in buckets.items():
            b = _norm_bucket(b)
            a = by_b.setdefault(b, {"db_n": 0.0, "p95_sum": 0.0, "p99_sum": 0.0, "neg_sum": 0.0, "gate_n": 0.0, "ok_sum": 0.0})
            # residual + edge weighting by db_n
            a["db_n"] += float(h.db_n)
            a["p95_sum"] += float(h.resid_p95) * float(max(h.db_n, 0))
            a["p99_sum"] += float(h.resid_p99) * float(max(h.db_n, 0))
            a["neg_sum"] += float(h.edge_neg_share) * float(max(h.db_n, 0))  # P89: weighted avg
            # ok_soft_rate weighting by eligible_n
            a["gate_n"] += float(h.eligible_n)
            a["ok_sum"] += float(h.ok_soft_rate) * float(max(h.eligible_n, 0))

    out: Dict[str, BucketHealth] = {}
    for b, a in by_b.items():
        db_n = int(a["db_n"])
        gate_n = int(a["gate_n"])
        p95 = float(a["p95_sum"] / a["db_n"]) if a["db_n"] > 0 else 0.0
        p99 = float(a["p99_sum"] / a["db_n"]) if a["db_n"] > 0 else 0.0
        neg = float(a["neg_sum"] / a["db_n"]) if a["db_n"] > 0 else 0.0  # P89
        ok_soft_rate = float(a["ok_sum"] / a["gate_n"]) if a["gate_n"] > 0 else 0.0
        out[b] = BucketHealth(bucket=b, db_n=db_n, resid_p95=p95, resid_p99=p99, edge_neg_share=neg, eligible_n=gate_n, ok_soft_rate=ok_soft_rate)
    return out


# ------------------------ main runner ------------------------

async def run() -> bool:
    if aioredis is None:
        logger.error("redis.asyncio not available")
        return False

    try:
        import asyncpg  # type: ignore
    except Exception:  # pragma: no cover
        logger.error("asyncpg is not available")
        return False
    db_url = os.getenv("DATABASE_URL", "postgresql://trading:trading@scanner-postgres:5432/scanner_analytics")
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    stream = os.getenv("OF_GATE_METRICS_STREAM", "metrics:of_gate")

    lookback_h = _env_int("PROMOTE_LOOKBACK_HOURS", "24")
    max_scan = _env_int("PROMOTE_MAX_SCAN", "200000")

    min_db_n = _env_int("PROMOTE_MIN_DB_SAMPLES", "100")
    max_p95 = _env_float("PROMOTE_MAX_P95_RESID_BPS", "3.0")
    max_p99 = _env_float("PROMOTE_MAX_P99_RESID_BPS", "8.0")
    # P89: new guardrail — block promote if edge (expected minus actual) is negative too often
    max_edge_neg_share = _env_float("PROMOTE_MAX_EDGE_NEG_SHARE", "0.40")
    min_gate_n = _env_int("PROMOTE_MIN_ELIGIBLE", "200")
    min_ok_soft = _env_float("PROMOTE_MIN_OK_SOFT", "0.05")
    default_bucket = str(os.getenv("PROMOTE_DEFAULT_BUCKET", "HIGH_VOL_LOW_LIQ") or "HIGH_VOL_LOW_LIQ").strip().upper()

    promote_order = _parse_allowlist(os.getenv("PROMOTE_ADD_ORDER", "HIGH_VOL,LOW_LIQ") or "HIGH_VOL,LOW_LIQ")
    if not promote_order:
        promote_order = ["HIGH_VOL", "LOW_LIQ"]

    promote_symbols_raw = str(os.getenv("PROMOTE_SYMBOLS", "") or "").strip()
    promote_symbols = [x.strip().upper() for x in promote_symbols_raw.replace(";", ",").split(",") if x.strip()]
    scope = str(os.getenv("PROMOTE_SCOPE", "per_symbol" if promote_symbols else "global") or "global").strip().lower()
    if scope not in ("global", "per_symbol"):
        scope = "global"

    apply = str(os.getenv("PROMOTE_APPLY", "0") or "0").strip() in ("1", "true", "True", "yes", "YES")

    min_apply_gap_sec = _env_int('PROMOTE_MIN_APPLY_GAP_SEC', '21600')

    # P89: MV and raw view names (configurable, sanitized to prevent SQL injection)
    stats_mv = _safe_ident(
        str(os.getenv("PROMOTE_STATS_MV", "mv_exec_slippage_eval_1h_stats") or "mv_exec_slippage_eval_1h_stats"),
        "mv_exec_slippage_eval_1h_stats",
    )
    stats_view = _safe_ident(
        str(os.getenv("PROMOTE_STATS_VIEW", "v_exec_slippage_eval") or "v_exec_slippage_eval"),
        "v_exec_slippage_eval",
    )

    # Path to write a cheap status JSON (cheap exporter path instead of Redis queries)
    status_path = os.getenv("PROMOTE_STATUS_PATH", "/var/lib/trade/of_reports/out/enforce/promoter/enforce_bucket_promoter_status.json")

    logger.info("Connecting: postgres=%s redis=%s mv=%s", db_url, redis_url, stats_mv)

    r = aioredis.Redis.from_url(redis_url, decode_responses=True)
    lock_key = os.getenv('ENFORCE_BUCKET_PROMOTER_LOCK_KEY', 'lock:enforce_bucket_promoter')
    lock_ttl = _env_int('ENFORCE_BUCKET_PROMOTER_LOCK_TTL_SEC', '1800')
    lock_token = ''
    if _acquire_lock is not None:
        lock_token = await _acquire_lock(r, key=lock_key, ttl_sec=lock_ttl)
        if not lock_token:
            # Another instance is running. Exit success (no-op).
            logger.warning('lock busy: %s', lock_key)
            try:
                await r.aclose()
            except Exception:
                pass
            return True

    try:
        conn = await asyncpg.connect(db_url)
    except Exception as e:
        logger.error("DB connect failed: %s", e)
        try:
            if lock_token and _release_lock is not None:
                await _release_lock(r, key=lock_key, token=lock_token)
        except Exception:
            pass
        try:
            await r.aclose()
        except Exception:
            pass
        return False

    # 1) DB: residual health per sym,bucket (P89: tries MV first, raw view as fallback)
    db_sym = await _fetch_db_health(conn, lookback_h=lookback_h, mv=stats_mv, view=stats_view)

    # 2) Redis stream: ok_rate per sym,bucket
    start_ms = _now_ms() - int(lookback_h * 3600 * 1000)
    gate_rows = await _scan_gate_metrics(r, stream=stream, start_ms=start_ms, max_scan=max_scan)

    sym_map = _merge_gate_health(db_sym, gate_rows)
    global_by_bucket = _aggregate_global_by_bucket(sym_map)

    # 3) Decide proposed allowlist (scope-aware)
    target_sym = ""  # "" means global
    cur_slip = ""
    cur_taker = ""

    def _key(base: str, sym: str) -> str:
        return f"{base}:{sym}" if sym else base

    async def _read_cfg_pref(base: str, sym: str) -> str:
        if sym:
            v = await r.get(_key(base, sym))
            if v:
                return str(v)
        v = await r.get(base)
        return str(v or "")

    apply_blocked_by_gap = False

    cand_syms: List[str] = [""]
    if scope == "per_symbol" and promote_symbols:
        cand_syms = promote_symbols[:]

    chosen_dec_slip: Optional[PromotionDecision] = None
    chosen_dec_taker: Optional[PromotionDecision] = None

    for sym in cand_syms:
        hmap = global_by_bucket if not sym else sym_map.get(sym, {})
        if sym and not hmap:
            continue

        cur_slip = await _read_cfg_pref("cfg:slippage_decomp_enforce_buckets", sym)
        cur_taker = await _read_cfg_pref("cfg:taker_flow_gate_enforce_buckets", sym)
        if not cur_slip:
            cur_slip = default_bucket
        if not cur_taker:
            cur_taker = default_bucket

        local_apply = bool(apply)
        if local_apply:
            try:
                ts_key = _key("state:enforce_bucket_promoter:last_apply_ts_ms", sym) if sym else "state:enforce_bucket_promoter:last_apply_ts_ms"
                last_apply_ts = await r.get(ts_key)
                if last_apply_ts and ((_now_ms() - _i(last_apply_ts, 0)) < (min_apply_gap_sec * 1000)):
                    local_apply = False
                    apply_blocked_by_gap = True
            except Exception:
                pass

        dec_slip = decide_next_allowlist(
            current_allow=cur_slip,
            health_by_bucket=hmap,
            promote_order=promote_order,
            default_bucket=default_bucket,
            min_db_n=min_db_n,
            max_p95=max_p95,
            max_p99=max_p99,
            max_edge_neg_share=max_edge_neg_share,  # P89
            min_eligible_n=min_gate_n,
            min_ok_soft_rate=min_ok_soft,
        )
        dec_taker = decide_next_allowlist(
            current_allow=cur_taker,
            health_by_bucket=hmap,
            promote_order=promote_order,
            default_bucket=default_bucket,
            min_db_n=min_db_n,
            max_p95=max_p95,
            max_p99=max_p99,
            max_edge_neg_share=max_edge_neg_share,  # P89
            min_eligible_n=min_gate_n,
            min_ok_soft_rate=min_ok_soft,
        )

        if (dec_slip.ok or dec_taker.ok) or sym == cand_syms[-1]:
            target_sym = sym
            chosen_dec_slip = dec_slip
            chosen_dec_taker = dec_taker
            apply = bool(local_apply) and bool(dec_slip.ok or dec_taker.ok)
            break

    if chosen_dec_slip is None or chosen_dec_taker is None:
        chosen_dec_slip = PromotionDecision(ok=False, new_allowlist=_allowlist_to_str([default_bucket]), added_bucket="", reasons=["no_decision"])  # type: ignore
        chosen_dec_taker = PromotionDecision(ok=False, new_allowlist=_allowlist_to_str([default_bucket]), added_bucket="", reasons=["no_decision"])  # type: ignore

    dec_slip = chosen_dec_slip
    dec_taker = chosen_dec_taker

    report = {
        "ts_ms": _now_ms(),
        "lookback_h": lookback_h,
        "apply": apply,
        "scope": scope,
        "target_sym": (target_sym or "GLOBAL"),
        "apply_blocked_by_gap": bool(apply_blocked_by_gap),
        "min_apply_gap_sec": int(min_apply_gap_sec),
        "current": {
            "slippage_decomp_enforce_buckets": cur_slip,
            "taker_flow_gate_enforce_buckets": cur_taker
        },
        "proposed": {
            "slippage_decomp_enforce_buckets": dec_slip.new_allowlist,
            "taker_flow_gate_enforce_buckets": dec_taker.new_allowlist
        },
        "decisions": {
            "slippage": {"ok": dec_slip.ok, "added": dec_slip.added_bucket, "reasons": dec_slip.reasons},
            "taker": {"ok": dec_taker.ok, "added": dec_taker.added_bucket, "reasons": dec_taker.reasons}
        },
#         "bucket_health": {
            b: {
                "db_n": h.db_n,
                "resid_p95": h.resid_p95,
                "resid_p99": h.resid_p99,
                "edge_neg_share": h.edge_neg_share,  # P89: exposed for exporter gauge + ops visibility
                "gate_n": h.eligible_n,
                "ok_soft_rate": h.ok_soft_rate,
            }
#             for b, h in sorted(global_by_bucket.items())
        }
#         "notes": "Promotion adds at most one bucket per run. Uses DB residual + edge_neg_share + gate ok_soft_rate guardrails. P89: MV-first strategy for DB queries.",
#     }

    # 4) Write proposal keys (TTL 3d)
    # Also write status file (cheap exporter path — avoids heavy Redis queries per scrape)
    try:
        _write_json_atomic(status_path, report)
    except Exception as e:
        logger.error("Failed to write status file: %s", e)

    try:
        ttl = _env_int("PROMOTE_PROPOSAL_TTL_SEC", "259200")
        await r.set("proposal:enforce_bucket_promotion_report", json.dumps(report, separators=(",", ":")))
        await r.expire("proposal:enforce_bucket_promotion_report", ttl)
        await r.set("proposal:slippage_decomp_enforce_buckets", dec_slip.new_allowlist)
        await r.expire("proposal:slippage_decomp_enforce_buckets", ttl)
        await r.set("proposal:taker_flow_gate_enforce_buckets", dec_taker.new_allowlist)
        await r.expire("proposal:taker_flow_gate_enforce_buckets", ttl)

        if target_sym:
            await r.set(f"proposal:slippage_decomp_enforce_buckets:{target_sym}", dec_slip.new_allowlist, ex=ttl)
            await r.set(f"proposal:taker_flow_gate_enforce_buckets:{target_sym}", dec_taker.new_allowlist, ex=ttl)
    except Exception as e:
        logger.error("Failed to write proposal keys: %s", e)

    # 5) Apply (optional) — guarded by auto_apply_guard to prevent concurrent/blocked applies
    if apply:
        if assert_auto_apply_not_blocked is not None:
            try:
                assert_auto_apply_not_blocked()
            except SystemExit:
                # Auto-apply is blocked; record in report but skip apply
                report.setdefault("auto_apply_blocked", True)
                report.setdefault("auto_apply_block_meta", {})
                logger.warning("auto_apply is blocked — skipping apply, keeping proposal keys")
                apply = False

        try:
            applied_changes = []
            now_ms = _now_ms()
            pipe = r.pipeline(transaction=False)
            
            # Persist last change for rollback controller (and exporter)
            pipe.set("state:enforce_bucket_promoter:last_apply_ts_ms", str(now_ms))
            pipe.set("state:enforce_bucket_promoter:last_apply_sym", (target_sym or "GLOBAL"))

            # prev/applied state (scope-aware)
            new_slip = dec_slip.new_allowlist if dec_slip.ok else cur_slip
            new_taker = dec_taker.new_allowlist if dec_taker.ok else cur_taker

            if target_sym:
                pipe.set(f"state:enforce_bucket_promoter:last_apply_ts_ms:{target_sym}", str(now_ms))
                pipe.set(f"state:enforce_bucket_promoter:prev_slippage_decomp_enforce_buckets:{target_sym}", str(cur_slip or ""))
                pipe.set(f"state:enforce_bucket_promoter:prev_taker_flow_gate_enforce_buckets:{target_sym}", str(cur_taker or ""))
                pipe.set(f"state:enforce_bucket_promoter:applied_slippage_decomp_enforce_buckets:{target_sym}", str(new_slip or ""))
                pipe.set(f"state:enforce_bucket_promoter:applied_taker_flow_gate_enforce_buckets:{target_sym}", str(new_taker or ""))
            else:
                pipe.set("state:enforce_bucket_promoter:prev_slippage_decomp_enforce_buckets", str(cur_slip or ""))
                pipe.set("state:enforce_bucket_promoter:prev_taker_flow_gate_enforce_buckets", str(cur_taker or ""))
                pipe.set("state:enforce_bucket_promoter:applied_slippage_decomp_enforce_buckets", str(new_slip or ""))
                pipe.set("state:enforce_bucket_promoter:applied_taker_flow_gate_enforce_buckets", str(new_taker or ""))

            # Apply cfg (scope-aware; prefer per-symbol keys when target_sym is set)
            if target_sym:
                if dec_slip.ok and dec_slip.new_allowlist != cur_slip:
                    pipe.set(f"cfg:slippage_decomp_enforce_buckets:{target_sym}", str(new_slip or ""))
                    applied_changes.append({"component": "slippage_decomp", "old": cur_slip, "new": dec_slip.new_allowlist, "added_bucket": dec_slip.added_bucket})
                if dec_taker.ok and dec_taker.new_allowlist != cur_taker:
                    pipe.set(f"cfg:taker_flow_gate_enforce_buckets:{target_sym}", str(new_taker or ""))
                    applied_changes.append({"component": "taker_flow_gate", "old": cur_taker, "new": dec_taker.new_allowlist, "added_bucket": dec_taker.added_bucket})
            else:
                if dec_slip.ok and dec_slip.new_allowlist != cur_slip:
                    pipe.set("cfg:slippage_decomp_enforce_buckets", str(new_slip or ""))
                    applied_changes.append({"component": "slippage_decomp", "old": cur_slip, "new": dec_slip.new_allowlist, "added_bucket": dec_slip.added_bucket})
                if dec_taker.ok and dec_taker.new_allowlist != cur_taker:
                    pipe.set("cfg:taker_flow_gate_enforce_buckets", str(new_taker or ""))
                    applied_changes.append({"component": "taker_flow_gate", "old": cur_taker, "new": dec_taker.new_allowlist, "added_bucket": dec_taker.added_bucket})
            
            await pipe.execute()
            
            if applied_changes:
                # Push apply event (audit log) to telemetry stream
                try:
                    for ch in applied_changes:
                        fields = {
                            "type": "apply",
                            "ts_ms": now_ms,
                            "sym": (target_sym or "GLOBAL"),
                            "component": ch.get("component"),
                            "old": ch.get("old"),
                            "new": ch.get("new"),
                            "added_bucket": ch.get("added_bucket")
                        }
                        await _xadd_event(r, stream="telemetry:enforce_bucket_promoter:events", fields=fields)
                except Exception:
                    pass

                # Notify ops channel on apply (with cooldown)
                try:
                    cd = _env_int("ENFORCE_BUCKET_NOTIFY_COOLDOWN_SEC", "1800")
                    parts = []
                    for ch in applied_changes:
                        parts.append(f"{ch.get('component')}:{ch.get('old')}->{ch.get('new')} add={ch.get('added_bucket')}")
                    txt = "[EnforceBucketPromoter] APPLY sym=" + (target_sym or "GLOBAL") + " " + " | ".join(parts)
                    await _notify_once(r, txt, cooldown_key="notify:enforce_bucket_promoter:cooldown", cooldown_sec=cd)
                except Exception:
                    pass
        except Exception as e:
            logger.error("Failed to apply cfg keys / write state: %s", e)

    logger.info("done: slip_ok=%s add=%s new=%s | taker_ok=%s add=%s new=%s",
                dec_slip.ok, dec_slip.added_bucket, dec_slip.new_allowlist,
                dec_taker.ok, dec_taker.added_bucket, dec_taker.new_allowlist)

    await conn.close()
    try:
        if lock_token and _release_lock is not None:
            await _release_lock(r, key=lock_key, token=lock_token)
    except Exception:
        pass
    try:
        await r.aclose()
    except Exception:
        pass
    return True


if __name__ == "__main__":
    ok = asyncio.run(run())
    raise SystemExit(0 if ok else 2)
