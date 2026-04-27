from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import json
import os
import time
import hmac
import hashlib
import secrets
from typing import Any, Dict, List

import redis

from tools.redis_window import read_recent_stream
from tools.ml_metrics_agg import agg_outcomes, agg_health_ml_confirm, agg_exec_risk
from core.share_map import parse_map, dump_map, merge_updates
from core.bucket_utils import bucket_from_scenario


def now_ms() -> int:
    """Returns current timestamp in milliseconds (epoch)."""
    return get_ny_time_millis()


def sign(bundle_id: str, secret: str) -> str:
    """Generate short HMAC signature for bundle_id (8 hex characters)."""
    return hmac.new(secret.encode(), bundle_id.encode(), hashlib.sha256).hexdigest()[:8]


def notify(r: redis.Redis, text: str, buttons=None) -> None:
    """Send notification to notify:telegram stream."""
    fields = {"type": "report", "text": text, "ts": str(now_ms())}
    if buttons is not None:
        fields["buttons"] = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"), fields, maxlen=200000, approximate=True)


def make_bundle_hset(cfg_key: str, changes: Dict[str, str], who: str, ttl: int):
    """Create bundle for HSET operations (compatible with recs_callback_worker_v2)."""
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    bid = secrets.token_hex(6)
    sig = sign(bid, secret)
    ts = now_ms()
    ops = [{"op": "HSET", "key": cfg_key, "field": k, "value": str(v)} for k, v in changes.items()]
    bundle = {"id": bid, "created_ms": ts, "ttl_sec": ttl, "who": who, "ops": ops, "meta": {"kind": "ml_promotion_v3"}}
    return bid, sig, bundle


def write_bundle(r: redis.Redis, bid: str, bundle: Dict[str, Any], ttl: int) -> None:
    """Write bundle to Redis (compatible with recs_callback_worker_v2)."""
    r.set(f"recs:bundle:{bid}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl)
    r.set(f"recs:status:{bid}", "PENDING", ex=ttl)


def ladder_next(cur: float) -> float:
    """Multi-step ladder: 0.05 -> 0.10 -> 0.20 -> 0.35 -> 0.50."""
    levels = [0.05, 0.10, 0.20, 0.35, 0.50]
    for lv in levels:
        if cur + 1e-12 < lv:
            return lv
    return cur


def thresholds_for_level(level: float, bucket: str) -> Dict[str, float]:
    """Utility-based promotion thresholds (Brier, ECE, meanR, tail_rate, ES05).
    
    Prevents "well-calibrated but negative expectancy" models.
    Thresholds tighten as level increases.
    """
    # Calibration gates
    brier = 0.23
    if level >= 0.10:
        brier = 0.225
    if level >= 0.20:
        brier = 0.220
    if level >= 0.35:
        brier = 0.215
    if level >= 0.50:
        brier = 0.210

    ece = 0.08
    if level >= 0.20:
        ece = 0.070
    if level >= 0.35:
        ece = 0.060
    if level >= 0.50:
        ece = 0.055

    # Utility gates
    meanR = -0.02
    if level >= 0.20:
        meanR = 0.00
    if level >= 0.35:
        meanR = 0.02
    if level >= 0.50:
        meanR = 0.03

    tail = 0.35
    if level >= 0.20:
        tail = 0.32
    if level >= 0.35:
        tail = 0.28
    if level >= 0.50:
        tail = 0.25

    es05 = -0.90
    if level >= 0.20:
        es05 = -0.85
    if level >= 0.35:
        es05 = -0.75
    if level >= 0.50:
        es05 = -0.70

    # Range bucket: slightly more lenient (range strategies can have different risk profile)
    if bucket == "range":
        brier -= 0.005
        ece -= 0.005
        meanR += 0.01
        tail -= 0.03
        es05 += 0.05

    return {"brier_max": brier, "ece_max": ece, "meanR_min": meanR, "tail_max": tail, "es05_min": es05}


def filter_rows(rows: List[Dict[str, Any]], *, bucket: str, symbol: str = "") -> List[Dict[str, Any]]:
    """Filter rows by bucket and optionally by symbol."""
    out = []
    bs = bucket.lower()
    sym = symbol.upper().strip()
    for r in rows:
        if str(r.get("bucket", "")).lower() != bs:
            continue
        if sym and str(r.get("symbol", "")).upper() != sym:
            continue
        out.append(r)
    return out


def pass_metrics(m: Dict[str, Any], thr: Dict[str, float]) -> bool:
    """Check if metrics pass all utility gates (Brier, ECE, meanR, tail_rate, ES05)."""
    return (
        float(m.get("brier", 1.0)) <= thr["brier_max"] and
        float(m.get("ece", 1.0)) <= thr["ece_max"] and
        float(m.get("meanR", -9)) >= thr["meanR_min"] and
        float(m.get("tail_rate", 9)) <= thr["tail_max"] and
        float(m.get("es05", -9)) >= thr["es05_min"]
    )


def main() -> None:
    """Main promotion ladder v3: per-symbol shares + utility gates + range exec-risk veto."""
    r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)
    cfg_key = os.getenv("ML_CONFIRM_CFG_KEY", "cfg:ml_confirm")
    ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)

    pending_base = os.getenv("ML_PROMO_PENDING_KEY", "meta:ml:promo:pending")
    cfg = r.hgetall(cfg_key) or {}

    # dual health windows gate (30m and 2h)
    ml_stream = os.getenv("ML_CONFIRM_METRICS_STREAM", "metrics:ml_confirm")
    max_scan_h = int(os.getenv("ML_PROMO_HEALTH_MAX_SCAN", "200000") or 200000)
    h30 = agg_health_ml_confirm(read_recent_stream(r, ml_stream, now_ms() - 30 * 60_000, max_scan_h))
    h120 = agg_health_ml_confirm(read_recent_stream(r, ml_stream, now_ms() - 120 * 60_000, max_scan_h))

    miss_max = float(os.getenv("ML_SRE_MISSING_RATE_MAX", "0.02") or 0.02)
    err_max = float(os.getenv("ML_SRE_ERR_RATE_MAX", "0.01") or 0.01)
    lat_max = float(os.getenv("ML_SRE_LAT_P99_MAX_MS", "6.0") or 6.0)

    def health_ok(h):
        return (h.get("n", 0) >= 200 and h["missing_rate"] <= miss_max and h["err_rate"] <= err_max and h["lat_p99_ms"] <= lat_max)

    if not (health_ok(h30) and health_ok(h120)):
        return

    out_stream = os.getenv("ML_OUTCOME_METRICS_STREAM", "metrics:ml_outcome")
    max_scan = int(os.getenv("ML_PROMO_MAX_SCAN", "700000") or 700000)

    short_h = float(os.getenv("ML_PROMO_WINDOW_HOURS", "24") or 24)
    long_h = float(os.getenv("ML_PROMO_LONG_HOURS", "168") or 168)

    rows_short = read_recent_stream(r, out_stream, now_ms() - int(short_h * 3600_000), max_scan)
    rows_long = read_recent_stream(r, out_stream, now_ms() - int(long_h * 3600_000), max_scan)

    # Per-symbol mins
    min_n_s = int(os.getenv("ML_PROMO_MIN_N_SYM_SHORT", "60") or 60)
    min_n_l = int(os.getenv("ML_PROMO_MIN_N_SYM_LONG", "250") or 250)
    max_syms = int(os.getenv("ML_PROMO_MAX_SYMBOLS_PER_RUN", "3") or 3)

    # Range exec veto caps
    exec_p90_30m_max = float(os.getenv("ML_RANGE_EXEC_P90_MAX_30M", "0.90") or 0.90)
    exec_p90_2h_max = float(os.getenv("ML_RANGE_EXEC_P90_MAX_2H", "0.85") or 0.85)
    exec_min_n_30m = int(os.getenv("ML_RANGE_EXEC_MIN_N_30M", "50") or 50)
    exec_min_n_2h = int(os.getenv("ML_RANGE_EXEC_MIN_N_2H", "120") or 120)

    # current maps
    m_tr = parse_map(cfg.get("enforce_share_trend_by_symbol") or "")
    m_rg = parse_map(cfg.get("enforce_share_range_by_symbol") or "")

    # Candidate per-bucket symbol promotions
    for bucket in ("trend", "range"):
        pkey = f"{pending_base}:{bucket}:sym"
        if r.get(pkey):
            continue

        # Determine symbol set from outcomes (we only promote symbols with matured outcomes)
        syms = sorted({str(x.get("symbol", "")).upper() for x in rows_long if str(x.get("bucket", "")).lower() == bucket and str(x.get("symbol", ""))})
        if not syms:
            continue

        updates: Dict[str, float] = {}
        picked = 0

        for sym in syms:
            if picked >= max_syms:
                break

            rs = filter_rows(rows_short, bucket=bucket, symbol=sym)
            rl = filter_rows(rows_long, bucket=bucket, symbol=sym)
            ms = agg_outcomes(rs)
            ml = agg_outcomes(rl)
            if ms.get("n", 0) < min_n_s or ml.get("n", 0) < min_n_l:
                continue

            # current share for symbol
            cur = (m_tr.get(sym) if bucket == "trend" else m_rg.get(sym))
            if cur is None:
                # fallback to bucket share
                try:
                    cur = float(cfg.get(f"enforce_share_{bucket}", cfg.get("enforce_share", "0.0")) or 0.0)
                except Exception:
                    cur = 0.0

            nxt = ladder_next(float(cur))
            if nxt <= float(cur) + 1e-12:
                continue

            thr = thresholds_for_level(nxt, bucket=bucket)
            if not (pass_metrics(ms, thr) and pass_metrics(ml, thr)):
                continue

            # Range exec-risk veto (per-symbol, dual windows 30m and 2h)
            if bucket == "range":
                # Use ml_confirm metrics (has exec_risk_norm)
                w30 = filter_rows(read_recent_stream(r, ml_stream, now_ms() - 30 * 60_000, max_scan_h), bucket=bucket, symbol=sym)
                w120 = filter_rows(read_recent_stream(r, ml_stream, now_ms() - 120 * 60_000, max_scan_h), bucket=bucket, symbol=sym)
                ex30 = agg_exec_risk(w30)
                ex120 = agg_exec_risk(w120)
                if ex30.get("n", 0) < exec_min_n_30m or ex120.get("n", 0) < exec_min_n_2h:
                    continue
                if float(ex30.get("exec_p90", 1.0)) > exec_p90_30m_max:
                    continue
                if float(ex120.get("exec_p90", 1.0)) > exec_p90_2h_max:
                    continue

            updates[sym] = float(nxt)
            picked += 1

        if not updates:
            continue

        # write bundle updating JSON map field
        if bucket == "trend":
            field = "enforce_share_trend_by_symbol"
            new_map = merge_updates(m_tr, updates)
        else:
            field = "enforce_share_range_by_symbol"
            new_map = merge_updates(m_rg, updates)

        changes = {
            field: dump_map(new_map),
            "updated_ms": str(now_ms()),
        }
        bid, sig, bundle = make_bundle_hset(cfg_key, changes, who=f"ml_promo_v3_sym_share:{bucket}", ttl=ttl)
        write_bundle(r, bid, bundle, ttl)
        r.set(pkey, json.dumps({"bundle_id": bid, "bucket": bucket, "kind": "sym_share"}, separators=(",", ":")), ex=ttl)

        buttons = [[
            {"text": "👀 Preview diff", "callback": f"recs:preview2:{bid}:{sig}"},
            {"text": "✅ Confirm apply", "callback": f"recs:confirm:{bid}:{sig}"},
            {"text": "❌ Reject", "callback": f"recs:reject:{bid}:{sig}"},
        ]]
        notify(r,
               f"<b>ML Promotion v3: PER-SYMBOL SHARE UPDATE ({bucket})</b>\\n"
               f"updates=<code>{updates}</code>\\n"
               f"health30=<code>{h30}</code>\\nhealth120=<code>{h120}</code>",
               buttons)
        return

    # Optional: Challenger promotion (global) can remain handled by v4/v3; not repeated here to keep v3 focused.
    return


if __name__ == "__main__":
    main()

