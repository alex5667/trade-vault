from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
from typing import Any

import redis

from tools.ml_metrics_agg import agg_health_ml_confirm, agg_outcomes
from tools.redis_window import read_recent_stream
from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS


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
    r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM), fields, maxlen=200000, approximate=True)


def make_bundle_hset(cfg_key: str, changes: dict[str, str], *, who: str, ttl: int) -> tuple[str, str, dict[str, Any]]:
    """Create bundle for HSET operations (compatible with recs_callback_worker_v2)."""
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    bundle_id = secrets.token_hex(6)
    sig = sign(bundle_id, secret)
    ts = now_ms()
    ops = [{"op": "HSET", "key": cfg_key, "field": k, "value": str(v)} for k, v in changes.items()]
    bundle = {"id": bundle_id, "created_ms": ts, "ttl_sec": ttl, "who": who, "ops": ops, "meta": {"kind": "ml_promotion_v2"}}
    return bundle_id, sig, bundle


def write_bundle(r: redis.Redis, bundle_id: str, bundle: dict[str, Any], ttl: int) -> None:
    """Write bundle to Redis (compatible with recs_callback_worker_v2)."""
    r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl)
    r.set(f"recs:status:{bundle_id}", "PENDING", ex=ttl)


def ladder_next(cur: float) -> float:
    """Multi-step ladder: 0.05 -> 0.10 -> 0.20 -> 0.35 -> 0.50 (can tune)."""
    levels = [0.05, 0.10, 0.20, 0.35, 0.50]
    for lv in levels:
        if cur + 1e-12 < lv:
            return lv
    return cur


def thresholds_for_level(level: float, *, bucket: str) -> dict[str, float]:
    """Stricter thresholds as share increases; range slightly stricter than trend.
    
    Thresholds are tuned as you collect data:
    - brier_max: maximum allowed Brier score
    - ece_max: maximum allowed ECE (Expected Calibration Error)
    - win_min: minimum win rate
    
    Args:
        level: Current share level (0.05, 0.10, 0.20, 0.35, 0.50)
        bucket: Bucket name (trend/range)
        
    Returns:
        Dict with brier_max, ece_max, win_min
    """
    base = 0.23
    if level >= 0.10:
        base = 0.225
    if level >= 0.20:
        base = 0.220
    if level >= 0.35:
        base = 0.215
    if level >= 0.50:
        base = 0.210

    ece = 0.08
    if level >= 0.20:
        ece = 0.07
    if level >= 0.35:
        ece = 0.06
    if level >= 0.50:
        ece = 0.055

    win = 0.45
    if level >= 0.20:
        win = 0.47
    if level >= 0.35:
        win = 0.48
    if level >= 0.50:
        win = 0.49

    if bucket == "range":
        base -= 0.005
        ece -= 0.005
        win += 0.01

    return {"brier_max": base, "ece_max": ece, "win_min": win}


def filter_bucket(rows: list[dict[str, Any]], bucket: str) -> list[dict[str, Any]]:
    """Filter rows by bucket."""
    return [r for r in rows if (r.get("bucket", "")).lower() == bucket]


def main() -> None:
    """Main promotion ladder v2: bucket-aware, dual-window (24h + 7d), ECE + Brier gates."""
    r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)
    cfg_key = os.getenv("ML_CONFIRM_CFG_KEY", "cfg:ml_confirm")
    ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)

    pending_key = os.getenv("ML_PROMO_PENDING_KEY", "meta:ml:promo:pending")
    if r.get(pending_key):
        return

    cfg = r.hgetall(cfg_key) or {}

    # dual health window gate (30m and 2h)
    ml_stream = os.getenv("ML_CONFIRM_METRICS_STREAM", RS.ML_CONFIRM_METRICS)
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

    out_stream = os.getenv("ML_OUTCOME_METRICS_STREAM", RS.ML_OUTCOME_METRICS)
    max_scan = int(os.getenv("ML_PROMO_MAX_SCAN", "500000") or 500000)

    short_h = float(os.getenv("ML_PROMO_WINDOW_HOURS", "24") or 24)
    long_h = float(os.getenv("ML_PROMO_LONG_HOURS", "168") or 168)

    rows_short = read_recent_stream(r, out_stream, now_ms() - int(short_h * 3600_000), max_scan)
    rows_long = read_recent_stream(r, out_stream, now_ms() - int(long_h * 3600_000), max_scan)

    min_n_short = int(os.getenv("ML_PROMO_MIN_N", "200") or 200)
    min_n_long = int(os.getenv("ML_PROMO_LONG_MIN_N", "800") or 800)

    # bucket loop: promote challenger / increase share per bucket
    for bucket in ("trend", "range"):
        # per-bucket pending
        pkey_b = f"{pending_key}:{bucket}"
        if r.get(pkey_b):
            continue

        rs = filter_bucket(rows_short, bucket)
        rl = filter_bucket(rows_long, bucket)

        promo_s = agg_outcomes(rs)
        promo_l = agg_outcomes(rl)

        if promo_s.get("n", 0) < min_n_short or promo_l.get("n", 0) < min_n_long:
            continue

        # current share for bucket (fallback to enforce_share)
        cur_share = float(cfg.get(f"enforce_share_{bucket}", cfg.get("enforce_share", "0.0")) or 0.0)
        next_share = ladder_next(cur_share)
        if next_share <= cur_share + 1e-12:
            continue

        thr = thresholds_for_level(next_share, bucket=bucket)

        # dual-window pass required
        def pass_metrics(m):
            return (float(m["brier"]) <= thr["brier_max"] and float(m["ece"]) <= thr["ece_max"] and float(m["win_rate"]) >= thr["win_min"])

        if not (pass_metrics(promo_s) and pass_metrics(promo_l)):
            continue

        has_ch = ("brier_ch" in promo_s) and ((cfg.get("challenger_ver", "")).strip() != "")
        min_brier_improv = float(os.getenv("ML_PROMO_MIN_BRIER_IMPROV", "0.01") or 0.01)

        # If challenger exists and wins on BOTH windows -> propose promotion first (safer)
        if has_ch and ("brier_ch" in promo_l):
            b_a_s, b_c_s = float(promo_s["brier"]), float(promo_s["brier_ch"])
            b_a_l, b_c_l = float(promo_l["brier"]), float(promo_l["brier_ch"])
            if (b_a_s - b_c_s) >= min_brier_improv and (b_a_l - b_c_l) >= min_brier_improv:
                changes = {
                    "model_path": (cfg.get("challenger_model_path", "")),
                    "meta_path": (cfg.get("challenger_meta_path", "")),
                    "model_ver": (cfg.get("challenger_ver", "")),
                    "challenger_model_path": (cfg.get("model_path", "")),
                    "challenger_meta_path": (cfg.get("meta_path", "")),
                    "challenger_ver": (cfg.get("model_ver", "")),
                    "updated_ms": str(now_ms()),
                }
                bid, sig, bundle = make_bundle_hset(cfg_key, changes, who=f"ml_promo_v2_promote_challenger:{bucket}", ttl=ttl)
                write_bundle(r, bid, bundle, ttl)
                r.set(pkey_b, json.dumps({"bundle_id": bid, "kind": "promote_challenger", "bucket": bucket}, separators=(",", ":")), ex=ttl)

                buttons = [[
                    {"text": "👀 Preview diff", "callback": f"recs:preview2:{bid}:{sig}"},
                    {"text": "✅ Confirm apply", "callback": f"recs:confirm:{bid}:{sig}"},
                    {"text": "❌ Reject", "callback": f"recs:reject:{bid}:{sig}"},
                ]]
                txt = (
                    f"<b>ML Promotion v2: PROMOTE CHALLENGER ({bucket})</b>\\n"
                    f"short_n=<code>{promo_s['n']}</code> brier a/c=<code>{b_a_s:.4f}/{b_c_s:.4f}</code> ece a/c=<code>{promo_s.get('ece',0):.4f}/{promo_s.get('ece_ch',0):.4f}</code>\\n"
                    f"long_n=<code>{promo_l['n']}</code> brier a/c=<code>{b_a_l:.4f}/{b_c_l:.4f}</code>\\n"
                    f"health30=<code>{h30}</code>\\nhealth120=<code>{h120}</code>"
                )
                notify(r, txt, buttons)
                return

        # else propose share increase for this bucket
        changes = {
            f"enforce_share_{bucket}": f"{next_share:.4f}",
            "updated_ms": str(now_ms()),
        }
        bid, sig, bundle = make_bundle_hset(cfg_key, changes, who=f"ml_promo_v2_increase_share:{bucket}", ttl=ttl)
        write_bundle(r, bid, bundle, ttl)
        r.set(pkey_b, json.dumps({"bundle_id": bid, "kind": "increase_share", "bucket": bucket}, separators=(",", ":")), ex=ttl)

        buttons = [[
            {"text": "👀 Preview diff", "callback": f"recs:preview2:{bid}:{sig}"},
            {"text": "✅ Confirm apply", "callback": f"recs:confirm:{bid}:{sig}"},
            {"text": "❌ Reject", "callback": f"recs:reject:{bid}:{sig}"},
        ]]
        txt = (
            f"<b>ML Promotion v2: INCREASE SHARE ({bucket})</b>\\n"
            f"share: <code>{cur_share:.4f}</code> -> <code>{next_share:.4f}</code>\\n"
            f"short: n=<code>{promo_s['n']}</code> brier=<code>{promo_s['brier']:.4f}</code> ece=<code>{promo_s['ece']:.4f}</code> win=<code>{promo_s['win_rate']:.3f}</code>\\n"
            f"long: n=<code>{promo_l['n']}</code> brier=<code>{promo_l['brier']:.4f}</code> ece=<code>{promo_l['ece']:.4f}</code> win=<code>{promo_l['win_rate']:.3f}</code>\\n"
            f"thresholds(next): brier<=<code>{thr['brier_max']}</code> ece<=<code>{thr['ece_max']}</code> win>=<code>{thr['win_min']}</code>\\n"
            f"health30=<code>{h30}</code>\\nhealth120=<code>{h120}</code>"
        )
        notify(r, txt, buttons)
        return


if __name__ == "__main__":
    main()

