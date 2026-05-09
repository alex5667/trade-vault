from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
from typing import Any

import redis

from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS


def now_ms() -> int:
    """Returns current timestamp in milliseconds (epoch)."""
    return get_ny_time_millis()


def pctl(xs: list[float], q: float) -> float:
    """Compute percentile."""
    if not xs:
        return 0.0
    xs = sorted(xs)
    i = int(round((len(xs) - 1) * q))
    i = max(0, min(len(xs) - 1, i))
    return float(xs[i])


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


def read_recent_stream(r: redis.Redis, stream: str, since_ms: int, max_scan: int) -> list[dict[str, Any]]:
    """Read recent messages from stream (reverse scan until since_ms)."""
    rows = []
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
                ts = int(float(fields.get("ts_ms", 0) or 0))
            except Exception:
                ts = 0
            if ts and ts < since_ms:
                scanned = max_scan
                break
            rows.append(dict(fields))
    rows.reverse()
    return rows


def agg_outcomes(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate outcome metrics from metrics:ml_outcome rows."""
    n = 0
    briers = []
    briers_ch = []
    wins = 0
    wins_ch = 0
    rm = []
    rm_ch = []
    for r in rows:
        n += 1
        y = int(float(r.get("y", 0) or 0))
        wins += y
        briers.append(float(r.get("brier", 0.0) or 0.0))
        rm.append(float(r.get("r_mult", 0.0) or 0.0))

        if "brier_chal" in r:
            briers_ch.append(float(r.get("brier_chal", 0.0) or 0.0))
            # y is same
            wins_ch += y
            rm_ch.append(float(r.get("r_mult", 0.0) or 0.0))
    out = {
        "n": n,
        "win_rate": (wins / n) if n else 0.0,
        "brier": (sum(briers) / len(briers)) if briers else 0.0,
        "r_mean": (sum(rm) / len(rm)) if rm else 0.0,
        "r_p05": pctl(rm, 0.05) if rm else 0.0,
        "r_p50": pctl(rm, 0.50) if rm else 0.0,
    }
    if briers_ch:
        out.update({
            "n_ch": len(briers_ch),
            "brier_ch": (sum(briers_ch) / len(briers_ch)),
            "r_mean_ch": (sum(rm_ch) / len(rm_ch)) if rm_ch else 0.0,
        })
    return out


def agg_health_ml_confirm(rows: list[dict[str, Any]]) -> dict[str, Any]:
    """Aggregate health metrics from metrics:ml_confirm rows."""
    n = len(rows)
    if n == 0:
        return {"n": 0}
    missing = 0
    err = 0
    lat = []
    p = []
    for r in rows:
        missing += 1 if int(float(r.get("missing", 0) or 0)) == 1 else 0
        err += 1 if ((r.get("err", "")) or "").strip() != "" else 0
        lat.append(float(r.get("latency_ms", 0.0) or 0.0))
        p.append(float(r.get("p_edge", 0.0) or 0.0))
    return {
        "n": n,
        "missing_rate": missing / n,
        "err_rate": err / n,
        "lat_p99_ms": pctl(lat, 0.99),
        "p_edge_p50": pctl(p, 0.50),
    }


def make_bundle_hset(cfg_key: str, changes: dict[str, str], *, who: str, ttl: int) -> tuple[str, str, dict[str, Any], list[dict[str, Any]]]:
    """Create bundle for HSET operations."""
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    bundle_id = secrets.token_hex(6)
    sig = sign(bundle_id, secret)
    ts = now_ms()

    ops = []
    for k, v in changes.items():
        ops.append({"op": "HSET", "key": cfg_key, "field": k, "value": str(v)})

    bundle = {"id": bundle_id, "created_ms": ts, "ttl_sec": ttl, "who": who, "ops": ops, "meta": {"kind": "ml_promotion"}}
    return bundle_id, sig, bundle, ops


def write_bundle(r: redis.Redis, bundle_id: str, bundle: dict[str, Any], ttl: int) -> None:
    """Write bundle to Redis."""
    r.set(f"recs:bundle:{bundle_id}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl)
    r.set(f"recs:status:{bundle_id}", "PENDING", ex=ttl)


def main() -> None:
    """Main promotion ladder evaluation: propose promote challenger or increase share."""
    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    r = redis.Redis.from_url(redis_url, decode_responses=True)

    cfg_key = os.getenv("ML_CONFIRM_CFG_KEY", "cfg:ml_confirm")
    ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)

    pending_key = os.getenv("ML_PROMO_PENDING_KEY", "meta:ml:promo:pending")
    if r.get(pending_key):
        return

    cfg = r.hgetall(cfg_key) or {}
    enforce_share = float(cfg.get("enforce_share", "0.0") or 0.0)
    max_share = float(os.getenv("ML_PROMO_MAX_SHARE", "0.50") or 0.50)
    step = float(os.getenv("ML_PROMO_STEP", "0.05") or 0.05)

    # windows
    promo_hours = float(os.getenv("ML_PROMO_WINDOW_HOURS", "24") or 24)
    base_hours = float(os.getenv("ML_PROMO_BASELINE_HOURS", "168") or 168)
    max_scan = int(os.getenv("ML_PROMO_MAX_SCAN", "500000") or 500000)

    out_stream = os.getenv("ML_OUTCOME_METRICS_STREAM", "metrics:ml_outcome")
    rows_p = read_recent_stream(r, out_stream, now_ms() - int(promo_hours * 3600_000), max_scan)
    rows_b = read_recent_stream(r, out_stream, now_ms() - int(base_hours * 3600_000), max_scan)

    promo = agg_outcomes(rows_p)
    base = agg_outcomes(rows_b)

    # health gate from metrics:ml_confirm
    ml_stream = os.getenv("ML_CONFIRM_METRICS_STREAM", "metrics:ml_confirm")
    health_win_min = float(os.getenv("ML_PROMO_HEALTH_MIN", "60") or 60)
    h_rows = read_recent_stream(r, ml_stream, now_ms() - int(health_win_min * 60_000), int(os.getenv("ML_PROMO_HEALTH_MAX_SCAN", "200000") or 200000))
    health = agg_health_ml_confirm(h_rows)

    # thresholds
    min_n = int(os.getenv("ML_PROMO_MIN_N", "200") or 200)
    brier_max = float(os.getenv("ML_PROMO_BRIER_MAX", "0.22") or 0.22)
    win_min = float(os.getenv("ML_PROMO_WIN_MIN", "0.45") or 0.45)

    miss_max = float(os.getenv("ML_SRE_MISSING_RATE_MAX", "0.02") or 0.02)
    err_max = float(os.getenv("ML_SRE_ERR_RATE_MAX", "0.01") or 0.01)
    lat_max = float(os.getenv("ML_SRE_LAT_P99_MAX_MS", "6.0") or 6.0)

    # challenger promotion thresholds
    min_brier_improv = float(os.getenv("ML_PROMO_MIN_BRIER_IMPROV", "0.01") or 0.01)

    # safety gates
    if health.get("n", 0) < 200:
        return
    if health["missing_rate"] > miss_max or health["err_rate"] > err_max or health["lat_p99_ms"] > lat_max:
        return

    if promo.get("n", 0) < min_n:
        return

    has_ch = ("brier_ch" in promo) and ((cfg.get("challenger_ver", "")).strip() != "")

    # Action 1: promote challenger if significantly better
    if has_ch:
        brier_a = float(promo["brier"])
        brier_c = float(promo["brier_ch"])
        if (brier_a - brier_c) >= min_brier_improv:
            # proposal: swap active <-> challenger pointers (keep previous as challenger)
            changes = {
                "model_path": (cfg.get("challenger_model_path", "")),
                "meta_path": (cfg.get("challenger_meta_path", "")),
                "model_ver": (cfg.get("challenger_ver", "")),
                "challenger_model_path": (cfg.get("model_path", "")),
                "challenger_meta_path": (cfg.get("meta_path", "")),
                "challenger_ver": (cfg.get("model_ver", "")),
                "updated_ms": str(now_ms()),
            }
            bid, sig, bundle, _ops = make_bundle_hset(cfg_key, changes, who="ml_promotion_promote_challenger", ttl=ttl)
            write_bundle(r, bid, bundle, ttl)
            r.set(pending_key, json.dumps({"bundle_id": bid, "kind": "promote_challenger"}, separators=(",", ":")), ex=ttl)

            buttons = [[
                {"text": "👀 Preview diff", "callback": f"recs:preview2:{bid}:{sig}"},
                {"text": "✅ Confirm apply", "callback": f"recs:confirm:{bid}:{sig}"},
                {"text": "❌ Reject", "callback": f"recs:reject:{bid}:{sig}"},
            ]]
            txt = (
                "<b>ML Promotion Proposal: PROMOTE CHALLENGER</b>\n"
                f"active_ver=<code>{cfg.get('model_ver','na')}</code> chal_ver=<code>{cfg.get('challenger_ver','na')}</code>\n"
                f"promo_n=<code>{promo['n']}</code> brier_active=<code>{brier_a:.4f}</code> brier_ch=<code>{brier_c:.4f}</code>\n"
                f"improv=<code>{(brier_a-brier_c):.4f}</code> (min={min_brier_improv})\n"
                f"health=<code>{health}</code>"
            )
            notify(r, txt, buttons)
            return

    # Action 2: increase enforce_share if stable (no challenger or not better)
    if enforce_share < max_share:
        if float(promo["brier"]) <= brier_max and float(promo["win_rate"]) >= win_min:
            new_share = min(max_share, enforce_share + step)
            changes = {
                "enforce_share": f"{new_share:.4f}",
                "updated_ms": str(now_ms()),
            }
            bid, sig, bundle, _ops = make_bundle_hset(cfg_key, changes, who="ml_promotion_increase_share", ttl=ttl)
            write_bundle(r, bid, bundle, ttl)
            r.set(pending_key, json.dumps({"bundle_id": bid, "kind": "increase_share"}, separators=(",", ":")), ex=ttl)

            buttons = [[
                {"text": "👀 Preview diff", "callback": f"recs:preview2:{bid}:{sig}"},
                {"text": "✅ Confirm apply", "callback": f"recs:confirm:{bid}:{sig}"},
                {"text": "❌ Reject", "callback": f"recs:reject:{bid}:{sig}"},
            ]]
            txt = (
                "<b>ML Promotion Proposal: INCREASE ENFORCE SHARE</b>\n"
                f"share: <code>{enforce_share:.4f}</code> -> <code>{new_share:.4f}</code>\n"
                f"promo_n=<code>{promo['n']}</code> brier=<code>{promo['brier']:.4f}</code> win=<code>{promo['win_rate']:.3f}</code>\n"
                f"thresholds: brier_max=<code>{brier_max}</code> win_min=<code>{win_min}</code>\n"
                f"health=<code>{health}</code>"
            )
            notify(r, txt, buttons)
            return


if __name__ == "__main__":
    main()

