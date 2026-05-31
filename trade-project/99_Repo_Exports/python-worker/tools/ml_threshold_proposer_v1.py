from __future__ import annotations

import hashlib
import hmac
import json
import os
import secrets
from typing import Any

import redis

from core.share_map import dump_map, merge_updates, parse_map
from tools.ml_metrics_agg_v3 import agg_health_ml_confirm, pick_threshold
from tools.redis_window import read_recent_stream
from utils.time_utils import get_ny_time_millis
from core.redis_keys import RedisStreams as RS


def now_ms() -> int:
    """Get current timestamp in milliseconds."""
    return get_ny_time_millis()


def sign(bundle_id: str, secret: str) -> str:
    """Generate HMAC signature for bundle ID."""
    return hmac.new(secret.encode(), bundle_id.encode(), hashlib.sha256).hexdigest()[:8]


def notify(r: redis.Redis, text: str, buttons=None) -> None:
    """Send notification to Telegram stream with optional inline buttons.
    
    Args:
        r: Redis client
        text: Message text (HTML supported)
        buttons: Optional list of button rows (each row is list of button dicts)
    """
    fields = {"type": "report", "text": text, "ts": str(now_ms())}
    if buttons is not None:
        fields["buttons"] = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM), fields, maxlen=200000, approximate=True)


def make_bundle_hset(cfg_key: str, changes: dict[str, str], who: str, ttl: int):
    """Create recommendation bundle for HSET operations.
    
    Args:
        cfg_key: Redis hash key to modify (e.g., "cfg:ml_confirm")
        changes: Dict of field->value changes
        who: Creator identifier
        ttl: TTL in seconds
        
    Returns:
        Tuple of (bundle_id, signature, bundle_dict)
    """
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    bid = secrets.token_hex(6)
    sig = sign(bid, secret)
    ts = now_ms()
    ops = [{"op": "HSET", "key": cfg_key, "field": k, "value": str(v)} for k, v in changes.items()]
    bundle = {
        "id": bid,
        "created_ms": ts,
        "ttl_sec": ttl,
        "who": who,
        "ops": ops,
        "meta": {"kind": "ml_threshold_proposal_v1"},
    }
    return bid, sig, bundle


def write_bundle(r: redis.Redis, bid: str, bundle: dict[str, Any], ttl: int) -> None:
    """Write bundle and status to Redis.
    
    Args:
        r: Redis client
        bid: Bundle ID
        bundle: Bundle dict
        ttl: TTL in seconds
    """
    r.set(f"recs:bundle:{bid}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl)
    r.set(f"recs:status:{bid}", "PENDING", ex=ttl)


def _f(x: Any, d: float = 0.0) -> float:
    """Convert to float with default."""
    try:
        return float(x)
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    """Convert to int with default."""
    try:
        return int(float(x))
    except Exception:
        return d


def filter_rows(rows: list[dict[str, Any]], bucket: str, symbol: str) -> list[dict[str, Any]]:
    """Filter rows by bucket and symbol.
    
    Args:
        rows: List of message field dicts
        bucket: Bucket name (trend/range)
        symbol: Symbol name (uppercase)
        
    Returns:
        Filtered list
    """
    b = bucket.lower()
    s = symbol.upper()
    return [r for r in rows if (r.get("bucket", "")).lower() == b and (r.get("symbol", "")).upper() == s]


def impact(rows_confirm: list[dict[str, Any]], bucket: str, symbol: str, old_t: float, new_t: float) -> dict[str, int]:
    """Estimate additional blocks from threshold change.
    
    Counts rows in metrics:ml_confirm where enforce=1, ok_rule=1, missing=0,
    and computes how many would be blocked with old_t vs new_t.
    
    Args:
        rows_confirm: List of message field dicts from metrics:ml_confirm
        bucket: Bucket name
        symbol: Symbol name
        old_t: Old threshold
        new_t: New threshold
        
    Returns:
        Dict with total, blocked_old, blocked_new, delta_block
    """
    b = bucket.lower()
    s = symbol.upper()
    total = 0
    blocked_old = 0
    blocked_new = 0
    for r in rows_confirm:
        if (r.get("bucket", "")).lower() != b:
            continue
        if (r.get("symbol", "")).upper() != s:
            continue
        if _i(r.get("enforce", 0), 0) != 1:
            continue
        if _i(r.get("ok_rule", 0), 0) != 1:
            continue
        if _i(r.get("missing", 0), 0) == 1:
            continue
        p = _f(r.get("p_edge", 0.0), 0.0)
        total += 1
        if p < old_t:
            blocked_old += 1
        if p < new_t:
            blocked_new += 1
    return {"total": total, "blocked_old": blocked_old, "blocked_new": blocked_new, "delta_block": blocked_new - blocked_old}


def main() -> None:
    """Main entry point for ML threshold proposer.
    
    Reads metrics:ml_outcome and metrics:ml_confirm, proposes p_min updates per symbol,
    and sends Telegram notification with approve/reject buttons.
    """
    r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)
    cfg_key = os.getenv("ML_CONFIRM_CFG_KEY", "cfg:ml_confirm")
    cfg = r.hgetall(cfg_key) or {}
    ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)

    pending_key = os.getenv("ML_THRESH_PENDING_KEY", "meta:ml:thresh:pending")
    if r.get(pending_key):
        return

    # dual health gate (30m & 2h)
    ml_confirm_stream = os.getenv("ML_CONFIRM_METRICS_STREAM", RS.ML_CONFIRM_METRICS)
    max_scan_h = int(os.getenv("ML_PROMO_HEALTH_MAX_SCAN", "200000") or 200000)

    h30 = agg_health_ml_confirm(read_recent_stream(r, ml_confirm_stream, now_ms() - 30 * 60_000, max_scan_h))
    h120 = agg_health_ml_confirm(read_recent_stream(r, ml_confirm_stream, now_ms() - 120 * 60_000, max_scan_h))

    miss_max = float(os.getenv("ML_SRE_MISSING_RATE_MAX", "0.02") or 0.02)
    err_max = float(os.getenv("ML_SRE_ERR_RATE_MAX", "0.01") or 0.01)
    lat_max = float(os.getenv("ML_SRE_LAT_P99_MAX_MS", "6.0") or 6.0)

    def health_ok(h):
        return (
            h.get("n", 0) >= 200
            and h["missing_rate"] <= miss_max
            and h["err_rate"] <= err_max
            and h["lat_p99_ms"] <= lat_max
        )

    if not (health_ok(h30) and health_ok(h120)):
        return

    # outcome windows
    out_stream = os.getenv("ML_OUTCOME_METRICS_STREAM", RS.ML_OUTCOME_METRICS)
    max_scan_o = int(os.getenv("ML_THRESH_MAX_SCAN_OUTCOME", "700000") or 700000)
    short_h = float(os.getenv("ML_THRESH_SHORT_HOURS", "24") or 24)
    long_h = float(os.getenv("ML_THRESH_LONG_HOURS", "168") or 168)

    rows_short = read_recent_stream(r, out_stream, now_ms() - int(short_h * 3600_000), max_scan_o)
    rows_long = read_recent_stream(r, out_stream, now_ms() - int(long_h * 3600_000), max_scan_o)

    # impact window on ml_confirm
    impact_h = float(os.getenv("ML_THRESH_IMPACT_HOURS", "24") or 24)
    rows_confirm_impact = read_recent_stream(r, ml_confirm_stream, now_ms() - int(impact_h * 3600_000), max_scan_h)

    # thresholds / constraints
    min_n_s = int(os.getenv("ML_THRESH_MIN_N_SHORT", "80") or 80)
    min_n_l = int(os.getenv("ML_THRESH_MIN_N_LONG", "300") or 300)
    max_syms = int(os.getenv("ML_THRESH_MAX_SYMBOLS_PER_RUN", "5") or 5)
    delta_min = float(os.getenv("ML_THRESH_DELTA_MIN", "0.02") or 0.02)

    tail_max = float(os.getenv("ML_THRESH_TAIL_MAX", "0.32") or 0.32)
    meanR_min = float(os.getenv("ML_THRESH_MEANR_MIN", "0.0") or 0.0)
    es05_min = float(os.getenv("ML_THRESH_ES05_MIN", "-0.85") or -0.85)

    # optional range exec veto
    exec_p90_30m_max = float(os.getenv("ML_RANGE_EXEC_P90_MAX_30M", "0.90") or 0.90)
    exec_p90_2h_max = float(os.getenv("ML_RANGE_EXEC_P90_MAX_2H", "0.85") or 0.85)
    exec_min_n_30m = int(os.getenv("ML_RANGE_EXEC_MIN_N_30M", "50") or 50)
    exec_min_n_2h = int(os.getenv("ML_RANGE_EXEC_MIN_N_2H", "120") or 120)

    # current maps
    pmap_tr = parse_map(cfg.get("p_min_trend_by_symbol") or "")
    pmap_rg = parse_map(cfg.get("p_min_range_by_symbol") or "")

    # default bucket pmins
    p_bucket_tr = _f(cfg.get("p_min_trend", cfg.get("p_min_default", 0.55)), 0.55)
    p_bucket_rg = _f(cfg.get("p_min_range", cfg.get("p_min_default", 0.55)), 0.55)

    # candidate symbols from long window outcomes
    def syms_for_bucket(bucket: str) -> list[str]:
        """Get unique symbols for bucket from long window."""
        b = bucket.lower()
        return sorted({(r.get("symbol", "")).upper() for r in rows_long if (r.get("bucket", "")).lower() == b and (r.get("symbol", ""))})

    grid = [round(x, 2) for x in [0.45 + 0.01 * i for i in range(31)]]  # 0.45..0.75

    proposals = {"trend": {}, "range": {}}
    impacts = {"trend": {}, "range": {}}
    stats = {"trend": {}, "range": {}}

    for bucket in ("trend", "range"):
        syms = syms_for_bucket(bucket)
        if not syms:
            continue

        picked = 0
        for sym in syms:
            if picked >= max_syms:
                break

            rs = filter_rows(rows_short, bucket, sym)
            rl = filter_rows(rows_long, bucket, sym)
            if len(rs) < min_n_s or len(rl) < min_n_l:
                continue

            # optional exec veto for range: require low exec_risk_norm in 30m & 2h windows
            if bucket == "range":
                w30 = [r for r in rows_confirm_impact if (r.get("bucket", "")).lower() == "range" and (r.get("symbol", "")).upper() == sym and "exec_risk_norm" in r]
                # for 2h use separate read for strict window (avoid bias)
                w30 = [r for r in w30 if (now_ms() - _i(r.get("ts_ms", 0), 0)) <= 30 * 60_000]
                w120 = [r for r in rows_confirm_impact if (r.get("bucket", "")).lower() == "range" and (r.get("symbol", "")).upper() == sym and "exec_risk_norm" in r]
                w120 = [r for r in w120 if (now_ms() - _i(r.get("ts_ms", 0), 0)) <= 120 * 60_000]
                # compute p90 quickly
                ex30 = sorted([_f(x.get("exec_risk_norm", 0.0), 0.0) for x in w30])
                ex120 = sorted([_f(x.get("exec_risk_norm", 0.0), 0.0) for x in w120])

                def p90(xs):
                    if not xs:
                        return 0.0
                    return xs[int(round((len(xs) - 1) * 0.90))]

                if len(ex30) < exec_min_n_30m or len(ex120) < exec_min_n_2h:
                    continue
                if p90(ex30) > exec_p90_30m_max or p90(ex120) > exec_p90_2h_max:
                    continue

            old = None
            if bucket == "trend":
                old = pmap_tr.get(sym, p_bucket_tr)
            else:
                old = pmap_rg.get(sym, p_bucket_rg)

            new_t, s_stat, l_stat = pick_threshold(
                rs, rl, grid=grid, min_n_short=min_n_s, min_n_long=min_n_l, tail_max=tail_max, meanR_min=meanR_min, es05_min=es05_min
            )
            if new_t <= 0.0:
                continue

            if abs(float(new_t) - float(old)) < delta_min:
                continue

            proposals[bucket][sym] = float(new_t)
            stats[bucket][sym] = {"old": float(old), "new": float(new_t), "short": s_stat, "long": l_stat}
            impacts[bucket][sym] = impact(rows_confirm_impact, bucket, sym, float(old), float(new_t))
            picked += 1

        if proposals[bucket]:
            break  # one bucket per run (reduce spam)

    if not (proposals["trend"] or proposals["range"]):
        return

    # Build changes: update JSON map field(s)
    changes = {"updated_ms": str(now_ms())}
    text_lines = []
    who = "ml_threshold_proposer_v1"

    if proposals["trend"]:
        new_map = merge_updates(pmap_tr, proposals["trend"])
        changes["p_min_trend_by_symbol"] = dump_map(new_map)
        text_lines.append("<b>p_min_trend_by_symbol proposals</b>")
        for sym, val in proposals["trend"].items():
            st = stats["trend"][sym]
            imp = impacts["trend"][sym]
            text_lines.append(
                f"{sym}: <code>{st['old']:.2f}</code> -> <code>{st['new']:.2f}</code> | "
                f"short n={st['short']['n']} meanR={st['short']['meanR']:.3f} tail={st['short']['tail_rate']:.3f} | "
                f"long n={st['long']['n']} meanR={st['long']['meanR']:.3f} tail={st['long']['tail_rate']:.3f} | "
                f"impact Δblock={imp['delta_block']} / total={imp['total']}"
            )

    if proposals["range"]:
        new_map = merge_updates(pmap_rg, proposals["range"])
        changes["p_min_range_by_symbol"] = dump_map(new_map)
        text_lines.append("<b>p_min_range_by_symbol proposals</b>")
        for sym, val in proposals["range"].items():
            st = stats["range"][sym]
            imp = impacts["range"][sym]
            text_lines.append(
                f"{sym}: <code>{st['old']:.2f}</code> -> <code>{st['new']:.2f}</code> | "
                f"short n={st['short']['n']} meanR={st['short']['meanR']:.3f} tail={st['short']['tail_rate']:.3f} | "
                f"long n={st['long']['n']} meanR={st['long']['meanR']:.3f} tail={st['long']['tail_rate']:.3f} | "
                f"impact Δblock={imp['delta_block']} / total={imp['total']}"
            )

    bid, sig, bundle = make_bundle_hset(cfg_key, changes, who=who, ttl=ttl)
    write_bundle(r, bid, bundle, ttl)
    r.set(pending_key, json.dumps({"bundle_id": bid, "kind": "pmin_proposal"}, separators=(",", ":")), ex=ttl)

    buttons = [[
        {"text": "👀 Preview diff", "callback": f"recs:preview2:{bid}:{sig}"},
        {"text": "✅ Confirm apply", "callback": f"recs:confirm:{bid}:{sig}"},
        {"text": "❌ Reject", "callback": f"recs:reject:{bid}:{sig}"},
    ]]

    header = "<b>ML Threshold Proposal v6 (p_min by symbol)</b>"
    health = f"health30=<code>{h30}</code>\\nhealth120=<code>{h120}</code>"
    body = "\\n".join(text_lines)
    notify(r, header + "\\n" + health + "\\n" + body, buttons)


if __name__ == "__main__":
    main()

