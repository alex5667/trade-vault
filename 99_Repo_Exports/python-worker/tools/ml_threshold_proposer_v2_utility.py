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
    return get_ny_time_millis()


def notify(r: redis.Redis, text: str, buttons=None) -> None:
    fields = {"type": "report", "text": text, "ts": str(now_ms())}
    if buttons is not None:
        fields["buttons"] = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM), fields, maxlen=200000, approximate=True)


def make_bundle_hset(cfg_key: str, changes: dict[str, str], who: str, ttl: int) -> tuple[str, str, dict[str, Any]]:
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    bid = secrets.token_hex(6)
    sig = hmac.new(secret.encode(), bid.encode(), hashlib.sha256).hexdigest()[:8]
    ts = now_ms()
    ops = [{"op": "HSET", "key": cfg_key, "field": k, "value": str(v)} for k, v in changes.items()]
    bundle = {"id": bid, "created_ms": ts, "ttl_sec": ttl, "who": who, "ops": ops, "meta": {"kind": "ml_threshold_proposal_v2_utility"}}
    return bid, sig, bundle


def write_bundle(r: redis.Redis, bid: str, bundle: dict[str, Any], ttl: int) -> None:
    r.set(f"recs:bundle:{bid}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl)
    r.set(f"recs:status:{bid}", "PENDING", ex=ttl)


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return d


def filter_rows(rows: list[dict[str, Any]], bucket: str, symbol: str) -> list[dict[str, Any]]:
    b = bucket.lower()
    s = symbol.upper()
    out = []
    for r in rows:
        if (r.get("bucket", "")).lower() != b:
            continue
        if (r.get("symbol", "")).upper() != s:
            continue
        out.append(r)
    return out


def ece(rows: list[dict[str, Any]], *, n_bins: int = 10, thr: float = 0.0) -> float:
    # ECE on selected set p_edge>=thr; rows must have p_edge and y
    bins_n = [0] * n_bins
    bins_p = [0.0] * n_bins
    bins_y = [0.0] * n_bins
    n = 0
    for r in rows:
        p = _f(r.get("p_edge", 0.0), 0.0)
        if p < thr:
            continue
        y = _i(r.get("y", 0), 0)
        bi = int(min(n_bins - 1, max(0, int(p * n_bins))))
        bins_n[bi] += 1
        bins_p[bi] += p
        bins_y[bi] += float(y)
        n += 1
    if n == 0:
        return 0.0
    e = 0.0
    for i in range(n_bins):
        if bins_n[i] == 0:
            continue
        avg_p = bins_p[i] / bins_n[i]
        avg_y = bins_y[i] / bins_n[i]
        e += (bins_n[i] / n) * abs(avg_p - avg_y)
    return float(e)


def brier(rows: list[dict[str, Any]], *, thr: float = 0.0) -> float:
    # Brier on selected set p_edge>=thr
    xs = []
    for r in rows:
        p = _f(r.get("p_edge", 0.0), 0.0)
        if p < thr:
            continue
        y = float(_i(r.get("y", 0), 0))
        xs.append((p - y) ** 2)
    return float(sum(xs) / len(xs)) if xs else 0.0


def selected_stats(rows: list[dict[str, Any]], *, thr: float) -> dict[str, Any]:
    # rows must have p_edge, r_mult, y
    sel = [r for r in rows if _f(r.get("p_edge", 0.0), 0.0) >= thr]
    n = len(sel)
    if n == 0:
        return {"n": 0}
    rm = [_f(r.get("r_mult", 0.0), 0.0) for r in sel]
    meanR = sum(rm) / n
    tail = sum(1 for x in rm if x <= -1.0) / n
    k = max(1, int(round(0.05 * n)))
    es05 = sum(sorted(rm)[:k]) / k
    win = sum(_i(r.get("y", 0), 0) for r in sel) / n
    return {"n": n, "meanR": float(meanR), "tail_rate": float(tail), "es05": float(es05), "win_rate": float(win)}


def impact(rows_confirm: list[dict[str, Any]], bucket: str, symbol: str, old_t: float, new_t: float) -> dict[str, int]:
    # expected extra blocks among enforce+ok_rule+not missing
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
    r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)
    cfg_key = os.getenv("ML_CONFIRM_CFG_KEY", "cfg:ml_confirm")
    cfg = r.hgetall(cfg_key) or {}
    ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)

    pending_key = os.getenv("ML_THRESH_PENDING_KEY", "meta:ml:thresh:pending")
    if r.get(pending_key):
        return

    # health gate: 30m + 2h (metrics:ml_confirm)
    ml_confirm_stream = os.getenv("ML_CONFIRM_METRICS_STREAM", "metrics:ml_confirm")
    max_scan_h = int(os.getenv("ML_PROMO_HEALTH_MAX_SCAN", "200000") or 200000)

    h30 = agg_health_ml_confirm(read_recent_stream(r, ml_confirm_stream, now_ms() - 30 * 60_000, max_scan_h))
    h120 = agg_health_ml_confirm(read_recent_stream(r, ml_confirm_stream, now_ms() - 120 * 60_000, max_scan_h))

    miss_max = float(os.getenv("ML_SRE_MISSING_RATE_MAX", "0.02") or 0.02)
    err_max = float(os.getenv("ML_SRE_ERR_RATE_MAX", "0.01") or 0.01)
    lat_max = float(os.getenv("ML_SRE_LAT_P99_MAX_MS", "6.0") or 6.0)

    def health_ok(h):
        return (h.get("n", 0) >= 200 and h["missing_rate"] <= miss_max and h["err_rate"] <= err_max and h["lat_p99_ms"] <= lat_max)

    if not (health_ok(h30) and health_ok(h120)):
        return

    # outcome windows
    out_stream = os.getenv("ML_OUTCOME_METRICS_STREAM", "metrics:ml_outcome")
    max_scan_o = int(os.getenv("ML_THRESH_MAX_SCAN_OUTCOME", "700000") or 700000)
    short_h = float(os.getenv("ML_THRESH_SHORT_HOURS", "24") or 24)
    long_h = float(os.getenv("ML_THRESH_LONG_HOURS", "168") or 168)

    rows_short_all = read_recent_stream(r, out_stream, now_ms() - int(short_h * 3600_000), max_scan_o)
    rows_long_all = read_recent_stream(r, out_stream, now_ms() - int(long_h * 3600_000), max_scan_o)

    # impact window from metrics:ml_confirm
    impact_h = float(os.getenv("ML_THRESH_IMPACT_HOURS", "24") or 24)
    rows_confirm = read_recent_stream(r, ml_confirm_stream, now_ms() - int(impact_h * 3600_000), max_scan_h)

    # constraints
    min_n_s = int(os.getenv("ML_THRESH_MIN_N_SHORT", "80") or 80)
    min_n_l = int(os.getenv("ML_THRESH_MIN_N_LONG", "300") or 300)
    max_syms = int(os.getenv("ML_THRESH_MAX_SYMBOLS_PER_RUN", "5") or 5)
    delta_min = float(os.getenv("ML_THRESH_DELTA_MIN", "0.02") or 0.02)

    tail_max = float(os.getenv("ML_THRESH_TAIL_MAX", "0.32") or 0.32)
    meanR_min = float(os.getenv("ML_THRESH_MEANR_MIN", "0.0") or 0.0)
    es05_min = float(os.getenv("ML_THRESH_ES05_MIN", "-0.85") or -0.85)

    ece_max = float(os.getenv("ML_THRESH_ECE_MAX", "0.08") or 0.08)
    brier_max = float(os.getenv("ML_THRESH_BRIER_MAX", "0.23") or 0.23)

    # drift guard: don't accept if calibration worsens too much vs old threshold (long window)
    max_ece_worsen = float(os.getenv("ML_THRESH_MAX_ECE_WORSEN", "0.01") or 0.01)
    max_brier_worsen = float(os.getenv("ML_THRESH_MAX_BRIER_WORSEN", "0.01") or 0.01)

    # current maps
    pmap_tr = parse_map(cfg.get("p_min_trend_by_symbol") or "")
    pmap_rg = parse_map(cfg.get("p_min_range_by_symbol") or "")

    p_bucket_tr = _f(cfg.get("p_min_trend", cfg.get("p_min_default", 0.55)), 0.55)
    p_bucket_rg = _f(cfg.get("p_min_range", cfg.get("p_min_default", 0.55)), 0.55)

    grid = [round(0.45 + 0.01 * i, 2) for i in range(36)]  # 0.45..0.80

    def symbols_for_bucket(bucket: str) -> list[str]:
        b = bucket.lower()
        return sorted({(r.get("symbol", "")).upper() for r in rows_long_all if (r.get("bucket", "")).lower() == b and (r.get("symbol", ""))})

    proposals: dict[str, dict[str, float]] = {"trend": {}, "range": {}}
    meta: dict[str, dict[str, Any]] = {"trend": {}, "range": {}}

    # propose one bucket per run to avoid spam
    for bucket in ("trend", "range"):
        picked = 0
        for sym in symbols_for_bucket(bucket):
            if picked >= max_syms:
                break

            rs = filter_rows(rows_short_all, bucket, sym)
            rl = filter_rows(rows_long_all, bucket, sym)
            if len(rs) < min_n_s or len(rl) < min_n_l:
                continue

            old = (pmap_tr.get(sym, p_bucket_tr) if bucket == "trend" else pmap_rg.get(sym, p_bucket_rg))

            new_t, s_stat, l_stat = pick_threshold(
                rs, rl, grid=grid,
                min_n_short=min_n_s, min_n_long=min_n_l,
                tail_max=tail_max, meanR_min=meanR_min, es05_min=es05_min,
            )
            if new_t <= 0.0:
                continue
            if abs(new_t - float(old)) < delta_min:
                continue

            # calibration checks on both windows for candidate threshold
            e_s = ece(rs, thr=new_t)
            e_l = ece(rl, thr=new_t)
            b_s = brier(rs, thr=new_t)
            b_l = brier(rl, thr=new_t)
            if e_s > ece_max or e_l > ece_max:
                continue
            if b_s > brier_max or b_l > brier_max:
                continue

            # drift guard vs old threshold (long window)
            e_old = ece(rl, thr=float(old))
            b_old = brier(rl, thr=float(old))
            if (e_l - e_old) > max_ece_worsen:
                continue
            if (b_l - b_old) > max_brier_worsen:
                continue

            imp = impact(rows_confirm, bucket, sym, float(old), float(new_t))

            proposals[bucket][sym] = float(new_t)
            meta[bucket][sym] = {
                "old": float(old), "new": float(new_t),
                "short": s_stat, "long": l_stat,
                "ece_short": float(e_s), "ece_long": float(e_l),
                "brier_short": float(b_s), "brier_long": float(b_l),
                "ece_old_long": float(e_old), "brier_old_long": float(b_old),
                "impact": imp,
            }
            picked += 1

        if proposals[bucket]:
            break

    if not (proposals["trend"] or proposals["range"]):
        return

    changes: dict[str, str] = {"updated_ms": str(now_ms())}
    lines: list[str] = []

    # store prev fields for rollback simplicity
    if "p_min_trend_by_symbol" in cfg:
        changes["p_min_trend_by_symbol_prev"] = cfg.get("p_min_trend_by_symbol", "") or ""
    else:
        changes["p_min_trend_by_symbol_prev"] = dump_map(pmap_tr)
    if "p_min_range_by_symbol" in cfg:
        changes["p_min_range_by_symbol_prev"] = cfg.get("p_min_range_by_symbol", "") or ""
    else:
        changes["p_min_range_by_symbol_prev"] = dump_map(pmap_rg)

    if proposals["trend"]:
        new_map = merge_updates(pmap_tr, proposals["trend"])
        changes["p_min_trend_by_symbol"] = dump_map(new_map)
        lines.append("<b>V7 p_min trend proposals (utility + calib + drift-guard)</b>")
        for sym, _ in proposals["trend"].items():
            m = meta["trend"][sym]
            imp = m["impact"]
            lines.append(
                f"{sym}: <code>{m['old']:.2f}</code>→<code>{m['new']:.2f}</code> | "
                f"short n={m['short']['n']} meanR={m['short']['meanR']:.3f} tail={m['short']['tail_rate']:.3f} es05={m['short']['es05']:.3f} "
                f"ece={m['ece_short']:.3f} brier={m['brier_short']:.3f} | "
                f"long n={m['long']['n']} meanR={m['long']['meanR']:.3f} tail={m['long']['tail_rate']:.3f} es05={m['long']['es05']:.3f} "
                f"ece={m['ece_long']:.3f} (old {m['ece_old_long']:.3f}) brier={m['brier_long']:.3f} (old {m['brier_old_long']:.3f}) | "
                f"Δblock={imp['delta_block']}/{imp['total']}"
            )

    if proposals["range"]:
        new_map = merge_updates(pmap_rg, proposals["range"])
        changes["p_min_range_by_symbol"] = dump_map(new_map)
        lines.append("<b>V7 p_min range proposals (utility + calib + drift-guard)</b>")
        for sym, _ in proposals["range"].items():
            m = meta["range"][sym]
            imp = m["impact"]
            lines.append(
                f"{sym}: <code>{m['old']:.2f}</code>→<code>{m['new']:.2f}</code> | "
                f"short n={m['short']['n']} meanR={m['short']['meanR']:.3f} tail={m['short']['tail_rate']:.3f} es05={m['short']['es05']:.3f} "
                f"ece={m['ece_short']:.3f} brier={m['brier_short']:.3f} | "
                f"long n={m['long']['n']} meanR={m['long']['meanR']:.3f} tail={m['long']['tail_rate']:.3f} es05={m['long']['es05']:.3f} "
                f"ece={m['ece_long']:.3f} (old {m['ece_old_long']:.3f}) brier={m['brier_long']:.3f} (old {m['brier_old_long']:.3f}) | "
                f"Δblock={imp['delta_block']}/{imp['total']}"
            )

    bid, sig, bundle = make_bundle_hset(cfg_key, changes, who="ml_threshold_proposer_v2_utility", ttl=ttl)
    write_bundle(r, bid, bundle, ttl)
    r.set(pending_key, json.dumps({"bundle_id": bid, "kind": "pmin_proposal_v7"}, separators=(",", ":")), ex=ttl)

    buttons = [[
        {"text": "👀 Preview diff", "callback": f"recs:preview2:{bid}:{sig}"},
        {"text": "✅ Confirm apply", "callback": f"recs:confirm:{bid}:{sig}"},
        {"text": "❌ Reject", "callback": f"recs:reject:{bid}:{sig}"},
    ]]

    header = "<b>ML Threshold Proposal v7 (utility + drift guard)</b>"
    health = f"health30=<code>{h30}</code>\nhealth120=<code>{h120}</code>"
    notify(r, header + "\n" + health + "\n" + "\n".join(lines), buttons)


if __name__ == "__main__":
    main()

