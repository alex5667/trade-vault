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
from tools.ml_metrics_agg_v3 import agg_health_ml_confirm, agg_selected
from core.share_map import parse_map, dump_map
from common.redis_errors import retry_redis_operation


def now_ms() -> int:
    return get_ny_time_millis()


def notify(r: redis.Redis, text: str, buttons=None) -> None:
    fields = {"type": "report", "text": text, "ts": str(now_ms())}
    if buttons is not None:
        fields["buttons"] = json.dumps(buttons, ensure_ascii=False, separators=(",", ":"))
    retry_redis_operation(
        lambda: r.xadd(os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram"), fields, maxlen=200000, approximate=True),
        operation_name="notify xadd",
    )


def make_bundle_hset(cfg_key: str, changes: Dict[str, str], who: str, ttl: int):
    secret = os.getenv("RECS_HMAC_SECRET", "CHANGE_ME")
    bid = secrets.token_hex(6)
    sig = hmac.new(secret.encode(), bid.encode(), hashlib.sha256).hexdigest()[:8]
    ts = now_ms()
    ops = [{"op": "HSET", "key": cfg_key, "field": k, "value": str(v)} for k, v in changes.items()]
    bundle = {"id": bid, "created_ms": ts, "ttl_sec": ttl, "who": who, "ops": ops, "meta": {"kind": "ml_rollback_proposal_v1"}}
    return bid, sig, bundle


def write_bundle(r: redis.Redis, bid: str, bundle: Dict[str, Any], ttl: int) -> None:
    retry_redis_operation(
        lambda: r.set(f"recs:bundle:{bid}", json.dumps(bundle, ensure_ascii=False, separators=(",", ":")), ex=ttl),
        operation_name="write_bundle set",
    )
    retry_redis_operation(
        lambda: r.set(f"recs:status:{bid}", "PENDING", ex=ttl),
        operation_name="write_bundle status",
    )


def _f(x: Any, d: float = 0.0) -> float:
    try:
        return float(x)
    except Exception:
        return d


def filter_rows(rows: List[Dict[str, Any]], bucket: str) -> List[Dict[str, Any]]:
    b = bucket.lower()
    return [r for r in rows if str(r.get("bucket", "")).lower() == b]


def main() -> None:
    r = redis.Redis.from_url(os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"), decode_responses=True)
    cfg_key = os.getenv("ML_CONFIRM_CFG_KEY", "cfg:ml_confirm")
    cfg = retry_redis_operation(
        lambda: r.hgetall(cfg_key) or {},
        operation_name="hgetall cfg",
    )
    ttl = int(os.getenv("RECS_TTL_SEC", "86400") or 86400)

    pending_key = os.getenv("ML_ROLLBACK_PENDING_KEY", "meta:ml:rollback:pending")
    if retry_redis_operation(
        lambda: r.get(pending_key),
        operation_name="get pending_key",
    ):
        return

    # health gate
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

    # outcome short window
    out_stream = os.getenv("ML_OUTCOME_METRICS_STREAM", "metrics:ml_outcome")
    max_scan_o = int(os.getenv("ML_THRESH_MAX_SCAN_OUTCOME", "700000") or 700000)
    short_h = float(os.getenv("ML_ROLLBACK_SHORT_HOURS", "24") or 24)
    rows_short_all = read_recent_stream(r, out_stream, now_ms() - int(short_h * 3600_000), max_scan_o)

    # rollback criteria (short window, selected set under current thresholds must not be "bad")
    tail_max = float(os.getenv("ML_ROLLBACK_TAIL_MAX", "0.40") or 0.40)
    meanR_min = float(os.getenv("ML_ROLLBACK_MEANR_MIN", "-0.02") or -0.02)
    es05_min = float(os.getenv("ML_ROLLBACK_ES05_MIN", "-1.00") or -1.00)
    min_n = int(os.getenv("ML_ROLLBACK_MIN_N", "150") or 150)

    # current maps and prev maps (stored by v7 proposer)
    cur_tr = cfg.get("p_min_trend_by_symbol", "") or "{}"
    cur_rg = cfg.get("p_min_range_by_symbol", "") or "{}"
    prev_tr = cfg.get("p_min_trend_by_symbol_prev", "") or ""
    prev_rg = cfg.get("p_min_range_by_symbol_prev", "") or ""

    if not prev_tr and not prev_rg:
        return

    # Evaluate "global" selected quality under current bucket thresholds (approx):
    # For rollback trigger we check bucket aggregates using bucket p_min (not per-symbol)
    p_bucket_tr = _f(cfg.get("p_min_trend", cfg.get("p_min_default", 0.55)), 0.55)
    p_bucket_rg = _f(cfg.get("p_min_range", cfg.get("p_min_default", 0.55)), 0.55)

    trend_rows = filter_rows(rows_short_all, "trend")
    range_rows = filter_rows(rows_short_all, "range")

    st_tr = agg_selected(trend_rows, p_bucket_tr)
    st_rg = agg_selected(range_rows, p_bucket_rg)

    def bad(st: Dict[str, Any]) -> bool:
        if st.get("n", 0) < min_n:
            return False
        if float(st.get("tail_rate", 0.0)) > tail_max:
            return True
        if float(st.get("meanR", 0.0)) < meanR_min:
            return True
        if float(st.get("es05", 0.0)) < es05_min:
            return True
        return False

    need_rb = bad(st_tr) or bad(st_rg)
    if not need_rb:
        return

    changes = {"updated_ms": str(now_ms())}
    lines = ["<b>ML Rollback proposal</b>", f"health30=<code>{h30}</code>", f"health120=<code>{h120}</code>"]
    lines.append(f"trend short selected: <code>{st_tr}</code>")
    lines.append(f"range short selected: <code>{st_rg}</code>")

    if prev_tr:
        changes["p_min_trend_by_symbol"] = prev_tr
        lines.append("rollback trend map -> *_prev")
    if prev_rg:
        changes["p_min_range_by_symbol"] = prev_rg
        lines.append("rollback range map -> *_prev")

    bid, sig, bundle = make_bundle_hset(cfg_key, changes, who="ml_rollback_proposer_v1", ttl=ttl)
    write_bundle(r, bid, bundle, ttl)
    retry_redis_operation(
        lambda: r.set(pending_key, json.dumps({"bundle_id": bid, "kind": "rollback"}, separators=(",", ":")), ex=ttl),
        operation_name="set pending_key",
    )

    buttons = [[
        {"text": "👀 Preview diff", "callback": f"recs:preview2:{bid}:{sig}"},
        {"text": "✅ Confirm apply", "callback": f"recs:confirm:{bid}:{sig}"},
        {"text": "❌ Reject", "callback": f"recs:reject:{bid}:{sig}"},
    ]]
    notify(r, "\n".join(lines), buttons)


if __name__ == "__main__":
    main()

