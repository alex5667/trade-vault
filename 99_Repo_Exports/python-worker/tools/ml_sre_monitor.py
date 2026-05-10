from __future__ import annotations
from core.redis_keys import RedisStreams as RS

"""
SRE мониторинг ML метрик (metrics:ml_confirm) и Triple Barrier Labeler (tb:last_ts_ms).

Пороговые алерты:
- p_edge_p50 < ML_SRE_PEDGE_P50_MIN
- missing_rate > ML_SRE_MISSING_RATE_MAX
- err_rate > ML_SRE_ERR_RATE_MAX
- lat_p99_ms > ML_SRE_LAT_P99_MAX_MS
- stream_stale_ms > ML_SRE_MAX_STALE_MS
- p_edge_zero_rate > ML_SRE_PEDGE_ZERO_RATE_MAX
- required_missing_rate > ML_SRE_REQUIRED_MISS_RATE_MAX
- TB: input_lag, label_stale, pending, group_lag
"""

import argparse
import html
import json
import os
from typing import Any

import redis

from common.redis_errors import retry_redis_operation
from tools.cfg_suggestions_lifecycle import check_suggestions_health
from utils.time_utils import get_ny_time_millis
import contextlib


def now_ms() -> int:
    return get_ny_time_millis()


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


def pctl(xs: list[float], q: float) -> float:
    if not xs:
        return 0.0
    xs = sorted(xs)
    i = int(round((len(xs) - 1) * q))
    i = max(0, min(len(xs) - 1, i))
    return float(xs[i])


def _read_stream_window(r, stream: str, start_ms: int, window_ms: int, *, max_scan: int = 200000) -> list[dict[str, Any]]:
    """Read stream items in [start_ms, start_ms+window_ms] by ts_ms field, return chronological with _ts_ms."""
    end_ms = start_ms + window_ms
    rows: list[dict[str, Any]] = []
    last_id = "+"
    scanned = 0
    while scanned < max_scan:
        try:
            batch = retry_redis_operation(
                operation=lambda: r.xrevrange(stream, max=last_id, min="-", count=2000),
                operation_name="xrevrange",
                max_retries=10,
                base_delay=1.0,
                max_delay=30.0,
                on_final_failure=lambda e: [],
            )
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
            if ts <= end_ms:
                d["_ts_ms"] = ts
                rows.append(d)
        if len(batch) < 2000:
            break
    rows.sort(key=lambda x: int(x.get("_ts_ms", 0)))
    return rows


def _notify(r, text: str) -> None:
    stream = os.getenv("NOTIFY_TELEGRAM_STREAM", RS.NOTIFY_TELEGRAM)
    lock_key = "sre:alert:lock:ml_confirm:_notify"
    # Deduplicate: only one process sends per 60s
    if not r.set(lock_key, "1", nx=True, ex=60):
        return

    with contextlib.suppress(Exception):
        retry_redis_operation(
            operation=lambda: r.xadd(stream, {"type": "report", "text": text, "ts": str(now_ms())}, maxlen=200000, approximate=True),
            operation_name="xadd_notify",
            max_retries=5,
            base_delay=1.0,
            max_delay=10.0,
            on_final_failure=lambda e: None,
        )


def _tb_health(
    r: redis.Redis,
    *,
    input_stream: str,
    labels_stream: str,
    group: str,
    max_input_lag_ms: int,
    max_label_stale_ms: int,
    max_pending: int,
) -> tuple[dict[str, Any], list[str]]:
    now = now_ms()
    alerts: list[str] = []

    last_ts_ms = _i(r.get("tb:last_ts_ms"), 0)
    last_label_ts_ms = _i(r.get("tb:last_label_ts_ms"), 0)
    last_err_ts_ms = _i(r.get("tb:last_err_ts_ms"), 0)

    input_lag_ms = (now - last_ts_ms) if last_ts_ms else 0
    label_stale_ms = (now - last_label_ts_ms) if last_label_ts_ms else 0
    err_age_ms = (now - last_err_ts_ms) if last_err_ts_ms else 0

    if not last_label_ts_ms:
        try:
            tail = r.xrevrange(labels_stream, max="+", min="-", count=1)
            if tail:
                _, fields = tail[0]
                last_label_ts_ms = _i(fields.get("ts_ms", fields.get("ts", 0)), 0)
                if last_label_ts_ms:
                    label_stale_ms = now - last_label_ts_ms
        except Exception:
            pass

    pending = 0
    group_lag_ms = 0
    if group:
        try:
            info = r.xpending(input_stream, group)
            pending = _i(info.get("pending", 0), 0) if isinstance(info, dict) else 0
        except Exception:
            try:
                groups = r.xinfo_groups(input_stream)
                for g in groups or []:
                    if (g.get("name", "")) == group:
                        pending = _i(g.get("pending", 0), 0)
                        break
            except Exception:
                pending = 0

        if max_pending > 0 and pending > max_pending:
            alerts.append(f"tb_pending>{max_pending}")

        try:
            groups = r.xinfo_groups(input_stream)
            last_delivered = None
            for g in groups or []:
                if (g.get("name", "")) == group:
                    last_delivered = (g.get("last-delivered-id", "") or "")
                    break
            stream_info = r.xinfo_stream(input_stream)
            last_id = (stream_info.get("last-generated-id", "") or "")
            if last_id and last_delivered and last_delivered != "0-0":
                try:
                    last_ms_val = int(last_id.split("-")[0])
                    delivered_ms = int(last_delivered.split("-")[0])
                    group_lag_ms = max(0, last_ms_val - delivered_ms)
                except Exception:
                    group_lag_ms = 0
        except Exception:
            group_lag_ms = 0

    if max_label_stale_ms > 0 and last_label_ts_ms and label_stale_ms > max_label_stale_ms:
        if pending > 0 or group_lag_ms > 5000 or label_stale_ms > (input_lag_ms + 300000):
            alerts.append(f"tb_label_stale_ms>{max_label_stale_ms}")

    if max_input_lag_ms > 0 and last_ts_ms:
        if input_lag_ms > max_input_lag_ms and (group_lag_ms > 5000 or pending > 0):
            alerts.append(f"tb_input_lag_ms>{max_input_lag_ms}")

    out = {
        "input_stream": input_stream,
        "labels_stream": labels_stream,
        "group": group,
        "last_ts_ms": last_ts_ms,
        "last_label_ts_ms": last_label_ts_ms,
        "last_err_ts_ms": last_err_ts_ms,
        "input_lag_ms": input_lag_ms,
        "label_stale_ms": label_stale_ms,
        "err_age_ms": err_age_ms,
        "pending": pending,
        "group_lag_ms": group_lag_ms,
    }
    return out, alerts


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--window-min", type=float, default=float(os.getenv("ML_SRE_WINDOW_MIN", "10") or 10))
    ap.add_argument("--dry-run", action="store_true")
    ap.add_argument("--emit-metrics", action="store_true", help="Emit Prometheus metrics (ignored for now)")
    ap.add_argument("--notify", action="store_true", help="Enable Telegram notifications")
    args = ap.parse_args()

    redis_url = os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0")
    stream = os.getenv("ML_CONFIRM_METRICS_STREAM", RS.ML_CONFIRM_METRICS)

    window_ms = int(args.window_min * 60_000)
    end_ms = now_ms()
    start_ms = end_ms - window_ms

    r = redis.Redis.from_url(redis_url, decode_responses=True)

    # Wrap critical initial reads in retry_redis_operation
    def _safe_get_thresholds():
        return {
            "p50_min": float(os.getenv("ML_SRE_PEDGE_P50_MIN", "0.20") or 0.20),
            "miss_max": float(os.getenv("ML_SRE_MISSING_RATE_MAX", "0.02") or 0.02),
            "err_max": float(os.getenv("ML_SRE_ERR_RATE_MAX", "0.01") or 0.01),
            "lat_p99_max": float(os.getenv("ML_SRE_LAT_P99_MAX_MS", "8.0") or 8.0),
            "p0_rate_max": float(os.getenv("ML_SRE_PEDGE_ZERO_RATE_MAX", "0.05") or 0.05),
            "req_miss_rate_max": float(os.getenv("ML_SRE_REQUIRED_MISS_RATE_MAX", "0.01") or 0.01),
            "max_stale_ms": int(float(os.getenv("ML_SRE_MAX_STALE_MS", str(window_ms)) or window_ms)),
        }

    # Verify Redis is UP and responsive before proceeding
    retry_redis_operation(
        operation=lambda: r.ping(),
        operation_name="ping_redis",
        max_retries=10
    )

    # thresholds
    pedge_p50_min = float(os.getenv("ML_SRE_PEDGE_P50_MIN", "0.20") or 0.20)
    miss_max = float(os.getenv("ML_SRE_MISSING_RATE_MAX", "0.02") or 0.02)
    err_max = float(os.getenv("ML_SRE_ERR_RATE_MAX", "0.01") or 0.01)
    lat_p99_max = float(os.getenv("ML_SRE_LAT_P99_MAX_MS", "8.0") or 8.0)
    p0_rate_max = float(os.getenv("ML_SRE_PEDGE_ZERO_RATE_MAX", "0.05") or 0.05)
    req_miss_rate_max = float(os.getenv("ML_SRE_REQUIRED_MISS_RATE_MAX", "0.01") or 0.01)
    max_stale_ms = int(float(os.getenv("ML_SRE_MAX_STALE_MS", str(window_ms)) or window_ms))

    # meta-model training health (optional)
    meta_enable = os.getenv("META_SRE_ENABLE", "1").lower() not in ("0", "false", "no")
    meta_alerts: list[str] = []
    meta_status = ""
    meta_train_stale_ms = None
    if meta_enable:
        try:
            meta_status = retry_redis_operation(
                operation=lambda: (r.get("meta_model:last_status") or ""),
                operation_name="get_meta_status"
            )
            meta_train_ts = retry_redis_operation(
                operation=lambda: _i(r.get("meta_model:last_train_ts_ms"), 0),
                operation_name="get_meta_train_ts"
            )
            if meta_train_ts:
                meta_train_stale_ms = now_ms() - meta_train_ts
                max_train_stale = _i(os.getenv("META_SRE_MAX_TRAIN_STALE_MS", "21600000"), 21600000)  # 6h
                if meta_train_stale_ms > max_train_stale:
                    meta_alerts.append("meta:meta_train_stale")
            if meta_status.startswith("err:"):
                meta_alerts.append("train_error")
            if meta_status.startswith("fail:"):
                meta_alerts.append("train_gate_fail")
        except Exception:
            # Non-critical, but log it if needed
            pass

    # meta-model A/B + drift health (optional)
    try:
        meta_ab_enable = int(os.getenv("META_AB_SRE_ENABLE", "1") or 1)
    except Exception:
        meta_ab_enable = 1
    if meta_ab_enable:
        try:
            # A/B health
            rep_ab = r.get("meta_ab:last_report") or ""
            rep_ab_ts = _i(r.get("meta_ab:last_ts_ms"), 0)
            max_ab_stale = _i(os.getenv("META_AB_MAX_STALE_MS", str(window_ms)), window_ms)
            if rep_ab_ts <= 0 or (now_ms() - rep_ab_ts) > max_ab_stale:
                meta_alerts.append("ab_stale")
            else:
                rep_ab_j = json.loads(rep_ab) if rep_ab else {}
                win = (rep_ab_j.get("winner", "") or "")
                if win == "challenger" and _f(rep_ab_j.get("delta_mean_r", 0.0)) > _f(os.getenv("META_AB_WIN_DELTA_R_MIN", "0.05")):
                    meta_alerts.append("ab_chal_win")

            # Drift health
            rep_drift = r.get("meta_drift:last_report") or ""
            rep_drift_ts = _i(r.get("meta_drift:last_ts_ms"), 0)
            max_drift_stale = _i(os.getenv("META_DRIFT_MAX_STALE_MS", str(window_ms)), window_ms)
            if rep_drift_ts <= 0 or (now_ms() - rep_drift_ts) > max_drift_stale:
                meta_alerts.append("drift_stale")
            else:
                rep_drift_j = json.loads(rep_drift) if rep_drift else {}
                if rep_drift_j.get("freeze", 0) == 1:
                    meta_alerts.append("drift_freeze")
        except Exception:
            meta_alerts.append("ab_drift_read_err")

    conf_p50_min = os.getenv("ML_SRE_CONF_P50_MIN", "")
    abstain_max = os.getenv("ML_SRE_ABSTAIN_RATE_MAX", "")

    # cfg:suggestions lifecycle health (P6.2)
    cfg_sugg_enable = os.getenv("CFG_SUGGESTIONS_SRE_ENABLE", "1") == "1"
    cfg_sugg_summary = None
    cfg_sugg_alerts: list[str] = []
    if cfg_sugg_enable:
        try:
            cfg_sugg_prefix = os.getenv("CFG_SUGGESTIONS_PREFIX", "cfg:suggestions:entry_policy")
            cfg_sugg_kind = os.getenv("CFG_SUGGESTIONS_KIND", "meta_freeze")
            cfg_sugg_scopes_raw = os.getenv("CFG_SUGGESTIONS_SCOPES", "ALL")
            cfg_sugg_scopes = [s.strip() for s in cfg_sugg_scopes_raw.split(",") if s.strip()]
            cfg_sugg_max_created_age = _i(os.getenv("CFG_SUGGESTIONS_MAX_CREATED_AGE_MS", "3600000"), 3600000)
            cfg_sugg_max_approved_age = _i(os.getenv("CFG_SUGGESTIONS_MAX_APPROVED_AGE_MS", "600000"), 600000)
            cfg_sugg_strict = os.getenv("CFG_SUGGESTIONS_SRE_STRICT", "0") == "1"

            cfg_sugg_summary, cfg_sugg_alerts = check_suggestions_health(
                r,
                prefix=cfg_sugg_prefix,
                kind=cfg_sugg_kind,
                scopes=cfg_sugg_scopes,
                max_created_age_ms=cfg_sugg_max_created_age,
                max_approved_age_ms=cfg_sugg_max_approved_age,
                strict=cfg_sugg_strict
            )
        except Exception as e:
            cfg_sugg_alerts.append(f"cfg_sugg_err:{str(e)[:50]}")

    rows = _read_stream_window(r, stream, start_ms, window_ms)

    ml_alerts = []
    n = len(rows)
    pedge = []
    lat_ms = []
    miss = 0
    err = 0
    abst = 0
    conf = []
    err_counts: dict[str, int] = {}
    allow_count = 0
    mode = "UNKNOWN"

    for d in rows:
        mode = (d.get("mode", mode)).upper()
        kind = (d.get("kind", "")).lower()
        if mode != "OFF" and kind != "none":
            pedge.append(_f(d.get("p_edge", 0.0), 0.0))
        if (d.get("latency_ms", "") or "").strip() != "":
            lat_ms.append(_f(d.get("latency_ms", 0.0), 0.0))
        else:
            lat_ms.append(_f(d.get("latency_us", 0.0), 0.0) / 1000.0)

        st = (d.get("status", "") or "").upper()
        miss_flag = _i(d.get("missing", d.get("missing_n", 0)), 0) > 0 or st.startswith("MISSING")
        miss += 1 if miss_flag else 0

        err_s = (d.get("err", "") or "").strip()
        if err_s != "":
            err += 1
            err_counts[err_s] = err_counts.get(err_s, 0) + 1

        abst += 1 if _i(d.get("abstain", 0), 0) == 1 else 0
        if (d.get("allow", "")).lower() in ("1", "true", "yes"):
            allow_count += 1

        conf_val = d.get("conf", None)
        if conf_val is not None and str(conf_val).strip() != "":
            conf.append(_f(conf_val, 0.0))

    if n > 0:
        n_eval = max(1, len(pedge))
        pedge_p50 = pctl(pedge, 0.50) if pedge else 0.0
        lat_p99 = pctl(lat_ms, 0.99)
        missing_rate = miss / n
        err_rate = err / n
        abstain_rate = abst / n
        allow_rate = allow_count / n
        conf_p50 = pctl(conf, 0.50) if conf else 0.0
        p0_rate = sum(1 for x in pedge if x <= 0.0001) / n_eval

        # Required fields check
        required = os.getenv("ML_SRE_REQUIRED_FIELDS", "p_edge,latency_ms,status,allow,ts_ms").split(",")
        required = [x.strip() for x in required if x.strip()]
        req_missing = 0
        for x in rows:
            for f in required:
                v = x.get(f)
                if v is None or (isinstance(v, str) and v.strip() == ""):
                    req_missing += 1
                    break
        req_miss_rate = req_missing / n

        last_ts = _i(rows[-1].get("_ts_ms", 0), 0)
        stale_ms = (now_ms() - last_ts) if last_ts else 0

        if pedge_p50 < pedge_p50_min:
            ml_alerts.append(f"p_edge_p50={pedge_p50:.3f}<{pedge_p50_min:.3f}")
        if missing_rate > miss_max:
            ml_alerts.append(f"missing_rate={missing_rate:.3f}>{miss_max:.3f}")
        if err_rate > err_max:
            ml_alerts.append(f"err_rate={err_rate:.3f}>{err_max:.3f}")
        if lat_p99 > lat_p99_max:
            ml_alerts.append(f"lat_p99_ms={lat_p99:.2f}>{lat_p99_max:.2f}")
        if max_stale_ms > 0 and last_ts and stale_ms > max_stale_ms:
            ml_alerts.append(f"stream_stale_ms={stale_ms}>{max_stale_ms}")
        if p0_rate > p0_rate_max:
            ml_alerts.append(f"p_edge_zero_rate={p0_rate:.3f}>{p0_rate_max:.3f}")
        if req_miss_rate > req_miss_rate_max:
            ml_alerts.append(f"required_missing_rate={req_miss_rate:.3f}>{req_miss_rate_max:.3f}")

        if meta_alerts:
            ml_alerts.append(f"meta:{','.join(meta_alerts)}")

        if abstain_max.strip() != "":
            try:
                amax = float(abstain_max)
                if abstain_rate > amax:
                    ml_alerts.append(f"abstain_rate={abstain_rate:.3f}>{amax:.3f}")
            except Exception:
                pass
        if conf_p50_min.strip() != "":
            try:
                cmin = float(conf_p50_min)
                if conf and conf_p50 < cmin:
                    ml_alerts.append(f"conf_p50={conf_p50:.3f}<{cmin:.3f}")
            except Exception:
                pass
    else:
        stale_ms = 0
        try:
            tail = r.xrevrange(stream, max="+", min="-", count=1)
            if tail:
                _, fields = tail[0]
                last_ts = _i(fields.get("ts_ms", fields.get("ts", 0)), 0)
                if last_ts:
                    stale_ms = now_ms() - last_ts
        except Exception:
            pass

        # Do not append stream_stale_ms alert when n=0 to prevent false-positives
        # during quiet market hours. Pipeline stuckness is handled by TB_LABELER.

        pedge_p50 = 0
        lat_p99 = 0
        missing_rate = 0
        err_rate = 0
        allow_rate = 0
        p0_rate = 0
        req_miss_rate = 0

    tb_alerts: list[str] = []
    tb = None
    if os.getenv("TB_SRE_ENABLE", "1") != "0":
        tb, tb_alerts = _tb_health(
            r,
            input_stream=os.getenv("TB_INPUT_STREAM", os.getenv("OF_INPUT_STREAM", RS.OF_INPUTS)),
            labels_stream=os.getenv("TB_LABELS_STREAM", RS.TB_LABELS),
            group=os.getenv("OF_INPUTS_GROUP", os.getenv("TB_INPUT_GROUP", "")),
            max_input_lag_ms=_i(os.getenv("TB_SRE_MAX_INPUT_LAG_MS", "120000"), 120000),
            max_label_stale_ms=_i(os.getenv("TB_SRE_MAX_LABEL_STALE_MS", "300000"), 300000),
            max_pending=_i(os.getenv("TB_SRE_MAX_PENDING", "5000"), 5000),
        )

    if not ml_alerts and not tb_alerts and not cfg_sugg_alerts:
        return

    if n > 0:
        allow_rate_str = f"{allow_rate:.3f}"
        pedge_p50_str = f"{pedge_p50:.3f}"
        lat_p99_str = f"{lat_p99:.2f}"
        p0_rate_str = f"{p0_rate:.3f}"
        req_miss_rate_str = f"{req_miss_rate:.4f}"
        missing_rate_str = f"{missing_rate:.3f}"
        err_rate_str = f"{err_rate:.3f}"
    else:
        allow_rate_str = "N/A"
        pedge_p50_str = "N/A"
        lat_p99_str = "N/A"
        p0_rate_str = "N/A"
        req_miss_rate_str = "N/A"
        missing_rate_str = "N/A"
        err_rate_str = "N/A"

    txt = (
        "<b>ML_CONFIRM SRE ALERT</b>\n"
        f"mode=<code>{mode}</code> n=<code>{n}</code>\n"
        f"allow_rate=<code>{allow_rate_str}</code>\n"
        f"p50=<code>{pedge_p50_str}</code> lat_p99_ms=<code>{lat_p99_str}</code>\n"
        f"stream_stale_ms=<code>{stale_ms}</code>\n"
        f"p_edge_zero_rate=<code>{p0_rate_str}</code>\n"
        f"required_missing_rate=<code>{req_miss_rate_str}</code>\n"
        f"missing_rate=<code>{missing_rate_str}</code> err_rate=<code>{err_rate_str}</code>\n"
        f"meta_status=<code>{meta_status}</code> meta_train_stale_ms=<code>{meta_train_stale_ms}</code>\n"
    )
    if ml_alerts:
        safe_ml_alerts = [html.escape(str(x), quote=True) for x in ml_alerts]
        alerts_str = ", ".join(safe_ml_alerts)
        txt += f"alerts=<code>{alerts_str}</code>\n"

    if tb is not None:
        txt += (
            "\n<b>TB_LABELER</b>\n"
            f"input_lag_ms=<code>{tb.get('input_lag_ms', 0)}</code> "
            f"label_stale_ms=<code>{tb.get('label_stale_ms', 0)}</code> "
            f"pending=<code>{tb.get('pending', 0)}</code> "
            f"group_lag_ms=<code>{tb.get('group_lag_ms', 0)}</code>\n"
        )
        if tb_alerts:
            tb_alerts_str = ", ".join(html.escape(str(x), quote=True) for x in tb_alerts)
            txt += f"tb_alerts=<code>{tb_alerts_str}</code>\n"

    if cfg_sugg_summary:
        scopes_esc = html.escape((cfg_sugg_summary.get('scopes', [])), quote=True)
        txt += (
            f"\n<b>CFG_SUGGESTIONS</b> {html.escape((cfg_sugg_summary.get('kind', '')), quote=True)} "
            f"scopes=<code>{scopes_esc}</code>\n"
            f"pending=<code>{cfg_sugg_summary.get('n_pending', 0)}</code> "
            f"approved=<code>{cfg_sugg_summary.get('n_approved', 0)}</code> "
            f"applied=<code>{cfg_sugg_summary.get('n_applied', 0)}</code>\n"
            f"oldest_pending_min=<code>{cfg_sugg_summary.get('oldest_pending_ms', 0)//60000}</code>\n"
        )
        stuck = cfg_sugg_summary.get("stuck_sids", [])
        if stuck:
            txt += "stuck=[" + ", ".join([html.escape(f"{s['sid']}({s['reason']},{s['age_s']}s)", quote=True) for s in stuck]) + "]\n"
        if cfg_sugg_alerts:
            cfg_alerts_str = ", ".join(html.escape(str(x), quote=True) for x in cfg_sugg_alerts)
            txt += f"cfg_sugg_alerts=<code>{cfg_alerts_str}</code>\n"

    if err_counts:
        sorted_errs = sorted(err_counts.items(), key=lambda x: x[1], reverse=True)
        top_err, count = sorted_errs[0]
        txt += f"\nTop Error: {html.escape(top_err)} ({count}/{n})"

    if args.dry_run:
        r.close()
        print(txt)
        return

    if args.notify:
        _notify(r, txt)
    r.close()

if __name__ == "__main__":
    main()
