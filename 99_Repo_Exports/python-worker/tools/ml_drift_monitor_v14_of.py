"""ML drift monitor for v14_of champion/challenger (continuous + Prometheus exporter).

What it does (every V14_DRIFT_INTERVAL_SEC, default 1800s = 30m):
  For each model kind in V14_DRIFT_KINDS (default "meta_lr,edge_stack_v1"):
    1. Read `metrics:ml_confirm` filtered by `kind` field within [now - baseline_hours, now]
    2. Split rows into current (last window_hours) vs baseline (older)
    3. Per bucket (trend / range / other):
       - PSI(p_edge_cal_base, p_edge_cal_cur)
       - KS-stat(p_edge_cal_base, p_edge_cal_cur)
       - mean drift Δ = mean(cur) − mean(base)
       - reliability ECE (cur, base), ECE delta — joined with trades:closed by sid for outcomes
    4. Expose Prometheus gauges + write summary blob to `metrics:ml_drift:v14_of:last`

Why this matters:
  - PSI > 0.2 = significant feature/score distribution shift → champion losing edge
  - PSI > 0.4 = major drift → consider rollback or retrain
  - KS > 0.2 = same direction, different scale
  - ECE drift > 0.05 = calibration broken
  - Per-kind separation makes champion vs challenger drift comparable

Env:
  REDIS_URL                       redis://redis-worker-1:6379/0
  V14_DRIFT_INTERVAL_SEC          1800       (default 30m)
  V14_DRIFT_WINDOW_HOURS          6          current window
  V14_DRIFT_BASELINE_HOURS        168        baseline = last 7 days minus current window
  V14_DRIFT_MAX_SCAN              80000      max stream rows to scan
  V14_DRIFT_KINDS                 meta_lr,edge_stack_v1
  V14_DRIFT_STREAM_PRED           metrics:ml_confirm
  V14_DRIFT_STREAM_TRADES         trades:closed
  V14_DRIFT_OUT_KEY               metrics:ml_drift:v14_of:last
  V14_DRIFT_PORT                  9844       Prometheus exporter port
"""
from __future__ import annotations

import asyncio
import json
import logging
import math
import os
import sys
import time
from typing import Any

from prometheus_client import Gauge, start_http_server
import redis.asyncio as aredis
import redis as sync_redis

logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
log = logging.getLogger("ml_drift_v14_of")


# ---------------------------------------------------------------------------
# Stat primitives (independent of scipy/numpy to keep deps minimal)
# ---------------------------------------------------------------------------

def _f(x: Any, default: float = 0.0) -> float:
    try:
        v = float(x)
        if not math.isfinite(v):
            return default
        return v
    except Exception:
        return default


def _i(x: Any, default: int = 0) -> int:
    try:
        return int(float(x))
    except Exception:
        return default


def ks_stat(a: list[float], b: list[float]) -> float:
    """Two-sample Kolmogorov-Smirnov statistic D.

    D = max_x |F_a(x) − F_b(x)|. Range [0, 1]. Larger = more drift.
    Linear merge of sorted samples.
    """
    if not a or not b:
        return 0.0
    sa = sorted(a)
    sb = sorted(b)
    na, nb = len(sa), len(sb)
    ia = ib = 0
    fa = fb = 0.0
    d_max = 0.0
    while ia < na and ib < nb:
        if sa[ia] < sb[ib]:
            ia += 1
            fa = ia / na
        elif sa[ia] > sb[ib]:
            ib += 1
            fb = ib / nb
        else:
            v = sa[ia]
            while ia < na and sa[ia] == v:
                ia += 1
            while ib < nb and sb[ib] == v:
                ib += 1
            fa = ia / na
            fb = ib / nb
        d = abs(fa - fb)
        if d > d_max:
            d_max = d
    return float(d_max)


def psi(a: list[float], b: list[float], *, bins: int = 10) -> float:
    """Population Stability Index: Σ (cur_i − base_i) · ln(cur_i / base_i).

    Bins are quantile-based on the BASELINE distribution. We use a small floor
    (eps) to avoid log(0) and 0-division.
    """
    if not a or not b:
        return 0.0
    sa = sorted(a)
    n = len(sa)
    # quantile edges on baseline
    edges: list[float] = []
    for i in range(1, bins):
        idx = int(round(i * n / bins))
        idx = max(1, min(n - 1, idx))
        edges.append(sa[idx])
    edges = sorted(set(edges))

    def _hist(xs: list[float]) -> list[int]:
        c = [0] * (len(edges) + 1)
        for x in xs:
            placed = False
            for i, e in enumerate(edges):
                if x <= e:
                    c[i] += 1
                    placed = True
                    break
            if not placed:
                c[-1] += 1
        return c

    ha = _hist(a)
    hb = _hist(b)
    na, nb = sum(ha), sum(hb)
    if na == 0 or nb == 0:
        return 0.0
    eps = 1e-6
    val = 0.0
    for ai, bi in zip(ha, hb):
        pa = max(eps, ai / na)
        pb = max(eps, bi / nb)
        val += (pa - pb) * math.log(pa / pb)
    return float(val)


def calibration_ece(p: list[float], y: list[int], *, n_bins: int = 10) -> float:
    """Expected Calibration Error (equal-width bins)."""
    if not p or not y or len(p) != len(y):
        return 0.0
    bins: list[tuple[list[float], list[int]]] = [([], []) for _ in range(n_bins)]
    for pv, yv in zip(p, y):
        idx = min(n_bins - 1, max(0, int(pv * n_bins)))
        bins[idx][0].append(pv)
        bins[idx][1].append(int(yv))
    n = len(p)
    ece = 0.0
    for ps, ys in bins:
        if not ps:
            continue
        avg_p = sum(ps) / len(ps)
        avg_y = sum(ys) / len(ys)
        ece += (len(ps) / n) * abs(avg_p - avg_y)
    return float(ece)


# ---------------------------------------------------------------------------
# Stream readers
# ---------------------------------------------------------------------------

async def _xrange_since(r: aredis.Redis, stream: str, min_ts_ms: int,
                        max_scan: int) -> list[dict[str, Any]]:
    """Read entries from stream where id >= min_ts_ms-0; flatten fields to dict."""
    cursor = f"{min_ts_ms}-0"
    out: list[dict[str, Any]] = []
    while len(out) < max_scan:
        chunk = await r.xrange(stream, min=cursor, max="+", count=2000)
        if not chunk:
            break
        for entry_id, fields in chunk:
            try:
                out.append(dict(fields))
            except Exception:
                continue
        last_id = chunk[-1][0]
        if last_id == cursor:
            break
        base, _, seq = last_id.partition("-")
        cursor = f"{base}-{int(seq) + 1}"
    return out


def _bucket_from_payload(p: dict[str, Any]) -> str:
    b = (p.get("bucket") or "").lower()
    if b in ("trend", "range"):
        return b
    return "other"


def _join_by_sid(preds: list[dict[str, Any]], trades: list[dict[str, Any]]) -> list[tuple[dict, dict]]:
    """Join predictions and trade outcomes by sid. Trades:closed sid might come
    in `sid` or `signal_id` field."""
    by_sid_pred = {p.get("sid", ""): p for p in preds if p.get("sid")}
    out: list[tuple[dict, dict]] = []
    for t in trades:
        sid = t.get("sid") or t.get("signal_id") or ""
        if sid and sid in by_sid_pred:
            out.append((by_sid_pred[sid], t))
    return out


# ---------------------------------------------------------------------------
# Prometheus gauges (per kind, per bucket)
# ---------------------------------------------------------------------------

g_psi = Gauge("ml_drift_v14_of_psi",
              "PSI(p_edge_cal) baseline vs current, by kind+bucket",
              ["kind", "bucket"])
g_ks = Gauge("ml_drift_v14_of_ks",
             "KS(p_edge_cal) baseline vs current, by kind+bucket",
             ["kind", "bucket"])
g_mean_cur = Gauge("ml_drift_v14_of_p_edge_mean_cur",
                   "Mean(p_edge_cal) in current window, by kind+bucket",
                   ["kind", "bucket"])
g_mean_base = Gauge("ml_drift_v14_of_p_edge_mean_base",
                    "Mean(p_edge_cal) in baseline window, by kind+bucket",
                    ["kind", "bucket"])
g_ece_cur = Gauge("ml_drift_v14_of_ece_cur",
                  "Reliability ECE in current window (joined with trades:closed)",
                  ["kind", "bucket"])
g_ece_drift = Gauge("ml_drift_v14_of_ece_drift",
                    "Δ ECE = ece_cur − ece_base; positive = calibration degrading",
                    ["kind", "bucket"])
g_n_cur = Gauge("ml_drift_v14_of_n_pred_cur",
                "Number of predictions in current window, by kind+bucket",
                ["kind", "bucket"])
g_n_base = Gauge("ml_drift_v14_of_n_pred_base",
                 "Number of predictions in baseline window, by kind+bucket",
                 ["kind", "bucket"])
g_n_joined_cur = Gauge("ml_drift_v14_of_n_joined_cur",
                       "Joined (pred+outcome) sample count, current window",
                       ["kind", "bucket"])
g_run_age = Gauge("ml_drift_v14_of_run_age_seconds",
                  "Seconds since last drift evaluation run")


# ---------------------------------------------------------------------------
# Drift computation
# ---------------------------------------------------------------------------

async def compute_drift(*, redis_url: str, stream_pred: str, stream_trades: str,
                        window_hours: float, baseline_hours: float,
                        max_scan: int, kinds: list[str]) -> dict[str, Any]:
    now_ms = int(time.time() * 1000)
    win_ms = int(window_hours * 3600 * 1000)
    base_ms = int(baseline_hours * 3600 * 1000)
    cur_min = now_ms - win_ms
    base_min = now_ms - base_ms

    r = aredis.from_url(redis_url, decode_responses=True)
    try:
        all_pred = await _xrange_since(r, stream_pred, base_min, max_scan)
        all_trades = await _xrange_since(r, stream_trades, base_min, max_scan)
    finally:
        await r.aclose()

    log.info("scanned pred=%d trades=%d for [%d..%d]", len(all_pred), len(all_trades), base_min, now_ms)

    out: dict[str, Any] = {
        "ts_ms": now_ms,
        "window_hours": window_hours,
        "baseline_hours": baseline_hours,
        "counts": {"pred_total": len(all_pred), "trades_total": len(all_trades)},
        "by_kind": {},
    }

    buckets = ["trend", "range", "other"]

    for kind in kinds:
        pred_k = [p for p in all_pred if (p.get("kind") or "") == kind]
        pred_cur_k = [p for p in pred_k if _i(p.get("ts_ms"), 0) >= cur_min]
        pred_base_k = [p for p in pred_k if base_min <= _i(p.get("ts_ms"), 0) < cur_min]

        joined_cur = _join_by_sid(pred_cur_k, all_trades)
        joined_base = _join_by_sid(pred_base_k, all_trades)

        kind_summary: dict[str, Any] = {
            "n_pred_cur": len(pred_cur_k),
            "n_pred_base": len(pred_base_k),
            "n_joined_cur": len(joined_cur),
            "n_joined_base": len(joined_base),
            "by_bucket": {},
        }

        for b in buckets:
            cur_p = [_f(p.get("p_edge_cal", p.get("p_edge", 0.0))) for p in pred_cur_k if _bucket_from_payload(p) == b]
            base_p = [_f(p.get("p_edge_cal", p.get("p_edge", 0.0))) for p in pred_base_k if _bucket_from_payload(p) == b]

            jc = [(p, t) for (p, t) in joined_cur if _bucket_from_payload(p) == b]
            jb = [(p, t) for (p, t) in joined_base if _bucket_from_payload(p) == b]
            cur_pp = [_f(p.get("p_edge_cal", p.get("p_edge", 0.0))) for (p, _) in jc]
            cur_y = [1 if _f(t.get("r_mult"), 0.0) > 0.0 else 0 for (_, t) in jc]
            base_pp = [_f(p.get("p_edge_cal", p.get("p_edge", 0.0))) for (p, _) in jb]
            base_y = [1 if _f(t.get("r_mult"), 0.0) > 0.0 else 0 for (_, t) in jb]

            psi_v = psi(base_p, cur_p) if base_p and cur_p else 0.0
            ks_v = ks_stat(base_p, cur_p) if base_p and cur_p else 0.0
            mean_cur = sum(cur_p) / len(cur_p) if cur_p else 0.0
            mean_base = sum(base_p) / len(base_p) if base_p else 0.0
            ece_cur = calibration_ece(cur_pp, cur_y) if cur_pp else 0.0
            ece_base = calibration_ece(base_pp, base_y) if base_pp else 0.0

            stat = {
                "n_pred_cur": len(cur_p),
                "n_pred_base": len(base_p),
                "n_joined_cur": len(jc),
                "n_joined_base": len(jb),
                "psi": psi_v,
                "ks": ks_v,
                "p_edge_mean_cur": mean_cur,
                "p_edge_mean_base": mean_base,
                "ece_cur": ece_cur,
                "ece_base": ece_base,
                "ece_drift": ece_cur - ece_base,
            }
            kind_summary["by_bucket"][b] = stat

            # Update Prometheus gauges
            g_psi.labels(kind=kind, bucket=b).set(psi_v)
            g_ks.labels(kind=kind, bucket=b).set(ks_v)
            g_mean_cur.labels(kind=kind, bucket=b).set(mean_cur)
            g_mean_base.labels(kind=kind, bucket=b).set(mean_base)
            g_ece_cur.labels(kind=kind, bucket=b).set(ece_cur)
            g_ece_drift.labels(kind=kind, bucket=b).set(ece_cur - ece_base)
            g_n_cur.labels(kind=kind, bucket=b).set(len(cur_p))
            g_n_base.labels(kind=kind, bucket=b).set(len(base_p))
            g_n_joined_cur.labels(kind=kind, bucket=b).set(len(jc))

        out["by_kind"][kind] = kind_summary

    g_run_age.set(0.0)
    return out


async def main_loop() -> None:
    redis_url = os.environ.get("REDIS_URL", "redis://redis-worker-1:6379/0")
    stream_pred = os.environ.get("V14_DRIFT_STREAM_PRED", "metrics:ml_confirm")
    stream_trades = os.environ.get("V14_DRIFT_STREAM_TRADES", "trades:closed")
    out_key = os.environ.get("V14_DRIFT_OUT_KEY", "metrics:ml_drift:v14_of:last")
    interval_sec = int(os.environ.get("V14_DRIFT_INTERVAL_SEC", "1800"))
    window_h = float(os.environ.get("V14_DRIFT_WINDOW_HOURS", "6"))
    baseline_h = float(os.environ.get("V14_DRIFT_BASELINE_HOURS", "168"))
    max_scan = int(os.environ.get("V14_DRIFT_MAX_SCAN", "80000"))
    kinds = [k.strip() for k in os.environ.get("V14_DRIFT_KINDS", "meta_lr,edge_stack_v1").split(",") if k.strip()]
    port = int(os.environ.get("V14_DRIFT_PORT", "9839"))

    log.info("starting drift monitor: port=%d kinds=%s interval=%ds", port, kinds, interval_sec)
    start_http_server(port)

    sync_r = sync_redis.Redis.from_url(redis_url, decode_responses=False)
    last_run_ms = 0

    while True:
        try:
            t0 = time.time()
            summary = await compute_drift(
                redis_url=redis_url,
                stream_pred=stream_pred, stream_trades=stream_trades,
                window_hours=window_h, baseline_hours=baseline_h,
                max_scan=max_scan, kinds=kinds,
            )
            summary["elapsed_sec"] = round(time.time() - t0, 2)
            last_run_ms = int(time.time() * 1000)
            try:
                sync_r.set(out_key, json.dumps(summary, separators=(",", ":")))
                log.info("drift run ok, elapsed=%.2fs → %s", summary["elapsed_sec"], out_key)
            except Exception as e:
                log.warning("set summary failed: %s", e)
        except Exception as e:
            log.error("drift compute crashed: %s", e)

        # Refresh run age gauge between runs
        sleep_left = interval_sec
        step = 30
        while sleep_left > 0:
            await asyncio.sleep(min(step, sleep_left))
            sleep_left -= step
            if last_run_ms > 0:
                age = (time.time() * 1000 - last_run_ms) / 1000.0
                g_run_age.set(age)


if __name__ == "__main__":
    try:
        asyncio.run(main_loop())
    except KeyboardInterrupt:
        sys.exit(0)
