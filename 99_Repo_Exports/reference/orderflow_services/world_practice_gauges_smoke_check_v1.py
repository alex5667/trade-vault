# -*- coding: utf-8 -*-
"""World-practice trackers smoke-check (v1)

Goal
----
Ensure that the new low-cardinality gauges / regime trackers are not silently
broken ("stuck" in 0/na) by validating the *producer stream* tail.

Source of truth: Redis Stream `metrics:of_gate`.
We validate that fields exist and are sane for a recent tail window.

Exit codes:
  0 OK
  2 ALERT (missing/invalid/stuck beyond thresholds)
  1 ERROR

Designed for periodic execution (hourly) by `services/of_timers_worker.py`.
"""

from __future__ import annotations

import argparse
import json
import logging
import math
import os
import sys
import time
from collections import Counter
from typing import Any, Dict, List, Tuple

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

logger = logging.getLogger("world_practice_smoke")


def _now_ms() -> int:
    return int(time.time() * 1000)


def _safe_int(v: Any, default: int = 0) -> int:
    try:
        return int(float(v))
    except Exception:
        return default


def _safe_float(v: Any, default: float = 0.0) -> float:
    try:
        x = float(v)
        if not math.isfinite(x):
            return default
        return x
    except Exception:
        return default


def _is_missing_label(v: Any) -> bool:
    s = str(v or "").strip().lower()
    return (not s) or s == "na"


def _bucket_ok(v: Any) -> bool:
    s = str(v or "").strip().upper()
    return s in ("NORMAL", "LOW_LIQ", "HIGH_VOL", "HIGH_VOL_LOW_LIQ")


def _top(counter: Counter, k: int = 10) -> List[Tuple[str, int]]:
    return [(str(a), int(b)) for a, b in counter.most_common(k)]


def main() -> int:
    ap = argparse.ArgumentParser(description="World-practice trackers smoke-check (v1)")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--stream", default=os.getenv("OF_GATE_METRICS_STREAM", "metrics:of_gate"))
    ap.add_argument("--limit", type=int, default=int(os.getenv("WP_SMOKE_LIMIT", "3000")))

    ap.add_argument("--max-age-ms", type=int, default=int(os.getenv("WP_SMOKE_MAX_AGE_MS", "900000")))  # 15m
    ap.add_argument("--min-recent", type=int, default=int(os.getenv("WP_SMOKE_MIN_RECENT", "200")))

    # thresholds
    ap.add_argument("--bucket-invalid-max", type=float, default=float(os.getenv("WP_SMOKE_BUCKET_INVALID_MAX", "0.05")))
    ap.add_argument("--vol-label-na-max", type=float, default=float(os.getenv("WP_SMOKE_VOL_LABEL_NA_MAX", "0.50")))
    ap.add_argument("--missing-max", type=float, default=float(os.getenv("WP_SMOKE_MISSING_MAX", "0.50")))

    ap.add_argument("--out-stream", default=os.getenv("WP_SMOKE_OUT_STREAM", "sre:world_practice_smoke"))

    args = ap.parse_args()

    if redis is None:
        logger.error("redis dependency is missing")
        return 1

    r = redis.from_url(args.redis_url, decode_responses=True)

    try:
        rows = r.xrevrange(args.stream, max="+", min="-", count=max(0, int(args.limit)))
    except Exception as e:
        logger.error(f"xrevrange failed: {e}")
        return 1

    now_ms = _now_ms()

    # Filter by recent window (ts_ms is required by contract)
    recent: List[Dict[str, Any]] = []
    max_ts = 0
    for _id, fields in rows:
        if not isinstance(fields, dict):
            continue
        ts = _safe_int(fields.get("ts_ms"), 0)
        if ts > max_ts:
            max_ts = ts
        if ts <= 0:
            continue
        if now_ms - ts <= int(args.max_age_ms):
            recent.append(fields)

    n_total = len(rows)
    n_recent = len(recent)
    no_data = 1 if n_recent == 0 else 0

    # counters
    bucket_invalid = 0
    vol_label_na = 0

    missing: Counter = Counter()
    bucket_dist: Counter = Counter()
    vol_dist: Counter = Counter()
    liq_dist: Counter = Counter()

    # numeric trackers for "stuck" heuristics
    max_abs_vol_ratio_z = 0.0
    max_vol_fast = 0.0
    max_vol_slow = 0.0
    max_eta_fill = 0.0
    max_fill_prob = 0.0
    # Exec-risk trackers (v16): detect stuck exec_pen when exec_risk is non-trivial.
    max_exec_risk_norm = 0.0
    max_exec_pen = 0.0
    max_spread_bps = 0.0
    max_expected_slip_eff = 0.0

    # What we expect to be present when pipeline is wired end-to-end
    key_fields = [
        "exec_regime_bucket",
        "vol_regime_label",
        "liq_regime_label",
        "vol_fast_bps",
        "vol_slow_bps",
        "vol_ratio",
        "vol_ratio_z",
        "res_recovered",
        "res_recovery_ms",

        # Execution-risk / slippage trackers (v16)
        "spread_bps_submit",
        "impact_proxy",
        "liq_score",
        "expected_slippage_bps",
        "expected_slippage_decomp_bps",
        "exec_risk_norm",
        "exec_pen",

        "fill_prob_proxy",
        "eta_fill_sec",
        "exec_fill_pen",

        # v23: adverse selection (realized drift)
        "adverse_rd_mean_bps",
        "adverse_rd_sigma_bps",
        "adverse_rd_z",
        "adverse_rd_bad_share",
        "adverse_rd_n",
        "adverse_rd_veto",
    ]

    for f in recent:
        b = str(f.get("exec_regime_bucket") or "").upper()
        bucket_dist[b or "na"] += 1
        if not _bucket_ok(b):
            bucket_invalid += 1

        vrl = str(f.get("vol_regime_label") or "").strip().lower()
        vol_dist[vrl or "na"] += 1
        if _is_missing_label(vrl):
            vol_label_na += 1

        lrl = str(f.get("liq_regime_label") or "").strip().lower()
        liq_dist[lrl or "na"] += 1

        # missing accounting
        for k in key_fields:
            if k not in f:
                missing[k] += 1
                continue
            vv = f.get(k)
            if k in ("vol_regime_label", "liq_regime_label"):
                if _is_missing_label(vv):
                    missing[k] += 1
            elif k == "exec_regime_bucket":
                if not _bucket_ok(vv):
                    missing[k] += 1
            else:
                # numeric
                s = str(vv or "").strip()
                if s == "":
                    missing[k] += 1
                else:
                    x = _safe_float(vv, default=float("nan"))
                    if not math.isfinite(x):
                        missing[k] += 1

        # stuck heuristics
        vz = abs(_safe_float(f.get("vol_ratio_z"), 0.0))
        if vz > max_abs_vol_ratio_z:
            max_abs_vol_ratio_z = vz
        vf = _safe_float(f.get("vol_fast_bps"), 0.0)
        if vf > max_vol_fast:
            max_vol_fast = vf
        vs = _safe_float(f.get("vol_slow_bps"), 0.0)
        if vs > max_vol_slow:
            max_vol_slow = vs

        eta = _safe_float(f.get("eta_fill_sec"), 0.0)
        if eta > max_eta_fill:
            max_eta_fill = eta
        fp = _safe_float(f.get("fill_prob_proxy"), 0.0)
        if fp > max_fill_prob:
            max_fill_prob = fp

        # Exec-risk / spread / slippage trackers (v16)
        er = _safe_float(f.get("exec_risk_norm"), 0.0)
        if er > max_exec_risk_norm:
            max_exec_risk_norm = er
        ep = _safe_float(f.get("exec_pen"), 0.0)
        if ep > max_exec_pen:
            max_exec_pen = ep
        sp = _safe_float(f.get("spread_bps_submit"), 0.0)
        if sp > max_spread_bps:
            max_spread_bps = sp
        es = _safe_float(f.get("expected_slippage_bps"), 0.0)
        if es > max_expected_slip_eff:
            max_expected_slip_eff = es

    def _share(x: int, n: int) -> float:
        return (float(x) / float(n)) if n > 0 else 0.0

    bucket_invalid_share = _share(bucket_invalid, n_recent)
    vol_label_na_share = _share(vol_label_na, n_recent)

    missing_share = {k: _share(int(missing.get(k, 0)), n_recent) for k in key_fields}

    # "stuck" heuristics: require enough recent points to avoid startup false positives
    stuck_vol = 0
    stuck_fill = 0
    stuck_exec = 0
    if n_recent >= int(args.min_recent):
        if max_vol_fast <= 0.0 and max_vol_slow <= 0.0 and max_abs_vol_ratio_z <= 0.01:
            stuck_vol = 1
        # fill: eta indicates L3-lite is producing, but fill_prob never moves from 0
        if max_eta_fill > 0.0 and max_fill_prob <= 0.001:
            stuck_fill = 1
        # exec: if exec risk is non-trivial but exec_pen never moves from 0
        # distinguishes "exec_risk computed" vs "exec_pen applied/propagated"
        if max_exec_risk_norm >= 0.10 and max_exec_pen <= 1e-6:
            stuck_exec = 1

    alert = 0
    issues: List[str] = []

    if no_data:
        # no recent rows -> do not alert; report as no_data
        alert = 0
    else:
        if bucket_invalid_share > float(args.bucket_invalid_max):
            issues.append(f"bucket_invalid_share>{args.bucket_invalid_max}")
        if vol_label_na_share > float(args.vol_label_na_max):
            issues.append(f"vol_label_na_share>{args.vol_label_na_max}")

        # any key field missing too often
        for k, sh in missing_share.items():
            if sh > float(args.missing_max):
                issues.append(f"missing_{k}")

        if stuck_vol == 1:
            issues.append("stuck_vol_all_zero")
        if stuck_fill == 1:
            issues.append("stuck_fill_prob_zero")
        if stuck_exec == 1:
            # exec_risk is being computed but exec_pen is not propagated — broken wiring.
            issues.append("stuck_exec_pen_zero")

        if issues:
            alert = 1

    out = {
        "ts_ms": str(now_ms),
        "stream": str(args.stream),
        "limit": str(int(args.limit)),
        "n_total": str(int(n_total)),
        "n_recent": str(int(n_recent)),
        "max_ts_ms": str(int(max_ts)),
        "age_ms": str(int(now_ms - max_ts)) if max_ts > 0 else "-1",
        "no_data": str(int(no_data)),
        "alert": str(int(alert)),
        "issues": ",".join(issues)[:400],
        "bucket_invalid_share": f"{bucket_invalid_share:.6f}",
        "vol_label_na_share": f"{vol_label_na_share:.6f}",
        "missing_share_json": json.dumps({k: float(f"{v:.6f}") for k, v in missing_share.items()}, ensure_ascii=False),
        "bucket_dist_json": json.dumps(_top(bucket_dist, 8), ensure_ascii=False),
        "vol_dist_json": json.dumps(_top(vol_dist, 8), ensure_ascii=False),
        "liq_dist_json": json.dumps(_top(liq_dist, 8), ensure_ascii=False),
        "stuck_vol": str(int(stuck_vol)),
        "stuck_fill": str(int(stuck_fill)),
        "stuck_exec": str(int(stuck_exec)),
        "max_abs_vol_ratio_z": f"{max_abs_vol_ratio_z:.4f}",
        "max_vol_fast_bps": f"{max_vol_fast:.4f}",
        "max_vol_slow_bps": f"{max_vol_slow:.4f}",
        "max_eta_fill_sec": f"{max_eta_fill:.4f}",
        "max_fill_prob": f"{max_fill_prob:.4f}",
        "max_exec_risk_norm": f"{max_exec_risk_norm:.4f}",
        "max_exec_pen": f"{max_exec_pen:.4f}",
        "max_spread_bps": f"{max_spread_bps:.4f}",
        "max_expected_slip_eff_bps": f"{max_expected_slip_eff:.4f}",
    }

    # write to out stream (best-effort)
    try:
        r.xadd(args.out_stream, out, maxlen=2000, approximate=True)
    except Exception:
        pass

    print(json.dumps(out, ensure_ascii=False))

    if alert == 1:
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
