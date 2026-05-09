from __future__ import annotations

# -*- coding: utf-8 -*-
"""P91 — LOB pressure smoke-check (v1)

Goal:
  Detect wiring regressions where LOB-pressure features are not produced or become stuck
  (e.g., book processor not updating, indicators not propagated, emit_gate_metrics missing fields).

Reads tail of Redis Stream `metrics:of_gate` and checks low-cardinality LOB summary fields:
  - queue imbalance summaries
  - microprice shift/divergence
  - depth slope/convexity imbalance
  - depth-weighted OBI + stability

Exit codes:
  0  OK (or no_data)
  2  ALERT (missing/invalid/stuck above thresholds)
  1  ERROR

Writes a compact JSON summary into `sre:lob_pressure_smoke` (for dashboards/exporters).
""",
import argparse
import json
import logging
import math
import os
import sys
from collections import defaultdict
from typing import Any

from utils.time_utils import get_ny_time_millis
import contextlib

try:
    import redis  # type: ignore
except Exception:  # pragma: no cover
    redis = None

logger = logging.getLogger("lob_pressure_smoke_check")


KEY_FLOAT_FIELDS = [
    "lob_qi_mean",
    "lob_qi_max_abs",
    "lob_qi_slope",
    "lob_micro_mid_div_bps",
    "lob_micro_shift_bps",
    "lob_depth_slope_imb",
    "lob_depth_convexity_imb",
    "lob_dw_obi",
    "lob_dw_obi_z",
    "lob_dw_obi_stability_score",
    "lob_dw_obi_stable_secs",
]

KEY_INT_FIELDS = [
    "lob_dw_obi_stable",
]

# Keys used for "stuck" detection: if all have near-zero range over recent window → suspicious.
STUCK_KEYS = [
    ("lob_micro_shift_bps", 0.05),
    ("lob_qi_mean", 0.02),
    ("lob_dw_obi_z", 0.05),
]


def _now_ms() -> int:
    return get_ny_time_millis()


def _safe_float(v: Any, default: float | None = None) -> float | None:
    if v is None:
        return default
    try:
        x = float(v)
    except Exception:
        return default
    if math.isnan(x) or math.isinf(x):
        return default
    return x


def _safe_int(v: Any, default: int | None = None) -> int | None:
    if v is None:
        return default
    try:
        return int(float(v))
    except Exception:
        return default


def _range_ok(key: str, x: float) -> bool:
    """Coarse sanity bounds to catch NaN/units/bug explosions.

    These bounds are intentionally loose to avoid false positives.
    """,
    ax = abs(x)

    if key in ("lob_qi_mean", "lob_qi_slope"):
        return ax <= 2.0
    if key == "lob_qi_max_abs":
        return x >= 0.0 and x <= 2.0

    if key in ("lob_micro_mid_div_bps", "lob_micro_shift_bps"):
        return ax <= 2000.0  # 20% in bps, extremely loose

    if key in ("lob_depth_slope_imb", "lob_depth_convexity_imb"):
        return ax <= 50.0

    if key == "lob_dw_obi":
        return ax <= 2.0
    if key == "lob_dw_obi_z":
        return ax <= 250.0

    if key == "lob_dw_obi_stability_score":
        return x >= -0.5 and x <= 2.0
    if key == "lob_dw_obi_stable_secs":
        return x >= 0.0 and x <= 7 * 24 * 3600

    return True


def _top_missing(missing_counts: dict[str, int], n: int = 10) -> list[tuple[str, int]]:
    items = sorted(missing_counts.items(), key=lambda kv: (-kv[1], kv[0]))
    return [(k, int(v)) for k, v in items[:n]]


def main() -> int:
    ap = argparse.ArgumentParser(description="P91 LOB pressure smoke-check (v1)")
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--stream", default=os.getenv("OF_GATE_METRICS_STREAM", "metrics:of_gate"))
    ap.add_argument("--limit", type=int, default=int(os.getenv("LOB_SMOKE_LIMIT", "2000")))
    ap.add_argument("--window-sec", type=int, default=int(os.getenv("LOB_SMOKE_WINDOW_SEC", "1800")))  # 30m
    ap.add_argument("--min-recent", type=int, default=int(os.getenv("LOB_SMOKE_MIN_RECENT", "200")))

    ap.add_argument("--missing-max", type=float, default=float(os.getenv("LOB_SMOKE_MISSING_MAX", "0.25")))
    ap.add_argument("--invalid-max", type=float, default=float(os.getenv("LOB_SMOKE_INVALID_MAX", "0.01")))

    ap.add_argument("--out-stream", default=os.getenv("LOB_SMOKE_OUT_STREAM", "sre:lob_pressure_smoke"))

    args = ap.parse_args()

    if redis is None:
        logger.error("redis dependency is missing")
        return 1

    r = redis.from_url(args.redis_url, decode_responses=True)

    try:
        rows = r.xrevrange(args.stream, max="+", min="-", count=max(0, int(args.limit)))
    except Exception as e:
        print(json.dumps({"error": f"xrevrange_failed:{e}"}, ensure_ascii=False))
        return 1

    now_ms = _now_ms()
    min_ts = now_ms - int(args.window_sec) * 1000

    n_recent = 0
    missing: dict[str, int] = defaultdict(int)
    invalid_total = 0

    # Track ranges for stuck detection
    mins: dict[str, float] = {}
    maxs: dict[str, float] = {}

    for _id, fields in rows:
        if not isinstance(fields, dict):
            continue

        ts = _safe_int(fields.get("ts_ms"), default=None)
        if ts is None or ts < min_ts:
            continue

        n_recent += 1

        for k in KEY_FLOAT_FIELDS:
            if k not in fields or fields.get(k) in (None, ""):
                missing[k] += 1
                continue
            x = _safe_float(fields.get(k), default=None)
            if x is None or (not _range_ok(k, x)):
                invalid_total += 1
                continue
            if k in dict(STUCK_KEYS):
                if k not in mins:
                    mins[k] = x
                    maxs[k] = x
                else:
                    mins[k] = min(mins[k], x)
                    maxs[k] = max(maxs[k], x)

        for k in KEY_INT_FIELDS:
            if k not in fields or fields.get(k) in (None, ""):
                missing[k] += 1
                continue
            xi = _safe_int(fields.get(k), default=None)
            if xi is None or xi not in (0, 1):
                invalid_total += 1

    no_data = 1 if n_recent == 0 else 0

    missing_shares: dict[str, float] = {}
    missing_max_share = 0.0
    if n_recent > 0:
        for k in KEY_FLOAT_FIELDS + KEY_INT_FIELDS:
            c = int(missing.get(k, 0))
            s = c / n_recent
            missing_shares[k] = float(s)
            missing_max_share = max(missing_max_share, s)

    invalid_share = (invalid_total / (n_recent or 1))

    stuck_lob = 0
    if n_recent >= int(args.min_recent):
        all_stuck = True
        for k, eps in STUCK_KEYS:
            if k not in mins or k not in maxs:
                all_stuck = False
                break
            if abs(maxs[k] - mins[k]) >= float(eps):
                all_stuck = False
                break
        stuck_lob = 1 if all_stuck else 0

    issues: list[str] = []
    if no_data == 0 and missing_max_share > float(args.missing_max):
        issues.append(f"missing_max_share>{float(args.missing_max):.3f}")
    if no_data == 0 and invalid_share > float(args.invalid_max):
        issues.append(f"invalid_share>{float(args.invalid_max):.3f}")
    if no_data == 0 and stuck_lob == 1:
        issues.append("stuck_lob")

    out = {
        "ts_ms": str(now_ms),
        "stream": str(args.stream),
        "limit": str(args.limit),
        "window_sec": str(args.window_sec),
        "n_recent": int(n_recent),
        "no_data": int(no_data),
        "missing_max_share": float(f"{missing_max_share:.6f}"),
        "missing_max": float(args.missing_max),
        "invalid_share": float(f"{invalid_share:.6f}"),
        "invalid_max": float(args.invalid_max),
        "stuck_lob": int(stuck_lob),
        "issues": issues,
        "top_missing": _top_missing(missing, 10),
    }

    # Write compact summary to output stream (optional consumer/exporter can turn this into Prom metrics)
    with contextlib.suppress(Exception):
        r.xadd(args.out_stream, {k: json.dumps(v, ensure_ascii=False) if isinstance(v, (dict, list)) else str(v) for k, v in out.items()}, maxlen=2000, approximate=True)

    print(json.dumps(out, ensure_ascii=False))

    if issues:
        return 2

    return 0


if __name__ == "__main__":  # pragma: no cover
    logging.basicConfig(level=os.getenv("LOG_LEVEL", "INFO"))
    sys.exit(main())
