from __future__ import annotations

"""A8 — Smoke-check for new derived microstructure features.

Goal
----
Detect wiring / runtime regressions where new features become "stuck" (e.g. always 0)
while the stream is alive, and detect NaN explosions early.

Why stream-based smoke-check
----------------------------
We validate the Redis stream `metrics:of_gate` because:
- it is produced in the same hot path as the model inputs
- it is independent from Prometheus scrape timing
- it makes it easy to run from a timer worker and emit a single alert/event

Alert conditions (v1)
---------------------
1) realized_vol_bps == 0 while there is enough data to compute it
   (proxy: realized_vol_no_data == 0 seen frequently in the recent window)
2) NaN_rate across the tracked fields exceeds a threshold.

Exit code
---------
- 0: ok / no alert
- 2: alert condition detected

The script always prints a single JSON line to stdout for the timer worker.
"""

import argparse
import json
import math
import os
import sys
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import redis

from utils.time_utils import get_ny_time_millis

KEY_FIELDS: tuple[str, ...] = (
    # A8 gauges / stream fields
    "depth_total_10",
    "gini_depth_10",
    "vwap_roll_diff_bps",
    "price_momentum_bps",
    "realized_vol_bps",
    "pressure_per_min",
    "liquidity_pressure",
    "info_flow",
)


def _now_ms() -> int:
    return get_ny_time_millis()


def _as_str(v: Any) -> str:
    if v is None:
        return ""
    if isinstance(v, (bytes, bytearray)):
        try:
            return v.decode("utf-8")
        except Exception:
            return str(v)
    return str(v)


def _parse_float(v: Any) -> float | None:
    if v is None:
        return None
    try:
        s = _as_str(v).strip()
        if s == "":
            return None
        return float(s)
    except Exception:
        return None


def _parse_int(v: Any) -> int | None:
    if v is None:
        return None
    try:
        s = _as_str(v).strip()
        if s == "":
            return None
        return int(float(s))
    except Exception:
        return None


def _is_bad_number(x: float | None) -> bool:
    if x is None:
        return True
    return not math.isfinite(x)


@dataclass
class RecentRow:
    ts_ms: int
    data: dict[str, Any]


def _read_recent_rows(
    r: redis.Redis,
    stream: str,
    *,
    recent_s: int,
    limit: int,
) -> tuple[list[RecentRow], dict[str, Any]]:
    """Read tail of a Redis stream and keep only rows within `recent_s` window."""

    now_ms = _now_ms()
    cutoff = now_ms - int(recent_s * 1000)

    # Use XREVRANGE for efficiency (tail-first).
    raw = r.xrevrange(stream, max="+", min="-", count=limit)

    n_total = len(raw)
    recent: list[RecentRow] = []
    max_ts_ms = 0

    for _id, fields in raw:
        # ts_ms is a required part of the metrics contract; still be defensive.
        ts = _parse_int(fields.get(b"ts_ms") if isinstance(fields, dict) else None)
        if ts is None:
            ts = _parse_int(fields.get("ts_ms")) if isinstance(fields, dict) else None
        ts = int(ts or 0)
        if ts > max_ts_ms:
            max_ts_ms = ts
        if ts >= cutoff:
            recent.append(RecentRow(ts_ms=ts, data=fields))

    meta = {
        "now_ms": now_ms,
        "cutoff_ms": cutoff,
        "n_total": n_total,
        "n_recent": len(recent),
        "max_ts_ms": max_ts_ms,
        "age_ms": (now_ms - max_ts_ms) if max_ts_ms > 0 else None,
    }
    return recent, meta


def _compute_nan_rate(rows: Iterable[RecentRow]) -> tuple[float, int, int, dict[str, int]]:
    bad = 0
    total = 0
    per_key_bad: dict[str, int] = dict.fromkeys(KEY_FIELDS, 0)

    for rr in rows:
        fields = rr.data
        for k in KEY_FIELDS:
            v = fields.get(k) if isinstance(fields, dict) else None
            if v is None and isinstance(fields, dict):
                v = fields.get(k.encode("utf-8"))
            x = _parse_float(v)
            total += 1
            if _is_bad_number(x):
                bad += 1
                per_key_bad[k] += 1

    rate = float(bad) / float(total) if total > 0 else 0.0
    return rate, bad, total, per_key_bad


def _compute_realized_vol_stuck(
    rows: Iterable[RecentRow],
    *,
    min_ready: int,
    eps_bps: float,
) -> tuple[bool, dict[str, Any]]:
    """Return (stuck, details)."""

    ready = 0
    max_abs = 0.0
    n_seen = 0

    for rr in rows:
        fields = rr.data

        rv_nd = fields.get("realized_vol_no_data") if isinstance(fields, dict) else None
        if rv_nd is None and isinstance(fields, dict):
            rv_nd = fields.get(b"realized_vol_no_data")
        rv_nd_i = _parse_int(rv_nd)

        rv = fields.get("realized_vol_bps") if isinstance(fields, dict) else None
        if rv is None and isinstance(fields, dict):
            rv = fields.get(b"realized_vol_bps")
        rv_f = _parse_float(rv)

        n_seen += 1
        if rv_nd_i == 0:
            ready += 1
            if rv_f is not None and math.isfinite(rv_f):
                a = abs(float(rv_f))
                if a > max_abs:
                    max_abs = a

    stuck = (ready >= int(min_ready)) and (max_abs <= float(eps_bps))
    details = {
        "rv_ready": ready,
        "rv_seen": n_seen,
        "rv_eps_bps": float(eps_bps),
        "rv_max_abs_bps": float(max_abs),
    }
    return stuck, details


def _compute_ready_counts(rows: Iterable[RecentRow]) -> dict[str, int]:
    """Count how often rolling trackers report ready (no_data==0).

    These counters help detect a different class of wiring regressions where a feature
    never becomes ready (no_data stays 1 forever) even though the stream is alive.
    """

    rv_ready = 0
    vwap_ready = 0
    for rr in rows:
        fields = rr.data

        rv_nd = fields.get("realized_vol_no_data") if isinstance(fields, dict) else None
        if rv_nd is None and isinstance(fields, dict):
            rv_nd = fields.get(b"realized_vol_no_data")
        rv_nd_i = _parse_int(rv_nd)
        if rv_nd_i == 0:
            rv_ready += 1

        vw_nd = fields.get("vwap_roll_no_data") if isinstance(fields, dict) else None
        if vw_nd is None and isinstance(fields, dict):
            vw_nd = fields.get(b"vwap_roll_no_data")
        vw_nd_i = _parse_int(vw_nd)
        if vw_nd_i == 0:
            vwap_ready += 1

    return {"rv_ready": rv_ready, "vwap_ready": vwap_ready}


def main(argv: list[str] | None = None) -> None:
    ap = argparse.ArgumentParser()

    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://localhost:6379/0"))
    ap.add_argument("--stream", default=os.getenv("OF_GATE_METRICS_STREAM", "metrics:of_gate"))
    ap.add_argument("--out-stream", default=os.getenv("A8_SMOKE_OUT_STREAM", "sre:new_features_smoke"))

    ap.add_argument("--recent-s", type=int, default=int(os.getenv("A8_SMOKE_RECENT_S", "600")))
    ap.add_argument("--limit", type=int, default=int(os.getenv("A8_SMOKE_LIMIT", "2000")))

    ap.add_argument("--nan-rate-max", type=float, default=float(os.getenv("A8_SMOKE_NAN_RATE_MAX", "0.01")))
    ap.add_argument("--rv-never-ready-min-recent", type=int, default=int(os.getenv("A8_SMOKE_RV_NEVER_READY_MIN_RECENT", "200")))

    ap.add_argument("--rv-min-ready", type=int, default=int(os.getenv("A8_SMOKE_RV_MIN_READY", "40")))
    ap.add_argument("--rv-eps-bps", type=float, default=float(os.getenv("A8_SMOKE_RV_EPS_BPS", "1e-6")))

    ap.add_argument("--maxlen", type=int, default=int(os.getenv("A8_SMOKE_OUT_MAXLEN", "2000")))

    args = ap.parse_args(argv)

    out: dict[str, Any] = {
        "ts_ms": _now_ms(),
        "stream": args.stream,
        "out_stream": args.out_stream,
        "recent_s": int(args.recent_s),
        "limit": int(args.limit),
        "key_fields": list(KEY_FIELDS),
    }

    alert = False
    issues: list[str] = []

    try:
        r = redis.Redis.from_url(args.redis_url, decode_responses=False)
        # quick connectivity check
        r.ping()

        rows, meta = _read_recent_rows(r, args.stream, recent_s=args.recent_s, limit=args.limit)
        out.update(meta)

        if meta.get("n_recent", 0) <= 0:
            out["no_data"] = True
        else:
            out["no_data"] = False

            nan_rate, bad_slots, total_slots, per_key_bad = _compute_nan_rate(rows)
            out.update(
                {
                    "nan_rate": float(nan_rate),
                    "nan_bad_slots": int(bad_slots),
                    "nan_total_slots": int(total_slots),
                    "nan_per_key_bad": per_key_bad,
                }
            )
            if float(nan_rate) > float(args.nan_rate_max):
                alert = True
                issues.append(f"nan_rate>{args.nan_rate_max}")

            stuck_rv, rv_details = _compute_realized_vol_stuck(
                rows,
                min_ready=args.rv_min_ready,
                eps_bps=args.rv_eps_bps,
            )
            out.update(rv_details)
            out["stuck_realized_vol"] = bool(stuck_rv)

            ready_counts = _compute_ready_counts(rows)
            out.update(ready_counts)

            if stuck_rv:
                alert = True
                issues.append("realized_vol_stuck_zero")

            # Wiring regression: tracker never becomes ready.
            # For liquid symbols, `*_no_data` should flip to 0 quickly.
            if int(meta.get("n_recent", 0)) >= int(args.rv_never_ready_min_recent):
                if int(ready_counts.get("rv_ready", 0)) == 0:
                    alert = True
                    issues.append("realized_vol_never_ready")
                if int(ready_counts.get("vwap_ready", 0)) == 0:
                    alert = True
                    issues.append("vwap_roll_never_ready")

        out["alert"] = bool(alert)
        out["issues"] = issues

        # Emit a small event for dashboards / alerting / forensics.
        try:
            r.xadd(
                args.out_stream,
                {k: json.dumps(v) if isinstance(v, (dict, list)) else _as_str(v) for k, v in out.items()},
                maxlen=int(args.maxlen),
                approximate=True,
            )
        except Exception:
            # never fail the smoke-check due to the out-stream
            pass

        print(json.dumps(out, ensure_ascii=False, sort_keys=True))
        sys.exit(2 if alert else 0)

    except SystemExit:
        raise
    except Exception as e:
        out["error"] = f"{type(e).__name__}: {e}"
        out["alert"] = True
        out["issues"] = ["exception"]
        print(json.dumps(out, ensure_ascii=False, sort_keys=True))
        sys.exit(2)


if __name__ == "__main__":
    main()
