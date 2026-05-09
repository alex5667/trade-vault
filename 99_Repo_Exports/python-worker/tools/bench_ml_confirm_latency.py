from __future__ import annotations

import argparse
import json
import os
from typing import Any

import redis

from common.redis_errors import retry_redis_operation
from utils.time_utils import get_ny_time_millis


def _now_ms() -> int:
    """Get current timestamp in milliseconds."""
    return get_ny_time_millis()


def _f(x: Any, d: float = 0.0) -> float:
    """Safe float conversion with default value."""
    try:
        if x is None:
            return d
        return float(x)
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    """Safe int conversion with default value."""
    try:
        if x is None:
            return d
        return int(float(x))
    except Exception:
        return d


def pctl(xs: list[float], q: float) -> float:
    """
    Calculate percentile from sorted list.
    
    Args:
        xs: List of float values
        q: Quantile (0.0 to 1.0)
    
    Returns:
        Percentile value
    """
    if not xs:
        return 0.0
    xs = sorted(xs)
    i = int(round((len(xs) - 1) * q))
    i = max(0, min(len(xs) - 1, i))
    return float(xs[i])


def _read_stream_window(r: redis.Redis, stream: str, start_ms: int, window_ms: int, *, max_scan: int = 300000) -> list[dict[str, Any]]:
    """
    Read messages from Redis stream within time window.
    
    Scans stream backwards from latest, collecting messages within [start_ms, start_ms + window_ms].
    Stops early if timestamp goes below start_ms.
    
    Args:
        r: Redis client
        stream: Stream name
        start_ms: Start timestamp (ms)
        window_ms: Window size (ms)
        max_scan: Maximum messages to scan (safety limit)
    
    Returns:
        List of message dicts with _ts_ms field added, sorted by timestamp
    """
    end_ms = start_ms + window_ms
    rows: list[dict[str, Any]] = []
    last_id = "+"
    scanned = 0
    while scanned < max_scan:
        batch = retry_redis_operation(
            lambda: r.xrevrange(stream, max=last_id, min="-", count=2000),
            operation_name=f"xrevrange {stream}",
        )
        if not batch:
            break
        if len(batch) == 1 and batch[0][0] == last_id:
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


def main() -> None:
    """
    Benchmark ML confirm latency metrics from Redis stream.
    
    Reads metrics:ml_confirm stream for last N minutes, computes:
    - Latency percentiles (p50/p95/p99)
    - p_edge percentiles (p50/p10/p90)
    - Allow/abstain/missing/error rates
    - Confidence percentiles (if available)
    - QPS
    
    Outputs JSON to file or stdout.
    """
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--stream", default=os.getenv("ML_CONFIRM_METRICS_STREAM", "metrics:ml_confirm"))
    ap.add_argument("--window-min", type=int, default=int(os.getenv("ML_BENCH_WINDOW_MIN", "60")))
    ap.add_argument("--out", default=os.getenv("ML_BENCH_OUT", ""))
    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    window_ms = int(args.window_min) * 60_000
    start_ms = _now_ms() - window_ms
    rows = _read_stream_window(r, args.stream, start_ms, window_ms)

    lat_ms: list[float] = []
    pedge: list[float] = []
    allow_n = 0
    abstain_n = 0
    miss_n = 0
    err_n = 0
    conf: list[float] = []

    for d in rows:
        pedge.append(_f(d.get("p_edge", 0.0), 0.0))
        # Support both latency_ms and latency_us fields
        if (d.get("latency_ms", "") or "").strip() != "":
            lat_ms.append(_f(d.get("latency_ms", 0.0), 0.0))
        else:
            lat_ms.append(_f(d.get("latency_us", 0.0), 0.0) / 1000.0)
        allow_n += 1 if _i(d.get("allow", 0), 0) == 1 else 0
        abstain_n += 1 if _i(d.get("abstain", 0), 0) == 1 else 0
        st = (d.get("status", "") or "").upper()
        miss_flag = _i(d.get("missing", d.get("missing_n", 0)), 0) > 0 or st.startswith("MISSING")
        miss_n += 1 if miss_flag else 0
        err_s = (d.get("err", d.get("error", "")) or "").strip()
        err_n += 1 if err_s != "" else 0
        if (d.get("conf", "") or "").strip() != "":
            conf.append(_f(d.get("conf", 0.0), 0.0))

    n = len(rows)
    qps = float(n) / float(window_ms / 1000.0) if window_ms > 0 else 0.0
    out = {
        "window_min": int(args.window_min),
        "n": int(n),
        "qps": float(qps),
        "latency_ms": {"p50": pctl(lat_ms, 0.50), "p95": pctl(lat_ms, 0.95), "p99": pctl(lat_ms, 0.99)},
        "p_edge": {"p50": pctl(pedge, 0.50), "p10": pctl(pedge, 0.10), "p90": pctl(pedge, 0.90)},
        "allow_rate": float(allow_n / n) if n > 0 else 0.0,
        "abstain_rate": float(abstain_n / n) if n > 0 else 0.0,
        "missing_rate": float(miss_n / n) if n > 0 else 0.0,
        "err_rate": float(err_n / n) if n > 0 else 0.0,
        "conf": {"p50": pctl(conf, 0.50), "p10": pctl(conf, 0.10)} if conf else {"p50": 0.0, "p10": 0.0},
        "ts_ms": _now_ms(),
    }

    if args.out:
        os.makedirs(os.path.dirname(args.out) or ".", exist_ok=True)
        with open(args.out, "w", encoding="utf-8") as f:
            json.dump(out, f, ensure_ascii=False, indent=2)
    else:
        print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

