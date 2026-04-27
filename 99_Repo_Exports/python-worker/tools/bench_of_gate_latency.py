from __future__ import annotations
from utils.time_utils import get_ny_time_millis

import argparse
import json
import os
import time
from typing import Any, Dict, List

import redis


def _now_ms() -> int:
    return get_ny_time_millis()


def _f(x: Any, d: float = 0.0) -> float:
    try:
        if x is None:
            return d
        return float(x)
    except Exception:
        return d


def _i(x: Any, d: int = 0) -> int:
    try:
        if x is None:
            return d
        return int(float(x))
    except Exception:
        return d


def pctl(xs: List[float], q: float) -> float:
    if not xs:
        return 0.0
    xs = sorted(xs)
    i = int(round((len(xs) - 1) * q))
    i = max(0, min(len(xs) - 1, i))
    return float(xs[i])


def _read_stream_window(r: redis.Redis, stream: str, start_ms: int, window_ms: int, *, max_scan: int = 400000) -> List[Dict[str, Any]]:
    end_ms = start_ms + window_ms
    rows: List[Dict[str, Any]] = []
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
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--stream", default=os.getenv("OF_GATE_METRICS_STREAM", "metrics:of_gate"))
    ap.add_argument("--window-min", type=int, default=int(os.getenv("OF_BENCH_WINDOW_MIN", "60")))
    ap.add_argument("--out", default=os.getenv("OF_BENCH_OUT", ""))
    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    window_ms = int(args.window_min) * 60_000
    start_ms = _now_ms() - window_ms
    rows = _read_stream_window(r, args.stream, start_ms, window_ms)

    lat_us: List[float] = []
    ml_lat_us: List[float] = []
    execn: List[float] = []
    ok = 0
    soft = 0
    meta_veto = 0
    book_bad = 0
    src_bad = 0
    dh_bad = 0

    dh_bad_th = float(os.getenv("OF_BENCH_DH_BAD_TH", "0.70") or 0.70)

    for d in rows:
        lat = _f(d.get("latency_us", 0.0), 0.0)
        if lat > 0:
            lat_us.append(lat)
        mlat = _f(d.get("ml_latency_us", 0.0), 0.0)
        if mlat > 0:
            ml_lat_us.append(mlat)
        en = _f(d.get("exec_risk_norm", 0.0), 0.0)
        if en > 0:
            execn.append(en)

        ok += 1 if _i(d.get("ok", 0), 0) == 1 else 0
        soft += 1 if _i(d.get("ok_soft", 0), 0) == 1 else 0
        meta_veto += 1 if _i(d.get("meta_veto", 0), 0) == 1 else 0
        book_bad += 1 if _i(d.get("book_health_ok", 1), 1) == 0 else 0
        src_bad += 1 if _i(d.get("source_consistency_ok", 1), 1) == 0 else 0
        dh = _f(d.get("data_health", 1.0), 1.0)
        dh_bad += 1 if dh < dh_bad_th else 0

    n = len(rows)
    qps = float(n) / float(window_ms / 1000.0) if window_ms > 0 else 0.0

    out = {
        "window_min": int(args.window_min),
        "n": int(n),
        "qps": float(qps),
        "build_latency_us": {"p50": pctl(lat_us, 0.50), "p95": pctl(lat_us, 0.95), "p99": pctl(lat_us, 0.99)},
        "ml_check_latency_us": {"p50": pctl(ml_lat_us, 0.50), "p95": pctl(ml_lat_us, 0.95), "p99": pctl(ml_lat_us, 0.99)},
        "exec_risk_norm": {"p50": pctl(execn, 0.50), "p90": pctl(execn, 0.90), "p99": pctl(execn, 0.99)},
        "ok_rate": float(ok / n) if n > 0 else 0.0,
        "soft_rate": float(soft / n) if n > 0 else 0.0,
        "meta_veto_rate": float(meta_veto / n) if n > 0 else 0.0,
        "book_bad_rate": float(book_bad / n) if n > 0 else 0.0,
        "source_inconsistency_rate": float(src_bad / n) if n > 0 else 0.0,
        "data_health_bad_rate": float(dh_bad / n) if n > 0 else 0.0,
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
