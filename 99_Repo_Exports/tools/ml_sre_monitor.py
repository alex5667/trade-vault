from __future__ import annotations

import argparse
import os
from typing import Any, Dict, List

import redis

from tools._ml_common import now_ms, pctl, safe_float, safe_int

def _read_stream_window(r: redis.Redis, stream: str, since_ms: int, max_scan: int) -> List[Dict[str, Any]]:
    rows = []
    last_id = "+"
    scanned = 0
    while scanned < max_scan:
        batch = r.xrevrange(stream, max=last_id, min="-", count=2000)
        if not batch:
            break
        for msg_id, fields in batch:
            scanned += 1
            if msg_id == last_id:
                continue
            last_id = msg_id
            ts = safe_int(fields.get("ts_ms", fields.get("ts", fields.get("timestamp", 0))), 0)
            if ts and ts < since_ms:
                scanned = max_scan
                break
            row = dict(fields)
            row["_ts_ms"] = ts
            rows.append(row)
    rows.reverse()
    return rows

def _notify(r: redis.Redis, text: str) -> None:
    stream = os.getenv("NOTIFY_TELEGRAM_STREAM", "notify:telegram")
    r.xadd(stream, {"type":"report","text":text,"ts":str(now_ms())}, maxlen=200000, approximate=True)

def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--redis-url", default=os.getenv("REDIS_URL", "redis://redis-worker-1:6379/0"))
    ap.add_argument("--stream", default=os.getenv("ML_CONFIRM_METRICS_STREAM", "metrics:ml_confirm"))
    ap.add_argument("--since-min", type=int, default=60)
    ap.add_argument("--max-scan", type=int, default=300000)
    args = ap.parse_args()

    r = redis.Redis.from_url(args.redis_url, decode_responses=True)
    since_ms = now_ms() - args.since_min * 60_000
    rows = _read_stream_window(r, args.stream, since_ms, args.max_scan)
    if not rows:
        return

    p = [safe_float(x.get("p_edge", 0.0), 0.0) for x in rows]
    lat = [safe_float(x.get("latency_ms", 0.0), 0.0) for x in rows]
    allow = [1 if str(x.get("allow","")).lower() in ("1","true","yes") else 0 for x in rows]
    mode = str(rows[-1].get("mode","")).upper()

    miss = sum(1 for x in rows if str(x.get("status","")).upper() == "MISSING")
    err = sum(1 for x in rows if str(x.get("status","")).upper() == "ERR")

    n = len(rows)
    allow_rate = sum(allow) / max(1,n)

    # thresholds (tune)
    p50_min = float(os.getenv("ML_SRE_PEDGE_P50_MIN", "0.20") or 0.20)
    miss_rate_max = float(os.getenv("ML_SRE_MISSING_RATE_MAX", "0.02") or 0.02)
    err_rate_max = float(os.getenv("ML_SRE_ERR_RATE_MAX", "0.01") or 0.01)
    lat_p99_max = float(os.getenv("ML_SRE_LAT_P99_MAX_MS", "6.0") or 6.0)

    miss_rate = miss / max(1,n)
    err_rate = err / max(1,n)

    p50 = pctl(p, 0.50)
    lat_p99 = pctl(lat, 0.99)

    alerts = []
    if p50 < p50_min:
        alerts.append(f"p_edge_p50<{p50_min}")
    if miss_rate > miss_rate_max:
        alerts.append(f"missing_rate>{miss_rate_max}")
    if err_rate > err_rate_max:
        alerts.append(f"err_rate>{err_rate_max}")
    if lat_p99 > lat_p99_max:
        alerts.append(f"lat_p99>{lat_p99_max}ms")

    if alerts:
        txt = (
            "<b>ML_CONFIRM SRE ALERT</b>\n"
            f"mode=<code>{mode}</code> n=<code>{n}</code>\n"
            f"allow_rate=<code>{allow_rate:.3f}</code>\n"
            f"p50=<code>{p50:.3f}</code> p90=<code>{pctl(p,0.90):.3f}</code> p99=<code>{pctl(p,0.99):.3f}</code>\n"
            f"lat_p99_ms=<code>{lat_p99:.3f}</code>\n"
            f"missing_rate=<code>{miss_rate:.3f}</code> err_rate=<code>{err_rate:.3f}</code>\n"
            f"alerts=<code>{alerts}</code>"
        )
        _notify(r, txt)

if __name__ == "__main__":
    main()
