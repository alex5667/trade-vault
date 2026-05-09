from __future__ import annotations

"""
Step 17: AB-ish benchmark for tick ingest latency histograms.

Fetches Prometheus /metrics twice and computes histogram deltas over an interval,
then prints p50/p95/p99 for:
  - tick_ingest_process_ms
  - tick_ingest_e2e_delay_ms

Usage:
  python -m tools.bench_tick_ingest_latency_ab --url http://localhost:8000/metrics --interval 30
  python -m tools.bench_tick_ingest_latency_ab --url http://localhost:8000/metrics --interval 30 --symbol BTCUSDT
"""


import argparse
import json
import os
import time
import urllib.request
from dataclasses import dataclass


@dataclass
class Hist:
    # bucket upper bound -> cumulative count
    buckets: dict[float, float]
    count: float
    sum: float


def _http_get(url: str, timeout: float = 5.0) -> str:
    req = urllib.request.Request(url, headers={"User-Agent": "tick-ingest-bench/1.0"})
    with urllib.request.urlopen(req, timeout=timeout) as r:
        return r.read().decode("utf-8", errors="replace")


def _parse_labels(lbl: str) -> dict[str, str]:
    # very small parser for {k="v",...}
    out: dict[str, str] = {}
    if not lbl:
        return out
    cur = ""
    in_q = False
    parts = []
    for ch in lbl:
        if ch == '"':
            in_q = not in_q
            cur += ch
        elif ch == ',' and not in_q:
            parts.append(cur)
            cur = ""
        else:
            cur += ch
    if cur:
        parts.append(cur)
    for p in parts:
        if "=" not in p:
            continue
        k, v = p.split("=", 1)
        k = k.strip()
        v = v.strip()
        if v.startswith('"') and v.endswith('"'):
            v = v[1:-1]
        out[k] = v
    return out


def parse_histogram(text: str, name: str, symbol: str | None = None) -> Hist:
    buckets: dict[float, float] = {}
    count = 0.0
    summ = 0.0
    for line in text.splitlines():
        if not line or line[0] == "#":
            continue
        if not line.startswith(name):
            continue
        try:
            metric, value_s = line.split(None, 1)
            value = float(value_s.strip())
        except Exception:
            continue

        labels = {}
        base = metric
        if "{" in metric and metric.endswith("}"):
            base, rest = metric.split("{", 1)
            rest = rest[:-1]
            labels = _parse_labels(rest)

        if symbol and labels.get("symbol") not in (symbol,):
            continue

        if base == f"{name}_count":
            count += value
        elif base == f"{name}_sum":
            summ += value
        elif base == f"{name}_bucket":
            le_s = labels.get("le")
            if le_s is None:
                continue
            if le_s == "+Inf":
                le = float("inf")
            else:
                le = float(le_s)
            buckets[le] = buckets.get(le, 0.0) + value
    return Hist(buckets=buckets, count=count, sum=summ)


def hist_delta(a: Hist, b: Hist) -> Hist:
    out_b = {}
    for le, v in b.buckets.items():
        out_b[le] = v - a.buckets.get(le, 0.0)
    return Hist(buckets=out_b, count=b.count - a.count, sum=b.sum - a.sum)


def quantile_from_buckets(delta: Hist, q: float) -> float | None:
    if delta.count <= 0:
        return None
    items = sorted(delta.buckets.items(), key=lambda x: x[0])
    total = delta.count
    target = q * total
    prev_le = 0.0
    prev_c = 0.0
    for le, c in items:
        if c >= target:
            if le == float("inf"):
                return prev_le
            bucket_count = c - prev_c
            if bucket_count <= 0:
                return le
            frac = (target - prev_c) / bucket_count
            return prev_le + frac * (le - prev_le)
        prev_le, prev_c = le, c
    return items[-1][0] if items else None


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--url", default=os.getenv("TICK_INGEST_BENCH_METRICS_URL", "http://localhost:8000/metrics"))
    ap.add_argument("--interval", type=float, default=30.0)
    ap.add_argument("--symbol", default=None)
    ap.add_argument("--json", action="store_true")
    args = ap.parse_args()

    try:
        t1 = _http_get(args.url)
    except Exception as e:
        print(f"Error fetching metrics from {args.url}: {e}")
        return 1

    h1_proc = parse_histogram(t1, "tick_ingest_process_ms", symbol=args.symbol)
    h1_e2e = parse_histogram(t1, "tick_ingest_e2e_delay_ms", symbol=args.symbol)

    time.sleep(max(0.1, float(args.interval)))

    try:
        t2 = _http_get(args.url)
    except Exception as e:
        print(f"Error fetching metrics from {args.url}: {e}")
        return 1

    h2_proc = parse_histogram(t2, "tick_ingest_process_ms", symbol=args.symbol)
    h2_e2e = parse_histogram(t2, "tick_ingest_e2e_delay_ms", symbol=args.symbol)

    d_proc = hist_delta(h1_proc, h2_proc)
    d_e2e = hist_delta(h1_e2e, h2_e2e)

    out = {
        "symbol": args.symbol,
        "interval_s": args.interval,
        "process": {
            "count": d_proc.count,
            "avg_ms": (d_proc.sum / d_proc.count) if d_proc.count > 0 else None,
            "p50_ms": quantile_from_buckets(d_proc, 0.50),
            "p95_ms": quantile_from_buckets(d_proc, 0.95),
            "p99_ms": quantile_from_buckets(d_proc, 0.99),
        },
        "e2e": {
            "count": d_e2e.count,
            "avg_ms": (d_e2e.sum / d_e2e.count) if d_e2e.count > 0 else None,
            "p50_ms": quantile_from_buckets(d_e2e, 0.50),
            "p95_ms": quantile_from_buckets(d_e2e, 0.95),
            "p99_ms": quantile_from_buckets(d_e2e, 0.99),
        },
    }

    if args.json:
        print(json.dumps(out, ensure_ascii=False, indent=2))
    else:
        sym = args.symbol or "*"
        print(f"tick-ingest latency over {args.interval:.1f}s, symbol={sym}")
        p = out["process"]
        e = out["e2e"]
        print(f"process: n={p['count']:.0f} avg={p['avg_ms']:.2f}ms" if p['avg_ms'] else f"process: n={p['count']:.0f} avg=N/A", end=" ")
        if p['p50_ms']: print(f"p50={p['p50_ms']:.2f} p95={p['p95_ms']:.2f} p99={p['p99_ms']:.2f}")
        else: print("p50=N/A p95=N/A p99=N/A")

        print(f"e2e:     n={e['count']:.0f} avg={e['avg_ms']:.2f}ms" if e['avg_ms'] else f"e2e:     n={e['count']:.0f} avg=N/A", end=" ")
        if e['p50_ms']: print(f"p50={e['p50_ms']:.2f} p95={e['p95_ms']:.2f} p99={e['p99_ms']:.2f}")
        else: print("p50=N/A p95=N/A p99=N/A")
    return 0


if __name__ == "__main__":
    import sys
    sys.exit(main())
