from __future__ import annotations
"""Assert latency and throughput budgets from benchmark results.

Enforces performance SLAs:
  - p99 latency must not exceed threshold (default: 3000us)
  - Throughput must meet minimum (default: 2000 calls/s)

Usage:
  python -m tools.assert_latency_budget --bench-json /path/to/bench.json --p99-us-max 3000 --throughput-min 2000
"""


import argparse
import json
import os


def main() -> None:
    ap = argparse.ArgumentParser(description="Assert latency and throughput budgets")
    ap.add_argument("--bench-json", required=True, help="benchmark output JSON from bench_replay_throughput")
    ap.add_argument("--p99-us-max", type=float, default=float(os.getenv("LAT_P99_US_MAX", "3000") or 3000), help="max allowed p99 latency in microseconds (default: 3000)")
    ap.add_argument("--throughput-min", type=float, default=float(os.getenv("THROUGHPUT_MIN", "2000") or 2000), help="min required throughput in calls/s (default: 2000)")
    args = ap.parse_args()

    rep = json.loads(open(args.bench_json, "r", encoding="utf-8").read())
    p99 = float(rep.get("p99_us", 0.0))
    thr = float(rep.get("throughput_calls_per_s", 0.0))

    if p99 > args.p99_us_max:
        raise SystemExit(f"latency_budget_exceeded p99_us={p99:.0f} > {args.p99_us_max:.0f}")
    if thr < args.throughput_min:
        raise SystemExit(f"throughput_too_low calls/s={thr:.0f} < {args.throughput_min:.0f}")


if __name__ == "__main__":
    main()

