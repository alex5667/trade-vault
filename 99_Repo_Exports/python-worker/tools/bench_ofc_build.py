#!/usr/bin/env python3

from __future__ import annotations


import argparse

from pathlib import Path

from typing import Any, Dict, List

import sys

import time


ROOT = Path(__file__).resolve().parents[1]

sys.path.insert(0, str(ROOT))


from core.of_confirm_engine import OFConfirmEngine  # noqa

from tools.ofc_common import ReplayRuntime, iter_ndjson  # noqa



def pct(xs: List[int], p: float) -> int:

    if not xs:

        return 0

    xs2 = sorted(xs)

    k = int(round((p / 100.0) * (len(xs2) - 1)))

    k = max(0, min(len(xs2) - 1, k))

    return xs2[k]



def main() -> int:

    ap = argparse.ArgumentParser(description="Micro-bench OFConfirmEngine.build() on OFC_CAPTURE sample.")

    ap.add_argument("--input", required=True)

    ap.add_argument("--warmup", type=int, default=50)

    ap.add_argument("--iters", type=int, default=300)

    ap.add_argument("--sort", default="bucket_id", choices=["bucket_id", "tick_ts_ms", "none"])

    args = ap.parse_args()


    rows = list(iter_ndjson(args.input))

    if args.sort != "none":

        rows.sort(key=lambda r: (r.get(args.sort) is None, r.get(args.sort, 0)))

    if not rows:

        print("No rows.")

        return 2


    engine = OFConfirmEngine(version=3)


    # warmup

    for i in range(min(args.warmup, len(rows))):

        r = rows[i]

        runtime = ReplayRuntime.from_snapshot(symbol=r.get("symbol", ""), snap=r.get("runtime_snapshot") or {})

        indicators = dict(r.get("indicators") or {})

        if indicators.get("bucket_id") is None and r.get("bucket_id") is not None:

            indicators["bucket_id"] = r.get("bucket_id")

        engine.build(

            symbol=str(r.get("symbol", "")),

            tf=str(r.get("tf", "1s")),

            direction=str(r.get("direction", "")),

            tick_ts_ms=int(r.get("tick_ts_ms", 0) or 0),

            price=float(r.get("price", 0.0) or 0.0),

            delta_z=float(r.get("delta_z", 0.0) or 0.0),

            runtime=runtime,

            cfg=r.get("cfg") or {},

            indicators=indicators,

            absorption=r.get("absorption") if isinstance(r.get("absorption"), dict) else None,

        )


    # iters

    lat_us: List[int] = []

    for i in range(args.iters):

        r = rows[i % len(rows)]

        runtime = ReplayRuntime.from_snapshot(symbol=r.get("symbol", ""), snap=r.get("runtime_snapshot") or {})

        indicators = dict(r.get("indicators") or {})

        if indicators.get("bucket_id") is None and r.get("bucket_id") is not None:

            indicators["bucket_id"] = r.get("bucket_id")

        t0 = time.perf_counter_ns()

        engine.build(

            symbol=str(r.get("symbol", "")),

            tf=str(r.get("tf", "1s")),

            direction=str(r.get("direction", "")),

            tick_ts_ms=int(r.get("tick_ts_ms", 0) or 0),

            price=float(r.get("price", 0.0) or 0.0),

            delta_z=float(r.get("delta_z", 0.0) or 0.0),

            runtime=runtime,

            cfg=r.get("cfg") or {},

            indicators=indicators,

            absorption=r.get("absorption") if isinstance(r.get("absorption"), dict) else None,

        )

        lat_us.append(int((time.perf_counter_ns() - t0) / 1000))


    print(f"iters={len(lat_us)}")

    print(f"p50_us={pct(lat_us, 50)} p95_us={pct(lat_us, 95)} p99_us={pct(lat_us, 99)} max_us={max(lat_us)}")

    return 0



if __name__ == "__main__":

    raise SystemExit(main())
