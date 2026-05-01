from __future__ import annotations
"""Micro-benchmark: overhead of tick-quality EMA computations.

This does NOT require Redis. It benchmarks pure Python update path:
- TickQualityEMA.update()
- metric label limiter + throttle checks

Usage:
  python -m tools.bench_tick_quality_overhead --n 200000 --symbols 50
"""

from utils.time_utils import get_ny_time_millis

import argparse
import os
import random
import time

from services.orderflow.tick_quality_ema import TickQualityEMA
from services.orderflow.metric_labels import TickMetricLimiter, _parse_allowlist, should_emit


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=200_000)
    ap.add_argument("--symbols", type=int, default=50)
    ap.add_argument("--tau-ms", type=int, default=300_000)
    ap.add_argument("--emit-min-ms", type=int, default=250)
    args = ap.parse_args()

    rng = random.Random(42)
    symbols = [f"SYM{i:03d}USDT" for i in range(int(args.symbols))]
    ema = TickQualityEMA(tau_ms=int(args.tau_ms))

    allow = _parse_allowlist(os.getenv("TICK_QUALITY_SYMBOL_ALLOWLIST"))
    limiter = TickMetricLimiter(
        allowlist=allow,
        mode=os.getenv("TICK_QUALITY_SYMBOL_LABEL_MODE", "collapse"),
        ema_min_update_ms=int(args.emit_min_ms),
    )
    last_emit = {}

    t0 = time.perf_counter()
    now_ms = get_ny_time_millis()
    for i in range(int(args.n)):
        sym = symbols[i % len(symbols)]
        # simulate:
        # unknown_side: 0/1
        unknown = 1.0 if (i % 17 == 0) else 0.0
        # ts_source flags:
        now_src = 1.0 if (i % 29 == 0) else 0.0
        stream_src = 1.0 if (i % 31 == 0) else 0.0
        skew_abs = float(rng.randint(0, 50_000))
        age_abs = float(rng.randint(0, 50_000))

        now_ms += 1
        ts_source = "now" if now_src > 0.5 else ("stream_id" if stream_src > 0.5 else "event")
        ema_vals = ema.update(
            symbol=sym,
            ts_ms=now_ms,
            unknown_side=unknown,
            ts_source=ts_source,
            abs_skew_ms=skew_abs,
            abs_age_ms=age_abs,
        )

        lab = limiter.label(sym)
        if lab is None:
            continue
        lm = last_emit.get(lab, 0)
        if should_emit(now_ms, lm, limiter.ema_min_update_ms):
            last_emit[lab] = now_ms
            # emulate gauge.set() cost minimally (no prometheus import here)
            _ = (
                ema_vals["unknown"],
                ema_vals["ts_now"],
                ema_vals["ts_stream_id"],
                ema_vals["skew_abs_ms"],
                ema_vals["age_abs_ms"],
            )


    t1 = time.perf_counter()
    dt = t1 - t0
    per = dt / float(args.n)
    print(f"n={args.n} symbols={args.symbols} dt_s={dt:.6f} per_tick_us={per*1e6:.3f}")
    print(f"emit_labels={len(last_emit)}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
