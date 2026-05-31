from __future__ import annotations

"""Нагрузочный/latency bench: synthetic + "from inputs" bench, p50/p95/p99 + budgets.

Why:
  Latency budgets (p50/p95/p99, ticks/s) для production monitoring.
  Добавить измерение dt в прод-метрики.

Usage:
  python -m tools.bench_of_confirm_engine --n 20000 --warmup 2000 --out /tmp/bench.json
"""


import argparse
import json
import time
from typing import Any

from core.of_confirm_engine import OFConfirmEngine


def pctl(xs: list[float], q: float) -> float:
    if not xs:
        return 0.0
    xs = sorted(xs)
    i = int(round((len(xs)-1)*q))
    i = max(0, min(len(xs)-1, i))
    return float(xs[i])


class _Runtime:
    def __init__(self) -> None:
        self.dynamic_cfg = {}
        self.pressure = None
        self.book_churn_hi = 0
        self.last_regime = "na"
        self.last_wp = type("wp", (), {"weak_any": False})()
        self.last_sweep = None
        self.last_reclaim = None
        self.last_obi_event = None
        self.last_iceberg_event = None
        self.last_ofi_event = None
        self.last_fp_edge = None

    def __getattr__(self, _name: str) -> Any:
        return None


def main() -> None:
    ap = argparse.ArgumentParser()
    ap.add_argument("--n", type=int, default=20000)
    ap.add_argument("--warmup", type=int, default=2000)
    ap.add_argument("--out", required=True)
    args = ap.parse_args()

    eng = OFConfirmEngine()
    rt = _Runtime()
    cfg = {}
    indicators = {
        "spread_bps": 2.0,
        "expected_slippage_bps": 1.0,
        "cancel_bid_rate_ema": 0.0,
        "cancel_ask_rate_ema": 0.0,
        "taker_buy_rate_ema": 0.0,
        "taker_sell_rate_ema": 0.0,
        "bucket_id": 0,
    }

    # warmup
    for i in range(args.warmup):
        eng.build(
            symbol="BTCUSDT",
            tf="1s",
            direction="LONG",
            tick_ts_ms=1700000000000+i,
            price=50000.0,
            delta_z=2.0,
            runtime=rt,
            cfg=cfg,
            indicators=indicators,
            absorption=None,
        )

    dts = []
    t0 = time.perf_counter()
    for i in range(args.n):
        t1 = time.perf_counter()
        eng.build(
            symbol="BTCUSDT",
            tf="1s",
            direction="LONG",
            tick_ts_ms=1700000000000+i,
            price=50000.0,
            delta_z=2.0,
            runtime=rt,
            cfg=cfg,
            indicators=indicators,
            absorption=None,
        )
        t2 = time.perf_counter()
        dts.append((t2 - t1) * 1e6)  # us
    t_end = time.perf_counter()

    out = {
        "n": args.n,
        "throughput_calls_per_s": args.n / (t_end - t0),
        "p50_us": pctl(dts, 0.50),
        "p95_us": pctl(dts, 0.95),
        "p99_us": pctl(dts, 0.99),
        "max_us": max(dts) if dts else 0.0,
    }
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

