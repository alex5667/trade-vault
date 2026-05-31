#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""Micro-benchmark: LiqMap injection hot-path latency.

Mirror of: orderflow_services/bench_liqmap_injection_latency_v1.py
"""

import asyncio
import json
import statistics
import time
from typing import Any, Dict


class _FakeAsyncRedis:
    def __init__(self, payload: bytes):
        self._payload = payload
        self.get_calls = 0

    async def get(self, key: str):
        self.get_calls += 1
        return self._payload


def _snap(*, ts_ms: int, symbol: str, window: str) -> bytes:
    return json.dumps(
        {
            "v": 1,
            "ts_ms": int(ts_ms),
            "symbol": str(symbol),
            "window": str(window),
            "levels": [
                {"side": "ask", "price": 101.0, "usd": 400_000.0, "cnt": 10},
                {"side": "bid", "price": 99.0, "usd": 500_000.0, "cnt": 12},
                {"side": "ask", "price": 102.0, "usd": 50_000.0, "cnt": 5},
                {"side": "bid", "price": 98.0, "usd": 40_000.0, "cnt": 4},
            ],
        }
    ).encode("utf-8")


def _pct(values, p: float) -> float:
    if not values:
        return float("nan")
    xs = sorted(values)
    k = int(round((len(xs) - 1) * p))
    return float(xs[max(0, min(len(xs) - 1, k))])


async def _run(*, iters: int, enable: bool) -> None:
    from services.orderflow.components.tick_processor import TickProcessor

    tp = TickProcessor.__new__(TickProcessor)
    tp._liqmap_cache = {}
    tp._liqmap_next_refresh_ts_ms = {}
    tp.redis = _FakeAsyncRedis(_snap(ts_ms=1_000_000 - 200, symbol="BTCUSDT", window="1h"))

    tp.liqmap_features_enable = bool(enable)
    tp.liqmap_features_windows = ["1h"]
    tp.liqmap_features_refresh_ms = 10_000
    tp.liqmap_features_fetch_interval_ms = 10_000
    tp.liqmap_features_failopen_stale_ms = 120_000
    tp.liqmap_snapshot_key_prefix = "liqmap:snapshot"
    tp.liqmap_near_band_bps = 200.0
    tp.liqmap_peak_min_share = 0.05

    class _Runtime:
        symbol = "BTCUSDT"

    warm_ind: Dict[str, Any] = {}
    await tp._inject_liqmap_features(runtime=_Runtime(), now_ms=1_000_000, price=100.0, indicators=warm_ind)

    dts_us = []
    base_ms = 1_000_000
    for i in range(iters):
        ind: Dict[str, Any] = {}
        t0 = time.perf_counter_ns()
        await tp._inject_liqmap_features(runtime=_Runtime(), now_ms=base_ms + i, price=100.0, indicators=ind)
        t1 = time.perf_counter_ns()
        dts_us.append((t1 - t0) / 1000.0)

    label = "ENABLED" if enable else "DISABLED"
    print(f"\n=== LiqMap injection latency ({label}) ===")
    print(f"iters: {iters}")
    print(f"redis.get calls (total): {tp.redis.get_calls}")
    print(f"p50_us: {statistics.median(dts_us):.2f}")
    print(f"p95_us: {_pct(dts_us, 0.95):.2f}")
    print(f"p99_us: {_pct(dts_us, 0.99):.2f}")


def main():
    import argparse

    ap = argparse.ArgumentParser()
    ap.add_argument("--iters", type=int, default=5000)
    args = ap.parse_args()

    asyncio.run(_run(iters=int(args.iters), enable=False))
    asyncio.run(_run(iters=int(args.iters), enable=True))


if __name__ == "__main__":
    main()
