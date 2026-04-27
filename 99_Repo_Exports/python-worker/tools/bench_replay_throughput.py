"""Benchmark engine replay throughput and latency.

Measures real-world performance of OFConfirmEngine.build() on replay inputs.
Reports p50/p95/p99 latency and throughput (calls/s).

Usage:
  python -m tools.bench_replay_throughput --inputs /path/to/inputs.ndjson --out /path/to/bench.json --n 20000
"""

from __future__ import annotations

import argparse
import json
import time
from typing import Any, Dict, Iterator, List

from core.of_confirm_engine import OFConfirmEngine


def iter_ndjson(path: str) -> Iterator[Dict[str, Any]]:
    """Iterator over NDJSON lines."""
    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            s = line.strip()
            if not s:
                continue
            yield json.loads(s)


def pctl(xs: List[float], q: float) -> float:
    """Calculate percentile from sorted list."""
    if not xs:
        return 0.0
    xs = sorted(xs)
    i = int(round((len(xs) - 1) * q))
    i = max(0, min(len(xs) - 1, i))
    return float(xs[i])


class _WpStub:
    """Weak progress stub."""
    def __init__(self, weak_any: bool = False) -> None:
        self.weak_any = weak_any


class _PressureStub:
    """Pressure stub."""
    def is_pressure_hi(self, *_args: Any, **_kwargs: Any) -> bool:
        return False


class RuntimeStub:
    """Minimal runtime stub for benchmark (no real market state)."""
    def __init__(self) -> None:
        self.dynamic_cfg: Dict[str, Any] = {}
        self.pressure = _PressureStub()
        self.book_churn_hi = 0
        self.last_regime = "na"
        self.last_div = None
        self.last_wp = _WpStub(False)
        self.last_sweep = None
        self.last_reclaim = None
        self.last_obi_event = None
        self.last_iceberg_event = None
        self.last_ofi_event = None
        self.last_fp_edge = None
        self.symbol = ""

    def __getattr__(self, _name: str) -> Any:
        return None


def main() -> None:
    ap = argparse.ArgumentParser(description="Benchmark engine replay throughput and latency")
    ap.add_argument("--inputs", required=True, help="NDJSON (direct OFInputsV1) used for replay bench")
    ap.add_argument("--n", type=int, default=20000, help="number of calls to measure (default: 20000)")
    ap.add_argument("--warmup", type=int, default=2000, help="warmup iterations (default: 2000)")
    ap.add_argument("--out", required=True, help="output JSON with metrics")
    args = ap.parse_args()

    eng = OFConfirmEngine()
    rt = RuntimeStub()

    # Load inputs
    rows = []
    for i, r in enumerate(iter_ndjson(args.inputs)):
        rows.append(r)
        if len(rows) >= max(args.n, args.warmup):
            break
    if len(rows) < max(1000, args.warmup):
        raise SystemExit(f"not_enough_inputs n={len(rows)} (need at least {max(1000, args.warmup)})")

    def call(r: Dict[str, Any]) -> None:
        """Single engine.build() call."""
        rt.symbol = str(r.get("symbol", "") or "")
        cfg = dict(r.get("cfg") or {})
        ind = dict(r.get("indicators") or {})
        if "spread_bps" in r:
            ind["spread_bps"] = float(r["spread_bps"])
        if "expected_slippage_bps" in r:
            ind["expected_slippage_bps"] = float(r["expected_slippage_bps"])
        eng.build(
            symbol=str(r.get("symbol", "")),
            tf="1s",
            direction=str(r.get("direction", "")),
            tick_ts_ms=int(r.get("ts_ms", 0)),
            price=float(r.get("price", r.get("entry_price", 0.0)) or 0.0),
            delta_z=float(r.get("delta_z", 0.0) or 0.0),
            runtime=rt,
            cfg=cfg,
            indicators=ind,
            absorption=r.get("absorption") if isinstance(r.get("absorption"), dict) else None,
        )

    # Warmup
    print(f"Warming up with {args.warmup} iterations...")
    for i in range(args.warmup):
        call(rows[i % len(rows)])

    # Benchmark
    print(f"Benchmarking {args.n} calls...")
    dts = []
    t0 = time.perf_counter()
    for i in range(args.n):
        r = rows[i % len(rows)]
        t1 = time.perf_counter()
        call(r)
        t2 = time.perf_counter()
        dts.append((t2 - t1) * 1e6)  # microseconds
    t3 = time.perf_counter()

    out = {
        "n": args.n,
        "throughput_calls_per_s": args.n / (t3 - t0),
        "p50_us": pctl(dts, 0.50),
        "p95_us": pctl(dts, 0.95),
        "p99_us": pctl(dts, 0.99),
        "max_us": max(dts),
    }
    with open(args.out, "w", encoding="utf-8") as f:
        json.dump(out, f, ensure_ascii=False, indent=2)
    print(json.dumps(out, ensure_ascii=False, indent=2))


if __name__ == "__main__":
    main()

