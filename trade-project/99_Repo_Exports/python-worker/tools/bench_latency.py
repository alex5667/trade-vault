from __future__ import annotations

import argparse
import importlib
import statistics
import time
from collections.abc import Callable
from typing import Any

from core.replay_io import iter_ndjson


def import_callable(path: str) -> Callable[[dict[str, Any]], dict[str, Any]]:
    if ":" not in path:
        raise ValueError("--runner must be module:function")
    mod_name, fn_name = path.split(":", 1)
    mod = importlib.import_module(mod_name)
    fn = getattr(mod, fn_name)
    if not callable(fn):
        raise TypeError(f"runner {path} is not callable")
    return fn  # type: ignore


def pct(xs: list[float], p: float) -> float:
    xs2 = sorted(xs)
    if not xs2:
        return 0.0
    k = int(round((p / 100.0) * (len(xs2) - 1)))
    return xs2[max(0, min(len(xs2) - 1, k))]


def main() -> int:
    ap = argparse.ArgumentParser()
    ap.add_argument("--inputs", required=True)
    ap.add_argument("--runner", required=True)
    ap.add_argument("--n", type=int, default=2000)
    ap.add_argument("--warmup", type=int, default=200)
    args = ap.parse_args()

    runner = import_callable(args.runner)
    rows = list(iter_ndjson(args.inputs))
    if not rows:
        raise SystemExit("inputs empty")

    # warmup
    for i in range(min(args.warmup, len(rows))):
        runner(rows[i])

    ms: list[float] = []
    for i in range(args.n):
        row = rows[i % len(rows)]
        t0 = time.perf_counter_ns()
        runner(row)
        dt_ms = (time.perf_counter_ns() - t0) / 1e6
        ms.append(dt_ms)

    print(f"n={len(ms)} p50={pct(ms,50):.3f} p95={pct(ms,95):.3f} p99={pct(ms,99):.3f} max={max(ms):.3f} mean={statistics.mean(ms):.3f}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())

