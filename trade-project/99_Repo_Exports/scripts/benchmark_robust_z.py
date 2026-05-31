#!/usr/bin/env python3
"""
Benchmark: Rolling Robust Z-score — CPU vs GPU (CuPy).

Consolidates three previous benchmark scripts:
  - benchmark_robust_z.py         (single-stream, various windows)
  - benchmark_robust_batch.py     (batch 10-stream, window=300)
  - benchmark_robust_batch_100.py (batch 100-stream, window=300)

Usage:
    # Single-stream benchmark (original behaviour of benchmark_robust_z.py)
    python scripts/benchmark_robust_z.py

    # Batch benchmark with 10 streams
    python scripts/benchmark_robust_z.py --mode batch --streams 10

    # Batch benchmark with 100 streams
    python scripts/benchmark_robust_z.py --mode batch --streams 100

    # Custom parameters
    python scripts/benchmark_robust_z.py --mode batch --streams 50 --window 500 --iters 500
"""
from __future__ import annotations

import argparse
import time
from collections import deque

import numpy as np

# CuPy is optional — GPU benchmarks are silently skipped when unavailable.
try:
    import cupy as cp  # type: ignore[import]
    _CUDA_AVAILABLE = True
except (ImportError, ModuleNotFoundError):
    cp = None  # type: ignore[assignment]
    _CUDA_AVAILABLE = False


# ---------------------------------------------------------------------------
# Single-stream implementations
# ---------------------------------------------------------------------------

class RollingRobustZ_CPU:
    """CPU single-stream robust Z-score with a sliding window."""

    def __init__(self, window_size: int = 100) -> None:
        self.window_size = window_size
        self.values: deque[float] = deque(maxlen=window_size)

    def update(self, value: float) -> None:
        self.values.append(value)

    def z(self, value: float) -> float:
        if len(self.values) < 2:
            return 0.0
        arr = np.array(self.values)
        median = np.median(arr)
        mad = np.median(np.abs(arr - median))
        if mad == 0:
            mad = 1e-6
        return float((value - median) / (1.4826 * mad))


class RollingRobustZ_GPU:
    """GPU single-stream robust Z-score using CuPy."""

    def __init__(self, window_size: int = 100) -> None:
        self.window_size = window_size
        self.values: deque[float] = deque(maxlen=window_size)

    def update(self, value: float) -> None:
        self.values.append(value)

    def z(self, value: float) -> float:
        if len(self.values) < 2:
            return 0.0
        try:
            gpu_arr = cp.array(self.values, dtype=cp.float64)
            median = float(cp.median(gpu_arr))
            mad = float(cp.median(cp.abs(gpu_arr - median)))
            if mad == 0:
                mad = 1e-6
            return float((value - median) / (1.4826 * mad))
        except Exception as exc:
            print(f"GPU Error: {exc}")
            return 0.0


# ---------------------------------------------------------------------------
# Batch implementations (M streams in parallel)
# ---------------------------------------------------------------------------

class BatchRollingRobustZ_CPU:
    """CPU batch robust Z-score: M streams processed together."""

    def __init__(self, num_streams: int = 10, window_size: int = 300) -> None:
        self.num_streams = num_streams
        self.window_size = window_size
        self.buffers: list[deque[float]] = [deque(maxlen=window_size) for _ in range(num_streams)]

    def update_and_calc(self, values: list[float]) -> list[float]:
        results: list[float] = []
        for i, val in enumerate(values):
            self.buffers[i].append(val)
            if len(self.buffers[i]) < 2:
                results.append(0.0)
                continue
            arr = np.array(self.buffers[i])
            median = np.median(arr)
            mad = np.median(np.abs(arr - median))
            results.append(float((val - median) / (1.4826 * mad + 1e-6)))
        return results


class BatchRollingRobustZ_GPU:
    """GPU batch robust Z-score: M streams in a single CuPy matrix."""

    def __init__(self, num_streams: int = 10, window_size: int = 300) -> None:
        self.num_streams = num_streams
        self.window_size = window_size
        # Shape: (M, N) — all streams maintained as a 2-D GPU array
        self.data = cp.zeros((num_streams, window_size), dtype=cp.float64)

    def update_and_calc(self, values: list[float]) -> "cp.ndarray":  # type: ignore[name-defined]
        v_gpu = cp.array(values, dtype=cp.float64)
        # Ring-buffer shift (O(N) copy — acceptable for benchmark purposes)
        self.data[:, :-1] = self.data[:, 1:]
        self.data[:, -1] = v_gpu
        medians = cp.median(self.data, axis=1)
        mads = cp.median(cp.abs(self.data - medians.reshape((-1, 1))), axis=1)
        return (v_gpu - medians) / (1.4826 * mads + 1e-6)


# ---------------------------------------------------------------------------
# Benchmark runners
# ---------------------------------------------------------------------------

def run_single_stream_benchmark(
    window_sizes: list[int] | None = None,
    iterations: int = 1000,
) -> None:
    """Benchmark single-stream CPU vs GPU for various window sizes."""
    if window_sizes is None:
        window_sizes = [100, 300, 1000, 5000, 10000]

    print(f"{'Window':<10} | {'Device':<10} | {'Time (ms/call)':<16} | {'Speedup':<10}")
    print("-" * 52)

    if _CUDA_AVAILABLE:
        cp.array([1.0, 2.0, 3.0])  # GPU warmup

    for N in window_sizes:
        data = np.random.randn(N + iterations).tolist()

        # --- CPU ---
        cpu_obj = RollingRobustZ_CPU(window_size=N)
        for x in data[:N]:
            cpu_obj.update(x)
        t0 = time.perf_counter()
        for x in data[N:]:
            cpu_obj.update(x)
            cpu_obj.z(x)
        avg_cpu = (time.perf_counter() - t0) / iterations * 1000

        print(f"{N:<10} | {'CPU':<10} | {avg_cpu:<16.4f} | {'1.0x':<10}")

        if not _CUDA_AVAILABLE:
            print(f"{N:<10} | {'GPU':<10} | {'N/A (no CUDA)':<16} | {'N/A':<10}")
            print("-" * 52)
            continue

        # --- GPU ---
        gpu_obj = RollingRobustZ_GPU(window_size=N)
        for x in data[:N]:
            gpu_obj.update(x)
        t0 = time.perf_counter()
        for x in data[N:]:
            gpu_obj.update(x)
            gpu_obj.z(x)
        avg_gpu = (time.perf_counter() - t0) / iterations * 1000

        speedup = avg_cpu / avg_gpu if avg_gpu > 0 else 0.0
        print(f"{N:<10} | {'GPU':<10} | {avg_gpu:<16.4f} | {speedup:.2f}x")
        print("-" * 52)


def run_batch_benchmark(
    num_streams: int = 10,
    window_size: int = 300,
    iterations: int = 1000,
) -> None:
    """Benchmark batch CPU vs GPU for M parallel streams."""
    print(f"Batch Benchmark: {num_streams} streams, Window {window_size}, {iterations} iters")

    inputs = np.random.randn(iterations, num_streams).tolist()

    # --- CPU ---
    cpu = BatchRollingRobustZ_CPU(num_streams, window_size)
    # warmup
    for v in inputs[:window_size]:
        cpu.update_and_calc(v)
    t0 = time.perf_counter()
    for v in inputs[window_size:]:
        cpu.update_and_calc(v)
    avg_cpu = (time.perf_counter() - t0) / max(1, iterations - window_size) * 1000
    print(f"CPU Time per batch: {avg_cpu:.4f} ms")

    if not _CUDA_AVAILABLE:
        print("GPU: N/A (CUDA not available)")
        return

    # --- GPU ---
    gpu = BatchRollingRobustZ_GPU(num_streams, window_size)
    gpu.update_and_calc(inputs[0])  # warmup
    t0 = time.perf_counter()
    for v in inputs:
        gpu.update_and_calc(v)
    avg_gpu = (time.perf_counter() - t0) / max(1, iterations) * 1000
    print(f"GPU Time per batch: {avg_gpu:.4f} ms")

    if avg_gpu > 0:
        print(f"Speedup: {avg_cpu / avg_gpu:.2f}x")


# ---------------------------------------------------------------------------
# CLI
# ---------------------------------------------------------------------------

def main() -> None:
    parser = argparse.ArgumentParser(
        description="Rolling Robust Z-score CPU vs GPU benchmark",
    )
    parser.add_argument(
        "--mode",
        choices=["single", "batch"],
        default="single",
        help="Benchmark mode (default: single)",
    )
    parser.add_argument("--streams", type=int, default=10, help="Number of streams for batch mode")
    parser.add_argument("--window", type=int, default=300, help="Sliding window size")
    parser.add_argument("--iters", type=int, default=1000, help="Number of iterations")
    args = parser.parse_args()

    if not _CUDA_AVAILABLE:
        print("[WARNING] CuPy / CUDA not available — GPU columns will show N/A.\n")

    if args.mode == "single":
        run_single_stream_benchmark(iterations=args.iters)
    else:
        run_batch_benchmark(
            num_streams=args.streams,
            window_size=args.window,
            iterations=args.iters,
        )


if __name__ == "__main__":
    main()
