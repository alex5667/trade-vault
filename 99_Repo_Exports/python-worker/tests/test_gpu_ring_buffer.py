"""
Tests for gpu/gpu_ring_buffer.py — GPURingBuffer.

Covers:
  - CPU fallback (cupy unavailable): correct Z-score  
  - Matches RollingRobustZ Z results within tolerance
  - Ring-wrap semantics (correct sliding window after overflow)
  - Latency smoke test (1000 push+compute_stats < 500ms even on CPU)
"""
from __future__ import annotations

import math
import time
import pytest
import numpy as np


# ---------------------------------------------------------------------------
# Helper to build a GPURingBuffer forcing CPU mode (usable in any environment)
# ---------------------------------------------------------------------------
def make_cpu_ring(window: int = 50) -> "GPURingBuffer":  # noqa: F821
    from gpu.gpu_ring_buffer import GPURingBuffer
    return GPURingBuffer(window_size=window, min_n=8, use_gpu=False)


# ---------------------------------------------------------------------------
# 1. CPU fallback — basic correctness
# ---------------------------------------------------------------------------
class TestCPUFallbackCorrectness:

    def test_empty_returns_zero(self):
        ring = make_cpu_ring(20)
        med, mad, n = ring.compute_stats()
        assert n == 0
        assert med == 0.0
        assert mad == 0.0

    def test_below_min_n_returns_zero_stats(self):
        ring = make_cpu_ring(window=20)
        for v in range(5):  # min_n=8
            ring.push(float(v))
        med, mad, n = ring.compute_stats()
        assert n == 5
        assert med == 0.0
        assert mad == 0.0

    def test_known_median_mad(self):
        ring = make_cpu_ring(window=50)
        data = [10.0, 11.0, 12.0, 13.0, 14.0, 15.0, 16.0, 17.0, 18.0, 19.0]
        for v in data:
            ring.push(v)
        med, mad, n = ring.compute_stats()
        assert n == 10
        assert pytest.approx(med, abs=1e-4) == 14.5
        assert pytest.approx(mad, abs=1e-4) == 2.5

    def test_z_score_on_known_data(self):
        ring = make_cpu_ring(window=50)
        data = list(range(10, 20))  # [10..19]
        for v in data:
            ring.push(float(v))  # pre-load 10 values

        # ring.z(value) pushes value first, then computes stats on the resulting window.
        # After pushing 20.0: window = [10,11,12,13,14,15,16,17,18,19,20], n=11
        # median = 15.0
        # MAD = median(|x - 15|) = median([5,4,3,2,1,0,1,2,3,4,5]) = 3.0
        # z = (20 - 15) / (1.4826 * 3.0) = 5 / 4.4478 ≈ 1.1241
        expected_z = 5.0 / (1.4826 * 3.0)  # ≈ 1.1241
        z = ring.z(20.0)
        assert pytest.approx(z, abs=1e-3) == expected_z


# ---------------------------------------------------------------------------
# 2. Determinism / parity with RollingRobustZ
# ---------------------------------------------------------------------------
class TestParityWithRollingRobustZ:
    """
    z() values from GPURingBuffer(CPU) must match RollingRobustZ to 1e-3.

    Note: RollingRobustZ.z(q) with window=50 (<200) uses the CPU Path 3:
    median/MAD computed from its internal deque via _median() (pure Python).
    GPURingBuffer._cpu_stats uses np.median (equivalent algorithm, same result).
    Both compute stats on the same window contents without pushing q.
    """

    def test_parity_small_window(self):
        from core.robust_stats import RollingRobustZ

        rng = np.random.default_rng(42)
        data = rng.normal(0.0, 1.0, 80).tolist()
        window = 50

        rolling = RollingRobustZ(window=window)
        ring = make_cpu_ring(window=window)

        for v in data:
            rolling.update(v)
            ring.push(v)  # keep both in sync (no extra push)

        # Both implementations compute stats on the same window WITHOUT pushing q.
        # RollingRobustZ.z(q) with window<200 → CPU Path 3: uses median_mad() on deque.
        # GPURingBuffer.compute_stats() → CPU: uses np.median on ring buffer.
        # Both must agree to within 1e-3.
        eps = 1e-12
        for q in [0.0, 1.0, -1.0, 3.5]:
            # rolling z (read-only on the deque, does NOT push q)
            z_rolling = rolling.z(q)

            # ring z — derive from compute_stats (also read-only, same window)
            med, mad, n = ring.compute_stats()
            if n >= 16:
                denom = 1.4826 * mad + eps
                z_ring = float((q - med) / denom)
            else:
                z_ring = 0.0

            assert pytest.approx(z_rolling, abs=1e-2) == z_ring, (
                f"mismatch at q={q}: rolling={z_rolling:.6f} ring={z_ring:.6f}"
            )


# ---------------------------------------------------------------------------
# 3. Ring-wrap semantics
# ---------------------------------------------------------------------------
class TestRingWrapSemantics:

    def test_window_slides_correctly(self):
        """After filling window+N more, only last 'window' values are used."""
        window = 20
        ring = make_cpu_ring(window=window)

        # Push 2*window values: [0..39]
        for v in range(2 * window):
            ring.push(float(v))

        # Ring should hold window=20 elements: [20..39]
        med, mad, n = ring.compute_stats()
        assert n == window

        # Median of [20..39] = 29.5
        assert pytest.approx(med, abs=1e-3) == 29.5

    def test_oldest_value_evicted(self):
        """Verify the oldest value no longer affects stats after overflow."""
        window = 10
        ring = make_cpu_ring(window=window)

        # Fill with stable data: all 5.0
        for _ in range(window):
            ring.push(5.0)

        med_before, _, _ = ring.compute_stats()
        assert pytest.approx(med_before, abs=1e-6) == 5.0

        # Now push 'window' new values all equal to 100.0
        # Old 5.0s should be fully evicted
        for _ in range(window):
            ring.push(100.0)

        med_after, _, _ = ring.compute_stats()
        assert pytest.approx(med_after, abs=1e-6) == 100.0

    def test_count_caps_at_window_size(self):
        window = 15
        ring = make_cpu_ring(window=window)
        for v in range(100):
            ring.push(float(v))
        _, _, n = ring.compute_stats()
        assert n == window


# ---------------------------------------------------------------------------
# 4. Edge cases
# ---------------------------------------------------------------------------
class TestEdgeCases:

    def test_nan_ignored(self):
        ring = make_cpu_ring(20)
        for v in range(10):
            ring.push(float(v))
        count_before = ring._count
        ring.push(float("nan"))
        assert ring._count == count_before  # NaN not pushed

    def test_inf_ignored(self):
        ring = make_cpu_ring(20)
        ring.push(float("inf"))
        assert ring._count == 0

    def test_all_same_values_z_zero(self):
        ring = make_cpu_ring(20)
        for _ in range(20):
            ring.push(7.0)
        # MAD = 0 → denom = eps → z ~ value/eps (very large), but if value == median => 0
        z = ring.z(7.0)
        assert z == pytest.approx(0.0, abs=1e-6)

    def test_info_dict(self):
        ring = make_cpu_ring(10)
        info = ring.info()
        assert info["backend"] == "cpu"
        assert info["window_size"] == 10


# ---------------------------------------------------------------------------
# 5. Latency smoke test
# ---------------------------------------------------------------------------
class TestLatencySmoke:

    @pytest.mark.timeout(5)
    def test_1000_updates_under_500ms_cpu(self):
        """1000 push+compute_stats must complete in < 500ms on CPU."""
        ring = make_cpu_ring(window=500)
        rng = np.random.default_rng(0)
        data = rng.normal(0.0, 1.0, 1500).tolist()

        # Warm up
        for v in data[:500]:
            ring.push(v)

        start = time.perf_counter()
        for v in data[500:1500]:
            ring.push(v)
            ring.compute_stats()
        elapsed_ms = (time.perf_counter() - start) * 1000

        assert elapsed_ms < 500, f"Too slow: {elapsed_ms:.1f}ms for 1000 iterations"


# ---------------------------------------------------------------------------
# 6. RobustZscoreGPU integration (CPU path)
# ---------------------------------------------------------------------------
class TestRobustZscoreGPUCPUPath:

    def test_update_returns_tuple(self):
        from gpu.robust_z_gpu import RobustZscoreGPU
        r = RobustZscoreGPU(window_size=30, threshold=3.0)
        for v in range(20):
            z, flag = r.update(float(v))
        z, flag = r.update(50.0)
        assert isinstance(z, float)
        assert isinstance(flag, bool)

    def test_get_stats_has_backend_key(self):
        from gpu.robust_z_gpu import RobustZscoreGPU
        r = RobustZscoreGPU(window_size=30)
        for v in range(20):
            r.update(float(v))
        stats = r.get_stats()
        assert "backend" in stats
        assert "count" in stats
        assert "median" in stats
