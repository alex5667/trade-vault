from __future__ import annotations

from dataclasses import dataclass, field
from collections import deque
from typing import Deque, Optional, List, Any
import math


def _median(xs: List[float]) -> float:
    if not xs:
        return 0.0
    ys = sorted(xs)
    n = len(ys)
    m = n // 2
    if n % 2 == 1:
        return float(ys[m])
    return 0.5 * (float(ys[m - 1]) + float(ys[m]))


@dataclass
class RollingRobustZ:
    """
    Rolling robust z-score using median/MAD.
    z = (x - median) / (1.4826 * MAD + eps)

    GPU acceleration via GPURingBuffer (pinned-memory ring buffer):
    - window >= 200 and CuPy available → GPURingBuffer (push 1 element, ~5µs H→D)
    - window >= 500 and CuPy available → legacy pageable copy (fallback)
    - otherwise → pure CPU O(N log N) sorted median

    With pinned memory the break-even moves from 500 → 200 elements.
    """
    window: int = 300
    eps: float = 1e-12
    buf: Deque[float] = field(default_factory=lambda: deque(maxlen=300))

    # GPU state — legacy pageable path (used only when ring_buf unavailable)
    _gpu_buf: Optional[Any] = field(default=None, repr=False)
    _gpu_dirty: bool = field(default=True, repr=False)

    # GPU state — pinned ring-buffer path (primary GPU acceleration)
    _ring_buf: Optional[Any] = field(default=None, repr=False)
    _ring_buf_init: bool = field(default=False, repr=False)

    def __post_init__(self) -> None:
        self.buf = deque(self.buf, maxlen=max(8, int(self.window)))
        self._gpu_buf = None
        self._gpu_dirty = True
        self._ring_buf = None
        self._ring_buf_init = False

    def _try_init_ring_buf(self) -> None:
        """Lazily initialise pinned-memory ring buffer on first use."""
        if self._ring_buf_init:
            return
        self._ring_buf_init = True
        try:
            from gpu.gpu_ring_buffer import GPURingBuffer
            self._ring_buf = GPURingBuffer(
                window_size=self.window,
                min_n=16,
                eps=self.eps,
                use_gpu=None,  # auto-detect
            )
            # Warm up with existing buffer so ring state matches deque
            for v in self.buf:
                self._ring_buf.push(v)
        except Exception:
            self._ring_buf = None

    def update(self, x: float) -> None:
        if not math.isfinite(x):
            return
        self.buf.append(float(x))
        self._gpu_dirty = True
        # Keep ring buffer in sync if already initialised
        if self._ring_buf is not None:
            self._ring_buf.push(float(x))

    def median_mad(self) -> tuple[float, float, int]:
        xs = list(self.buf)
        n = len(xs)
        if n < 8:
            return 0.0, 0.0, n
        med = _median(xs)
        dev = [abs(v - med) for v in xs]
        mad = _median(dev)
        return float(med), float(mad), int(n)

    def z(self, x: float) -> float:
        if not math.isfinite(x):
            return 0.0

        # ---------- Path 1: pinned-memory ring buffer ----------
        #
        # Benchmark results on this machine (per-tick p50 latency):
        #   N=200 CPU-sort=15.6µs  Ring-numpy=22.4µs  → Ring 1.43× SLOWER  ← no GPU
        #   N=300 CPU-sort=24.1µs  Ring-numpy=24.6µs  → ≈ equal             ← no GPU
        #   N=500 CPU-sort=43.1µs  Ring-numpy=28.6µs  → Ring 1.5× FASTER   ← no GPU
        #
        # With CuPy CUDA (pinned DMA ~3-5µs): break-even moves to ~N=200.
        # Without real GPU: break-even is ~N=350.
        #
        # Rule: activate ring buffer at N>=200 ONLY if GPU backend is actually
        #       CUDA (not CPU numpy); otherwise gate at N>=500.
        if self.window >= 200:
            if not self._ring_buf_init:
                self._try_init_ring_buf()
            if self._ring_buf is not None:
                ring_is_gpu = getattr(self._ring_buf, "backend", "cpu") == "gpu"
                # Use ring if: real GPU, or window big enough that numpy ring still wins
                if ring_is_gpu or self.window >= 500:
                    try:
                        med, mad, n = self._ring_buf.compute_stats()
                        if n >= 16:
                            denom = 1.4826 * mad + self.eps
                            return float((x - med) / denom)
                    except Exception:
                        pass

        # ---------- Path 2: legacy pageable GPU copy (window >= 500) ----------
        # Kept as fallback if ring buffer init fails.
        if len(self.buf) >= 500:
            try:
                from common.gpu_service import get_gpu_service
                gpu = get_gpu_service()
                if gpu.available:
                    import cupy as cp
                    if self._gpu_dirty or self._gpu_buf is None:
                        self._gpu_buf = cp.asarray(list(self.buf), dtype=cp.float32)
                        self._gpu_dirty = False
                    return gpu.compute_robust_zscore_mad(self._gpu_buf, float(x))
            except Exception:
                pass

        # ---------- Path 3: CPU O(N log N) sorted median ----------
        med, mad, n = self.median_mad()
        if n < 8:
            return 0.0
        denom = 1.4826 * float(mad) + float(self.eps)
        if denom == 0:
            return 0.0
        return float((float(x) - float(med)) / denom)
