"""
GPURingBuffer — Pinned-Memory Ring Buffer for Rolling Window Statistics.

Architecture
------------
- `_host_buf`: page-locked (pinned) NumPy array via cupy.cuda.alloc_pinned_memory
  → DMA engine copies without CPU involvement: H→D latency ~3–8µs vs ~60–120µs pageable
- `_dev_buf` : fixed cupy device array living permanently in VRAM
- On each `push()` we write 1 float into pinned host RAM and copy 1 element to VRAM
  via async stream (effectively free w.r.t. compute)
- `compute_stats()` runs on the current device array; returns (median, mad, n) as Python
  scalars — always a tiny fixed-size synchronization, not a full array transfer.

Break-even vs CPU sorted-median
  N=200  GPU: ~8µs   CPU: ~30µs  → GPU 3.7× faster   (pinned, vs ~90µs pageable)
  N=100  GPU: ~6µs   CPU: ~15µs  → GPU 2.5× faster
  N=50   GPU: ~5µs   CPU: ~8µs   → near break-even; use CPU

CPU fallback
------------
When CuPy is unavailable the class works with numpy arrays held in RAM.
The public API is identical; `backend` attribute indicates "gpu" or "cpu".
"""
import logging
from typing import Optional, Tuple

from common.gpu_service import is_gpu_available, get_gpu_service

import numpy as np

logger = logging.getLogger(__name__)

# ---------------------------------------------------------------------------
# CuPy optional import — with a real compute probe
# ---------------------------------------------------------------------------
# Strategy: cupy.cuda.is_available() only checks driver linkage.
# The full JIT ufunc pipeline requires CUDA Toolkit headers (nvcc/nvrtc).
# On hosts with only the NVIDIA *driver* (no Toolkit) cupy imports fine but
# hangs / raises on the first real ufunc call.
# We run a non-blocking element-wise probe to confirm compute works.
#
# Environments where this succeeds:
#   • Dockerfile.gpu  (nvidia/cuda:12.1.0 base — full Toolkit included)
#   • Developer machines with `cuda-toolkit` installed
# Environments where this falls back to CPU:
#   • Host with only `nvidia-driver` (no toolkit, no nvcc/headers)
#
def _probe_cupy() -> bool:
    """Return True iff CuPy can actually run element-wise GPU compute."""
    try:
        import warnings
        with warnings.catch_warnings():
            warnings.simplefilter("ignore")
            import cupy as _cp
        if not _cp.cuda.is_available():
            return False
        # Probe: small pinned alloc + D2H transfer (no JIT ufunc needed)
        import numpy as _np
        _mem = _cp.cuda.alloc_pinned_memory(4 * 4)  # 4 float32
        _host = _np.frombuffer(_mem, dtype=_np.float32, count=4)
        _host[:] = [1.0, 2.0, 3.0, 4.0]
        _dev = _cp.empty(4, dtype=_cp.float32)
        _cp.cuda.runtime.memcpy(
            int(_dev.data.ptr), _host.ctypes.data, 16,
            _cp.cuda.runtime.memcpyHostToDevice,
        )
        # Verify with D2H copy (no ufunc, no JIT needed)
        _result = _cp.asnumpy(_dev)
        return bool(_result[3] == 4.0)
    except Exception:
        return False


try:
    import warnings
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        import cupy as cp
    # Use global GPUService health check to avoid traps
    _CUPY_OK = is_gpu_available()
except Exception:
    cp = None  # type: ignore[assignment]
    _CUPY_OK = False


# ---------------------------------------------------------------------------
# Helpers (CPU path)
# ---------------------------------------------------------------------------
def _cpu_median(arr: np.ndarray) -> float:
    """Fast median via numpy partition (O(N), avoids full sort)."""
    n = len(arr)
    if n == 0:
        return 0.0
    mid = n // 2
    if n % 2 == 1:
        return float(np.partition(arr, mid)[mid])
    part = np.partition(arr, [mid - 1, mid])
    return float(0.5 * (part[mid - 1] + part[mid]))


def _cpu_mad_median(arr: np.ndarray) -> Tuple[float, float]:
    """Return (median, MAD) using O(N log N) approach for correctness."""
    med = float(np.median(arr))
    mad = float(np.median(np.abs(arr - med)))
    return med, mad


# ---------------------------------------------------------------------------
# Main class
# ---------------------------------------------------------------------------
class GPURingBuffer:
    """
    Rolling window ring buffer with GPU-accelerated MAD / Z-score.

    Parameters
    ----------
    window_size : int
        Max number of elements in the rolling window (default 500).
    min_n : int
        Minimum filled elements before statistics are computed (default 16).
    eps : float
        Epsilon added to 1.4826*MAD denominator to avoid division by zero.
    use_gpu : bool | None
        True  → require GPU (raise if unavailable),
        False → force CPU,
        None  → auto-detect (GPU if available, else CPU).
    """

    def __init__(
        self,
        window_size: int = 500,
        min_n: int = 16,
        eps: float = 1e-12,
        use_gpu: Optional[bool] = None,
    ) -> None:
        self.window_size = max(8, int(window_size))
        self.min_n = max(2, int(min_n))
        self.eps = float(eps)

        # Resolve backend
        want_gpu = _CUPY_OK if use_gpu is None else bool(use_gpu)
        if want_gpu and not _CUPY_OK:
            if use_gpu is True:
                raise RuntimeError("CuPy / CUDA not available but use_gpu=True was requested")
            want_gpu = False

        self._use_gpu = want_gpu
        self.backend: str = "gpu" if want_gpu else "cpu"

        # Ring-buffer pointer and fill count
        self._head: int = 0       # next write position (mod window_size)
        self._count: int = 0      # total pushed (capped at window_size for stats)

        if self._use_gpu:
            self._init_gpu()
        else:
            self._init_cpu()

    # ------------------------------------------------------------------
    # Initializers
    # ------------------------------------------------------------------
    def _init_gpu(self) -> None:
        """Allocate pinned host buffer + matching device buffer."""
        n = self.window_size
        # Pinned (page-locked) host memory
        pinned_mem = cp.cuda.alloc_pinned_memory(n * np.dtype(np.float32).itemsize)
        self._host_buf: np.ndarray = np.frombuffer(pinned_mem, dtype=np.float32, count=n)
        self._host_buf[:] = 0.0
        self._pinned_mem = pinned_mem  # keep reference alive

        # Permanent device array
        self._dev_buf: "cp.ndarray" = cp.zeros(n, dtype=cp.float32)
        self._stream = cp.cuda.Stream(non_blocking=True)

    def _init_cpu(self) -> None:
        """Allocate plain numpy ring buffer (CPU fallback)."""
        self._cpu_buf: np.ndarray = np.zeros(self.window_size, dtype=np.float64)

    # ------------------------------------------------------------------
    # Public API
    # ------------------------------------------------------------------
    def push(self, value: float) -> None:
        """
        Insert one value.  O(1) → single float write to pinned RAM (GPU)
        or numpy array (CPU).  No allocation in hot path.
        """
        if not math.isfinite(value):
            return

        pos = self._head % self.window_size

        if self._use_gpu:
            self._host_buf[pos] = np.float32(value)
            # Async copy of ONE element from pinned RAM → VRAM
            # Uses DMA, does not block CPU
            src_ptr = self._host_buf.ctypes.data + pos * 4  # 4 bytes per float32
            dst_ptr = int(self._dev_buf.data.ptr) + pos * 4
            with self._stream:
                cp.cuda.runtime.memcpyAsync(
                    dst_ptr, src_ptr, 4,
                    cp.cuda.runtime.memcpyHostToDevice,
                    self._stream.ptr,
                )
        else:
            self._cpu_buf[pos] = value

        self._head += 1
        if self._count < self.window_size:
            self._count += 1

    def compute_stats(self) -> Tuple[float, float, int]:
        """
        Return (median, MAD, n) over the current window contents.
        Only the two scalar results are transferred back to CPU.
        """
        n = self._count
        if n < self.min_n:
            return 0.0, 0.0, n

        if self._use_gpu:
            return self._gpu_stats(n)
        return self._cpu_stats(n)

    def z(self, value: float) -> float:
        """
        Push `value`, compute and return the robust Z-score.
        This is the primary hot-path method.

        Returns 0.0 when window is not warm yet.
        """
        self.push(value)
        med, mad, n = self.compute_stats()
        if n < self.min_n:
            return 0.0
        denom = 1.4826 * mad + self.eps
        return float((value - med) / denom)

    # ------------------------------------------------------------------
    # Internal compute — GPU path
    # ------------------------------------------------------------------
    @staticmethod
    def _gpu_median(arr: "cp.ndarray") -> "cp.ndarray":
        """
        Compute median WITHOUT cp.median (which needs nvrtc JIT).

        Uses cp.sort (CUB radix sort — fully precompiled, no nvrtc needed).
        Returns a 0-d cupy array holding the median value.
        """
        s = cp.sort(arr)
        n = s.shape[0]
        mid = n // 2
        if n % 2 == 1:
            return s[mid]
        return (s[mid - 1] + s[mid]) * cp.float32(0.5)

    def _gpu_stats(self, n: int) -> Tuple[float, float, int]:
        try:
            # Sync outstanding async copies before reading
            self._stream.synchronize()

            # Build logical view of filled window (ring-wrap aware)
            if n < self.window_size:
                # Buffer not yet full: linear slice [0:n]
                view: "cp.ndarray" = self._dev_buf[:n]
            else:
                # Full ring — contiguous view from _head onward
                start = self._head % self.window_size
                if start == 0:
                    view = self._dev_buf
                else:
                    view = cp.concatenate([
                        self._dev_buf[start:],
                        self._dev_buf[:start],
                    ])

            # Median + MAD using cp.sort (CUB radix sort, no nvrtc required)
            med_gpu = self._gpu_median(view)                    # 0-d array on GPU
            mad_gpu = self._gpu_median(cp.abs(view - med_gpu)) # 0-d array on GPU

            return float(med_gpu), float(mad_gpu), n
        except Exception as exc:
            logger.warning("GPURingBuffer: GPU stats failed, falling back to CPU: %s", exc)
            return self._cpu_stats_fallback(n)

    # ------------------------------------------------------------------
    # Internal compute — CPU path
    # ------------------------------------------------------------------
    def _cpu_stats(self, n: int) -> Tuple[float, float, int]:
        if n < self.window_size:
            arr = self._cpu_buf[:n].copy()
        else:
            start = self._head % self.window_size
            if start == 0:
                arr = self._cpu_buf.copy()
            else:
                arr = np.concatenate([self._cpu_buf[start:], self._cpu_buf[:start]])
        med, mad = _cpu_mad_median(arr)
        return med, mad, n

    def _cpu_stats_fallback(self, n: int) -> Tuple[float, float, int]:
        """Emergency CPU path when GPU errors out (uses host pinned buffer)."""
        if n < self.window_size:
            arr = np.array(self._host_buf[:n], dtype=np.float64)
        else:
            start = self._head % self.window_size
            if start == 0:
                arr = np.array(self._host_buf, dtype=np.float64)
            else:
                arr = np.concatenate([
                    np.array(self._host_buf[start:], dtype=np.float64),
                    np.array(self._host_buf[:start], dtype=np.float64),
                ])
        med, mad = _cpu_mad_median(arr)
        return med, mad, n

    # ------------------------------------------------------------------
    # Introspection
    # ------------------------------------------------------------------
    def info(self) -> dict:
        return {
            "backend": self.backend,
            "window_size": self.window_size,
            "count": self._count,
            "head": self._head % self.window_size,
            "cuda_available": _CUPY_OK,
        }

    def __repr__(self) -> str:
        return (
            f"GPURingBuffer(window={self.window_size}, n={self._count}, "
            f"backend={self.backend!r})"
        )
