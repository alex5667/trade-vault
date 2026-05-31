# -*- coding: utf-8 -*-
from __future__ import annotations

import math
import os
from dataclasses import dataclass
from typing import Any, Callable, Literal, Optional, Tuple

import numpy as np

try:
    import cupy as cp  # type: ignore
    _CUPY_OK = True
except Exception:
    cp = None  # type: ignore
    _CUPY_OK = False


Backend = Literal["auto", "gpu", "cpu"]


def _gpu_available() -> bool:
    if not _CUPY_OK:
        return False
    try:
        return cp.cuda.runtime.getDeviceCount() > 0  # type: ignore
    except Exception:
        return False


@dataclass
class RollingRobustZResult:
    z: float
    used_backend: str  # "gpu:custom" | "gpu:cupy" | "cpu:numpy"
    n: int
    median: float
    mad: float


class RollingRobustZscoreMAD:
    """
    Rolling robust z-score (median/MAD) на фиксированном окне (ring-buffer).
    """

    def __init__(
        self,
        window: int,
        *,
        backend: Backend = "auto",
        ignore_nan: bool = True,
        gpu_min_n: Optional[int] = None,
        dtype: str = "float32",
        compute_robust_zscore_mad: Optional[Callable[[Any, float, bool], float]] = None,
        hard_fallback_to_cpu: bool = True,
    ) -> None:
        if window <= 0:
            raise ValueError("window must be > 0")

        self.window = int(window)
        self.ignore_nan = bool(ignore_nan)
        self.compute_robust_zscore_mad = compute_robust_zscore_mad
        self.hard_fallback_to_cpu = bool(hard_fallback_to_cpu)

        if gpu_min_n is None:
            gpu_min_n = int(os.getenv("ROBUST_Z_GPU_MIN_N", "0"))

        self._pos = 0
        self._count = 0

        want_gpu = (backend in ("gpu", "auto")) and (self.window >= int(gpu_min_n)) and _gpu_available()

        if want_gpu:
            self.xp = cp  # type: ignore
            dt = cp.float32 if dtype == "float32" else cp.float64  # type: ignore
            self._buf = cp.full((self.window,), cp.nan, dtype=dt)  # type: ignore
            self._used_backend_base = "gpu"
        else:
            self.xp = np
            dt = np.float32 if dtype == "float32" else np.float64
            self._buf = np.full((self.window,), np.nan, dtype=dt)
            self._used_backend_base = "cpu"

    def _append(self, value: float) -> None:
        self._buf[self._pos] = value
        self._pos += 1
        if self._pos >= self.window:
            self._pos = 0
        if self._count < self.window:
            self._count += 1

    def _active_view(self) -> Any:
        if self._count >= self.window:
            return self._buf
        return self._buf[: self._count]

    def _to_float(self, x: Any) -> float:
        if _CUPY_OK and self.xp is cp:
            return float(x.item())
        return float(x)

    def _calc_cpu(self, arr: np.ndarray, value: float) -> RollingRobustZResult:
        if arr.size == 0:
            return RollingRobustZResult(0.0, "cpu:numpy", 0, 0.0, 0.0)

        if self.ignore_nan:
            med = float(np.nanmedian(arr))
            mad = float(np.nanmedian(np.abs(arr - med)))
        else:
            med = float(np.median(arr))
            mad = float(np.median(np.abs(arr - med)))

        denom = 1.4826 * mad
        if denom <= 0.0 or not math.isfinite(denom):
            return RollingRobustZResult(0.0, "cpu:numpy", int(arr.size), med, mad)

        z = (float(value) - med) / denom
        if not math.isfinite(z):
            z = 0.0
        return RollingRobustZResult(float(z), "cpu:numpy", int(arr.size), med, mad)

    def _calc_gpu(self, arr_gpu: Any, value: float) -> RollingRobustZResult:
        if self.compute_robust_zscore_mad is not None:
            try:
                z = float(self.compute_robust_zscore_mad(arr_gpu, float(value), bool(self.ignore_nan)))
                if math.isfinite(z):
                    return RollingRobustZResult(z, "gpu:custom", int(arr_gpu.size), 0.0, 0.0)
            except Exception:
                pass

        xp = self.xp  # cp
        if arr_gpu.size == 0:
            return RollingRobustZResult(0.0, "gpu:cupy", 0, 0.0, 0.0)

        if self.ignore_nan:
            med = xp.nanmedian(arr_gpu)  # type: ignore
            mad = xp.nanmedian(xp.abs(arr_gpu - med))  # type: ignore
        else:
            med = xp.median(arr_gpu)  # type: ignore
            mad = xp.median(xp.abs(arr_gpu - med))  # type: ignore

        med_f = self._to_float(med)
        mad_f = self._to_float(mad)

        denom = 1.4826 * mad_f
        if denom <= 0.0 or not math.isfinite(denom):
            return RollingRobustZResult(0.0, "gpu:cupy", int(arr_gpu.size), med_f, mad_f)

        z = (float(value) - med_f) / denom
        if not math.isfinite(z):
            z = 0.0
        return RollingRobustZResult(float(z), "gpu:cupy", int(arr_gpu.size), med_f, mad_f)

    def update(self, value: float) -> RollingRobustZResult:
        """
        Добавить значение в окно и вернуть robust z-score по текущему окну.
        """
        self._append(float(value))
        arr = self._active_view()

        if _CUPY_OK and self._used_backend_base == "gpu":
            try:
                return self._calc_gpu(arr, float(value))
            except Exception:
                if not self.hard_fallback_to_cpu:
                    raise
                buf_cpu = cp.asnumpy(self._buf)  # type: ignore
                self.xp = np
                self._buf = buf_cpu
                self._used_backend_base = "cpu"
                arr_cpu = self._active_view()
                return self._calc_cpu(arr_cpu, float(value))

        return self._calc_cpu(arr, float(value))

