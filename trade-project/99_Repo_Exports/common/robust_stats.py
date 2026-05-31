# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import math
from dataclasses import dataclass
from typing import Any, Literal, Tuple, Optional

import numpy as np

from config.gpu_config import GPU_ENABLE, GPU_MIN_N, GPU_BACKEND

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


def _is_cupy_array(x: Any) -> bool:
    return _CUPY_OK and isinstance(x, cp.ndarray)  # type: ignore


def _as_cupy(x: np.ndarray) -> "cp.ndarray":  # type: ignore
    return cp.asarray(x)  # type: ignore


def _robust_zscore_mad_cpu(x: np.ndarray, value: float, *, ignore_nan: bool) -> float:
    if x.size == 0:
        return 0.0
    if ignore_nan:
        med = float(np.nanmedian(x))
        mad = float(np.nanmedian(np.abs(x - med)))
    else:
        med = float(np.median(x))
        mad = float(np.median(np.abs(x - med)))

    denom = 1.4826 * mad
    if denom <= 0.0 or not math.isfinite(denom):
        return 0.0
    z = (float(value) - med) / denom
    return float(z) if math.isfinite(z) else 0.0


def _robust_zscore_mad_cupy(x_gpu: "cp.ndarray", value: float, *, ignore_nan: bool) -> float:  # type: ignore
    if x_gpu.size == 0:
        return 0.0

    v = cp.asarray(value, dtype=x_gpu.dtype)  # type: ignore

    if ignore_nan:
        med = cp.nanmedian(x_gpu)  # type: ignore
        mad = cp.nanmedian(cp.abs(x_gpu - med))  # type: ignore
    else:
        med = cp.median(x_gpu)  # type: ignore
        mad = cp.median(cp.abs(x_gpu - med))  # type: ignore

    denom = 1.4826 * mad
    denom_f = float(denom.item())
    if denom_f <= 0.0 or not math.isfinite(denom_f):
        return 0.0

    z = (v - med) / denom
    z_f = float(z.item())
    return z_f if math.isfinite(z_f) else 0.0


def robust_zscore_mad(
    x: Any,
    value: float,
    *,
    backend: Optional[Backend] = None,
    ignore_nan: bool = True,
    gpu_min_n: Optional[int] = None,
    compute_robust_zscore_mad: Optional[Any] = None,
) -> Tuple[float, str]:
    """
    Возвращает (z, used_backend), где used_backend: 'gpu:custom'/'gpu:cupy'/'cpu:numpy'

    Для маленьких окон (100–300 тиков) всегда использует CPU.
    GPU включается только для больших оффлайн-батчей (>= GPU_MIN_N).
    """
    if backend is None:
        backend = GPU_BACKEND

    if gpu_min_n is None:
        gpu_min_n = GPU_MIN_N

    if _is_cupy_array(x):
        x_np = None
        x_gpu = x
        n = int(x_gpu.size)  # type: ignore
    else:
        x_np = np.asarray(x, dtype=float)
        x_gpu = None
        n = int(x_np.size)

    # Явное разделение: CPU для маленьких окон, GPU для больших
    if backend == "cpu" or n < gpu_min_n:
        if x_np is None:
            x_np = cp.asnumpy(x_gpu)  # type: ignore
        return _robust_zscore_mad_cpu(x_np, value, ignore_nan=ignore_nan), "cpu:numpy"

    # GPU-реализация для больших окон
    want_gpu = backend in ("gpu", "auto") and GPU_ENABLE
    can_gpu = want_gpu and _gpu_available()

    if can_gpu:
        if x_gpu is None:
            x_gpu = _as_cupy(x_np)  # type: ignore

        if compute_robust_zscore_mad is not None:
            try:
                z = float(compute_robust_zscore_mad(x_gpu, float(value), bool(ignore_nan)))
                if math.isfinite(z):
                    return z, "gpu:custom"
            except Exception:
                pass

        try:
            return _robust_zscore_mad_cupy(x_gpu, value, ignore_nan=ignore_nan), "gpu:cupy"
        except Exception:
            pass

    # Fallback на CPU
    if x_np is None:
        x_np = cp.asnumpy(x_gpu)  # type: ignore
    return _robust_zscore_mad_cpu(x_np, value, ignore_nan=ignore_nan), "cpu:numpy"


def robust_zscore_mad_realtime(
    x: Any,
    value: float,
    *,
    ignore_nan: bool = True,
) -> float:
    """
    Robust z-score для realtime-обработки (delta-окна 100–300 тиков).
    Жестко использует CPU, никогда не задействует GPU.
    """
    if _is_cupy_array(x):
        x_np = cp.asnumpy(x)  # type: ignore
    else:
        x_np = np.asarray(x, dtype=float)

    return _robust_zscore_mad_cpu(x_np, value, ignore_nan=ignore_nan)


def robust_zscore_mad_offline(
    x: Any,
    value: float,
    *,
    ignore_nan: bool = True,
    compute_robust_zscore_mad: Optional[Any] = None,
) -> float:
    """
    Robust z-score для оффлайн-батчей (ресамплинг, backfill).
    Использует CPU/GPU по порогу GPU_MIN_N.
    """
    _, backend = robust_zscore_mad(
        x,
        value,
        backend="auto",
        ignore_nan=ignore_nan,
        compute_robust_zscore_mad=compute_robust_zscore_mad,
    )
    # Возвращаем только z-score, backend игнорируем для совместимости
    z, _ = robust_zscore_mad(
        x,
        value,
        backend="auto",
        ignore_nan=ignore_nan,
        compute_robust_zscore_mad=compute_robust_zscore_mad,
    )
    return z


@dataclass
class RobustZRollingResult:
    z: float
    used_backend: str  # "gpu:custom" | "gpu:cupy" | "cpu:numpy"
    n: int


class RobustZscoreMADRolling:
    """
    Rolling robust z-score (median/MAD) с ring-buffer на GPU (если доступен),
    без постоянного CPU->GPU transfer на каждом вызове.
    """

    def __init__(
        self,
        window: int,
        *,
        backend: Backend = "auto",
        ignore_nan: bool = True,
        gpu_min_n: Optional[int] = None,
        dtype: str = "float32",
        compute_robust_zscore_mad: Optional[Any] = None,
        hard_fallback_to_cpu: bool = True,
    ) -> None:
        if window <= 0:
            raise ValueError("window must be > 0")
        self.window = int(window)
        self.ignore_nan = bool(ignore_nan)
        self.compute_robust_zscore_mad = compute_robust_zscore_mad
        self.hard_fallback_to_cpu = bool(hard_fallback_to_cpu)

        if gpu_min_n is None:
            gpu_min_n = 0

        self._pos = 0
        self._count = 0

        want_gpu = (backend in ("gpu", "auto")) and _gpu_available() and (self.window >= int(gpu_min_n))

        if want_gpu:
            self._backend_base = "gpu"
            dt = cp.float32 if dtype == "float32" else cp.float64  # type: ignore
            self._buf = cp.full((self.window,), cp.nan, dtype=dt)  # type: ignore
        else:
            self._backend_base = "cpu"
            dt = np.float32 if dtype == "float32" else np.float64
            self._buf = np.full((self.window,), np.nan, dtype=dt)

    def _append(self, v: float) -> None:
        self._buf[self._pos] = v
        self._pos += 1
        if self._pos >= self.window:
            self._pos = 0
        if self._count < self.window:
            self._count += 1

    def _active(self) -> Any:
        return self._buf if self._count >= self.window else self._buf[: self._count]

    def _cpu_calc(self, arr: np.ndarray, value: float) -> RobustZRollingResult:
        if arr.size == 0:
            return RobustZRollingResult(0.0, "cpu:numpy", 0)

        if self.ignore_nan:
            med = float(np.nanmedian(arr))
            mad = float(np.nanmedian(np.abs(arr - med)))
        else:
            med = float(np.median(arr))
            mad = float(np.median(np.abs(arr - med)))

        denom = 1.4826 * mad
        if denom <= 0.0 or not math.isfinite(denom):
            return RobustZRollingResult(0.0, "cpu:numpy", int(arr.size))

        z = (float(value) - med) / denom
        if not math.isfinite(z):
            z = 0.0
        return RobustZRollingResult(float(z), "cpu:numpy", int(arr.size))

    def _gpu_calc(self, arr_gpu: Any, value: float) -> RobustZRollingResult:
        if self.compute_robust_zscore_mad is not None:
            try:
                z = float(self.compute_robust_zscore_mad(arr_gpu, float(value), bool(self.ignore_nan)))
                if math.isfinite(z):
                    return RobustZRollingResult(z, "gpu:custom", int(arr_gpu.size))
            except Exception:
                pass

        xp = cp  # type: ignore
        if arr_gpu.size == 0:
            return RobustZRollingResult(0.0, "gpu:cupy", 0)

        if self.ignore_nan:
            med = xp.nanmedian(arr_gpu)  # type: ignore
            mad = xp.nanmedian(xp.abs(arr_gpu - med))  # type: ignore
        else:
            med = xp.median(arr_gpu)  # type: ignore
            mad = xp.median(xp.abs(arr_gpu - med))  # type: ignore

        med_f = float(med.item())
        mad_f = float(mad.item())
        denom = 1.4826 * mad_f
        if denom <= 0.0 or not math.isfinite(denom):
            return RobustZRollingResult(0.0, "gpu:cupy", int(arr_gpu.size))

        z = (float(value) - med_f) / denom
        if not math.isfinite(z):
            z = 0.0
        return RobustZRollingResult(float(z), "gpu:cupy", int(arr_gpu.size))

    def update(self, value: float) -> RobustZRollingResult:
        self._append(float(value))
        arr = self._active()

        if self._backend_base == "gpu":
            try:
                return self._gpu_calc(arr, float(value))
            except Exception:
                if not self.hard_fallback_to_cpu:
                    raise
                buf_cpu = cp.asnumpy(self._buf)  # type: ignore
                self._buf = buf_cpu
                self._backend_base = "cpu"
                return self._cpu_calc(self._active(), float(value))

        return self._cpu_calc(arr, float(value))

