# -*- coding: utf-8 -*-
from __future__ import annotations

import os
import threading
from dataclasses import dataclass
from typing import Optional

_lock = threading.Lock()
_singleton: Optional["GpuComputeService"] = None


def _env_bool_any(names: list[str], default: str = "1") -> bool:
    for name in names:
        if name in os.environ:
            v = os.getenv(name, default).strip().lower()
            return v not in ("0", "false", "no", "off", "")
    v = default.strip().lower()
    return v not in ("0", "false", "no", "off", "")


@dataclass(frozen=True)
class GpuServiceSettings:
    enabled: bool = True
    device_id: int = 0
    warmup: bool = True
    pool_limit_mb: int = 0  # 0 = без лимита
    per_thread_stream: bool = True


def read_gpu_settings_from_env() -> GpuServiceSettings:
    return GpuServiceSettings(
        enabled=_env_bool_any(["GPU_ENABLE", "GPU_ENABLED"], "1"),
        device_id=int(os.getenv("GPU_DEVICE_ID", "0")),
        warmup=_env_bool_any(["GPU_WARMUP"], "1"),
        pool_limit_mb=int(os.getenv("GPU_POOL_LIMIT_MB", "0")),
        per_thread_stream=_env_bool_any(["GPU_PER_THREAD_STREAM"], "1"),
    )


def get_gpu_service(settings: Optional[GpuServiceSettings] = None) -> Optional["GpuComputeService"]:
    """
    Единая точка входа. Возвращает singleton или None (если отключено/нет GPU/ошибка).
    """
    global _singleton
    if _singleton is not None:
        return _singleton

    with _lock:
        if _singleton is not None:
            return _singleton

        s = settings or read_gpu_settings_from_env()
        if not s.enabled:
            return None

        try:
            svc = GpuComputeService(s)
        except Exception:
            return None

        _singleton = svc
        return _singleton


def reset_gpu_service_for_tests() -> None:
    global _singleton
    with _lock:
        _singleton = None


class GpuComputeService:
    """
    Один экземпляр на процесс.
    - фиксирует device
    - настраивает memory pool/pinned pool (один раз)
    - даёт методы compute_robust_zscore_mad / depth_sum_batch
    """

    def __init__(self, s: GpuServiceSettings) -> None:
        import cupy as cp  # type: ignore

        self.cp = cp
        n = int(cp.cuda.runtime.getDeviceCount())
        if n <= 0:
            raise RuntimeError("No CUDA devices")
        if s.device_id < 0 or s.device_id >= n:
            raise RuntimeError(f"GPU_DEVICE_ID={s.device_id} out of range (0..{n-1})")

        self.device_id = s.device_id
        self.device = cp.cuda.Device(self.device_id)
        self.device.use()
        # compatibility flags
        self.use_gpu = True

        self.pool = cp.cuda.MemoryPool()
        cp.cuda.set_allocator(self.pool.malloc)

        self.pinned_pool = cp.cuda.PinnedMemoryPool()
        cp.cuda.set_pinned_memory_allocator(self.pinned_pool.malloc)

        if s.pool_limit_mb > 0:
            try:
                self.pool.set_limit(size=s.pool_limit_mb * 1024 * 1024)
            except Exception:
                pass

        self._per_thread_stream = bool(s.per_thread_stream)
        self._tls = threading.local()
        self._shared_stream = None

        if s.warmup:
            st = self.stream()
            with st:
                _ = cp.zeros((1,), dtype=cp.float32)
            st.synchronize()

        # device info cache
        try:
            props = cp.cuda.runtime.getDeviceProperties(self.device_id)
            name = props["name"]
            if isinstance(name, bytes):
                name = name.decode()
            self.device_info = {
                "id": self.device_id,
                "name": name,
                "total_global_mem": props.get("totalGlobalMem"),
                "compute_capability": f"{props.get('major')}.{props.get('minor')}",
            }
        except Exception:
            self.device_info = {"id": self.device_id, "name": f"GPU-{self.device_id}"}

    def stream(self):
        cp = self.cp
        if not self._per_thread_stream:
            if self._shared_stream is None:
                self._shared_stream = cp.cuda.Stream(non_blocking=True)
            return self._shared_stream

        st = getattr(self._tls, "stream", None)
        if st is None:
            st = cp.cuda.Stream(non_blocking=True)
            self._tls.stream = st
        return st

    def compute_robust_zscore_mad(self, x_gpu, value: float, ignore_nan: bool = True) -> float:
        cp = self.cp
        with self.stream():
            if ignore_nan:
                med = cp.nanmedian(x_gpu)
                mad = cp.nanmedian(cp.abs(x_gpu - med))
            else:
                med = cp.median(x_gpu)
                mad = cp.median(cp.abs(x_gpu - med))
            denom = 1.4826 * mad
            if denom == 0:
                return 0.0
            z = (value - med) / denom
            return float(z.item())

    def depth_sum_batch(self, px_levels_gpu, qty_levels_gpu):
        cp = self.cp
        with self.stream():
            return cp.sum(qty_levels_gpu, axis=-1)

    # ---- compatibility helpers ----
    def is_gpu_available(self) -> bool:
        return True

    def get_device_info(self):
        return getattr(self, "device_info", None)

    def compute_l2_metrics_batch(
        self,
        books: list[dict],
        k_small: int = 5,
        k_large: int = 20,
        wall_mult: float = 3.0,
        wall_max_dist_bps: float = 15.0,
    ) -> list[dict | None]:
        """
        Batch compute L2 metrics on GPU.
        Expects books to have 'bids', 'asks' (lists of [px, sz]) and 'mid' (float).
        """
        cp = self.cp
        n = len(books)
        if n == 0:
            return []

        # 1. Prepare data tensors
        # We need uniform depth. Pick max depth from batch or fixed limit (e.g. 50).
        max_depth = 50
        
        # Allocate on host then transfer
        # Shape: (N, Depth, 2) -> (Price, Size)
        # We use a list to collect data then convert to numpy for efficiency
        # Pre-process on CPU (unfortunately unavoidable to parse mixed types etc)
        import numpy as np
        
        host_bids = np.zeros((n, max_depth, 2), dtype=np.float32)
        host_asks = np.zeros((n, max_depth, 2), dtype=np.float32)
        host_mids = np.zeros((n,), dtype=np.float32)
        ts_list = []
        
        valid_mask = [False] * n

        for i, book in enumerate(books):
            try:
                ts_list.append(book.get("ts", 0))
                mid = float(book.get("mid", 0))
                if mid <= 0:
                    continue
                
                host_mids[i] = mid
                
                # Helper to fill buffer
                def fill(source, dest):
                    cnt = 0
                    if source:
                        for row in source:
                            if cnt >= max_depth: break
                            try:
                                p, v = float(row[0]), float(row[1])
                                if p > 0 and v >= 0:
                                    dest[cnt, 0] = p
                                    dest[cnt, 1] = v
                                    cnt += 1
                            except: continue
                
                fill(book.get("bids"), host_bids[i])
                fill(book.get("asks"), host_asks[i])
                
                if host_bids[i, 0, 0] > 0 and host_asks[i, 0, 0] > 0:
                    valid_mask[i] = True
                    
            except Exception:
                pass

        # Transfer to GPU
        with self.stream():
            bids_gpu = cp.asarray(host_bids)
            asks_gpu = cp.asarray(host_asks)
            mids_gpu = cp.asarray(host_mids)
        
        # 2. Compute Metrics using Array Ops (Broadcasting)
        
        # Spread
        best_bid = bids_gpu[:, 0, 0]
        best_ask = asks_gpu[:, 0, 0]
        spread_bps = (best_ask - best_bid) / mids_gpu * 10000.0
        
        # Depth sums
        def get_depth_batch(arr_gpu, k):
            k_clamped = min(k, max_depth)
            return cp.sum(arr_gpu[:, :k_clamped, 1], axis=1)

        depth_bid_5 = get_depth_batch(bids_gpu, k_small)
        depth_ask_5 = get_depth_batch(asks_gpu, k_small)
        depth_bid_20 = get_depth_batch(bids_gpu, k_large)
        depth_ask_20 = get_depth_batch(asks_gpu, k_large)
        depth_bid_3 = get_depth_batch(bids_gpu, 3)
        depth_ask_3 = get_depth_batch(asks_gpu, 3)

        # OBI
        def calc_obi_batch(bd, ad):
            den = bd + ad
            res = (bd - ad) / cp.where(den == 0, 1.0, den)
            return cp.where(den == 0, 0.0, res)

        obi_5 = calc_obi_batch(depth_bid_5, depth_ask_5)
        obi_20 = calc_obi_batch(depth_bid_20, depth_ask_20)

        # Slope
        def calc_slope_batch(arr_gpu, k):
            k_clamped = min(k, max_depth)
            cum = cp.sum(arr_gpu[:, :k_clamped, 1], axis=1)
            pk = arr_gpu[:, k_clamped-1, 0]
            dist = cp.abs(pk - mids_gpu) / mids_gpu * 10000.0
            dist = cp.where(dist < 1e-6, 1e-6, dist)
            return cum / dist

        slope_bid_20 = calc_slope_batch(bids_gpu, k_large)
        slope_ask_20 = calc_slope_batch(asks_gpu, k_large)

        # Microprice
        def calc_microprice_batch(arr_gpu, k):
            k_clamped = min(k, max_depth)
            pp = arr_gpu[:, :k_clamped, 0]
            vv = arr_gpu[:, :k_clamped, 1]
            mids_col = mids_gpu.reshape(n, 1)
            dists = cp.abs(pp - mids_col) / mids_col * 10000.0
            weights = vv / (dists + 1.0)
            return cp.sum(weights * pp, axis=1), cp.sum(weights, axis=1)

        mp_num_b, mp_den_b = calc_microprice_batch(bids_gpu, k_large)
        mp_num_a, mp_den_a = calc_microprice_batch(asks_gpu, k_large)
        
        mp_tot_num = mp_num_b + mp_num_a
        mp_tot_den = mp_den_b + mp_den_a
        
        mp20 = cp.where(mp_tot_den == 0, mids_gpu, mp_tot_num / mp_tot_den)
        mp_shift_bps = (mp20 - mids_gpu) / mids_gpu * 10000.0
        
        # Wall detection
        def detect_wall_batch(arr_gpu, k):
            k_clamped = min(k, max_depth)
            pp = arr_gpu[:, :k_clamped, 0]
            vv = arr_gpu[:, :k_clamped, 1]
            meds = cp.median(vv, axis=1)
            thresh_col = (meds * wall_mult).reshape(n, 1)
            mids_col = mids_gpu.reshape(n, 1)
            
            cand_mask = vv >= thresh_col
            dists = cp.abs(pp - mids_col) / mids_col * 10000.0
            
            # Mask invalid or far candidates
            valid = cand_mask & (dists <= wall_max_dist_bps)
            # Find min dist. Replace invalid with inf
            dists_inf = cp.where(valid, dists, float('inf'))
            min_dist = cp.min(dists_inf, axis=1)
            
            is_wall = min_dist != float('inf')
            return is_wall, cp.where(is_wall, min_dist, 0.0)

        wall_bid, wall_bid_dist = detect_wall_batch(bids_gpu, k_small)
        wall_ask, wall_ask_dist = detect_wall_batch(asks_gpu, k_small)

        # 3. Fetch results to CPU
        # Get all arrays as numpy
        # It's faster to pull them individually or stacked, but individually is easier to code
        res_map = {
            "best_bid": best_bid, "best_ask": best_ask, "spread_bps": spread_bps,
            "depth_bid_5": depth_bid_5, "depth_ask_5": depth_ask_5,
            "depth_bid_20": depth_bid_20, "depth_ask_20": depth_ask_20,
            "depth_bid_3": depth_bid_3, "depth_ask_3": depth_ask_3,
            "obi_5": obi_5, "obi_20": obi_20,
            "slope_bid_20": slope_bid_20, "slope_ask_20": slope_ask_20,
            "microprice_20": mp20, "microprice_shift_bps_20": mp_shift_bps,
            "wall_bid": wall_bid, "wall_ask": wall_ask,
            "wall_bid_dist_bps": wall_bid_dist, "wall_ask_dist_bps": wall_ask_dist,
        }
        
        cpu_res = {k: cp.asnumpy(v) for k, v in res_map.items()}
        
        results = []
        for i in range(n):
            if not valid_mask[i]:
                results.append(None)
                continue
                
            r = {
                "ts": int(ts_list[i]),
                "mid": float(host_mids[i]),
            }
            # Fill dynamic fields
            for k, arr in cpu_res.items():
                val = arr[i]
                if k.startswith("wall") and "dist" not in k:
                    r[k] = bool(val)
                else:
                    r[k] = float(val)
            results.append(r)
            
        return results

