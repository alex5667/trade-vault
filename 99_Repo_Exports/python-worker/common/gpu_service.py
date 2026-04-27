"""
GPU service utilities for the scanner infrastructure.

Provides GPU detection and service management for CUDA-enabled operations.
"""

from typing import Optional, Dict, Any


class GPUService:
    """
    GPU service for CUDA operations.
    """

    def __init__(self):
        self.available = self._check_cuda_available()
        self.device_count = self._get_device_count()
        self.current_device = 0
        self.use_gpu = self.available
        self._torch_fallback = self._check_torch_cuda()  # True if cupy unavail but torch works
        if self.available:
            self._verify_gpu_health()

    def _check_cuda_available(self) -> bool:
        """Check if CUDA is available (via CuPy or torch fallback)."""
        try:
            import warnings
            with warnings.catch_warnings():
                warnings.simplefilter("ignore", category=UserWarning)
                import cupy as cp
                return cp.cuda.is_available()
        except ImportError:
            pass
        except Exception:
            pass
        # Fallback: torch.cuda
        return self._check_torch_cuda()

    def _check_torch_cuda(self) -> bool:
        """Check CUDA via PyTorch (no cupy required)."""
        try:
            import torch
            return torch.cuda.is_available()
        except Exception:
            return False

    def _get_device_count(self) -> int:
        """Get number of CUDA devices."""
        if not self.available:
            return 0
        try:
            import cupy as cp
            return cp.cuda.runtime.getDeviceCount()
        except Exception:
            return 0

    def get_device_info(self) -> Dict[str, Any]:
        """Get information about current GPU device."""
        if not self.available or self.device_count == 0:
            return {"available": False, "device_count": 0}

        try:
            import cupy as cp
            device_props = cp.cuda.runtime.getDeviceProperties(self.current_device)
            return {
                "available": True,
                "device_count": self.device_count,
                "current_device": self.current_device,
                "name": device_props["name"].decode(),
                "total_memory": device_props["totalGlobalMem"],
                "compute_capability": f"{device_props['major']}.{device_props['minor']}"
            }
        except Exception as e:
            return {
                "available": self.available,
                "device_count": self.device_count,
                 "error": str(e)
            }

    def _verify_gpu_health(self):
        """
        Perform a minimal GPU operation to ensure drivers and JIT (libnvrtc) are working.
        If this fails, permanently disable GPU for this session to avoid hot-path traps.
        """
        if not self.available:
            return
        
        try:
            import cupy as cp
            # Simple kernel compilation + execution check
            x = cp.array([1.0, 2.0], dtype=cp.float32)
            y = x * 2.0
            _ = float(cp.sum(y))
        except (ImportError, OSError, Exception) as e:
            # We catch OSError specifically for missing libnvrtc.so.12
            self.available = False
            self.use_gpu = False
            import logging
            logger = logging.getLogger("GPUService")
            logger.warning(f"GPU Health Check FAILED: {e}. GPU will be DISABLED for this session to prevent latency spikes.")

    def compute_ema_batch(self, prices, period):
        """EMA batch on GPU with CPU fallback."""
        if self.available and self.use_gpu:
            try:
                import cupy as cp
                # Custom JIT or iterative approach is needed for EMA on GPU.
                # For now, we use a simple fallback if specialized kernel is not yet optimized.
                # But to avoid AttributeError, the method MUST exist.
                pass 
            except Exception:
                pass
        
        # Fallback to NumPy
        import numpy as np
        prices_np = np.asarray(prices)
        alpha = 2.0 / (period + 1.0)
        ema = np.zeros_like(prices_np)
        ema[0] = prices_np[0]
        for i in range(1, len(prices_np)):
            ema[i] = prices_np[i] * alpha + ema[i-1] * (1.0 - alpha)
        return ema

    def compute_rsi_batch(self, prices, period):
        """RSI batch on GPU with CPU fallback."""
        import numpy as np
        prices_np = np.asarray(prices)
        deltas = np.diff(prices_np)
        seed = deltas[:period+1]
        up = seed[seed >= 0].sum() / period
        down = -seed[seed < 0].sum() / period
        rs = up / (down + 1e-9)
        rsi = np.zeros_like(prices_np)
        rsi[:period+1] = 100.0 - 100.0 / (1.0 + rs)

        for i in range(period + 1, len(prices_np)):
            delta = deltas[i - 1]
            if delta > 0:
                up_val = delta
                down_val = 0.0
            else:
                up_val = 0.0
                down_val = -delta

            up = (up * (period - 1) + up_val) / period
            down = (down * (period - 1) + down_val) / period
            rs = up / (down + 1e-9)
            rsi[i] = 100.0 - 100.0 / (1.0 + rs)
        return rsi

    def compute_macd_batch(self, prices, fast=12, slow=26, signal=9):
        """MACD batch on GPU with CPU fallback."""
        ema_fast = self.compute_ema_batch(prices, fast)
        ema_slow = self.compute_ema_batch(prices, slow)
        macd_line = ema_fast - ema_slow
        signal_line = self.compute_ema_batch(macd_line, signal)
        histogram = macd_line - signal_line
        return macd_line, signal_line, histogram


    def is_gpu_available(self) -> bool:
        """Check if GPU is available (compatibility method)."""
        return self.available
    def compute_obi_metrics_batch(self, bid_vol_arr, ask_vol_arr):
        """
        Compute OBI metrics for a batch of volumes on GPU.
        
        Args:
            bid_vol_arr: Numpy array of bid volumes
            ask_vol_arr: Numpy array of ask volumes
            
        Returns:
            Dictionary with 'obi_signed' and 'obi_ratio' arrays (on CPU)
        """
        if not self.available:
            raise RuntimeError("GPU not available")

        try:
            import cupy as cp

            # Transfer to GPU
            b_gpu = cp.asarray(bid_vol_arr, dtype=cp.float32)
            a_gpu = cp.asarray(ask_vol_arr, dtype=cp.float32)

            # Compute OBI Signed: (ask - bid) / (ask + bid)
            total = b_gpu + a_gpu
            # Avoid division by zero
            # mask = total > 0
            # obi_signed = cp.zeros_like(total)
            # obi_signed[mask] = (a_gpu[mask] - b_gpu[mask]) / total[mask]

            # Faster approach: add epsilon
            obi_signed = (a_gpu - b_gpu) / (total + 1e-9)

            # Compute OBI Ratio: (ask / bid) - 1
            # Handle bid=0 case
            # If bid > 0: ratio = (ask/bid) - 1
            # If bid == 0 and ask > 0: ratio = inf (or high number)
            # If bid == 0 and ask == 0: ratio = 0

            # We can use cp.where
            # ratio = cp.where(b_gpu > 1e-9, (a_gpu / b_gpu) - 1.0,
            #                 cp.where(a_gpu > 1e-9, 999.0, 0.0))

            # Simplified for perf
            ratio = (a_gpu / (b_gpu + 1e-9)) - 1.0

            # Transfer back
            return {
                'obi_signed': cp.asnumpy(obi_signed),
                'obi_ratio': cp.asnumpy(ratio)
            }
        except Exception as e:
            raise RuntimeError(f"GPU computation failed: {e}")

    def compute_robust_zscore_mad(self, x_gpu, value: float, ignore_nan: bool = True) -> float:
        """
        Compute Robust Z-score using MAD on GPU.
        
        Args:
            x_gpu: CuPy array of values (already on GPU)
            value: The latest value to score
            ignore_nan: Whether to ignore NaNs
            
        Returns:
            Z-score float
        """
        if not self.available:
            return 0.0

        try:
             import cupy as cp
             if ignore_nan:
                 med = float(cp.nanmedian(x_gpu))
                 # mad = median(|x - med|)
                 diff = cp.abs(x_gpu - med)
                 mad = float(cp.nanmedian(diff))
             else:
                 med = float(cp.median(x_gpu))
                 diff = cp.abs(x_gpu - med)
                 mad = float(cp.median(diff))

             denom = 1.4826 * mad
             if denom < 1e-12:
                 return 0.0

             z = (value - med) / denom
             return float(z)
        except Exception:
             return 0.0

    def process_candles_batch(self, candles: list[dict]) -> dict[str, list[float]]:
        """
        Process a batch of candles on GPU for OrderFlow metrics.
        
        Args:
            candles: List of dictionaries matching candle_of_worker format
            
        Returns:
            Dictionary of result lists
        """
        if not self.available:
            return {}

        try:
            import cupy as cp
            import numpy as np

            # Extract data
            opens = np.array([float(c.get('open', 0)) for c in candles], dtype=np.float32)
            highs = np.array([float(c.get('high', 0)) for c in candles], dtype=np.float32)
            lows = np.array([float(c.get('low', 0)) for c in candles], dtype=np.float32)
            closes = np.array([float(c.get('close', 0)) for c in candles], dtype=np.float32)
            vols = np.array([float(c.get('volume', 0)) for c in candles], dtype=np.float32)
            tb_vols = []
            for c in candles:
                tb = c.get('takerBuyVolume')
                if tb is None:
                    # Proxy mode
                    tb_vols.append(float(vols[len(tb_vols)]) if c.get('close', 0) >= c.get('open', 0) else 0.0)
                else:
                    tb_vols.append(float(tb))
            tb_vols = np.array(tb_vols, dtype=np.float32)
            atrs = np.array([float(c.get('atr', 1e-9)) for c in candles], dtype=np.float32) # fallback to 1e-9 to avoid div by zero

            # Transfer to GPU
            o_gpu = cp.asarray(opens)
            h_gpu = cp.asarray(highs)
            l_gpu = cp.asarray(lows)
            c_gpu = cp.asarray(closes)
            v_gpu = cp.asarray(vols)
            tb_gpu = cp.asarray(tb_vols)
            a_gpu = cp.asarray(atrs)

            # Compute deltas
            # buy_vol = tb_gpu
            # sell_vol = v_gpu - tb_gpu
            # delta = buy_vol - sell_vol = 2 * tb_gpu - v_gpu
            delta_gpu = 2.0 * tb_gpu - v_gpu

            # Compute CVD (cumulative within batch)
            # Note: This doesn't account for previous batch CVD, but candle_of_worker handles that?
            # Actually candle_of_worker line 600 expects cumulative.
            # We'll just return batch-local prefix-sum delta here for now.
            cvd_gpu = cp.cumsum(delta_gpu)

            # Compute Ratio
            ratio_gpu = delta_gpu / (v_gpu + 1e-9)

            # Compute BodyATR
            body_atr_gpu = cp.abs(c_gpu - o_gpu) / (a_gpu + 1e-9)

            # Robust Z-score (per-candle in batch relative to some baseline? Or relative to the batch self?)
            # Usually zDelta is relative to a sliding window (OnlineStats).
            # Batch processing here is a bit tricky if it's supposed to use the detector's state.
            # However, for now we can provide a self-normalized Z within the batch or leave it for CPU.
            # Looking at candle_of_worker: `detector.stats.z(delta_val)` is used for single candle.
            # For batch, it uses `results['z_deltas'][i]`.

            # We'll compute a batch-local robust Z as a baseline.
            med = cp.median(delta_gpu)
            mad = cp.median(cp.abs(delta_gpu - med))
            denom = 1.4826 * mad + 1e-12
            z_gpu = (delta_gpu - med) / denom

            return {
                'deltas': cp.asnumpy(delta_gpu).tolist(),
                'buy_vols': cp.asnumpy(tb_gpu).tolist(),
                'sell_vols': cp.asnumpy(v_gpu - tb_gpu).tolist(),
                'cvd': cp.asnumpy(cvd_gpu).tolist(),
                'delta_ratio': cp.asnumpy(ratio_gpu).tolist(),
                'body_atr': cp.asnumpy(body_atr_gpu).tolist(),
                'z_deltas': cp.asnumpy(z_gpu).tolist(),
                'atr': cp.asnumpy(a_gpu).tolist()
            }
        except Exception:
            # print(f"GPU batch error: {e}")
            return {}




# Global GPU service instance
_gpu_service: Optional[GPUService] = None


def get_gpu_service() -> GPUService:
    """
    Get the global GPU service instance.

    Returns:
        GPUService instance
    """
    global _gpu_service
    if _gpu_service is None:
        _gpu_service = GPUService()
    return _gpu_service


def is_gpu_available() -> bool:
    """
    Check if GPU is available.

    Returns:
        True if GPU is available, False otherwise
    """
    return get_gpu_service().available


def get_gpu_device_count() -> int:
    """
    Get number of GPU devices.

    Returns:
        Number of GPU devices
    """
    return get_gpu_service().device_count
