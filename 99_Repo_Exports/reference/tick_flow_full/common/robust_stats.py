"""
Robust statistical utilities for the scanner infrastructure.

Provides robust statistical calculations including rolling MAD-based z-score.
"""

import numpy as np
from typing import Optional, List, Tuple
from collections import deque


class RobustZscoreMADRolling:
    """
    Rolling MAD (Median Absolute Deviation) based z-score calculator.

    This provides robust outlier detection that is less sensitive to
    extreme values compared to standard deviation based methods.
    
    Supports GPU acceleration via RobustZscoreGPU if configured.
    """

    def __init__(self, window_size: int = 100, threshold: float = 3.0):
        """
        Initialize the rolling MAD z-score calculator.

        Args:
            window_size: Size of the rolling window
            threshold: Z-score threshold for outlier detection
        """
        self.window_size = window_size
        self.threshold = threshold
        
        # GPU Acceleration Logic
        self.gpu_backend = None
        try:
            from config.gpu_config import GPU_ENABLE, GPU_MIN_N
            
            # Check if GPU is enabled and window size justifies overhead
            if GPU_ENABLE and window_size >= GPU_MIN_N:
                try:
                    from gpu.robust_z_gpu import RobustZscoreGPU
                    self.gpu_backend = RobustZscoreGPU(window_size, threshold)
                    # logging.info("RobustZscoreMADRolling initialized with GPU backend") initialization logs usually handled by caller or once
                except ImportError:
                    pass  # Cupy not available or other import error
                except Exception:
                    pass  # Fallback to CPU
        except ImportError:
            pass  # config not found

        if not self.gpu_backend:
            self.values = deque(maxlen=window_size)
            self.mad_values = deque(maxlen=window_size)

    def update(self, value: float) -> Tuple[float, bool]:
        """
        Update with new value and return z-score and outlier flag.

        Args:
            value: New value to add

        Returns:
            Tuple of (z_score, is_outlier)
        """
        if self.gpu_backend:
            return self.gpu_backend.update(value)

        self.values.append(value)

        if len(self.values) < 2:
            return 0.0, False

        # Calculate median and MAD
        values_array = np.array(list(self.values))
        median = np.median(values_array)

        # MAD = median(|x - median|)
        mad = np.median(np.abs(values_array - median))

        # Avoid division by zero
        if mad == 0:
            mad = 1e-6

        # Calculate z-score
        z_score = (value - median) / (1.4826 * mad)  # 1.4826 makes MAD consistent with std for normal distribution

        # Check if outlier
        is_outlier = abs(z_score) > self.threshold

        return z_score, is_outlier

    def get_stats(self) -> dict:
        """
        Get current statistics.

        Returns:
            Dictionary with current statistics
        """
        if self.gpu_backend:
            return self.gpu_backend.get_stats()

        if not self.values:
            return {"count": 0, "median": 0.0, "mad": 0.0}

        values_array = np.array(list(self.values))
        median = np.median(values_array)
        mad = np.median(np.abs(values_array - median))

        return {
            "count": len(self.values),
            "median": median,
            "mad": mad,
            "window_size": self.window_size,
            "threshold": self.threshold
        }


def rolling_median(values: List[float], window_size: int) -> List[float]:
    """
    Calculate rolling median.

    Args:
        values: List of values
        window_size: Window size

    Returns:
        List of rolling medians
    """
    if len(values) < window_size:
        return [np.median(values)] * len(values)

    result = []
    for i in range(len(values)):
        start = max(0, i - window_size + 1)
        window = values[start:i+1]
        result.append(np.median(window))

    return result


def rolling_mad(values: List[float], window_size: int) -> List[float]:
    """
    Calculate rolling MAD (Median Absolute Deviation).

    Args:
        values: List of values
        window_size: Window size

    Returns:
        List of rolling MADs
    """
    if len(values) < window_size:
        values_array = np.array(values)
        median = np.median(values_array)
        mad = np.median(np.abs(values_array - median))
        return [mad] * len(values)

    result = []
    for i in range(len(values)):
        start = max(0, i - window_size + 1)
        window = values[start:i+1]
        window_array = np.array(window)
        median = np.median(window_array)
        mad = np.median(np.abs(window_array - median))
        result.append(mad)

    return result


def robust_zscore(values: List[float], window_size: int = 20) -> List[float]:
    """
    Calculate robust z-scores using rolling MAD.

    Args:
        values: List of values
        window_size: Window size for rolling statistics

    Returns:
        List of z-scores
    """
    if len(values) < 2:
        return [0.0] * len(values)

    medians = rolling_median(values, window_size)
    mads = rolling_mad(values, window_size)

    z_scores = []
    for i, (value, median, mad) in enumerate(zip(values, medians, mads)):
        if mad == 0:
            mad = 1e-6
        z_score = (value - median) / (1.4826 * mad)
        z_scores.append(z_score)

    return z_scores
