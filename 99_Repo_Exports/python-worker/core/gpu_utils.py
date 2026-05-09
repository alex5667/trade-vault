"""
GPU Utilities for transparent CPU/GPU array operations.
"""
import logging
import os

import numpy as np

logger = logging.getLogger(__name__)

# Lazy loading of CuPy
_xp = None
_gpu_available = None

def is_gpu_available() -> bool:
    """Check if GPU is available and CuPy is installed."""
    global _gpu_available
    if _gpu_available is not None:
        return _gpu_available

    try:
        # Check env var first to force CPU if needed
        if os.getenv("FORCE_CPU", "0").lower() in ("1", "true", "yes", "on"):
            _gpu_available = False
            return False

        import cupy as cp
        if not cp.cuda.is_available():
            logger.warning("CuPy installed but CUDA not available.")
            _gpu_available = False
        else:
            _gpu_available = True
    except ImportError:
        _gpu_available = False

    return _gpu_available

def get_xp():
    """
    Return cupy module if GPU is available, else numpy.
    Acts as a 'numpy-like' namespace.
    """
    global _xp
    if _xp is not None:
        return _xp

    if is_gpu_available():
        import cupy as cp
        _xp = cp
        logger.info("Using CuPy (GPU) backend")
    else:
        _xp = np
        logger.info("Using NumPy (CPU) backend")

    return _xp

def to_cpu(array):
    """Safely convert array to CPU numpy array."""
    if hasattr(array, "get"): # CuPy array
        return array.get()
    if hasattr(array, "cpu"): # Torch tensor
        return array.cpu().numpy()
    if isinstance(array, np.ndarray):
        return array
    return np.array(array)

def to_gpu(array):
    """Safely convert array to GPU cupy array if available."""
    if not is_gpu_available():
        return np.array(array)

    xp = get_xp()
    return xp.array(array)
