from __future__ import annotations

"""
Deprecated shim for backward compatibility.
Uses the unified GPU singleton from common.gpu_service without creating a second service.
"""

from common.gpu_service import GPUService as GpuComputeService
from common.gpu_service import get_gpu_device_count, get_gpu_service, is_gpu_available

__all__ = [
    "GpuComputeService",
    "get_gpu_service",
    "is_gpu_available",
    "get_gpu_device_count"
]

