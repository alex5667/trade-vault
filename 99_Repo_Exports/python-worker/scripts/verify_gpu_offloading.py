#!/usr/bin/env python3
"""
Verification script for GPU offloading fixes.
"""
import sys
import time
import numpy as np
from pathlib import Path

# Add python-worker to path
project_root = Path(__file__).parent.parent
sys.path.insert(0, str(project_root))

from common.gpu_service import get_gpu_service
import common.gpu_service
print(f"DEBUG: common.gpu_service loaded from: {common.gpu_service.__file__}")
from core.robust_stats import RollingRobustZ

def verify_gpu_methods():
    print("Verifying GPU methods...")
    gpu = get_gpu_service()
    if not gpu.available:
        print("❌ GPU not available, skipping verification")
        return

    print(f"✅ GPU Available: {gpu.get_device_info()['name']}")

    # 1. OBI Batch
    print("\nTesting compute_obi_metrics_batch...")
    bids = np.array([100.0, 200.0, 0.0], dtype=np.float32)
    asks = np.array([150.0, 100.0, 100.0], dtype=np.float32)
    
    # Expected:
    # 0: (150-100)/250 = 0.2, (150/100)-1 = 0.5
    # 1: (100-200)/300 = -0.333, (100/200)-1 = -0.5
    # 2: (100-0)/100 = 1.0, inf
    
    res = gpu.compute_obi_metrics_batch(bids, asks)
    print(f"OBI Signed: {res['obi_signed']}")
    print(f"OBI Ratio: {res['obi_ratio']}")
    
    assert np.isclose(res['obi_signed'][0], 0.2), "OBI Signed[0] failed"
    assert np.isclose(res['obi_ratio'][0], 0.5), "OBI Ratio[0] failed"
    print("✅ compute_obi_metrics_batch passed")

    # 2. Robust Z
    print("\nTesting compute_robust_zscore_mad...")
    data = np.random.normal(0, 1, 1000).astype(np.float32)
    import cupy as cp
    data_gpu = cp.asarray(data)
    
    val = 2.0
    z = gpu.compute_robust_zscore_mad(data_gpu, val)
    print(f"Z-score for {val}: {z}")
    assert isinstance(z, float), "Result should be float"
    print("✅ compute_robust_zscore_mad passed")

def benchmark_robust_z():
    print("\nBenchmarking RollingRobustZ (CPU vs GPU)...")
    
    sizes = [300, 2000, 10000]
    
    for N in sizes:
        print(f"\nWindow Size: {N}")
        rrz = RollingRobustZ(window=N)
        # Fill buffer
        for _ in range(N):
            rrz.update(np.random.randn())
            
        # CPU Benchmark (force by small window or manually? It switches at 1000)
        # Actually logic switches at > 1000.
        
        start = time.time()
        for _ in range(100):
            rrz.z(1.5)
        dt = time.time() - start
        print(f"Time for 100 calls: {dt*1000:.2f} ms")
        if N > 1000:
             print(" (Should be using GPU)")
        else:
             print(" (Should be using CPU)")

if __name__ == "__main__":
    try:
        verify_gpu_methods()
        benchmark_robust_z()
    except Exception as e:
        print(f"\n❌ FAILED: {e}")
        sys.exit(1)
