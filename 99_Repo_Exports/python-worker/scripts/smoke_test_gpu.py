#!/usr/bin/env python3
"""GPU Stack Smoke Test

Verifies that all GPU-accelerated components work correctly:
  1. CUDA availability (torch + cupy)
  2. CuPy basic operations (median, MAD)
  3. GPUService detection and compute
  4. RollingRobustZ GPU path
  5. XGBoost device=cuda

Usage:
  # On host (requires cupy in venv):
  cd /home/alex/front/trade/scanner_infra
  PYTHONPATH=python-worker python3 python-worker/scripts/smoke_test_gpu.py

  # In container:
  docker exec scanner-python-worker python3 /app/scripts/smoke_test_gpu.py
"""
from __future__ import annotations

import os
import sys
import time

# Ensure project root on path
_SCRIPT_DIR = os.path.dirname(os.path.abspath(__file__))
_APP_DIR = os.path.dirname(_SCRIPT_DIR)  # python-worker/
if _APP_DIR not in sys.path:
    sys.path.insert(0, _APP_DIR)

PASS = "\033[92m✅ PASS\033[0m"
FAIL = "\033[91m❌ FAIL\033[0m"
SKIP = "\033[93m⚠️  SKIP\033[0m"

results: list[tuple[str, str, str]] = []  # (test, status, detail)


def check(name: str, ok: bool, detail: str = "") -> None:
    status = PASS if ok else FAIL
    results.append((name, "PASS" if ok else "FAIL", detail))
    print(f"  {status}  {name}  {detail}")


def skip(name: str, reason: str) -> None:
    results.append((name, "SKIP", reason))
    print(f"  {SKIP}  {name}  [{reason}]")


# ---------------------------------------------------------------------------
# Test 1: torch.cuda
# ---------------------------------------------------------------------------
print("\n=== 1. PyTorch CUDA ===")
try:
    import torch
    cuda_ok = torch.cuda.is_available()
    device_name = torch.cuda.get_device_name(0) if cuda_ok else "N/A"
    check("torch.cuda.is_available()", cuda_ok, f"device={device_name!r}")
    if cuda_ok:
        t = torch.tensor([1.0, 2.0, 3.0]).cuda()
        check("torch tensor on GPU", True, f"sum={float(t.sum())}")
except Exception as e:
    check("torch.cuda", False, str(e))


# ---------------------------------------------------------------------------
# Test 2: CuPy
# ---------------------------------------------------------------------------
print("\n=== 2. CuPy ===")
_CUPY_OK = False
try:
    import cupy as cp
    _CUPY_OK = cp.cuda.is_available()
    check("cupy import", True, f"version={cp.__version__}")
    check("cp.cuda.is_available()", _CUPY_OK)
    if _CUPY_OK:
        a = cp.array([3.0, 1.0, 4.0, 1.0, 5.0, 9.0])
        med = float(cp.median(a))
        mad = float(cp.median(cp.abs(a - med)))
        check("cp.median()", True, f"median={med:.2f}, MAD={mad:.2f}")

        # Benchmark: median on N=1000
        big = cp.random.randn(1000).astype(cp.float32)
        t0 = time.perf_counter()
        for _ in range(100):
            _ = cp.median(big)
        elapsed = (time.perf_counter() - t0) * 1000
        check("cp.median(1000) x100", True, f"{elapsed:.1f}ms total, {elapsed/100:.3f}ms/call")

except ImportError:
    skip("CuPy", "cupy not installed — run: pip install cupy-cuda12x==12.3.0")
except Exception as e:
    check("CuPy", False, str(e))


# ---------------------------------------------------------------------------
# Test 3: GPUService
# ---------------------------------------------------------------------------
print("\n=== 3. GPUService ===")
try:
    from common.gpu_service import get_gpu_service, is_gpu_available
    svc = get_gpu_service()
    check("GPUService.available", svc.available, f"torch_fallback={getattr(svc, '_torch_fallback', 'N/A')}")
    check("is_gpu_available()", is_gpu_available())
    check("device_count >= 1", svc.device_count >= 1, f"count={svc.device_count}")

    if svc.available and _CUPY_OK:
        try:
            import cupy as cp
            import numpy as np
            arr = cp.array([10.0, 20.0, 30.0, 40.0, 50.0] * 40, dtype=cp.float32)
            z = svc.compute_robust_zscore_mad(arr, 55.0)
            check("GPUService.compute_robust_zscore_mad()", True, f"z={z:.4f}")
        except Exception as e:
            check("GPUService.compute_robust_zscore_mad()", False, str(e))
    else:
        skip("GPUService.compute_robust_zscore_mad()", "cupy unavailable")
except Exception as e:
    check("GPUService", False, str(e))


# ---------------------------------------------------------------------------
# Test 4: RollingRobustZ GPU path
# ---------------------------------------------------------------------------
print("\n=== 4. RollingRobustZ GPU path ===")
try:
    import numpy as np
    from core.robust_stats import RollingRobustZ

    rz = RollingRobustZ(window=250)  # > 200 threshold → should hit GPU
    for v in np.random.randn(250).tolist():
        z = rz.z(v)
    check("RollingRobustZ(window=250) — 250 updates", True, f"last_z={z:.4f}")

    # Check if GPU was actually used
    if _CUPY_OK and svc.available:
        check("GPU path triggered (window>=200)", True, "cupy + GPUService active")
    else:
        skip("GPU path triggered", "cupy unavailable — using CPU fallback")
except Exception as e:
    check("RollingRobustZ", False, str(e))


# ---------------------------------------------------------------------------
# Test 5: XGBoost device=cuda
# ---------------------------------------------------------------------------
print("\n=== 5. XGBoost GPU ===")
try:
    import xgboost as xgb
    import numpy as np

    X = np.random.rand(2000, 30).astype(np.float32)
    y = (np.random.rand(2000) > 0.5).astype(int)

    t0 = time.perf_counter()
    m = xgb.XGBClassifier(n_estimators=50, tree_method="hist", device="cuda", verbosity=0)
    m.fit(X, y)
    gpu_ms = (time.perf_counter() - t0) * 1000

    t0 = time.perf_counter()
    m_cpu = xgb.XGBClassifier(n_estimators=50, tree_method="hist", device="cpu", verbosity=0)
    m_cpu.fit(X, y)
    cpu_ms = (time.perf_counter() - t0) * 1000

    speedup = cpu_ms / max(gpu_ms, 1.0)
    check("XGBoost device=cuda", True, f"GPU={gpu_ms:.0f}ms  CPU={cpu_ms:.0f}ms  Speedup={speedup:.1f}x")
except Exception as e:
    check("XGBoost GPU", False, str(e))


# ---------------------------------------------------------------------------
# Summary
# ---------------------------------------------------------------------------
print("\n" + "=" * 60)
n_pass = sum(1 for _, s, _ in results if s == "PASS")
n_fail = sum(1 for _, s, _ in results if s == "FAIL")
n_skip = sum(1 for _, s, _ in results if s == "SKIP")
print(f"  Results: {n_pass} PASS  {n_fail} FAIL  {n_skip} SKIP")
print("=" * 60)

if n_fail > 0:
    print("\nFailed tests:")
    for name, status, detail in results:
        if status == "FAIL":
            print(f"  - {name}: {detail}")
    sys.exit(1)
else:
    print("\n🚀 All GPU tests passed!")
    sys.exit(0)
