from __future__ import annotations

"""Unit tests for Phase D (P3) flow toxicity helpers."""


import sys
from pathlib import Path

# Ensure SoT packages are importable: core.*, services.*
sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from services.orderflow.flow_toxicity import compute_ofi_norm_notional, normal_cdf


def test_compute_ofi_norm_notional_basic_ratio():
    # ofi_best_qty=10, mid=100 => ofi_usd=1000
    # depth_usd_near=10000 => ratio=0.1
    out = compute_ofi_norm_notional(ofi_best_qty=10.0, mid=100.0, depth_usd_near=10_000.0)
    assert abs(out - 0.1) < 1e-12


def test_compute_ofi_norm_notional_fail_open_on_missing_depth_or_mid():
    assert compute_ofi_norm_notional(ofi_best_qty=10.0, mid=0.0, depth_usd_near=1000.0) == 0.0
    assert compute_ofi_norm_notional(ofi_best_qty=10.0, mid=100.0, depth_usd_near=0.0) == 0.0


def test_normal_cdf_sanity():
    assert abs(normal_cdf(0.0) - 0.5) < 1e-12
    assert normal_cdf(3.0) > 0.99
    assert normal_cdf(-3.0) < 0.01
