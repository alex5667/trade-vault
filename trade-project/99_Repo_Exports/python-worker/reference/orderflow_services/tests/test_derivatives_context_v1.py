from __future__ import annotations

import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[3]))

from services.orderflow.derivatives_context import basis_bps, build_snapshot, robust_zscore


def test_basis_bps_positive_premium():
    out = basis_bps(mark_price=100.8, index_price=100.0)
    assert round(out, 4) == 80.0


def test_robust_zscore_uses_median_mad():
    hist = [0.0001, 0.00011, 0.00009, 0.00012, 0.0001, 0.00011, 0.0001]
    z = robust_zscore(x=0.0010, history=hist)
    assert z > 3.0


def test_build_snapshot_sets_extreme_flags():
    snap = build_snapshot(
        symbol="BTCUSDT",
        ts_ms=1,
        venue="binance",
        funding_rate=0.0010,
        funding_history=[0.0001, 0.0001, 0.00011, 0.00009, 0.0001, 0.00012],
        premium_index=0.0010,
        mark_price=101.5,
        index_price=100.0,
        open_interest=50000.0,
        previous_open_interest=47000.0,
        funding_extreme_abs=0.0008,
        basis_extreme_abs_bps=10.0,
        oi_accel_abs_usd=100000.0,
    )
    assert snap.symbol == "BTCUSDT"
    assert snap.funding_extreme == 1
    assert snap.basis_extreme == 1
    assert snap.oi_accel == 1
    assert snap.oi_notional_usd > 0.0
