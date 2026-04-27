from __future__ import annotations

import pytest

from microstructure.realized_spread_tracker import RealizedSpreadTrackerHorizon


def test_realized_spread_ema_updates_and_pending_drains() -> None:
    tr = RealizedSpreadTrackerHorizon(horizon_ms=1000, ema_alpha=0.5, max_pending=100)

    # ts=0 trade buy at mid=100.5
    s0 = tr.update(ts_ms=0, bid=100.0, ask=101.0, trade_side=+1, trade_volume=1.0)
    assert s0.pending_len == 1
    assert s0.realized_bps_ema is None

    # ts=500 another buy at same mid
    s1 = tr.update(ts_ms=500, bid=100.0, ask=101.0, trade_side=+1, trade_volume=1.0)
    assert s1.pending_len == 2

    # ts=1000: still not matured for the first? (1000-0 >= 1000) => matured YES at boundary
    # move mid to 101.5 => +1.0 on mid => realized_bps ~ (1.0/100.5)*10000 = 99.50 bps
    s2 = tr.update(ts_ms=1000, bid=101.0, ask=102.0, trade_side=0, trade_volume=0.0)
    assert s2.n_realized >= 1
    assert s2.realized_bps_ema is not None
    first_ema = float(s2.realized_bps_ema)

    # ts=1500: second pending matures, mid moves further to 102.5 => realized_bps bigger
    s3 = tr.update(ts_ms=1500, bid=102.0, ask=103.0, trade_side=0, trade_volume=0.0)
    assert s3.n_realized >= 2
    assert s3.realized_bps_ema is not None
    # EMA должен измениться
    assert float(s3.realized_bps_ema) != pytest.approx(first_ema)
    # pending должен быть 0 (оба matured)
    assert s3.pending_len == 0


def test_realized_spread_max_pending_enforced() -> None:
    tr = RealizedSpreadTrackerHorizon(horizon_ms=10_000, ema_alpha=0.1, max_pending=3)
    # 4 trades without draining -> должен сработать cap и dropped_due_to_cap
    tr.update(ts_ms=0, bid=100.0, ask=101.0, trade_side=+1, trade_volume=1.0)
    tr.update(ts_ms=1, bid=100.0, ask=101.0, trade_side=+1, trade_volume=1.0)
    tr.update(ts_ms=2, bid=100.0, ask=101.0, trade_side=-1, trade_volume=1.0)
    s = tr.update(ts_ms=3, bid=100.0, ask=101.0, trade_side=+1, trade_volume=1.0)
    assert s.pending_len == 3
    assert s.dropped_due_to_cap == 1
