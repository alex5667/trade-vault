from core.ofi_tracker import OFIStabilityTracker


def test_ofi_best_level_same_prices_qty_change():
    tr = OFIStabilityTracker(window_ms=3000, z_window=64)
    ofi = tr.compute_ofi_best_level(
        prev_bid_px=100.0, prev_bid_qty=10.0,
        prev_ask_px=101.0, prev_ask_qty=12.0,
        bid_px=100.0, bid_qty=15.0,
        ask_px=101.0, ask_qty=10.0,
    )
    assert ofi == 7.0


def test_ofi_stability_increases_without_flips():
    tr = OFIStabilityTracker(window_ms=3000, z_window=128)
    ts = 1_000_000
    stable_last = 0.0
    for _ in range(8):
        ts += 400
        ofi_z, stable_secs, score = tr.update(
            ts_ms=ts,
            ofi=50.0,
            depth_qty=1000.0,
            deadband_abs=0.0,
            deadband_frac_depth=0.0,
            z_full=3.0,
        )
        assert score >= 0.0
        assert stable_secs >= stable_last
        stable_last = stable_secs
